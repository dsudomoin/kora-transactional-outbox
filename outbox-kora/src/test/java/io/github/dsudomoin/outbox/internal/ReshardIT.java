package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.kora.KoraOutboxTransactions;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The emergency reshard at bootstrap (an explicit {@code outbox.shards} other than the cursors'), its guards (other
 * instances' rows from their bootstrap and heartbeat, a relay still ticking, the drain checks under locks that hold
 * relays and writers), the shard count an instance without {@code outbox.shards} adopts, and what a running instance
 * does once the cursors' count is not its own.
 */
class ReshardIT {

    private static final OutboxDeadLetterHandler HOOK = letters -> List.of();

    private final TestDatabase db = TestDb.newDatabase(20);
    private final String table = TestDb.uniqueTable();
    private final TableNames names = TableNames.of(table);
    private final RecordingHandler recording = new RecordingHandler();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();

    private record Node(OutboxSchema schema, Maintenance maintenance, OutboxRelay relay, Outbox outbox) {}

    @AfterEach
    void release() {
        relays.forEach(OutboxRelay::close);
        pool.shutdownNow();
        db.release();
    }

    @Test
    void drainedOutboxReshardsAtBootstrap() throws Exception {
        var old = start(config(4));
        old.relay().start();
        for (int i = 0; i < 40; i++) {
            send(old.outbox(), "k" + i % 10, "a" + i);
        }
        old.relay().drain();
        old.relay().close();
        quiet();

        Node next;
        try (var err = new CapturedStderr()) {
            next = start(config(8));
            assertThat(err.text()).contains("INFO").contains("resharded " + table + " from 4 to 8 shards");
        }
        assertThat(cursorShards()).isEqualTo(8);
        assertThat(queryLong("select count(*) from " + names.checkpoint())).isZero();
        next.relay().start();
        for (int i = 0; i < 40; i++) {
            send(next.outbox(), "k" + i % 10, "b" + i);
        }
        next.relay().drain();

        var byKey = new LinkedHashMap<String, List<String>>();
        for (var r : recording.delivered()) {
            byKey.computeIfAbsent(new String(r.key(), UTF_8), k -> new ArrayList<>())
                    .add(new String(r.payload(), UTF_8));
        }
        assertThat(recording.delivered()).hasSize(80);
        for (int k = 0; k < 10; k++) {
            var expected = new ArrayList<String>();
            for (var prefix : List.of("a", "b")) {
                for (int i = k; i < 40; i += 10) {
                    expected.add(prefix + i);
                }
            }
            assertThat(byKey.get("k" + k)).as("key order across the reshard").isEqualTo(expected);
        }
    }

    @Test
    void undrainedOutboxRefusesNamingEachCheck() throws Exception {
        var old = start(config(4));
        send(old.outbox(), "k", "unrelayed");
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, created_at)"
                + " values (-1, 0, gen_random_uuid(), 't', now())");
        execute("insert into " + names.parked() + " (id, shard, message_id, topic, created_at, due_at)"
                + " values (-2, 1, gen_random_uuid(), 't', now(), now())");
        execute("insert into " + names.cursor() + " (shard, tick_id, target_tick_id) select 2, max(id), max(id) from "
                + names.tick());
        execute("create table " + names.partitionPrefix() + "123456 (like " + table + ")");
        stopped();

        assertThatThrownBy(() -> schema(config(8)).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outbox.shards = 8 but " + names.cursor() + " holds cursors for 4 shards")
                .hasMessageContaining("1 rows in " + names.retry())
                .hasMessageContaining("1 rows in " + names.parked())
                .hasMessageContaining("1 shards with a batch in progress ([2])")
                .hasMessageContaining("1 committed rows not relayed")
                .hasMessageContaining("1 standalone ranges ([" + names.partitionPrefix() + "123456])")
                .hasMessageContaining("stop writers, wait for outbox.shard.lag 0, stop all instances");
        assertThat(cursorShards()).as("nothing changed").isEqualTo(4);
    }

    @Test
    void liveInstanceWithOtherShardsRefuses() throws Exception {
        start(config(4));
        execute("delete from " + names.instance());
        execute("insert into " + names.instance() + " (instance_id, shards, expires_at)"
                + " values ('host-a/42/0badcafe', 4, now() + interval '1 minute')");
        quiet();

        assertThatThrownBy(() -> schema(config(8)).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("host-a/42/0badcafe (4 shards")
                .hasMessageContaining("outbox.shards");
        assertThat(cursorShards()).isEqualTo(4);

        execute("update " + names.instance() + " set expires_at = now() - interval '1 second'");
        schema(config(8)).start();
        assertThat(cursorShards()).as("its heartbeat expired").isEqualTo(8);
    }

    @Test
    void anInstanceIsSeenFromItsBootstrapOn() throws Exception {
        var a = start(config(4)); // its relay, so its heartbeat, not started
        quiet();

        assertThatThrownBy(() -> schema(config(8)).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(a.schema().instanceId() + " (4 shards");
        assertThat(cursorShards()).isEqualTo(4);
    }

    @Test
    void aTickingRelayRefusesTheReshard() throws Exception {
        var old = start(config(4));
        old.relay().start();
        execute("delete from " + names.instance()); // the previous release writes no heartbeat

        var schema = schema(config(8));
        schema.tickWait = Duration.ofSeconds(1);
        assertThatThrownBy(schema::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("(?s).*a relay is still running \\(latest tick \\d+ ms ago\\).*");
        assertThat(cursorShards()).isEqualTo(4);
    }

    @Test
    void theTickGuardIsWaitedOutWhenTheRelayStops() throws Exception {
        var old = start(config(4));
        old.relay().start();
        execute("delete from " + names.instance());
        var schema = schema(config(8));
        schema.tickFloor = Duration.ZERO; // the bound is then 2 × 20 ms + 5 s

        var resharding = pool.submit(() -> {
            schema.start();
            return null;
        });
        Thread.sleep(500);
        assertThat(resharding.isDone()).as("it waits while the relay ticks").isFalse();
        old.relay().close();

        resharding.get(30, SECONDS);
        assertThat(cursorShards()).isEqualTo(8);
    }

    @Test
    void anOldLongPauseAmongTheTicksIsWaitedAtMostTheBoundedWait() throws Exception {
        start(config(4));
        stopped();
        // the newest tick 4 s old, 10 min after the one before: 3 × that gap is 30 min, capped at the 8 s wait
        execute("update " + names.tick() + " set created_at = now() - interval '10 minutes 4 seconds'");
        execute("insert into " + names.tick() + " (snapshot, created_at) select pg_current_snapshot(),"
                + " now() - interval '4 seconds'");
        var schema = schema(config(8));
        schema.tickFloor = Duration.ZERO;
        schema.tickWait = Duration.ofSeconds(8);

        long started = System.nanoTime();
        try (var err = new CapturedStderr()) {
            schema.start();
            assertThat(err.text()).as("the cadence term counts, above the own 5 s bound, capped at the wait")
                    .contains("for the latest tick to be 8000 ms old").contains("resharded");
        }
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(8));
        assertThat(cursorShards()).isEqualTo(8);
    }

    @Test
    void aLockHeldByARunningInstanceRefusesWithTheProcedure() throws Exception {
        start(config(4));
        stopped();
        try (var holder = TestDb.connect()) {
            TestSql.execute(holder, "select pg_advisory_lock(" + resolvedNames().internalNamespace() + ", "
                    + TableNames.LOCK_MAINTENANCE + ")");

            assertThatThrownBy(() -> schema(config(8)).start())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("waited 5 s for a lock")
                    .hasMessageContaining("stop all instances");
        }
        assertThat(cursorShards()).isEqualTo(4);
    }

    @Test
    void runningInstanceStopsOnAShardMismatch() throws Exception {
        var node = start(config(4));
        reshardBehindItsBack(8);

        try (var err = new CapturedStderr()) {
            assertThat(node.maintenance().runOnce()).as("nothing maintained").isFalse();
            assertThat(node.maintenance().halted()).isTrue();
            assertThat(err.text()).contains("ERROR").contains(names.cursor() + " holds cursors for 8 shards")
                    .contains("runs with outbox.shards = 4");
        }
        node.relay().start();
        Thread.sleep(300);
        assertThat(recording.delivered()).as("the relay takes no shards").isEmpty();
        assertThatThrownBy(node.relay()::drain).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("8 shards");

        reshardBehindItsBack(4);
        assertThat(node.maintenance().runOnce()).isTrue();
        assertThat(node.maintenance().halted()).as("resumes once they match again").isFalse();
    }

    @Test
    void aMismatchedWriterIsRefusedWithinOneBeat() throws Exception {
        var node = start(config(4).relayEnabled(false).maintenanceInterval(Duration.ofMillis(200)));
        node.relay().start();
        send(node.outbox(), "k", "before");

        reshardBehindItsBack(8);
        await(() -> {
            try {
                send(node.outbox(), "k", "after");
                return false;
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining(names.cursor() + " holds cursors for 8 shards")
                        .hasMessageContaining("outbox.shards = 4");
                return true;
            }
        });
        assertThatThrownBy(() -> db.inTx((SqlFunction0<List<UUID>>) () ->
                node.outbox().sendAll(List.of(OutboxMessage.of("t", "k", new byte[0])))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("8 shards");

        reshardBehindItsBack(4);
        await(() -> {
            try {
                send(node.outbox(), "k", "again");
                return true;
            } catch (IllegalStateException e) {
                return false;
            }
        });
    }

    @Test
    void hookActiveOnlyWhereTheRelayRuns() throws Exception {
        var writer = start(config(4).relayEnabled(false), HOOK);
        writer.relay().start();
        var relaying = start(config(4), HOOK);
        relaying.relay().start();

        await(() -> queryLong("select count(*) from " + names.instance() + " where last_seen > now() - interval"
                + " '10 seconds' and hook_active is not null") == 2);
        await(() -> queryString("select string_agg(hook_active::text, ',' order by hook_active) from "
                + names.instance()).equals("false,true"));
        assertThat(queryString("select hook_active::text from " + names.instance() + " where instance_id = '"
                + writer.schema().instanceId() + "'")).as("a writer-only pod runs no dead-letter pass")
                .isEqualTo("false");
    }

    @Test
    void heartbeatRowIsGoneAfterShutdown() throws Exception {
        var node = start(config(4).relayEnabled(false));
        node.relay().start();

        await(() -> queryLong("select count(*) from " + names.instance()) == 1);
        assertThat(queryString("select instance_id from " + names.instance()))
                .isEqualTo(node.schema().instanceId())
                .matches("[^/]+/" + ProcessHandle.current().pid() + "/[0-9a-f]{8}");
        assertThat(queryString("select shards || ' ' || hook_active || ' ' || (expires_at > now()) from "
                + names.instance())).isEqualTo("4 false true");

        node.relay().close();
        assertThat(queryLong("select count(*) from " + names.instance())).isZero();
    }

    @Test
    void aBeatAfterStopDoesNotBringTheRowBack() throws Exception {
        var node = start(config(4));
        var heartbeat = new Heartbeat(db, node.schema(), node.schema().config(), false);

        heartbeat.stop();
        heartbeat.beat();

        assertThat(queryLong("select count(*) from " + names.instance())).isZero();
    }

    @Test
    void aFailedBeatKeepsTheScheduleGoing() throws Exception {
        var node = start(config(4).maintenanceInterval(Duration.ofMillis(50)));
        execute("delete from " + names.instance());
        var failed = new AtomicBoolean();
        JdbcConnectionFactory failingOnce = new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                return db.withConnection(callback);
            }

            @Override
            public Connection currentConnection() {
                return db.currentConnection();
            }

            @Override
            public Connection newConnection() {
                if (failed.compareAndSet(false, true)) {
                    throw new AssertionError("not an Exception: would cancel a schedule that catches only those");
                }
                return db.newConnection();
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return db.telemetry();
            }
        };
        var heartbeat = new Heartbeat(
                new KoraOutboxTransactions(failingOnce), node.schema(), node.schema().config(), false);
        try {
            heartbeat.start();
            await(() -> queryLong("select count(*) from " + names.instance()) == 1);
            assertThat(failed).isTrue();
        } finally {
            heartbeat.stop();
        }
    }

    @Test
    void writersWaitDuringTheChecksAndThenGoOn() throws Exception {
        var old = start(config(4));
        var writer = new PgOutbox(db, old.schema(), 8, () -> {}, TestTelemetry.kora());
        stopped();
        try (var holder = TestDb.connect()) {
            holder.setAutoCommit(false);
            // holds the retry check, which runs under SHARE on the outbox
            TestSql.execute(holder, "lock table " + names.retry() + " in access exclusive mode");
            var resharding = pool.submit(() -> {
                schema(config(8)).start();
                return null;
            });
            await(() -> queryLong("select count(*) from pg_locks where relation = '" + table + "'::regclass"
                    + " and mode = 'ShareLock' and granted") == 1);
            var writing = pool.submit(() -> send(writer, "k", "during"));
            Thread.sleep(300);
            assertThat(writing.isDone()).as("the writer waits for the checks").isFalse();

            holder.commit();
            resharding.get(10, SECONDS);
            assertThat(writing.get(10, SECONDS)).isNotNull();
        }
        assertThat(cursorShards()).isEqualTo(8);
        var next = start(config(8));
        next.relay().drain();
        assertThat(recording.delivered()).extracting(r -> new String(r.payload(), UTF_8)).containsExactly("during");
    }

    @Test
    void aWriteCommittedWhileTheReshardWaitsForItsLockIsSeenUnderARepeatableReadDefault() throws Exception {
        start(config(4));
        stopped();
        var options = new Properties();
        options.setProperty("options", "-c default_transaction_isolation=repeatable\\ read");
        var repeatableRead = TestDb.newDatabase(2, options);
        try (var writer = TestDb.connect()) {
            // a writer of the old count mid-transaction, e.g. a frozen instance or one of a release without heartbeat
            writer.setAutoCommit(false);
            TestSql.execute(writer, "insert into " + table + " (shard, message_id, topic) values (3, gen_random_uuid(),"
                    + " 't')");
            var resharding = pool.submit(() -> {
                new OutboxSchema(repeatableRead, config(8)).start();
                return null;
            });
            await(() -> queryLong("select count(*) from pg_locks where relation = '" + table + "'::regclass"
                    + " and mode = 'ShareLock' and not granted") == 1);
            writer.commit();

            assertThatThrownBy(() -> resharding.get(10, SECONDS)).hasCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("1 committed rows not relayed");
        } finally {
            repeatableRead.release();
        }
        assertThat(cursorShards()).isEqualTo(4);
    }

    @Test
    void anUnsetShardCountAdoptsTheExistingOne() throws Exception {
        start(config(32));
        long ticks = queryLong("select count(*) from " + names.tick());

        try (var err = new CapturedStderr()) {
            var node = start(config(null));
            assertThat(node.schema().config().shards()).isEqualTo(32);
            assertThat(err.text()).contains("INFO").contains("outbox.shards is not set")
                    .contains(names.cursor() + " holds cursors for 32 shards").doesNotContain("resharded");
        }
        assertThat(cursorShards()).isEqualTo(32);
        assertThat(queryLong("select count(*) from " + names.tick())).as("no reshard").isEqualTo(ticks);

        var fresh = new OutboxSchema(db, new TestOutboxConfig().shards(null));
        fresh.start();
        assertThat(fresh.config().shards()).as("a new outbox").isEqualTo(OutboxConfig.DEFAULT_SHARDS);
    }

    @Test
    void anExplicitShardCountReshardsOnlyWhenDrained() throws Exception {
        var old = start(config(4));
        send(old.outbox(), "k", "x");
        stopped();

        assertThatThrownBy(() -> schema(config(8)).start())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("1 committed rows not relayed");
        assertThat(cursorShards()).isEqualTo(4);

        old.relay().drain();
        stopped();
        start(config(8));
        assertThat(cursorShards()).isEqualTo(8);
        start(config(8));
        assertThat(cursorShards()).as("the same count again: nothing to do").isEqualTo(8);
    }

    private TestOutboxConfig config(Integer shards) {
        return new TestOutboxConfig().table(table).shards(shards);
    }

    private OutboxSchema schema(TestOutboxConfig config) {
        return new OutboxSchema(db, config);
    }

    private Node start(TestOutboxConfig config) throws SQLException {
        return start(config, null);
    }

    /** Bootstrap with {@code config}, and a relay (not started), maintenance and an outbox over it. */
    private Node start(TestOutboxConfig config, OutboxDeadLetterHandler hook) throws SQLException {
        var schema = schema(config);
        schema.start();
        var resolved = schema.config();
        var maintenance = new Maintenance(db, schema.names(), resolved, new OutboxMetrics(null));
        var relay = new OutboxRelay(db, resolved, schema, recording, TestTelemetry.kora(), maintenance,
                new DeadLetterPass(db, schema.names(), resolved, hook, new OutboxMetrics(null)));
        relays.add(relay);
        return new Node(schema, maintenance, relay,
                new PgOutbox(db, schema, resolved.shards(), relay::wake, TestTelemetry.kora()));
    }

    private UUID send(Outbox outbox, String key, String payload) {
        return db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", key, payload.getBytes(UTF_8))));
    }

    /** The relays stopped long ago: every tick dated 10 minutes back, with no gap between them. */
    private void quiet() throws SQLException {
        execute("update " + names.tick() + " set created_at = now() - interval '10 minutes'");
    }

    /** Every instance stopped long ago: no instance row, no recent tick. */
    private void stopped() throws SQLException {
        execute("delete from " + names.instance());
        quiet();
    }

    /** What another instance's reshard to {@code shards} leaves: one cursor per shard at the latest tick. */
    private void reshardBehindItsBack(int shards) throws SQLException {
        execute("delete from " + names.cursor());
        execute("delete from " + names.checkpoint());
        execute("insert into " + names.cursor() + " (shard, tick_id) select s, (select max(id) from " + names.tick()
                + ") from generate_series(0, " + shards + " - 1) s");
    }

    private TableNames resolvedNames() throws SQLException {
        try (var c = TestDb.connect()) {
            return names.resolve(c);
        }
    }

    private long cursorShards() throws SQLException {
        return queryLong("select count(distinct shard) from " + names.cursor());
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void await(Condition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.holds()) {
            assertThat(System.nanoTime()).as("awaited condition").isLessThan(deadline);
            Thread.sleep(20);
        }
    }

    private static void execute(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, sql);
        }
    }

    private static long queryLong(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            return TestSql.queryLong(c, sql);
        }
    }

    private static String queryString(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            return TestSql.queryString(c, sql);
        }
    }
}
