package io.github.dsudomoin.kora.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.PgOutbox;
import io.github.dsudomoin.kora.outbox.internal.TableNames;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.kora.outbox.testing.TestSql;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The emergency reshard at bootstrap (an explicit {@code outbox.shards} other than the cursors'), its guards (other
 * instances' heartbeats, a relay still ticking, the drain checks under a lock that holds writers), the shard count an
 * instance without {@code outbox.shards} adopts, and the mismatch guard of a running instance.
 */
class ReshardIT {

    private final JdbcDatabase db = TestDb.newDatabase(20);
    private final String table = TestDb.uniqueTable();
    private final TableNames names = TableNames.of(table);
    private final RecordingHandler recording = new RecordingHandler();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();

    private record Node(OutboxSchema schema, Maintenance maintenance, OutboxRelay relay, Outbox outbox) {}

    @AfterEach
    void release() {
        relays.forEach(OutboxRelay::release);
        pool.shutdownNow();
        db.release();
    }

    @Test
    void drainedOutboxReshardsAtBootstrap() throws Exception {
        var old = start(config(4));
        old.relay().init();
        for (int i = 0; i < 40; i++) {
            send(old.outbox(), "k" + i % 10, "a" + i);
        }
        old.relay().drain();
        old.relay().release();
        quiet();

        Node next;
        try (var err = new CapturedStderr()) {
            next = start(config(8));
            assertThat(err.text()).contains("INFO").contains("resharded " + table + " from 4 to 8 shards");
        }
        assertThat(cursorShards()).isEqualTo(8);
        assertThat(queryLong("select count(*) from " + names.checkpoint())).isZero();
        next.relay().init();
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
        quiet();

        assertThatThrownBy(() -> new OutboxSchema(db, config(8)).init())
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
        start(config(4).maintenanceInterval(Duration.ofSeconds(10)));
        execute("insert into " + names.instance() + " (instance_id, shards) values ('host-a/42/0badcafe', 4)");
        quiet();

        assertThatThrownBy(() -> new OutboxSchema(db, config(8).maintenanceInterval(Duration.ofSeconds(10))).init())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("host-a/42/0badcafe (4 shards")
                .hasMessageContaining("outbox.shards");
        assertThat(cursorShards()).isEqualTo(4);

        execute("update " + names.instance() + " set last_seen = now() - interval '21 seconds'");
        new OutboxSchema(db, config(8).maintenanceInterval(Duration.ofSeconds(10))).init();
        assertThat(cursorShards()).as("aged beyond 2 × maintenanceInterval").isEqualTo(8);
    }

    @Test
    void aTickingRelayRefusesTheReshard() throws Exception {
        var old = start(config(4));
        old.relay().init();
        await(() -> queryLong("select count(*) from " + names.instance()) == 1);
        execute("delete from " + names.instance()); // the previous release writes no heartbeat

        assertThatThrownBy(() -> new OutboxSchema(db, config(8)).init())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("(?s).*a relay is still running \\(latest tick \\d+ ms ago\\).*");
        assertThat(cursorShards()).isEqualTo(4);
    }

    @Test
    void runningInstanceStopsOnAShardMismatch() throws Exception {
        var node = start(config(4));
        reshardBehindItsBack(8);

        try (var err = new CapturedStderr()) {
            node.maintenance().runOnce();
            assertThat(node.maintenance().halted()).isTrue();
            assertThat(err.text()).contains("ERROR").contains(names.cursor() + " holds cursors for 8 shards")
                    .contains("runs with outbox.shards = 4");
        }
        node.relay().init();
        send(node.outbox(), "k", "x");
        Thread.sleep(300);
        assertThat(recording.delivered()).as("the relay takes no shards").isEmpty();
        assertThatThrownBy(node.relay()::drain).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("8 shards");

        reshardBehindItsBack(4);
        node.maintenance().runOnce();
        assertThat(node.maintenance().halted()).as("resumes once they match again").isFalse();
        await(() -> recording.delivered().size() == 1);
    }

    @Test
    void heartbeatRowIsGoneAfterShutdown() throws Exception {
        var node = start(config(4).relayEnabled(false));
        node.relay().init();

        await(() -> queryLong("select count(*) from " + names.instance()) == 1);
        assertThat(queryString("select instance_id from " + names.instance()))
                .matches("[^/]+/" + ProcessHandle.current().pid() + "/[0-9a-f]{8}");
        assertThat(queryString("select shards || ' ' || hook_active from " + names.instance())).isEqualTo("4 false");

        node.relay().release();
        assertThat(queryLong("select count(*) from " + names.instance())).isZero();
    }

    @Test
    void writersWaitDuringTheChecksAndThenGoOn() throws Exception {
        var old = start(config(4));
        var writer = new PgOutbox(db, old.schema(), 8, () -> {}, new OutboxMetrics(null));
        quiet();
        try (var holder = TestDb.connect()) {
            holder.setAutoCommit(false);
            // holds the retry check, which runs under SHARE on the outbox
            TestSql.execute(holder, "lock table " + names.retry() + " in access exclusive mode");
            var resharding = pool.submit(() -> {
                new OutboxSchema(db, config(8)).init();
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
        fresh.init();
        assertThat(fresh.config().shards()).as("a new outbox").isEqualTo(OutboxConfig.DEFAULT_SHARDS);
    }

    @Test
    void anExplicitShardCountReshardsOnlyWhenDrained() throws Exception {
        var old = start(config(4));
        send(old.outbox(), "k", "x");
        quiet();

        assertThatThrownBy(() -> new OutboxSchema(db, config(8)).init())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("1 committed rows not relayed");
        assertThat(cursorShards()).isEqualTo(4);

        old.relay().drain();
        quiet();
        start(config(8));
        assertThat(cursorShards()).isEqualTo(8);
        start(config(8));
        assertThat(cursorShards()).as("the same count again: nothing to do").isEqualTo(8);
    }

    private TestOutboxConfig config(Integer shards) {
        return new TestOutboxConfig().table(table).shards(shards);
    }

    /** Bootstrap with {@code config}, and a relay (not started), maintenance and an outbox over it. */
    private Node start(TestOutboxConfig config) throws SQLException {
        var schema = new OutboxSchema(db, config);
        schema.init();
        var resolved = schema.config();
        var maintenance = new Maintenance(db, schema.names(), resolved, new OutboxMetrics(null));
        var relay = new OutboxRelay(db, resolved, schema, recording, new OutboxMetrics(null), null, maintenance,
                new DeadLetterPass(db, schema.names(), resolved, null, new OutboxMetrics(null)));
        relays.add(relay);
        return new Node(schema, maintenance, relay,
                new PgOutbox(db, schema, resolved.shards(), relay::wake, new OutboxMetrics(null)));
    }

    private UUID send(Outbox outbox, String key, String payload) {
        return db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", key, payload.getBytes(UTF_8))));
    }

    /** The relays stopped long ago: their ticks are dated a minute back. */
    private void quiet() throws SQLException {
        execute("update " + names.tick() + " set created_at = created_at - interval '1 minute'");
    }

    /** What another instance's reshard to {@code shards} leaves: one cursor per shard at the latest tick. */
    private void reshardBehindItsBack(int shards) throws SQLException {
        execute("delete from " + names.cursor());
        execute("delete from " + names.checkpoint());
        execute("insert into " + names.cursor() + " (shard, tick_id) select s, (select max(id) from " + names.tick()
                + ") from generate_series(0, " + shards + " - 1) s");
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
