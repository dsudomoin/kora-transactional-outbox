package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.OutboxRelay;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

class XidRecoveryIT {

    private final JdbcDatabase db = TestDb.newDatabase(10);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private OutboxSchema schema;
    private TableNames names;

    @AfterEach
    void release() {
        relays.forEach(OutboxRelay::release);
        db.release();
    }

    @Test
    void maintenanceRecoversARegressionItself() throws Exception {
        bootstrap();
        var handler = new RecordingHandler();
        var maintenance = maintenance();
        var relay = relay(handler, maintenance);
        relay.init();
        var outbox = outbox(relay);
        var sent = new ArrayList<>(send(outbox, messages("before", 20)));
        await(Duration.ofSeconds(5), () -> handler.delivered().size() >= 20);
        long forged = forgeRegression();
        sent.addAll(send(outbox, messages("after", 20))); // xids the forged tick takes for relayed
        await(Duration.ofSeconds(5), () -> latestTickId() > forged); // workers go on ticking past it

        try (var other = TestDb.connect()) { // two instances detect it: only the maintenance lock holder recovers
            advisory(other, "pg_advisory_lock");
            assertThat(maintenance.runOnce()).isFalse();
            assertThat(maintenance.halted()).isTrue();
            assertThat(tickExists(forged)).isTrue();
            advisory(other, "pg_advisory_unlock");
        }
        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("xid regression detected").contains("recovered: ")
                    .contains(" rows re-sent");
        }
        assertThat(maintenance.halted()).isFalse();
        assertThat(haltRows()).isZero();
        assertThat(tickExists(forged)).isFalse();
        await(Duration.ofSeconds(10), () -> delivered(handler).containsAll(sent));
    }

    @Test
    void bootstrapRecoversBeforeTheRelayStarts() throws Exception {
        bootstrap();
        var sent = new ArrayList<>(send(outbox(null), messages("before", 10))); // never relayed
        forgeRegression();
        sent.addAll(send(outbox(null), messages("after", 10)));

        try (var err = new CapturedStderr()) {
            bootstrap(); // the restart

            assertThat(err.text()).contains("ERROR").contains("recovered: 20 rows re-sent");
        }
        assertThat(haltRows()).isZero();
        var handler = new RecordingHandler();
        var maintenance = maintenance();
        relay(handler, maintenance).init();
        assertThat(maintenance.halted()).isFalse();
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 20);
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(sent);
    }

    @Test
    void aboveMaxRowsTheRelayStaysHalted() throws Exception {
        config.maxRows(10);
        bootstrap();
        send(outbox(null), messages("m", 100));
        long forged = forgeRegression();
        var maintenance = maintenance();

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("100 rows to re-send exceed outbox.recovery.maxRows 10")
                    .contains("OutboxAdmin.recoverXidRegression()")
                    .contains("POST /kora-outbox/admin/v1/recovery/xid-regression");
        }
        assertThat(maintenance.halted()).isTrue();
        assertThat(haltRows()).isOne();
        assertThat(tickExists(forged)).isTrue();
        assertThat(count(names.base())).isEqualTo(100);
        try (var err = new CapturedStderr()) {
            bootstrap(); // a restart starts halted with the same ERROR

            assertThat(err.text()).contains("100 rows to re-send exceed outbox.recovery.maxRows 10");
        }
        assertThat(tickExists(forged)).isTrue();
    }

    @Test
    void haltModeNeverRecoversByItself() throws Exception {
        config.xidRegression("halt");
        bootstrap();
        send(outbox(null), messages("m", 5));
        long forged = forgeRegression();
        var maintenance = maintenance();

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("outbox.recovery.xidRegression = halt")
                    .contains("OutboxAdmin.recoverXidRegression()");
        }
        assertThat(maintenance.halted()).isTrue();
        assertThat(tickExists(forged)).isTrue();
        assertThat(count(names.base())).isEqualTo(5);
    }

    @Test
    void bootstrapStartsHaltedInsteadOfFailing() throws Exception {
        config.xidRegression("halt");
        bootstrap();
        send(outbox(null), messages("m", 5));
        forgeRegression();

        try (var err = new CapturedStderr()) {
            bootstrap(); // no exception: the admin call has to be reachable

            assertThat(err.text()).contains("ERROR").contains("xid regression detected").contains("relay halted");
        }
        assertThat(haltRows()).isOne();
        var handler = new RecordingHandler();
        var maintenance = maintenance();
        var relay = relay(handler, maintenance);
        relay.init();
        assertThat(maintenance.halted()).isTrue();
        Thread.sleep(300); // the workers would deliver within milliseconds
        assertThat(handler.delivered()).isEmpty();
        assertThatThrownBy(relay::drain).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("outbox halted");
    }

    @Test
    void recoveryRefusesWhenTheShardCountChanged() throws Exception {
        bootstrap(); // 4 shards
        send(outbox(null), messages("m", 5));
        long forged = forgeRegression();
        var resharded = new TestOutboxConfig().table(config.table()).shards(8);
        var maintenance = new Maintenance(db, names, resharded, new OutboxMetrics(null));

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("outbox.shards = 8").contains("4 shards");
        }
        assertThat(maintenance.halted()).isTrue();
        assertThat(tickExists(forged)).isTrue();
        assertThat(count(names.base())).isEqualTo(5);
    }

    @Test
    void standaloneRangesAreSetAside() throws Exception {
        config.xidsPerPartition(1000).premake(10);
        bootstrap();
        var range = ranges().stream()
                .filter(b -> b.to() != null)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        var orphanId = UUID.randomUUID();
        try (var c = TestDb.connect()) { // a range with a row, detached by someone else
            execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic) values (1000000, 0, '"
                    + range.from() + "', '" + orphanId + "', 't')");
            while (currentXid() < Long.parseLong(range.to())) { // writers have moved on to the next range
                TestDb.burnXids(200);
            }
            execute(c, "alter table " + names.base() + " detach partition " + range.name() + " concurrently");
        }
        send(outbox(null), messages("m", 3));
        forgeRegression();
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));

        assertThat(maintenance.runOnce()).isTrue(); // the recovery
        var setAside = names.base() + "_o" + range.from();
        assertThat(exists(range.name())).isFalse();
        assertThat(exists(setAside)).isTrue();
        assertThat(count(setAside)).isOne();
        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue(); // the next run's orphan sweep keeps it

            assertThat(err.text()).contains("ERROR").contains(setAside)
                    .contains("OutboxAdmin.restoreOrphan(\"" + setAside + "\", fromXid)");
        }
        assertThat(exists(setAside)).isTrue();
        assertThat(registry.get("outbox.partitions.orphaned").gauge().value()).isEqualTo(1);
        try (var c = TestDb.connect()) {
            assertThat(maintenance.orphanRestore.restore(c, setAside, 0, Duration.ofSeconds(5))).isOne();
        }
        assertThat(exists(setAside)).isFalse();
        assertThat(count(names.base(), List.of(orphanId))).isOne();
    }

    @Test
    void rowsOutsideTheOutboxStayWhereTheyAre() throws Exception {
        config.headFailuresBeforeDivert(1);
        bootstrap();
        var down = new AtomicBoolean(true);
        var handler = new RecordingHandler()
                .failAlways(r -> down.get() && "r".equals(key(r)), () -> new RuntimeException("target down"));
        var maintenance = maintenance();
        var relay = relay(handler, maintenance);
        relay.init();
        var outbox = outbox(relay);
        var held = send(outbox, msg("t", "r", "head"), msg("t", "r", "follower"));
        await(Duration.ofSeconds(5), () -> count(names.retry()) == 2); // the head diverted with its follower
        forgeRegression();
        var other = send(outbox, msg("t", "k", "other"));

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(maintenance.halted()).isFalse();
        assertThat(count(names.retry(), held)).as("the retry rows survive").isEqualTo(2);
        assertThat(count(names.base(), held)).as("not copied into the outbox").isZero();
        down.set(false);
        await(Duration.ofSeconds(10),
                () -> delivered(handler).containsAll(held) && delivered(handler).containsAll(other));
        assertThat(handler.delivered()).extracting(r -> new String(r.payload(), UTF_8))
                .filteredOn(p -> !p.equals("other"))
                .containsExactly("head", "follower");
        await(Duration.ofSeconds(5), () -> count(names.retry()) == 0);
    }

    @Test
    void writersWaitButDoNotFail() throws Exception {
        config.relayEnabled(false).batchSize(1000);
        bootstrap();
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.base() + " (shard, message_id, topic, payload) select s % "
                    + config.shards() + ", gen_random_uuid(), 't', convert_to('p' || s, 'UTF8')"
                    + " from generate_series(1, 50000) s");
        }
        forgeRegression();
        var expected = new HashSet<>(messageIds());
        var outbox = outbox(null);
        var stop = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var written = new ConcurrentLinkedQueue<UUID>();
        var pool = Executors.newFixedThreadPool(5);
        var tasks = new ArrayList<Future<?>>();
        try {
            for (int i = 0; i < 4; i++) {
                var key = "w" + i;
                tasks.add(pool.submit(() -> {
                    while (!stop.get()) {
                        try {
                            written.addAll(send(outbox, msg("t", key, "w")));
                        } catch (Throwable t) {
                            errors.add(t);
                        }
                    }
                }));
            }
            tasks.add(pool.submit(() -> { // other traffic takes xids while the recovery runs
                while (!stop.get()) {
                    try {
                        TestDb.burnXids(20);
                    } catch (SQLException e) {
                        errors.add(e);
                    }
                }
            }));
            await(Duration.ofSeconds(5), () -> written.size() >= 40);

            try (var err = new CapturedStderr()) {
                assertThat(maintenance().runOnce()).isTrue();

                assertThat(err.text()).contains("recovered: ");
            }
            long after = written.size();
            await(Duration.ofSeconds(5), () -> written.size() >= after + 40);
        } finally {
            stop.set(true);
            for (var task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();
        }

        assertThat(errors).isEmpty();
        expected.addAll(written);
        var handler = new RecordingHandler();
        relay(handler, maintenance()).drain();
        assertThat(delivered(handler)).containsAll(expected);
    }

    @Test
    void recoveryLeavesTheSessionSettingsAsTheyWere() throws Exception {
        bootstrap();
        send(outbox(null), messages("m", 3));
        forgeRegression();

        try (var c = TestDb.connect()) { // a pooled connection: its partition DDL ran inside the transaction
            assertThat(new XidRecovery(names, config).recover(c)).isEqualTo(3);

            assertThat(c.getAutoCommit()).isTrue();
            assertThat(queryString(c, "select current_setting('lock_timeout') || ' ' || current_setting("
                    + "'statement_timeout')")).isEqualTo("0 0");
        }
    }

    private void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
    }

    /**
     * As after a dump/restore: a tick whose snapshot xmax lies 10⁹ xids past the counter, made a minute ago. Returns
     * its id.
     */
    private long forgeRegression() throws SQLException {
        try (var c = TestDb.connect()) {
            long xmax = currentXid() + 1_000_000_000L;
            return queryLong(c, "insert into " + names.tick() + " (snapshot, created_at) values ('" + xmax + ":" + xmax
                    + ":'::pg_snapshot, now() - interval '1 minute') returning id");
        }
    }

    private Maintenance maintenance() {
        return new Maintenance(db, names, config, new OutboxMetrics(null));
    }

    private OutboxRelay relay(OutboxHandler handler, Maintenance maintenance) {
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance);
        relays.add(relay);
        return relay;
    }

    private PgOutbox outbox(OutboxRelay relay) {
        return new PgOutbox(db, schema, config.shards(), relay == null ? () -> {} : relay::wake,
                new OutboxMetrics(null));
    }

    private List<UUID> send(PgOutbox outbox, OutboxMessage... messages) {
        return db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(messages)));
    }

    private static OutboxMessage[] messages(String prefix, int n) {
        return IntStream.range(0, n).mapToObj(i -> msg("t", "k" + i % 5, prefix + i)).toArray(OutboxMessage[]::new);
    }

    private static OutboxMessage msg(String topic, String key, String payload) {
        return OutboxMessage.of(topic, key, payload.getBytes(UTF_8));
    }

    private static String key(OutboxRecord r) {
        return r.key() == null ? null : new String(r.key(), UTF_8);
    }

    private static Set<UUID> delivered(RecordingHandler handler) {
        var ids = new HashSet<UUID>();
        handler.delivered().forEach(r -> ids.add(r.messageId()));
        return ids;
    }

    private void advisory(Connection c, String function) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_MAINTENANCE);
            ps.execute();
        }
    }

    private long latestTickId() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select max(id) from " + names.tick());
        }
    }

    private boolean tickExists(long id) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + names.tick() + " where id = " + id) == 1;
        }
    }

    private long haltRows() throws SQLException {
        return count(names.halt());
    }

    private static long count(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + table);
        }
    }

    private static long count(String table, List<UUID> messageIds) throws SQLException {
        try (var c = TestDb.connect();
                var ps = c.prepareStatement("select count(*) from " + table + " where message_id = any(?)")) {
            ps.setArray(1, c.createArrayOf("uuid", messageIds.toArray()));
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private List<UUID> messageIds() throws SQLException {
        try (var c = TestDb.connect();
                var st = c.createStatement();
                var rs = st.executeQuery("select message_id from " + names.base())) {
            var ids = new ArrayList<UUID>();
            while (rs.next()) {
                ids.add(rs.getObject(1, UUID.class));
            }
            return ids;
        }
    }

    private List<Bound> ranges() throws SQLException {
        try (var c = TestDb.connect()) {
            return PartitionDdl.listRanges(c, names.base());
        }
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select (to_regclass(?) is not null)::text", table).equals("true");
        }
    }

    private static long currentXid() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
        }
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void await(Duration timeout, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(5);
        }
    }
}
