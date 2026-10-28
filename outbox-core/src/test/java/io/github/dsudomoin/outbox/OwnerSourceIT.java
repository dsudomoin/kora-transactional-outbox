package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Migrations plus a DML-only application role plus an owner source: the rendered script, run by the owner role,
 * grants the application role what it needs; the runtime's binding logs in as the application role, its owner
 * source as the owner.
 */
class OwnerSourceIT {

    private final String schema = "s" + TestSql.uniqueTable();
    private final String owner = "o" + TestSql.uniqueTable();
    private final String app = "a" + TestSql.uniqueTable();
    private final TestOutboxConfig config = new TestOutboxConfig().table(schema + ".outbox")
            .inboxTable(schema + ".inbox").schemaBootstrap(false);
    private final TableNames names = TableNames.of(config.table());
    private final List<AutoCloseable> runtimes = new ArrayList<>();

    OwnerSourceIT() throws SQLException {
        sql("create role " + owner + " login password 'pw'");
        sql("create role " + app + " login password 'pw'");
        sql("grant pg_read_all_stats to " + owner); // as README recommends: no hidden activity before a tail swap
        sql("create schema " + schema + " authorization " + owner);
        sql("grant usage on schema " + schema + " to " + app);
    }

    @AfterEach
    void close() throws Exception {
        for (int i = runtimes.size() - 1; i >= 0; i--) {
            runtimes.get(i).close();
        }
    }

    @Test
    void bootstrapOnWithOwnerFails() {
        var builder = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()),
                new JdbcTransactions(as(app)), record -> {}).ownerConnections(as(owner)::getConnection);

        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("outbox.schema.bootstrap must be false when an owner connection source is set: objects"
                        + " it created would leave the application role without rights on them");
        assertThatThrownBy(() -> InboxRuntime.builder(new TestOutboxConfig(), new JdbcTransactions(as(app)))
                .ownerConnections(as(owner)::getConnection).build()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("outbox.schema.bootstrap must be false");
    }

    @Test
    void ownerOnAnotherDatabaseFails() throws Exception {
        migrate();
        var other = "d" + TestSql.uniqueTable();
        sql("create database " + other + " owner " + owner);
        var elsewhere = as(owner);
        elsewhere.setDatabaseName(other);
        var runtime = runtime(OutboxRuntime.builder(config, new JdbcTransactions(as(app)), record -> {})
                .ownerConnections(elsewhere::getConnection).build());

        assertThatThrownBy(runtime::start).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outbox owner connection source reaches another database than the application's")
                .hasMessageContaining(other).hasMessageContaining(as(app).getDatabaseName());
        try (var c = elsewhere.getConnection()) {
            assertThat(TestSql.queryLong(c, "select count(*) from pg_class where relname like 'outbox%'"))
                    .as("no DDL there").isZero();
        }
    }

    @Test
    void ownerWithoutDdlRightsFails() throws Exception {
        migrate();
        var runtime = runtime(OutboxRuntime.builder(config, new JdbcTransactions(as(app)), record -> {})
                .ownerConnections(as(app)::getConnection).build());

        assertThatThrownBy(runtime::start).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("outbox schema check of " + config.table() + " failed")
                .hasMessageContaining("role " + app + " does not own " + names.base());
    }

    @Test
    void dmlOnlyLifecycleUnderLoad() throws Exception {
        migrate();
        var interval = Duration.ofMillis(500);
        config.xidsPerPartition(1000).premake(4).maintenanceInterval(interval).cursorAnchorInterval(interval)
                .inboxPartitionInterval(Duration.ofMinutes(1)).inboxPremake(Duration.ofMinutes(5))
                .inboxRetention(Duration.ofMinutes(10))
                .retry(Duration.ofMillis(50), Duration.ofMillis(200), 0).relayEnabled(true);
        var transactions = new JdbcTransactions(pooled(app));
        ConnectionSource ownerSource = as(owner)::getConnection;
        var handler = new KeyedHandler();
        OutboxDeadLetterHandler hook = letters -> letters.stream()
                .map(l -> l.redrives() == 0 ? DeadLetterDecision.redrive() : DeadLetterDecision.keep()).toList();
        var sent = ConcurrentHashMap.<UUID>newKeySet();
        var accepted = ConcurrentHashMap.<UUID>newKeySet();
        var writerErrors = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
        try (var err = new CapturedStderr()) {
            var outbox = runtime(OutboxRuntime.builder(config, transactions, handler).deadLetterHandler(hook)
                    .ownerConnections(ownerSource).build());
            outbox.start();
            var inbox = runtime(InboxRuntime.builder(config, transactions).ownerConnections(ownerSource).build());
            inbox.start();

            var stop = new AtomicBoolean();
            var writers = new ArrayList<Thread>();
            for (int w = 0; w < 4; w++) {
                int writer = w;
                var thread = new Thread(() -> {
                    for (int i = 0; !stop.get() && writerErrors.isEmpty(); i++) {
                        var key = new String[] {"r", "q", "p", "d", "d", "d"}[i % 6] + writer + "-" + (i % 50);
                        var payload = ("m" + i).getBytes(UTF_8);
                        var id = UuidV7.next();
                        transactions.inTx(c -> {
                            sent.add(outbox.outbox().send(OutboxMessage.of("t", key, payload)));
                            if (inbox.inbox().tryAccept(id)) {
                                accepted.add(id);
                            }
                            return null;
                        });
                    }
                });
                thread.setUncaughtExceptionHandler((t, e) -> writerErrors.add(e));
                thread.start();
                writers.add(thread);
            }
            Thread.sleep(8_000);
            restoreDetachedRange(outbox.admin()); // a foreign orphan: every row of it back into the outbox
            Thread.sleep(8_000);
            forgeXidRegression(); // the next tick halts the relay; maintenance recovers it on the owner source
            Thread.sleep(14_000);
            stop.set(true);
            for (var t : writers) {
                t.join();
            }

            assertThat(writerErrors).as("writers' failures").isEmpty();
            try {
                await(Duration.ofSeconds(60), () -> undecided(sent, handler).isEmpty());
            } catch (AssertionError e) {
                throw new AssertionError(where(undecided(sent, handler)), e);
            }
            await(Duration.ofSeconds(10), () -> {
                var since = outbox.admin().status().maintenanceSinceSuccessSeconds();
                return since != null && since < 2 * interval.toMillis() / 1000.0;
            });
            // the SQLState, not the bare code: that turns up inside message ids
            assertThat(err.text()).doesNotContain("permission denied").doesNotContain("SQLState 42501");
        }
        assertThat(handler.outcomes).as("retried, requeued and dead-lettered keys")
                .contains("retry", "requeue", "dead");
        try (var c = TestDataSources.newDataSource().getConnection()) {
            assertThat(TestSql.queryLong(c, "select count(*) from " + names.dead() + " where redrives = 1"))
                    .as("letters redriven by the hook and dead again").isPositive();
            assertThat(TestSql.queryLong(c, "select count(*) from pg_class r join pg_namespace n on n.oid ="
                    + " r.relnamespace where n.nspname = '" + schema + "' and r.relname like 'outbox_x%' and"
                    + " pg_get_userbyid(r.relowner) <> '" + owner + "'")).as("ranges made by another role").isZero();
            assertThat(TestSql.queryLong(c, "select count(*) from " + names.halt())).as("recovered").isZero();
        }
        assertThat(accepted).isNotEmpty();
    }

    /** Sent messages neither delivered nor kept in the dead table. */
    private Set<UUID> undecided(Set<UUID> sent, KeyedHandler handler) throws SQLException {
        var open = new HashSet<>(sent);
        open.removeAll(handler.delivered);
        try (var c = TestDataSources.newDataSource().getConnection(); var st = c.createStatement();
                var rs = st.executeQuery("select message_id from " + names.dead() + " where status = 'kept'")) {
            while (rs.next()) {
                open.remove(rs.getObject(1, UUID.class));
            }
        }
        return open;
    }

    /** Where the undecided messages are: their rows in the outbox and in its retry, parked and dead tables. */
    private String where(Set<UUID> undecided) throws SQLException {
        var ids = undecided.stream().limit(20).map(id -> "'" + id + "'").toList();
        var report = new StringBuilder(undecided.size() + " undecided, e.g.");
        try (var c = TestDataSources.newDataSource().getConnection(); var st = c.createStatement();
                var rs = st.executeQuery("select message_id, convert_from(key, 'UTF8'), string_agg(t, ' ') from ("
                        + "select message_id, key, 'outbox(xid ' || xid || ')' t from " + names.base()
                        + " union all select message_id, key, 'retry' from " + names.retry()
                        + " union all select message_id, key, 'parked' from " + names.parked()
                        + " union all select message_id, key, 'dead(' || status || ')' from " + names.dead()
                        + ") r where message_id in (" + String.join(", ", ids) + ") group by 1, 2")) {
            while (rs.next()) {
                report.append(' ').append(rs.getString(2)).append(": ").append(rs.getString(3)).append(';');
            }
        }
        return report.toString();
    }

    /**
     * Detaches the oldest range wholly below the xid counter by hand, as an operator might, leaving a foreign orphan
     * (maintenance keeps it, as it holds rows), then restores it through the admin from its lower bound: every row of
     * it, delivered or not, goes back into the outbox.
     */
    private void restoreDetachedRange(OutboxAdmin admin) throws Exception {
        String range = null;
        try (var c = as(owner).getConnection()) {
            int ns = names.resolve(c).internalNamespace(); // no maintenance run meanwhile: one detach at a time
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (range == null) {
                TestSql.execute(c, "select pg_advisory_lock(" + ns + ", " + TableNames.LOCK_MAINTENANCE + ")");
                // the cast only on range names: the planner may evaluate it on rows of any join order
                var from = "case when r.relname ~ '^outbox_x[0-9]+$' then substr(r.relname, 9)::bigint end";
                // its bound, not from + xidsPerPartition: a swapped tail is attached with its rows as a wider range,
                // writers may still be inside it; null for the tail
                var to = "substring(pg_get_expr(r.relpartbound, r.oid) from 'TO \\(''(\\d+)''\\)')::bigint";
                range = TestSql.queryString(c, "select (select n.nspname || '.' || r.relname from pg_inherits i"
                        + " join pg_class r on r.oid = i.inhrelid join pg_namespace n on n.oid = r.relnamespace"
                        + " where i.inhparent = ?::regclass and r.relname ~ '^outbox_x[0-9]+$' and " + to
                        + " <= pg_current_xact_id()::text::bigint order by " + from + " limit 1)", names.base());
                if (range == null) {
                    TestSql.execute(c, "select pg_advisory_unlock(" + ns + ", " + TableNames.LOCK_MAINTENANCE + ")");
                    assertThat(System.nanoTime()).as("a range below the xid counter").isLessThan(deadline);
                    Thread.sleep(100);
                }
            }
            TestSql.execute(c, "alter table " + names.base() + " detach partition " + range + " concurrently");
            TestSql.execute(c, "select pg_advisory_unlock(" + ns + ", " + TableNames.LOCK_MAINTENANCE + ")");
        }
        long lower = Long.parseLong(range.substring(range.lastIndexOf("_x") + 2));
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (true) { // a call gives up on keys open sends hold: call again, as README says
            try {
                assertThat(admin.restoreOrphan(range, lower)).as("rows moved back").isPositive();
                return;
            } catch (RuntimeException e) {
                assertThat(System.nanoTime()).as("restoreOrphan(" + range + ") within 30 s: " + e).isLessThan(deadline);
                Thread.sleep(200);
            }
        }
    }

    private void forgeXidRegression() throws SQLException {
        try (var c = TestDataSources.newDataSource().getConnection()) {
            long xmax = TestSql.queryLong(c, "select pg_current_xact_id()::text::bigint") + 1_000_000_000L;
            TestSql.execute(c, "insert into " + names.tick() + " (snapshot, created_at) values ('" + xmax + ":" + xmax
                    + ":'::pg_snapshot, now() - interval '1 minute')");
        }
    }

    private void migrate() throws SQLException {
        try (var c = as(owner).getConnection()) {
            TestSql.execute(c, OutboxSql.outbox(config, app));
            TestSql.execute(c, OutboxSql.inbox(config, app));
        }
    }

    private <T extends AutoCloseable> T runtime(T runtime) {
        runtimes.add(runtime);
        return runtime;
    }

    /**
     * A pool as {@code role}: the writers and the relay at full speed on a new connection per transaction take every
     * local port within seconds (each closed one waits in TIME_WAIT).
     */
    @SuppressWarnings("deprecation") // the driver's own pool is enough for a test
    private PGPoolingDataSource pooled(String role) {
        var pool = new PGPoolingDataSource();
        pool.setDataSourceName(role); // close() requires one; the role is unique to the test
        pool.setUrl(TestDataSources.jdbcUrl());
        pool.setUser(role);
        pool.setPassword("pw");
        pool.setMaxConnections(32);
        runtimes.add(pool::close); // closed last: the runtimes, registered later, close first
        return pool;
    }

    private static PGSimpleDataSource as(String role) {
        var ds = (PGSimpleDataSource) TestDataSources.newDataSource();
        ds.setUser(role);
        ds.setPassword("pw");
        return ds;
    }

    private static void sql(String sql) throws SQLException {
        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, sql);
        }
    }

    private static void await(Duration timeout, Check condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(50);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }

    /**
     * By key, per message id: {@code r…} retried once, {@code q…} requeued once, {@code p…} permanently failed, the
     * rest delivered.
     */
    private static final class KeyedHandler implements OutboxHandler {

        final Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        final Set<String> outcomes = ConcurrentHashMap.newKeySet();
        private final Set<UUID> failedOnce = ConcurrentHashMap.newKeySet();

        @Override
        public void handle(OutboxRecord record) {
            throw new UnsupportedOperationException("handleBatch answers");
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            var outcomes = new ArrayList<Outcome>(records.size());
            for (var record : records) {
                var kind = new String(record.key(), UTF_8).charAt(0);
                Outcome outcome;
                if (kind == 'p') {
                    outcome = Outcome.permanent(new IllegalArgumentException("never"));
                    this.outcomes.add("dead");
                } else if (kind != 'd' && failedOnce.add(record.messageId())) {
                    outcome = kind == 'r' ? Outcome.retry(new IOException("down")) : Outcome.requeue();
                    this.outcomes.add(kind == 'r' ? "retry" : "requeue");
                } else {
                    delivered.add(record.messageId());
                    outcome = Outcome.delivered();
                }
                outcomes.add(outcome);
            }
            return outcomes;
        }
    }
}
