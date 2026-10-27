package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.internal.XidRecovery;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The xid-regression recovery after a real logical restore: an outbox on cluster A, {@code pg_dump -Fc}, restored into
 * a fresh cluster B whose xid counter lies far below the ticks it got from A, as after a migration into another
 * cluster. Two containers of its own per test, so B's counter is always a fresh cluster's.
 */
class XidRegressionDumpRestoreIT {

    private static final String DUMP = "/tmp/outbox.dump";

    private final PostgreSQLContainer a = new PostgreSQLContainer("postgres:16-alpine");
    private final PostgreSQLContainer b = new PostgreSQLContainer("postgres:16-alpine");
    // B's writers insert before the recovery, so B's counter (about 1 000 after the restore) needs a range restored
    // from A, which bootstraps at about 100 000: [0, 1 000 000) holds both, whatever the test default becomes
    private final TestOutboxConfig config = new TestOutboxConfig().xidsPerPartition(1_000_000);
    private final List<OutboxRelay> relays = new ArrayList<>();
    private final List<JdbcDatabase> databases = new ArrayList<>();
    @TempDir
    Path dir;

    @BeforeEach
    void start() {
        Startables.deepStart(a, b).join();
    }

    @AfterEach
    void stop() {
        try {
            relays.forEach(OutboxRelay::release);
        } finally {
            try {
                databases.forEach(JdbcDatabase::release);
            } finally {
                a.stop();
                b.stop();
            }
        }
    }

    @Test
    void restoredDatabaseRecoversAndDeliversEverything() throws Exception {
        // first: a tick only comes with a relay step, so every tick A makes must lie above the burnt xids
        burnXids(a, 100_000);
        var dbA = database(a);
        var schemaA = bootstrap(dbA);
        var handlerA = new RecordingHandler();
        var relayA = relay(dbA, schemaA, handlerA);
        relayA.init();
        var outboxA = new PgOutbox(dbA, schemaA, config.shards(), relayA::wake, new OutboxMetrics(null));
        var first = send(dbA, outboxA, "first", 1000);
        await(Duration.ofSeconds(10), () -> deliveries(handlerA).keySet().containsAll(first));
        relayA.release();
        var undelivered = send(dbA, outboxA, "undelivered", 500);

        dumpAndRestore();
        assertRegressed(schemaA);

        var handlerB = new RecordingHandler();
        OutboxSchema schemaB;
        var writers = new Writers(b, config.table(), config.shards(), 2); // a service writing before the library starts
        try {
            await(Duration.ofSeconds(10), () -> writers.written.size() >= 50);
            var dbB = database(b);
            try (var err = new CapturedStderr()) {
                schemaB = bootstrap(dbB);

                assertThat(err.text()).contains("ERROR").contains("xid regression detected").contains("recovered: ")
                        .contains(" rows re-sent");
            }
            assertThat(haltRows(b, schemaB)).isZero();
            relay(dbB, schemaB, handlerB).init();
            int before = writers.written.size();
            await(Duration.ofSeconds(10), () -> writers.written.size() >= before + 50);
        } finally {
            writers.stop();
        }

        assertThat(writers.errors).isEmpty();
        var expected = new HashSet<>(undelivered);
        expected.addAll(writers.written);
        await(Duration.ofSeconds(30), () -> deliveries(handlerB).keySet().containsAll(expected));
        assertThat(haltRows(b, schemaB)).isZero();
        var all = new HashSet<>(expected);
        all.addAll(first);
        var deliveries = deliveries(handlerB);
        assertThat(all).containsAll(deliveries.keySet());
        assertThat(first).allSatisfy(id -> assertThat(deliveries.getOrDefault(id, 0)).as("deliveries of %s", id)
                .isLessThanOrEqualTo(1));
    }

    /**
     * {@code ./gradlew :outbox-kora:test -Pbenchmark --tests '*XidRegressionDumpRestoreIT'}: the writer pause of a
     * recovery that re-sends a million rows (key {@code u0..u9999}, payload 300 bytes), Postgres 16 defaults in a
     * container. One writer inserts a row per transaction meanwhile; its longest insert is the stall.
     */
    @Test
    @Tag("benchmark")
    void recoveryPausePerMillionRows() throws Exception {
        burnXids(a, 100_000);
        var schemaA = bootstrap(database(a));
        try (var c = connect(a)) {
            execute(c, "insert into " + config.table() + " (shard, message_id, topic, key, payload) select s % "
                    + config.shards() + ", gen_random_uuid(), 'orders', convert_to('u' || s % 10000, 'UTF8'),"
                    + " convert_to(repeat('p', 300), 'UTF8') from generate_series(1, 1000000) s");
        }
        dumpAndRestore();
        try (var c = connect(b)) { // pg_restore leaves no statistics: plan the recovery as on a live table
            execute(c, "vacuum analyze");
        }
        assertRegressed(schemaA);
        config.xidRegression("halt"); // bootstrap only records the halt: the recovery is the timed call below
        var schemaB = bootstrap(database(b));
        var recovery = new XidRecovery(schemaB.names(), config);

        long rows;
        long counted;
        long sent;
        long took;
        var writer = new Writers(b, config.table(), config.shards(), 1);
        try (var c = connect(b)) {
            await(Duration.ofSeconds(10), () -> writer.written.size() >= 100);
            long start = System.nanoTime();
            rows = recovery.rowsToSave(c); // as the automatic recovery does first, under no lock
            counted = System.nanoTime() - start;
            start = System.nanoTime();
            sent = recovery.recover(c);
            took = System.nanoTime() - start;
            int before = writer.written.size();
            await(Duration.ofSeconds(10), () -> writer.written.size() >= before + 100);
        } finally {
            writer.stop();
        }

        assertThat(writer.errors).isEmpty();
        assertThat(rows).isGreaterThanOrEqualTo(1_000_000);
        assertThat(sent).isGreaterThanOrEqualTo(rows);
        assertThat(haltRows(b, schemaB)).isZero();
        System.out.printf("xid regression recovery: %d rows re-sent; recover %d ms, longest writer stall %d ms"
                + " (rowsToSave counted %d rows in %d ms before it, under no lock)%n", sent,
                TimeUnit.NANOSECONDS.toMillis(took), TimeUnit.NANOSECONDS.toMillis(writer.longest.get()), rows,
                TimeUnit.NANOSECONDS.toMillis(counted));
    }

    /** {@code pg_dump -Fc} of A's database, restored into B's, empty until then. */
    private void dumpAndRestore() throws Exception {
        exec(a, "pg_dump", "-U", a.getUsername(), "-d", a.getDatabaseName(), "-Fc", "-f", DUMP);
        var file = dir.resolve("outbox.dump");
        a.copyFileFromContainer(DUMP, file.toString());
        b.copyFileToContainer(MountableFile.forHostPath(file), DUMP);
        exec(b, "pg_restore", "-U", b.getUsername(), "-d", b.getDatabaseName(), DUMP);
    }

    private static void exec(PostgreSQLContainer pg, String... command) throws Exception {
        var result = pg.execInContainer(command);
        assertThat(result.getExitCode()).as("%s: %s", command[0], result.getStderr()).isZero();
    }

    /** One transaction per xid, committed by a DO block: one statement, so fast. */
    private static void burnXids(PostgreSQLContainer pg, int n) throws SQLException {
        try (var c = connect(pg)) {
            execute(c, "set synchronous_commit = off");
            execute(c, "do $$ begin for i in 1.." + n + " loop perform pg_current_xact_id(); commit; end loop; end $$");
        }
    }

    private JdbcDatabase database(PostgreSQLContainer pg) {
        var db = TestDb.newDatabase(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword(), 10);
        databases.add(db);
        return db;
    }

    private OutboxSchema bootstrap(JdbcDatabase db) throws SQLException {
        var schema = new OutboxSchema(db, config);
        schema.init();
        return schema;
    }

    private OutboxRelay relay(JdbcDatabase db, OutboxSchema schema, OutboxHandler handler) {
        var maintenance = new Maintenance(db, schema.names(), config, new OutboxMetrics(null));
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance,
                new DeadLetterPass(db, schema.names(), config, null, new OutboxMetrics(null)));
        relays.add(relay);
        return relay;
    }

    /** {@code n} messages, 100 per transaction. */
    private static List<UUID> send(JdbcDatabase db, PgOutbox outbox, String prefix, int n) {
        var ids = new ArrayList<UUID>();
        for (int from = 0; from < n; from += 100) {
            var messages = IntStream.range(from, Math.min(n, from + 100))
                    .mapToObj(i -> OutboxMessage.of("t", "k" + i % 5, (prefix + i).getBytes(UTF_8)))
                    .toList();
            ids.addAll(db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages)));
        }
        return ids;
    }

    private static long haltRows(PostgreSQLContainer pg, OutboxSchema schema) throws SQLException {
        try (var c = connect(pg)) {
            return queryLong(c, "select count(*) from " + schema.names().halt());
        }
    }

    /** B before its bootstrap: its xid counter below the highest xmax of the ticks restored from A. */
    private void assertRegressed(OutboxSchema schemaA) throws SQLException {
        try (var c = connect(b)) {
            assertThat(queryLong(c, "select (pg_snapshot_xmax(pg_current_snapshot())::text::bigint < (select"
                    + " max(pg_snapshot_xmax(snapshot)::text::bigint) from " + schemaA.names().tick() + "))::int"))
                    .as("B's counter below A's ticks").isOne();
        }
    }

    private static Map<UUID, Integer> deliveries(RecordingHandler handler) {
        var counts = new HashMap<UUID, Integer>();
        handler.delivered().forEach(r -> counts.merge(r.messageId(), 1, Integer::sum));
        return counts;
    }

    private static Connection connect(PostgreSQLContainer pg) throws SQLException {
        return DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    }

    /**
     * Threads inserting one unkeyed row per autocommit transaction into the outbox until stopped, as a service's sends
     * do, without the library: the message ids committed, the longest insert, and the first error of each thread.
     */
    private static final class Writers {

        final Queue<UUID> written = new ConcurrentLinkedQueue<>();
        final Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicLong longest = new AtomicLong();
        private final AtomicBoolean stop = new AtomicBoolean();
        private final List<Thread> threads = new ArrayList<>();

        Writers(PostgreSQLContainer pg, String table, int shards, int n) {
            for (int i = 0; i < n; i++) {
                var t = new Thread(() -> {
                    try (var c = connect(pg); var ps = c.prepareStatement("insert into " + table
                            + " (shard, message_id, topic, payload) values (?, ?, 'w', ?)")) {
                        while (!stop.get()) {
                            var id = UUID.randomUUID();
                            ps.setShort(1, (short) ThreadLocalRandom.current().nextInt(shards));
                            ps.setObject(2, id);
                            ps.setBytes(3, "w".getBytes(UTF_8));
                            long start = System.nanoTime();
                            ps.executeUpdate();
                            longest.accumulateAndGet(System.nanoTime() - start, Math::max);
                            written.add(id);
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }, "writer-" + i);
                t.start();
                threads.add(t);
            }
        }

        void stop() throws InterruptedException {
            stop.set(true);
            for (var t : threads) {
                t.join(TimeUnit.SECONDS.toMillis(30));
            }
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
