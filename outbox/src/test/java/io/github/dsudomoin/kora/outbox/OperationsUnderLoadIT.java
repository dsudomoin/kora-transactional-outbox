package io.github.dsudomoin.kora.outbox;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.internal.Cursors;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.internal.PgInbox;
import io.github.dsudomoin.kora.outbox.internal.PgOutbox;
import io.github.dsudomoin.kora.outbox.internal.PgOutboxAdmin;
import io.github.dsudomoin.kora.outbox.internal.TableNames;
import io.github.dsudomoin.kora.outbox.internal.Ticks;
import io.github.dsudomoin.kora.outbox.internal.UuidV7;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * The operations that put rows back, orphan restore and redrive, and the inbox re-attach, while {@value #WRITERS}
 * writers run for the whole test: outbox writers send one keyed message per transaction through {@link Outbox#send}
 * in {@code inTx}, keys over {@value #KEYS} values that every restored and redriven row shares, while a running relay
 * delivers and its maintenance runs every 50 ms; inbox writers record new ids and redeliver recorded ones through
 * {@link Inbox#tryAccept}, its maintenance running every 50 ms.
 */
class OperationsUnderLoadIT {

    private static final int WRITERS = 8;
    private static final int KEYS = 200;
    /** Three restore chunks. */
    private static final int ORPHAN_ROWS = 2500;
    private static final long ORPHAN_IDS = 1_000_000_000L;
    private static final Duration SWAP_LOCK_TIMEOUT = Duration.ofMillis(200);
    /**
     * A writer of a key a restore chunk holds waits for the chunk, which waits for each of its key locks at most
     * {@code swapLockTimeout}, as the tail swap may stall one; the margin covers the chunk's own work and a loaded
     * machine, as in {@link TailSwapIT}.
     */
    private static final Duration WRITER_BOUND = SWAP_LOCK_TIMEOUT.plusMillis(500);
    /** For every wait on the library, and every loop of calls. */
    private static final Duration BOUND = Duration.ofSeconds(60);
    private static final long HOUR = Duration.ofHours(1).toMillis();
    private static final String LOCK_NOT_AVAILABLE = "55P03";

    private final JdbcDatabase db = TestDb.newDatabase(30);
    // premake 16 ranges of 10 000 xids: the extension is due with 8 left, over 80 000 xids, so the seconds maintenance
    // is paused to make an orphan never let rows reach the tail
    private final TestOutboxConfig config = new TestOutboxConfig()
            .xidsPerPartition(10_000)
            .premake(16)
            .swapLockTimeout(SWAP_LOCK_TIMEOUT)
            .maintenanceInterval(Duration.ofMillis(50))
            .inboxPartitionInterval(Duration.ofHours(1))
            .inboxRetention(Duration.ofHours(2))
            .inboxPremake(Duration.ofHours(3));
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final RecordingHandler recording = new RecordingHandler();
    private final CapturedStderr log = new CapturedStderr();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final List<Thread> threads = new ArrayList<>();
    private final Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
    private final Queue<Sent> sent = new ConcurrentLinkedQueue<>();
    private final AtomicLong commits = new AtomicLong();
    private final AtomicLong maxLatency = new AtomicLong();
    private TableNames names;
    private OutboxRelay relay;
    private Maintenance maintenance;
    private PgInbox inbox;

    /** A writer's committed send: its message id, key, and {@link System#nanoTime()} when its {@code inTx} returned. */
    private record Sent(UUID id, String key, long at) {}

    /** A row put back into the outbox: its message id and key, null when unkeyed. */
    private record Row(UUID id, @Nullable String key) {}

    /** A standalone range {@link #fill} gave rows: those from {@code from} on, and the one below it. */
    private record Orphan(String table, long from, List<Row> rows, UUID below) {}

    @AfterEach
    void release() throws InterruptedException {
        stopLoad();
        if (relay != null) {
            relay.release();
        }
        if (inbox != null) {
            inbox.release();
        }
        db.release();
        log.close();
    }

    @Test
    void oursOrphanIsRestoredUnderLoad() throws Exception {
        start();
        Orphan orphan;
        long restoreStarted;
        try (var pause = TestDb.connect(); var c = TestDb.connect()) {
            maintenanceLock(pause, "pg_advisory_lock"); // after the run in progress, if any
            var range = relayedRange(c);
            // as dropProcessed marks and detaches a range, by a run killed before its drop
            execute(c, "insert into " + names.dropping() + " (table_name) values ('" + names.unqualified(range.name())
                    + "')");
            execute(c, "alter table " + names.base() + " detach partition " + range.name() + " concurrently");
            // far above the counter: the horizon, which the relay moves on meanwhile, stays below them
            orphan = fill(c, range, queryLong(c, "select pg_current_xact_id()::text::bigint") + 100_000_000L);
            restoreStarted = System.nanoTime();
            maintenanceLock(pause, "pg_advisory_unlock");
        }

        await(BOUND, () -> !exists(orphan.table()));
        // a chunk that met a key a writer held rolled back, and the next run went on
        int failedRuns = occurrences(log.text(), "could not move the rows of " + orphan.table());

        var delivered = finish("oursOrphanIsRestoredUnderLoad", (failedRuns + 1) + " maintenance runs restored "
                + orphan.rows().size() + " rows");

        assertThat(Duration.ofNanos(maxLatency.get())).as("the slowest writer transaction").isLessThan(WRITER_BOUND);
        assertPutBackInOrder(delivered, orphan.rows().stream().collect(Collectors.toMap(r -> r, r -> restoreStarted)));
        assertThat(delivered).as("below the horizon: gone with the table").doesNotContainKey(orphan.below());
    }

    @Test
    void foreignOrphanIsRestoredByTheAdminUnderLoad() throws Exception {
        start();
        var admin = new PgOutboxAdmin(db, names, config, maintenance, relay::wake);
        Orphan orphan;
        try (var pause = TestDb.connect(); var c = TestDb.connect()) {
            maintenanceLock(pause, "pg_advisory_lock");
            var range = relayedRange(c);
            execute(c, "alter table " + names.base() + " detach partition " + range.name() + " concurrently");
            // undelivered rows from its upper bound on, the cut the call is given; its own rows lie below it
            orphan = fill(c, range, Long.parseLong(range.to()));
            maintenanceLock(pause, "pg_advisory_unlock");
        }
        await(Duration.ofSeconds(10), () -> gauge("outbox.partitions.orphaned") == 1); // kept, left to the admin

        long restoreStarted = System.nanoTime();
        int calls = 0;
        long deadline = restoreStarted + BOUND.toNanos();
        while (exists(orphan.table())) {
            assertThat(System.nanoTime()).as("restored within %s, %d calls", BOUND, calls).isLessThan(deadline);
            calls++;
            try {
                admin.restoreOrphan(orphan.table(), orphan.from());
            } catch (RuntimeSqlException e) { // a chunk met a key a writer held: rolled back, the next call goes on
                assertThat(e.getCause()).isInstanceOfSatisfying(SQLException.class,
                        s -> assertThat(s.getSQLState()).as(s.toString()).isEqualTo(LOCK_NOT_AVAILABLE));
            }
        }

        var delivered = finish("foreignOrphanIsRestoredByTheAdminUnderLoad", calls + " restoreOrphan calls restored "
                + orphan.rows().size() + " rows");

        assertThat(Duration.ofNanos(maxLatency.get())).as("the slowest writer transaction").isLessThan(WRITER_BOUND);
        assertPutBackInOrder(delivered, orphan.rows().stream().collect(Collectors.toMap(r -> r, r -> restoreStarted)));
        assertThat(delivered).as("below the cut: gone with the table").doesNotContainKey(orphan.below());
    }

    @Test
    void inboxOrphanIsAttachedBackUnderLoad() throws Exception {
        inbox = new PgInbox(db, config, new OutboxMetrics(registry));
        inbox.init(); // its maintenance runs every 50 ms
        var table = TableNames.of(config.inbox().table()).base();
        long now = System.currentTimeMillis();
        var current = inboxRange(table, now);
        // the next hour's: inside the dedup window, ranges on both sides of it, and the one fresh ids reach last
        var detached = inboxRange(table, now + HOUR);
        var pools = List.of(Collections.synchronizedList(new ArrayList<UUID>()),
                Collections.synchronizedList(new ArrayList<UUID>()));
        var before = new ArrayList<UUID>();
        for (int i = 0; i < 1000; i++) {
            before.add(v7(detached));
        }
        assertThat(db.inTx((SqlFunction0<Set<UUID>>) () -> inbox.tryAcceptAll(before))).hasSize(before.size());
        pools.get(1).addAll(before);
        var reattached = new AtomicBoolean(); // until then no writer reaches the detached range: its inserts would fail
        for (int i = 0; i < WRITERS; i++) {
            run("inbox-writer-" + i, () -> {
                var random = ThreadLocalRandom.current();
                while (!stopping.get()) {
                    int range = reattached.get() && random.nextBoolean() ? 1 : 0;
                    var pool = pools.get(range);
                    boolean redelivered = random.nextBoolean() && !pool.isEmpty();
                    var id = redelivered ? pool.get(random.nextInt(pool.size())) : v7(range == 0 ? current : detached);
                    long started = System.nanoTime();
                    try {
                        boolean accepted = db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id));
                        maxLatency.accumulateAndGet(System.nanoTime() - started, Math::max);
                        commits.incrementAndGet();
                        if (accepted == redelivered) {
                            errors.add(new AssertionError((redelivered ? "a recorded id accepted again: "
                                    : "a new id taken as a duplicate: ") + id));
                        } else if (accepted) {
                            pool.add(id);
                        }
                    } catch (RuntimeException e) {
                        errors.add(e);
                    }
                }
            });
        }
        await(BOUND, () -> commits.get() >= 1000);

        try (var pause = TestDb.connect(); var c = TestDb.connect()) {
            var lock = "(" + TableNames.of(table).resolve(c).internalNamespace() + ", "
                    + TableNames.LOCK_INBOX_MAINTENANCE + ")";
            execute(pause, "select pg_advisory_lock" + lock); // after the run in progress, if any
            execute(c, "alter table " + table + " detach partition " + detached.name() + " concurrently");
            assertThat(PartitionDdl.listRanges(c, table)).doesNotContain(detached);
            long during = commits.get();
            await(BOUND, () -> commits.get() >= during + 1000); // ids recorded while it is detached
            execute(pause, "select pg_advisory_unlock" + lock);
        }
        await(BOUND, () -> inboxRanges(table).contains(detached));
        reattached.set(true);
        long attached = commits.get();
        await(BOUND, () -> commits.get() >= attached + 1000); // its ids recorded before, and new ones, under load
        stopLoad();
        System.out.printf("OperationsUnderLoadIT inboxOrphanIsAttachedBackUnderLoad: %d writer transactions, %d ids"
                + " recorded, the slowest %.1f ms%n", commits.get(), pools.get(0).size() + pools.get(1).size(),
                maxLatency.get() / 1e6);

        assertThat(errors).as("writer failures").isEmpty();
        assertThat(log.text()).contains(detached.name() + ", a range of " + table + " detached inside the dedup window,"
                + " is attached back");
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            for (var pool : pools) {
                for (int i = 0; i < pool.size(); i += 1000) {
                    var batch = pool.subList(i, Math.min(pool.size(), i + 1000));
                    assertThat(inbox.tryAcceptAll(c, batch)).as("recorded ids taken as new").isEmpty();
                }
            }
            c.rollback();
        }
        assertThat(gauge("outbox.inbox.partitions.orphaned")).isZero();
    }

    @Test
    void adminRedriveByFilterUnderLoad() throws Exception {
        start();
        var admin = new PgOutboxAdmin(db, names, config, maintenance, relay::wake);
        var letters = new HashMap<Long, Row>(); // dead letter id: what goes back
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.dead() + " (id, shard, message_id, topic, key, payload, created_at,"
                    + " attempts, error) select g, 0, gen_random_uuid(), 't', convert_to('k' || (g - 1), 'UTF8'),"
                    + " convert_to('dead' || g, 'UTF8'), now(), 3, 'boom' from generate_series(1, " + KEYS + ") g");
            try (var st = c.createStatement(); var rs = st.executeQuery("select id, message_id,"
                    + " convert_from(key, 'UTF8') from " + names.dead())) {
                while (rs.next()) {
                    letters.put(rs.getLong(1), new Row(rs.getObject(2, UUID.class), rs.getString(3)));
                }
            }
        }
        var filter = new DeadLetterFilter("t", null, null, null, null, null);
        var redriven = new HashMap<Row, Long>(); // when the call that redrove it began
        int calls = 0;
        long deadline = System.nanoTime() + BOUND.toNanos();
        while (redriven.size() < letters.size()) {
            assertThat(System.nanoTime()).as("redriven within %s, %d calls", BOUND, calls).isLessThan(deadline);
            calls++;
            var dead = deadIds();
            long started = System.nanoTime();
            long done;
            try {
                done = admin.redriveDeadLetters(filter); // skips letters whose key an open send holds
            } catch (RuntimeSqlException e) { // its insert waited for a tail swap: rolled back, the next call goes on
                assertThat(e.getCause()).isInstanceOfSatisfying(SQLException.class,
                        s -> assertThat(s.getSQLState()).as(s.toString()).isEqualTo(LOCK_NOT_AVAILABLE));
                done = 0;
            }
            dead.removeAll(deadIds());
            assertThat(dead).as("the letters call %d says it redrove", calls).hasSize((int) done);
            dead.forEach(id -> redriven.put(letters.get(id), started));
        }

        var delivered = finish("adminRedriveByFilterUnderLoad", calls + " redriveDeadLetters calls redrove "
                + redriven.size() + " letters");

        assertPutBackInOrder(delivered, redriven);
    }

    /** Bootstraps the outbox, starts the relay with its maintenance and the writers, and waits for 1000 commits. */
    private void start() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
        maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));
        relay = new OutboxRelay(db, config, schema, recording, new OutboxMetrics(null), null, maintenance);
        var outbox = new PgOutbox(db, schema, config.shards(), relay::wake, new OutboxMetrics(null));
        relay.init();
        for (int i = 0; i < WRITERS; i++) {
            run("writer-" + i, () -> {
                while (!stopping.get()) {
                    var key = "k" + ThreadLocalRandom.current().nextInt(KEYS);
                    long started = System.nanoTime();
                    try {
                        var id = db.inTx((SqlFunction0<UUID>) () ->
                                outbox.send(OutboxMessage.of("t", key, key.getBytes(UTF_8))));
                        long at = System.nanoTime();
                        maxLatency.accumulateAndGet(at - started, Math::max);
                        sent.add(new Sent(id, key, at));
                        commits.incrementAndGet();
                    } catch (RuntimeException e) {
                        errors.add(e);
                    }
                }
            });
        }
        await(BOUND, () -> commits.get() >= 1000);
    }

    /**
     * Stops the writers, checks that none failed and that a drain delivers every committed message; prints the slowest
     * writer transaction. Returns each delivered message id's first position in delivery order.
     */
    private Map<UUID, Integer> finish(String test, String operation) throws Exception {
        stopLoad();
        System.out.printf("OperationsUnderLoadIT %s: %s; %d writer commits, the slowest %.1f ms%n", test, operation,
                commits.get(), maxLatency.get() / 1e6);

        assertThat(errors).as("writer failures").isEmpty();
        relay.drain();
        var delivered = new HashMap<UUID, Integer>();
        var records = recording.delivered();
        for (int i = 0; i < records.size(); i++) {
            delivered.putIfAbsent(records.get(i).messageId(), i);
        }
        assertThat(sent.stream().map(Sent::id).filter(id -> !delivered.containsKey(id)).toList())
                .as("committed, never delivered").isEmpty();
        return delivered;
    }

    /**
     * Every row put back was delivered, and after every message of its key a writer committed before {@code since}
     * of it: a row put back is a new message.
     */
    private void assertPutBackInOrder(Map<UUID, Integer> delivered, Map<Row, Long> since) {
        assertThat(since.keySet().stream().map(Row::id).filter(id -> !delivered.containsKey(id)).toList())
                .as("put back, never delivered").isEmpty();
        var byKey = sent.stream().collect(Collectors.groupingBy(Sent::key));
        var early = new ArrayList<String>();
        var compared = new AtomicLong();
        since.forEach((row, at) -> {
            if (row.key() == null) {
                return;
            }
            int position = delivered.get(row.id());
            for (var s : byKey.getOrDefault(row.key(), List.of())) {
                if (s.at() >= at) {
                    continue;
                }
                compared.incrementAndGet();
                if (delivered.get(s.id()) > position) {
                    early.add(row + " at " + position + ", before " + s.id() + " at " + delivered.get(s.id())
                            + ", committed " + TimeUnit.NANOSECONDS.toMillis(at - s.at()) + " ms before the call");
                }
            }
        });
        assertThat(compared).as("writer messages committed earlier on the keys put back").hasPositiveValue();
        assertThat(early).as("delivered before a message of its key committed earlier").isEmpty();
    }

    /** The lowest range at or below the slowest shard's horizon, once there is one: maintenance must be paused. */
    private Bound relayedRange(Connection c) throws Exception {
        long deadline = System.nanoTime() + BOUND.toNanos();
        while (true) {
            long tick = new Cursors(names).latestAll(c, config.shards()).stream().mapToLong(Cursor::tickId).min()
                    .orElseThrow();
            long horizon = new Ticks(names, config.shards()).get(c, tick).snapshot().xmin();
            var range = PartitionDdl.listRanges(c, names.base()).stream()
                    .filter(b -> b.to() != null && Long.parseLong(b.to()) <= horizon)
                    .min(Comparator.comparingLong(b -> Long.parseLong(b.from())));
            if (range.isPresent()) {
                return range.get();
            }
            assertThat(System.nanoTime()).as("the relay passed a range within %s", BOUND).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    /**
     * Gives the standalone {@code range} one row below {@code from} and {@value #ORPHAN_ROWS} from it on, which the
     * relay never saw: on a stale shard, over the writers' keys and some unkeyed, as {@code MaintenanceIT} makes them.
     */
    private Orphan fill(Connection c, Bound range, long from) throws SQLException {
        // DETACH CONCURRENTLY keeps the bounds as a CHECK; the plain DETACH of MaintenanceIT's orphans leaves none
        var checks = new ArrayList<String>();
        try (var ps = c.prepareStatement("select conname from pg_constraint where conrelid = to_regclass(?)"
                + " and contype = 'c'")) {
            ps.setString(1, range.name());
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    checks.add(rs.getString(1));
                }
            }
        }
        for (var check : checks) {
            execute(c, "alter table " + range.name() + " drop constraint " + check);
        }
        execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic) values (" + ORPHAN_IDS + ", 0,"
                + " '" + (Long.parseLong(range.from()) + 1) + "', gen_random_uuid(), 't')");
        execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic, key, payload)"
                + " select " + ORPHAN_IDS + " + s, 999, (" + from + " + s % 7)::text::xid8, gen_random_uuid(), 't',"
                + " case when s % 10 > 0 then convert_to('k' || s % " + KEYS + ", 'UTF8') end, convert_to('p' || s,"
                + " 'UTF8') from generate_series(1, " + ORPHAN_ROWS + ") s");
        var rows = new ArrayList<Row>();
        UUID below = null;
        try (var st = c.createStatement(); var rs = st.executeQuery("select message_id, convert_from(key, 'UTF8'),"
                + " xid >= '" + from + "'::xid8 from " + range.name() + " where id >= " + ORPHAN_IDS)) {
            while (rs.next()) {
                if (rs.getBoolean(3)) {
                    rows.add(new Row(rs.getObject(1, UUID.class), rs.getString(2)));
                } else {
                    below = rs.getObject(1, UUID.class);
                }
            }
        }
        assertThat(rows).hasSize(ORPHAN_ROWS);
        return new Orphan(range.name(), from, rows, below);
    }

    private void stopLoad() throws InterruptedException {
        stopping.set(true);
        for (var t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    private interface Body {
        void run() throws Exception;
    }

    private void run(String name, Body body) {
        var t = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                errors.add(e);
            }
        }, name);
        t.setDaemon(true);
        threads.add(t);
        t.start();
    }

    /** {@code function(internalNs, LOCK_MAINTENANCE)}: maintenance runs skip while this session holds it. */
    private void maintenanceLock(Connection c, String function) throws SQLException {
        execute(c, "select " + function + "(" + names.internalNamespace() + ", " + TableNames.LOCK_MAINTENANCE + ")");
    }

    private Set<Long> deadIds() throws SQLException {
        var ids = new HashSet<Long>();
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select id from " + names.dead())) {
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
        }
        return ids;
    }

    private static List<Bound> inboxRanges(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return PartitionDdl.listRanges(c, table);
        }
    }

    private static Bound inboxRange(String table, long millis) throws SQLException {
        return inboxRanges(table).stream()
                .filter(b -> millis(b.from()) <= millis && millis < millis(b.to()))
                .findFirst()
                .orElseThrow();
    }

    private static long millis(String bound) {
        return UuidV7.timestampMillis(UUID.fromString(bound));
    }

    /** A v7 id of a random time in {@code range}. */
    private static UUID v7(Bound range) {
        var r = ThreadLocalRandom.current();
        long from = millis(range.from());
        long millis = from + r.nextLong(millis(range.to()) - from);
        return new UUID((millis << 16) | 0x7000L | (r.nextLong() & 0x0FFFL),
                (r.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L);
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select (to_regclass(?) is not null)::text", table).equals("true");
        }
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    private static int occurrences(String text, String part) {
        return text.split(Pattern.quote(part), -1).length - 1;
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
