package io.github.dsudomoin.kora.outbox;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.internal.Cursors;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.Hashing;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.internal.PgOutbox;
import io.github.dsudomoin.kora.outbox.internal.Ticks;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * Torture test: concurrent writers over a small key space and three topics with random rollbacks, each
 * sending its outbox rows before touching the entity row, some unkeyed and some through {@code sendAll} over several
 * keys; three relay instances with maintenance every second; a flaky handler (transient and permanent failures,
 * simulated crashes mid-batch, two windows with the target down); a long transaction open for the first 20 s that
 * sends on every shard right after taking its xid and again near its end. Replay a failure with the logged seed:
 * environment variable {@code TORTURE_SEED} (the writes and the permanent failures replay exactly).
 */
@Tag("slow")
class TortureIT {

    private static final int WRITERS = 8;
    private static final int TRANSACTIONS = 1500;
    private static final List<String> TOPICS = List.of("a", "b", "c");
    private static final int KEYS = 30;
    private static final String LONG_TOPIC = "long";
    private static final Duration LONG_TRANSACTION = Duration.ofSeconds(20);
    /** The long transaction's second sends: this long before it commits. */
    private static final Duration LONG_TAIL = Duration.ofSeconds(2);
    /**
     * Target-down windows {from, length} after the long transaction took its xid: one amid the writes, one around the
     * long transaction's commit, so shards sit mid-batch (probing) while its rows become visible.
     */
    private static final List<Duration[]> DOWN = List.of(
            new Duration[] {Duration.ofSeconds(6), Duration.ofSeconds(2)},
            new Duration[] {Duration.ofSeconds(19), Duration.ofMillis(2500)});
    /** Writers burst {@code BURST} transactions, then pause, so writes still run when the long transaction ends. */
    private static final int BURST = 50;
    private static final Duration BURST_PAUSE = Duration.ofSeconds(1);
    private static final Duration FINAL_DRAIN = Duration.ofMinutes(2);

    private final long seed = seed();
    private final JdbcDatabase db = TestDb.newDatabase(40);
    private final TestOutboxConfig config = new TestOutboxConfig()
            .xidsPerPartition(5000)
            .premake(4)
            .shards(4)
            .workers(2)
            .batchSize(10) // a shard falling a few ticks behind continues its batch in chunks (target, lastId)
            .maintenanceInterval(Duration.ofSeconds(1));
    private final List<OutboxRelay> relays = new ArrayList<>();
    /** The writers and the long transaction; not the common pool, which may have a single thread. */
    private final ExecutorService threads = Executors.newFixedThreadPool(WRITERS + 1);
    private PartitionMonitor partitions;

    @AfterEach
    void release() throws Exception {
        threads.shutdownNow();
        if (partitions != null) {
            partitions.close();
        }
        relays.forEach(OutboxRelay::release);
        db.release();
    }

    @Test
    void invariantsHoldUnderConcurrencyCrashesAndALongTransaction() throws Exception {
        System.out.println("TortureIT seed " + seed + " (replay with TORTURE_SEED=" + seed + ")");
        long started = System.nanoTime();
        var schema = new OutboxSchema(db, config);
        schema.init();
        var names = schema.names();
        String commitOrder = names.base() + "_commit_order";
        String counter = names.base() + "_counter";
        var longKeys = keyPerShard(LONG_TOPIC);
        try (var c = TestDb.connect()) {
            // key and n are null for unkeyed messages
            execute(c, "create table " + commitOrder + " (message_id uuid primary key, topic text not null, key text,"
                    + " n int)");
            execute(c, "create table " + counter
                    + " (topic text, key text, n int not null default 0, primary key (topic, key))");
            execute(c, "insert into " + counter + " (topic, key) select t, 'k' || k from unnest(array['"
                    + String.join("','", TOPICS) + "']) t cross join generate_series(0, " + (KEYS - 1) + ") k");
            execute(c, "insert into " + counter + " (topic, key) select '" + LONG_TOPIC + "', unnest(array['"
                    + String.join("','", longKeys) + "'])");
        }
        partitions = new PartitionMonitor(names.base());
        partitions.start();

        var handler = new FlakyHandler(seed);
        Set<UUID> committed = ConcurrentHashMap.newKeySet();
        Set<UUID> rolledBack = ConcurrentHashMap.newKeySet();
        var holdsXid = new CompletableFuture<Long>();
        var silent = new PgOutbox(db, schema, config.shards(), () -> {}, new OutboxMetrics(null));
        var longTransaction = CompletableFuture.runAsync(
                () -> longTransaction(silent, longKeys, holdsXid, commitOrder, counter, committed), threads);
        long xidTaken = holdsXid.get(10, TimeUnit.SECONDS); // older than every write
        for (var window : DOWN) {
            handler.down(xidTaken + window[0].toNanos(), xidTaken + window[0].plus(window[1]).toNanos());
        }

        for (int i = 0; i < 3; i++) {
            var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null,
                    new Maintenance(db, names, config, new OutboxMetrics(null)));
            relays.add(relay);
            relay.init();
        }

        var writers = new ArrayList<CompletableFuture<Void>>();
        for (int w = 0; w < WRITERS; w++) {
            var outbox = new PgOutbox(db, schema, config.shards(), relays.get(w % relays.size())::wake,
                    new OutboxMetrics(null));
            var random = new SplittableRandom(seed + w);
            int writer = w;
            writers.add(CompletableFuture.runAsync(() -> {
                for (int tx = 1; tx <= TRANSACTIONS; tx++) {
                    write(outbox, writer, tx, random, commitOrder, counter, committed, rolledBack);
                    if (tx % BURST == 0) {
                        sleep(BURST_PAUSE);
                    }
                }
            }, threads));
        }
        CompletableFuture.allOf(writers.toArray(CompletableFuture[]::new)).get(150, TimeUnit.SECONDS);
        long writersDone = System.nanoTime();
        longTransaction.get(LONG_TRANSACTION.toSeconds() + 10, TimeUnit.SECONDS);

        handler.reliable = true;
        drainEverything(names.retry());
        int droppedWhileRunning = partitions.dropped();
        relays.forEach(OutboxRelay::release);
        assertThat(new Maintenance(db, names, config, new OutboxMetrics(null)).runOnce()).isTrue();
        partitions.close();
        partitions.poll();

        try (var c = TestDb.connect()) {
            var order = commitOrder(c, commitOrder);
            Set<UUID> dead = dead(c, names.dead());
            var delivered = handler.first.keySet();
            long unkeyed = order.values().stream().filter(row -> row.key() == null).count();

            System.out.printf("TortureIT seed %d: committed %d (unkeyed %d), rolled back %d, delivered %d,"
                            + " duplicates %d, dead %d, transient failures %d, target-down failures %d, crashes %d,"
                            + " partitions dropped %d (while relaying %d), writers %.1f s, total %.1f s%n",
                    seed, committed.size(), unkeyed, rolledBack.size(), delivered.size(), handler.duplicates(),
                    dead.size(), handler.transientFailures.get(), handler.downFailures.get(), handler.crashes.get(),
                    partitions.dropped(), droppedWhileRunning, (writersDone - started) / 1e9,
                    (System.nanoTime() - started) / 1e9);

            // differences, not whole sets: a failure must print only the offending ids
            assertThat(minus(order.keySet(), committed)).as("commit_order rows of transactions not seen committed")
                    .isEmpty();
            assertThat(minus(committed, order.keySet())).as("committed without commit_order rows").isEmpty();
            assertThat(both(committed, rolledBack)).as("both committed and rolled back").isEmpty();
            assertThat(minus(minus(committed, delivered), dead)).as("committed but neither delivered nor dead")
                    .isEmpty();
            assertThat(both(delivered, rolledBack)).as("rolled back but delivered").isEmpty();
            assertThat(minus(delivered, committed)).as("delivered but never committed").isEmpty();
            assertThat(minus(dead, handler.permanentAt.keySet())).as("dead but never failed permanently").isEmpty();
            assertThat(minus(handler.permanentAt.keySet(), dead)).as("failed permanently but not dead").isEmpty();
            assertThat(both(dead, delivered)).as("dead but delivered").isEmpty();
            assertThat(orderViolations(order, dead, handler.first, handler.permanentAt))
                    .as("per (topic, key) first deliveries in commit order (and outbox id order), none before a dead"
                            + " predecessor was attempted")
                    .isEmpty();
            // a crash rolls back its whole shard step, whose calls handed over at most batchSize retry heads and a
            // batchSize chunk, all redelivered; nothing else rolls back delivered work (the reliable phase does not
            // crash), so no slack: more duplicates mean two steps delivered the same rows
            assertThat(handler.duplicates()).as("duplicates for %d crashes", handler.crashes.get())
                    .isLessThanOrEqualTo(handler.crashes.get() * 2L * config.batchSize());

            assertThat(droppedWhileRunning).as("partitions dropped while relaying").isPositive();
            assertThat(queryLong(c, "select count(*) from " + names.retry())).as("retry table").isZero();
            long xmin = slowestCursorXmin(c, schema);
            for (var range : PartitionDdl.listRanges(c, names.base())) {
                if (range.to() != null && Long.parseLong(range.to()) <= xmin) {
                    assertThat(queryLong(c, "select count(*) from " + range.name()))
                            .as("rows left in droppable partition %s (slowest cursor xmin %d)", range, xmin)
                            .isZero();
                }
            }
        }
    }

    private static <T> Set<T> minus(Set<T> a, Set<T> b) {
        var result = new HashSet<>(a);
        result.removeAll(b);
        return result;
    }

    private static <T> Set<T> both(Set<T> a, Set<T> b) {
        var result = new HashSet<>(a);
        result.retainAll(b);
        return result;
    }

    /**
     * Per (topic, key): the committed non-dead messages in commit order ({@code commit_order.n}) against the order of
     * their first successful deliveries; their outbox ids rise in commit order; and none was first delivered before a
     * dead predecessor's first permanent failure. One line per diverging key.
     */
    private static List<String> orderViolations(Map<UUID, Row> order, Set<UUID> dead, Map<UUID, Delivery> first,
            Map<UUID, Long> permanentAt) {
        var byCommit = new HashMap<String, List<UUID>>(); // dead included
        var byDelivery = new HashMap<String, List<UUID>>();
        order.values().stream()
                .filter(row -> row.key() != null)
                .sorted(Comparator.comparingInt(Row::n))
                .forEach(row -> byCommit.computeIfAbsent(row.route(), k -> new ArrayList<>()).add(row.messageId()));
        first.entrySet().stream()
                .filter(e -> order.containsKey(e.getKey()) && order.get(e.getKey()).key() != null)
                .sorted(Comparator.comparingLong(e -> e.getValue().sequence()))
                .forEach(e -> byDelivery.computeIfAbsent(order.get(e.getKey()).route(), k -> new ArrayList<>())
                        .add(e.getKey()));
        var violations = new ArrayList<String>();
        byCommit.forEach((route, all) -> {
            var committed = all.stream().filter(id -> !dead.contains(id)).toList();
            var delivered = byDelivery.getOrDefault(route, List.of());
            for (int i = 0; i < Math.max(committed.size(), delivered.size()); i++) {
                var expected = i < committed.size() ? committed.get(i) : null;
                var actual = i < delivered.size() ? delivered.get(i) : null;
                if (expected == null || !expected.equals(actual)) {
                    violations.add(route + " at #" + i + ": commit order has " + describe(expected, order, first)
                            + ", first deliveries have " + describe(actual, order, first));
                    return;
                }
                if (i > 0 && first.get(expected).outboxId() < first.get(committed.get(i - 1)).outboxId()) {
                    violations.add(route + " at #" + i + ": outbox id below its predecessor's: "
                            + describe(committed.get(i - 1), order, first) + " then " + describe(expected, order, first));
                    return;
                }
            }
            UUID lastDead = null;
            long deadAttempted = 0;
            for (var id : all) {
                if (dead.contains(id)) {
                    lastDead = id;
                    deadAttempted = Math.max(deadAttempted, permanentAt.getOrDefault(id, 0L));
                } else if (first.get(id).sequence() < deadAttempted) {
                    violations.add(route + ": " + describe(id, order, first) + " delivered before its dead"
                            + " predecessor " + lastDead + " (n " + order.get(lastDead).n() + ") failed at #"
                            + deadAttempted);
                    return;
                }
            }
        });
        return violations;
    }

    private static String describe(UUID id, Map<UUID, Row> order, Map<UUID, Delivery> first) {
        if (id == null) {
            return "nothing";
        }
        var delivery = first.get(id);
        return id + " (n " + order.get(id).n() + (delivery == null ? ", never delivered)"
                : ", outbox id " + delivery.outboxId() + ", delivery #" + delivery.sequence() + ")");
    }

    /**
     * One writer transaction, 10% rolled back: 2–5 messages for one (topic, key) in 5%, {@code sendAll} of 2–5
     * messages over random keys (every fifth unkeyed) in 5%, one unkeyed message in 3%, otherwise one keyed message.
     * The outbox rows go first, then the counter rows, then the commit_order rows.
     */
    private void write(PgOutbox outbox, int writer, int tx, SplittableRandom random, String commitOrder,
            String counter, Set<UUID> committed, Set<UUID> rolledBack) {
        int kind = random.nextInt(100);
        boolean rollback = random.nextInt(100) < 10;
        boolean bulk = kind >= 5 && kind < 10;
        var messages = new ArrayList<OutboxMessage>();
        if (kind < 5) {
            var topic = TOPICS.get(random.nextInt(TOPICS.size()));
            var key = "k" + random.nextInt(KEYS);
            for (int i = 0, n = 2 + random.nextInt(4); i < n; i++) {
                messages.add(OutboxMessage.of(topic, key, payload(writer, tx, i)));
            }
        } else if (bulk) {
            for (int i = 0, n = 2 + random.nextInt(4); i < n; i++) {
                var topic = TOPICS.get(random.nextInt(TOPICS.size()));
                var key = "k" + random.nextInt(KEYS);
                messages.add(random.nextInt(5) == 0
                        ? OutboxMessage.of(topic, payload(writer, tx, i))
                        : OutboxMessage.of(topic, key, payload(writer, tx, i)));
            }
        } else if (kind < 13) {
            messages.add(OutboxMessage.of(TOPICS.get(random.nextInt(TOPICS.size())), payload(writer, tx, 0)));
        } else {
            var key = "k" + random.nextInt(KEYS);
            messages.add(OutboxMessage.of(TOPICS.get(random.nextInt(TOPICS.size())), key, payload(writer, tx, 0)));
        }
        var ids = new ArrayList<UUID>(messages.size());
        try {
            db.inTx((SqlFunction0<Void>) () -> {
                if (bulk) {
                    ids.addAll(outbox.sendAll(messages));
                } else {
                    for (var m : messages) {
                        ids.add(outbox.send(m));
                    }
                }
                record(db.currentConnection(), messages, ids, commitOrder, counter);
                if (rollback) {
                    throw new RolledBack();
                }
                return null;
            });
            committed.addAll(ids);
        } catch (RolledBack e) {
            rolledBack.addAll(ids);
        }
    }

    /**
     * Holds an xid for {@link #LONG_TRANSACTION}: sends one message per shard on {@link #LONG_TOPIC} right after taking
     * it and again {@link #LONG_TAIL} before committing (through {@code send(Connection, ...)}, so no wake-up), then
     * records them like a writer. Its rows sit in the oldest xid range and turn visible only at its commit.
     */
    private void longTransaction(PgOutbox outbox, List<String> keys, CompletableFuture<Long> holdsXid,
            String commitOrder, String counter, Set<UUID> committed) {
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            execute(c, "select pg_current_xact_id()");
            holdsXid.complete(System.nanoTime());
            var messages = new ArrayList<OutboxMessage>();
            var ids = new ArrayList<UUID>();
            for (int round = 0; round < 2; round++) {
                if (round == 1) {
                    execute(c, "select pg_sleep(" + LONG_TRANSACTION.minus(LONG_TAIL).toMillis() / 1000.0 + ")");
                }
                for (int k = 0; k < keys.size(); k++) {
                    var m = OutboxMessage.of(LONG_TOPIC, keys.get(k), payload(WRITERS, round, k));
                    messages.add(m);
                    ids.add(outbox.send(c, m));
                }
            }
            execute(c, "select pg_sleep(" + LONG_TAIL.toMillis() / 1000.0 + ")");
            record(c, messages, ids, commitOrder, counter);
            c.commit();
            committed.addAll(ids);
        } catch (Exception e) {
            holdsXid.completeExceptionally(e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Counter rows per (topic, key) (the per-key advisory lock taken by the send serialises their writers, so the value
     * orders their commits), then commit_order rows: per key consecutive {@code n} in send order, null for unkeyed.
     */
    private static void record(Connection c, List<OutboxMessage> messages, List<UUID> ids, String commitOrder,
            String counter) throws SQLException {
        var byRoute = new TreeMap<String, List<Integer>>();
        for (int i = 0; i < messages.size(); i++) {
            var m = messages.get(i);
            if (m.key() != null) {
                byRoute.computeIfAbsent(m.topic() + "/" + new String(m.key(), UTF_8), r -> new ArrayList<>()).add(i);
            }
        }
        var n = new Integer[messages.size()];
        try (var ps = c.prepareStatement("update " + counter + " set n = n + ? where topic = ? and key = ? returning n")) {
            for (var indexes : byRoute.values()) {
                var m = messages.get(indexes.get(0));
                ps.setInt(1, indexes.size());
                ps.setString(2, m.topic());
                ps.setString(3, new String(m.key(), UTF_8));
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    int last = rs.getInt(1);
                    for (int j = 0; j < indexes.size(); j++) {
                        n[indexes.get(j)] = last - indexes.size() + 1 + j;
                    }
                }
            }
        }
        try (var ps = c.prepareStatement("insert into " + commitOrder + " values (?, ?, ?, ?)")) {
            for (int i = 0; i < messages.size(); i++) {
                var m = messages.get(i);
                ps.setObject(1, ids.get(i));
                ps.setString(2, m.topic());
                ps.setString(3, m.key() == null ? null : new String(m.key(), UTF_8));
                ps.setObject(4, n[i]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** A token naming the message (writer, transaction, index), which decides its permanent failure on replay. */
    private static byte[] payload(int writer, int tx, int index) {
        return Long.toString(writer * 10_000_000L + tx * 100L + index).getBytes(UTF_8);
    }

    /** The first key {@code <topic>-<i>} of each shard. */
    private List<String> keyPerShard(String topic) {
        var keys = new String[config.shards()];
        int found = 0;
        for (int i = 0; found < keys.length; i++) {
            var key = topic + "-" + i;
            int shard = Hashing.shard(Hashing.keyHash(topic, key.getBytes(UTF_8)), config.shards());
            if (keys[shard] == null) {
                keys[shard] = key;
                found++;
            }
        }
        return List.of(keys);
    }

    /**
     * Drains until the retry table is empty: a drain stops at heads not yet due. A drain that finds a shard stuck (a
     * pause left by the flaky phase) is retried within {@link #FINAL_DRAIN}.
     */
    private void drainEverything(String retryTable) throws Exception {
        long deadline = System.nanoTime() + FINAL_DRAIN.toNanos();
        String reported = null;
        while (true) {
            try {
                relays.get(0).drain();
                try (var c = TestDb.connect()) {
                    if (queryLong(c, "select count(*) from " + retryTable) == 0) {
                        return;
                    }
                }
            } catch (IllegalStateException e) {
                if (!e.getMessage().equals(reported)) {
                    System.out.println("TortureIT: drain retried: " + e.getMessage());
                    reported = e.getMessage();
                }
            }
            assertThat(System.nanoTime()).as("drained within %s", FINAL_DRAIN).isLessThan(deadline);
            Thread.sleep(50);
        }
    }

    /** A commit_order row; {@code key} and {@code n} are null for unkeyed messages. */
    private record Row(UUID messageId, String topic, String key, Integer n) {
        String route() {
            return topic + "/" + key;
        }
    }

    private static Map<UUID, Row> commitOrder(Connection c, String table) throws Exception {
        var rows = new HashMap<UUID, Row>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select * from " + table)) {
            while (rs.next()) {
                var id = rs.getObject(1, UUID.class);
                rows.put(id, new Row(id, rs.getString(2), rs.getString(3), rs.getObject(4, Integer.class)));
            }
        }
        return rows;
    }

    private static Set<UUID> dead(Connection c, String table) throws Exception {
        var ids = new HashSet<UUID>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select message_id from " + table)) {
            while (rs.next()) {
                assertThat(ids.add(rs.getObject(1, UUID.class))).as("dead once").isTrue();
            }
        }
        return ids;
    }

    private long slowestCursorXmin(Connection c, OutboxSchema schema) throws Exception {
        long tickId = new Cursors(schema.names()).latestAll(c, config.shards()).stream()
                .mapToLong(Cursor::tickId).min().orElseThrow();
        return new Ticks(schema.names(), config.shards()).get(c, tickId).snapshot().xmin();
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static long seed() {
        var replay = System.getenv("TORTURE_SEED");
        return replay != null ? Long.parseLong(replay) : ThreadLocalRandom.current().nextLong();
    }

    private static final class RolledBack extends RuntimeException {
        RolledBack() {
            super("rolled back on purpose", null, false, false);
        }
    }

    /** Sequence of the first successful delivery of a message and its outbox id. */
    private record Delivery(long sequence, long outboxId) {}

    /**
     * Default {@code handleBatch} over a flaky {@code handle}: every record fails while the target is down, 3%
     * transient failures otherwise, permanent failures decided by the payload token (0.5%, the same on every
     * redelivery and replay), and about every 200th call crashes with an {@link Error} at a random point, after
     * delivering the records before it. Each call draws from its own seeded random. First successes and first
     * permanent failures get numbers from one sequence.
     */
    private static final class FlakyHandler implements OutboxHandler {

        private final long seed;
        private final AtomicLong calls = new AtomicLong();
        private final AtomicLong sequence = new AtomicLong();
        private final ThreadLocal<Call> current = new ThreadLocal<>();
        private final List<long[]> down = new CopyOnWriteArrayList<>();
        final Map<UUID, Delivery> first = new ConcurrentHashMap<>();
        final Map<UUID, Integer> deliveries = new ConcurrentHashMap<>();
        final Map<UUID, Long> permanentAt = new ConcurrentHashMap<>();
        final AtomicInteger transientFailures = new AtomicInteger();
        final AtomicInteger downFailures = new AtomicInteger();
        final AtomicInteger crashes = new AtomicInteger();
        volatile boolean reliable;

        FlakyHandler(long seed) {
            this.seed = seed;
        }

        private static final class Call {
            final SplittableRandom random;
            final int crashAt;
            int handled;

            Call(SplittableRandom random, int crashAt) {
                this.random = random;
                this.crashAt = crashAt;
            }
        }

        /** The target is down from {@code from} to {@code until} ({@code nanoTime}). */
        void down(long from, long until) {
            down.add(new long[] {from, until});
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            var random = new SplittableRandom(seed ^ (calls.incrementAndGet() * 0x9E3779B97F4A7C15L));
            var call = new Call(random, !reliable && random.nextInt(200) == 0 ? random.nextInt(records.size() + 1) : -1);
            current.set(call);
            try {
                var outcomes = OutboxHandler.super.handleBatch(records);
                if (call.crashAt >= 0) { // the crash point was not reached among the attempted records
                    throw crash();
                }
                return outcomes;
            } finally {
                current.remove();
            }
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            var call = current.get();
            if (call.handled++ == call.crashAt) {
                throw crash();
            }
            var id = record.messageId();
            if (!reliable && targetDown()) {
                downFailures.incrementAndGet();
                throw new Exception("target down");
            }
            if (isPermanent(record)) {
                permanentAt.putIfAbsent(id, sequence.incrementAndGet());
                throw new OutboxPermanentException("poison " + id);
            }
            if (!reliable && call.random.nextInt(1000) < 30) {
                transientFailures.incrementAndGet();
                throw new Exception("flaky " + id);
            }
            first.putIfAbsent(id, new Delivery(sequence.incrementAndGet(), record.id()));
            deliveries.merge(id, 1, Integer::sum);
        }

        private boolean targetDown() {
            long now = System.nanoTime();
            return down.stream().anyMatch(w -> now - w[0] >= 0 && now - w[1] < 0);
        }

        private boolean isPermanent(OutboxRecord record) {
            long token = Long.parseLong(new String(record.payload(), UTF_8));
            return new SplittableRandom(seed ^ (token * 0x9E3779B97F4A7C15L)).nextInt(200) == 0;
        }

        private AssertionError crash() {
            crashes.incrementAndGet();
            return new AssertionError("simulated crash mid-batch");
        }

        long duplicates() {
            return deliveries.values().stream().mapToLong(n -> n - 1).sum();
        }
    }

    /** Polls the range partitions of the outbox; {@link #dropped()} counts those seen once and gone now. */
    private static final class PartitionMonitor {

        private final String base;
        private final Set<String> seen = ConcurrentHashMap.newKeySet();
        private final Set<String> current = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean stopped = new AtomicBoolean();
        private final Thread thread = new Thread(this::run, "torture-partitions");

        PartitionMonitor(String base) {
            this.base = base;
        }

        void start() throws Exception {
            poll();
            thread.setDaemon(true);
            thread.start();
        }

        /** Stops polling; idempotent. */
        void close() throws InterruptedException {
            stopped.set(true);
            thread.join();
        }

        int dropped() {
            synchronized (this) {
                var gone = new HashSet<>(seen);
                gone.removeAll(current);
                return gone.size();
            }
        }

        private void run() {
            while (!stopped.get()) {
                try {
                    poll();
                    Thread.sleep(100);
                } catch (Exception e) {
                    if (stopped.get()) {
                        return;
                    }
                    System.out.println("TortureIT: partition poll failed: " + e);
                }
            }
        }

        void poll() throws Exception {
            try (var c = TestDb.connect()) {
                var ranges = PartitionDdl.listRanges(c, base).stream()
                        .filter(b -> b.to() != null)
                        .map(Bound::name)
                        .toList();
                synchronized (this) {
                    current.clear();
                    current.addAll(ranges);
                    seen.addAll(ranges);
                }
            }
        }
    }
}
