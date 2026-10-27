package io.github.dsudomoin.kora.outbox;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.internal.BatchClassifier;
import io.github.dsudomoin.kora.outbox.internal.Cursors;
import io.github.dsudomoin.kora.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.kora.outbox.internal.Hashing;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.internal.PgOutbox;
import io.github.dsudomoin.kora.outbox.internal.TableNames;
import io.github.dsudomoin.kora.outbox.internal.Ticks;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * Torture test: concurrent writers over a small key space and three topics with random rollbacks, each
 * sending its outbox rows before touching the entity row, some unkeyed and some through {@code sendAll} over several
 * keys; three relay instances with maintenance every second; a flaky handler (transient and permanent failures,
 * requeues, simulated crashes mid-batch, two windows with the target down) whose permanent failures reach a
 * dead-letter hook that keeps, discards, redrives or redrives later at random; a long transaction open for the first
 * 20 s that sends on every shard right after taking its xid and again near its end. Replay a failure with the logged
 * seed: environment variable {@code TORTURE_SEED} (the writes, the permanent failures and the hook's decisions replay
 * exactly).
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

        var hook = new DecidingHook(seed);
        var handler = new FlakyHandler(seed, hook.redrives);
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
                    new Maintenance(db, names, config, new OutboxMetrics(null)),
                    new DeadLetterPass(db, names, config, hook, new OutboxMetrics(null)));
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
        drainEverything(names, hook);
        int droppedWhileRunning = partitions.dropped();
        relays.forEach(OutboxRelay::release);
        assertThat(new Maintenance(db, names, config, new OutboxMetrics(null)).runOnce()).isTrue();
        partitions.close();
        partitions.poll();

        try (var c = TestDb.connect()) {
            var order = commitOrder(c, commitOrder);
            Set<UUID> dead = dead(c, names.dead());
            var delivered = handler.first.keySet();
            var failed = handler.permanentAt.keySet(); // dead, kept, discarded or redriven
            var kept = hook.lastDecided(d -> d instanceof DeadLetterDecision.Keep);
            var discarded = hook.lastDecided(d -> d instanceof DeadLetterDecision.Discard);
            var redriven = minus(minus(hook.last.keySet(), kept), discarded); // last decided redrive or redriveAt
            var dropped = new HashSet<>(kept);
            dropped.addAll(discarded);
            long unkeyed = order.values().stream().filter(row -> row.key() == null).count();

            System.out.printf("TortureIT seed %d: committed %d (unkeyed %d), rolled back %d, delivered %d,"
                            + " duplicates %d, failed permanently %d, dead letters handed %d (last kept %d, discarded"
                            + " %d, redriven %d), requeue answers %d (first delivered or failed permanently after"
                            + " parking %d),"
                            + " transient failures %d, target-down failures %d, crashes %d, partitions dropped %d (while"
                            + " relaying %d), writers %.1f s, total %.1f s%n",
                    seed, committed.size(), unkeyed, rolledBack.size(), delivered.size(), handler.duplicates(),
                    failed.size(), hook.handed.get(), kept.size(), discarded.size(), redriven.size(),
                    handler.requeueAnswers.get(), handler.requeued.size(),
                    handler.transientFailures.get(), handler.downFailures.get(), handler.crashes.get(),
                    partitions.dropped(), droppedWhileRunning, (writersDone - started) / 1e9,
                    (System.nanoTime() - started) / 1e9);

            // differences, not whole sets: a failure must print only the offending ids
            assertThat(minus(order.keySet(), committed)).as("commit_order rows of transactions not seen committed")
                    .isEmpty();
            assertThat(minus(committed, order.keySet())).as("committed without commit_order rows").isEmpty();
            assertThat(both(committed, rolledBack)).as("both committed and rolled back").isEmpty();
            assertThat(minus(minus(committed, delivered), dropped))
                    .as("committed but neither delivered nor last decided keep or discard").isEmpty();
            assertThat(both(delivered, rolledBack)).as("rolled back but delivered").isEmpty();
            assertThat(minus(delivered, committed)).as("delivered but never committed").isEmpty();
            assertThat(kept).as("letters last decided keep").isNotEmpty();
            assertThat(discarded).as("letters last decided discard").isNotEmpty();
            assertThat(redriven).as("letters last decided redrive").isNotEmpty();
            assertThat(hook.redrivesAhead).as("letters whose redrives() exceeds the redrive decisions for them")
                    .isEmpty();
            assertThat(minus(failed, hook.last.keySet())).as("failed permanently but never handed to the hook")
                    .isEmpty();
            assertThat(minus(hook.last.keySet(), failed)).as("handed to the hook but never failed permanently")
                    .isEmpty();
            assertThat(minus(dead, kept)).as("dead but not last decided keep").isEmpty();
            assertThat(minus(kept, dead)).as("last decided keep but not dead").isEmpty();
            assertThat(both(dropped, delivered)).as("last decided keep or discard but delivered").isEmpty();
            assertThat(minus(redriven, delivered)).as("last decided redrive but not delivered").isEmpty();
            // a redriven message is a new row after the later messages of its key: out of the order check, as the dead
            assertThat(orderViolations(order, failed, handler.requeued, handler.passedRequeue, handler.first,
                    handler.permanentAt))
                    .as("per (topic, key) each message first delivered after every never-requeued one committed"
                            + " before it was first delivered or failed permanently; outbox ids in commit order of the"
                            + " never-failed")
                    .isEmpty();
            // a crash fails only its own call, as a whole: each record it delivered is delivered again, once per
            // such call; nothing else redelivers delivered work (the reliable phase does not crash), so a message
            // delivered more often means two steps delivered the same row
            assertThat(handler.deliveries.entrySet().stream()
                    .filter(e -> e.getValue() - 1 > handler.crashedDeliveries.getOrDefault(e.getKey(), 0))
                    .map(e -> e.getKey() + " delivered " + e.getValue() + " times, "
                            + handler.crashedDeliveries.getOrDefault(e.getKey(), 0) + " in crashed calls")
                    .toList())
                    .as("deliveries beyond the first that no crashed call explains").isEmpty();

            assertThat(droppedWhileRunning).as("partitions dropped while relaying").isPositive();
            assertThat(queryLong(c, "select count(*) from " + names.retry())).as("retry table").isZero();
            assertThat(queryLong(c, "select count(*) from " + names.parked())).as("parked table").isZero();
            long xmin = horizonXmin(c, schema);
            for (var range : PartitionDdl.listRanges(c, names.base())) {
                if (range.to() != null && Long.parseLong(range.to()) <= xmin) {
                    assertThat(queryLong(c, "select count(*) from " + range.name()))
                            .as("rows left in droppable partition %s (horizon xmin %d)", range, xmin)
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
     * Per (topic, key), in commit order ({@code commit_order.n}): the outbox ids of the never-dead never-requeued
     * messages rise; and for each pair A committed before B, A never requeued and B never dead, B was first delivered
     * after A's first delivery, or after A's first permanent failure when A ever died. {@code dead}: the messages that
     * ever failed permanently, redriven ones included (their redriven row is delivered after later ones).
     * {@code requeued}: the messages that parked before their first delivery or first permanent failure, which their
     * key may pass. {@code passed}: pairs [A, B] where the handler itself first delivered B after answering requeue
     * for A in a call that then crashed, which discards the answer, so A never parked. One line per diverging key.
     */
    private static List<String> orderViolations(Map<UUID, Row> order, Set<UUID> dead, Set<UUID> requeued,
            Set<List<UUID>> passed, Map<UUID, Delivery> first, Map<UUID, Long> permanentAt) {
        var byCommit = new HashMap<String, List<UUID>>(); // dead and requeued included
        order.values().stream()
                .filter(row -> row.key() != null)
                .sorted(Comparator.comparingInt(Row::n))
                .forEach(row -> byCommit.computeIfAbsent(row.route(), k -> new ArrayList<>()).add(row.messageId()));
        var violations = new ArrayList<String>();
        byCommit.forEach((route, all) -> {
            var committed = all.stream().filter(id -> !dead.contains(id) && !requeued.contains(id)).toList();
            for (int i = 1; i < committed.size(); i++) {
                if (first.get(committed.get(i)).outboxId() < first.get(committed.get(i - 1)).outboxId()) {
                    violations.add(route + ": outbox id below its predecessor's: "
                            + describe(committed.get(i - 1), order, first) + " then "
                            + describe(committed.get(i), order, first));
                    return;
                }
            }
            for (int j = 1; j < all.size(); j++) {
                var b = all.get(j);
                if (dead.contains(b)) {
                    continue;
                }
                long delivered = first.get(b).sequence();
                for (var a : all.subList(0, j)) {
                    if (requeued.contains(a)) {
                        continue;
                    }
                    boolean aDead = dead.contains(a);
                    long before = aDead ? permanentAt.getOrDefault(a, 0L) : first.get(a).sequence();
                    if (delivered < before && !passed.contains(List.of(a, b))) {
                        violations.add(route + ": " + describe(b, order, first) + " first delivered before "
                                + (aDead ? "the first permanent failure (#" + before + ") of " : "")
                                + describe(a, order, first) + ", committed before it");
                        return;
                    }
                }
            }
        });
        return violations;
    }

    private static String describe(UUID id, Map<UUID, Row> order, Map<UUID, Delivery> first) {
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
     * Drains until the retry and parked tables are empty (a drain stops at heads and parked rows not yet due) and the
     * dead letters are settled: none pending or scheduled before the drain, so every redriven row was there for its
     * tick, none handed to the hook during it (a redriven row that died again and was redriven anew comes after its
     * tick), none pending or scheduled after it. A drain that finds a shard stuck (a pause left by the flaky phase)
     * is retried within {@link #FINAL_DRAIN}.
     */
    private void drainEverything(TableNames names, DecidingHook hook) throws Exception {
        long deadline = System.nanoTime() + FINAL_DRAIN.toNanos();
        String reported = null;
        var undecided = "select count(*) from " + names.dead() + " where status in ('pending', 'scheduled')";
        while (true) {
            try {
                long handed = hook.handed.get(); // before the count: a letter handed after it counts as handed
                boolean settled;
                try (var c = TestDb.connect()) {
                    settled = queryLong(c, undecided) == 0;
                }
                relays.get(0).drain();
                try (var c = TestDb.connect()) {
                    if (settled && hook.handed.get() == handed
                            && queryLong(c, "select count(*) from " + names.retry()) == 0
                            && queryLong(c, "select count(*) from " + names.parked()) == 0
                            && queryLong(c, undecided) == 0) {
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

    /** The xmin of maintenance's horizon: the lowest effective tick of the busy shards, the latest when none. */
    private long horizonXmin(Connection c, OutboxSchema schema) throws Exception {
        var ticks = new Ticks(schema.names(), config.shards());
        long latest = ticks.latest(c).id();
        long tickId = Cursors.busyTick(new Cursors(schema.names(), config.shards()).positions(c, latest), latest);
        return ticks.get(c, tickId).snapshot().xmin();
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
     * A flaky {@code handle}: every record fails while the target is down, 3% transient failures otherwise, permanent
     * failures decided by the payload token (0.5%, the same on every redelivery and replay; a redriven message's by its
     * token and its redrive decisions so far, 25%, so some die again), and about every 200th call
     * crashes: {@code handle} fails with an {@link Error} at a random point (a Retry for that record, as the default
     * {@code handleBatch} catches it) and the call throws an {@link Error} once it has handed every record. A quarter
     * of the calls, and every call of the reliable phase, run the default {@code handleBatch}; the others run its loop
     * (per-key skip included) with requeues: about 3% of the records it would attempt are requeued instead, and after
     * each requeue the call winds down with even odds ({@code NotAttempted} for every later record, as a call cut at
     * its soft deadline answers). Each call draws from its own seeded random. First successes and first permanent
     * failures get numbers from one sequence.
     */
    private static final class FlakyHandler implements OutboxHandler {

        private final long seed;
        /** The hook's redrive decisions per message. */
        private final Map<UUID, Integer> redrives;
        private final AtomicLong calls = new AtomicLong();
        private final AtomicLong sequence = new AtomicLong();
        private final ThreadLocal<Call> current = new ThreadLocal<>();
        private final List<long[]> down = new CopyOnWriteArrayList<>();
        final Map<UUID, Delivery> first = new ConcurrentHashMap<>();
        final Map<UUID, Integer> deliveries = new ConcurrentHashMap<>();
        /** Deliveries in calls that then crashed: each is delivered again. */
        final Map<UUID, Integer> crashedDeliveries = new ConcurrentHashMap<>();
        final Map<UUID, Long> permanentAt = new ConcurrentHashMap<>();
        /**
         * The messages whose first delivery or first permanent failure, whichever came first, carried
         * {@code requeues() > 0}: they parked before it. A requeue answer the relay did not apply (its call crashed or
         * paused the shard) parks nothing.
         */
        final Set<UUID> requeued = ConcurrentHashMap.newKeySet();
        /** Pairs [M, R]: a call that then crashed first delivered R after answering requeue for M, of R's key. */
        final Set<List<UUID>> passedRequeue = ConcurrentHashMap.newKeySet();
        final AtomicInteger requeueAnswers = new AtomicInteger();
        final AtomicInteger transientFailures = new AtomicInteger();
        final AtomicInteger downFailures = new AtomicInteger();
        final AtomicInteger crashes = new AtomicInteger();
        volatile boolean reliable;

        FlakyHandler(long seed, Map<UUID, Integer> redrives) {
            this.seed = seed;
            this.redrives = redrives;
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
            boolean reliable = this.reliable;
            var call = new Call(random, !reliable && random.nextInt(200) == 0 ? random.nextInt(records.size() + 1) : -1);
            current.set(call);
            try {
                var outcomes = reliable || random.nextInt(4) == 0
                        ? OutboxHandler.super.handleBatch(records)
                        : attempt(records, random);
                if (call.crashAt >= 0) { // whether or not handle() reached the crash point
                    crashes.incrementAndGet();
                    throw new AssertionError("simulated crash mid-batch");
                }
                return outcomes;
            } finally {
                current.remove();
            }
        }

        /** {@link OutboxHandler#handleBatch}'s default loop, with the requeues and wind-downs (not reliable). */
        private List<Outcome> attempt(List<OutboxRecord> records, SplittableRandom random) {
            var outcomes = new ArrayList<Outcome>(records.size());
            var failed = new HashSet<Object>();
            var answeredRequeue = new HashMap<Object, List<UUID>>(); // per key
            boolean crashing = current.get().crashAt >= 0;
            boolean anyDelivered = false;
            boolean windDown = false;
            for (var record : records) {
                var key = BatchClassifier.orderingKey(record);
                if (windDown || failed.contains(key) || (!anyDelivered && failed.size() >= 3)
                        || OutboxHandler.windDownRequested()) {
                    outcomes.add(Outcome.notAttempted());
                    continue;
                }
                if (random.nextInt(100) < 3) {
                    requeueAnswers.incrementAndGet();
                    answeredRequeue.computeIfAbsent(key, k -> new ArrayList<>()).add(record.messageId());
                    outcomes.add(Outcome.requeue());
                    windDown = random.nextBoolean();
                    continue;
                }
                try {
                    // a message is in one call at a time: no other call delivers it before handle() below does
                    boolean firstDelivery = !first.containsKey(record.messageId());
                    handle(record);
                    outcomes.add(Outcome.delivered());
                    anyDelivered = true;
                    if (crashing && firstDelivery) {
                        for (var m : answeredRequeue.getOrDefault(key, List.of())) {
                            passedRequeue.add(List.of(m, record.messageId()));
                        }
                    }
                } catch (OutboxPermanentException e) {
                    outcomes.add(Outcome.permanent(e));
                } catch (Throwable e) { // an Error too, as in the default
                    outcomes.add(Outcome.retry(e));
                    failed.add(key);
                }
            }
            return outcomes;
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            var call = current.get();
            if (call.handled++ == call.crashAt) {
                throw new AssertionError("simulated crash point");
            }
            var id = record.messageId();
            if (!reliable && targetDown()) {
                downFailures.incrementAndGet();
                throw new Exception("target down");
            }
            if (isPermanent(record)) {
                if (permanentAt.putIfAbsent(id, sequence.incrementAndGet()) == null && record.requeues() > 0) {
                    requeued.add(id);
                }
                throw new OutboxPermanentException("poison " + id);
            }
            if (!reliable && call.random.nextInt(1000) < 30) {
                transientFailures.incrementAndGet();
                throw new Exception("flaky " + id);
            }
            // a redriven message's fresh row may park: its first permanent failure came first and decides
            if (first.putIfAbsent(id, new Delivery(sequence.incrementAndGet(), record.id())) == null
                    && record.requeues() > 0 && !permanentAt.containsKey(id)) {
                requeued.add(id);
            }
            deliveries.merge(id, 1, Integer::sum);
            if (call.crashAt >= 0) {
                crashedDeliveries.merge(id, 1, Integer::sum);
            }
        }

        private boolean targetDown() {
            long now = System.nanoTime();
            return down.stream().anyMatch(w -> now - w[0] >= 0 && now - w[1] < 0);
        }

        private boolean isPermanent(OutboxRecord record) {
            int redriven = redrives.getOrDefault(record.messageId(), 0);
            return new SplittableRandom(mix(seed, record.payload(), redriven)).nextInt(redriven == 0 ? 200 : 4) == 0;
        }

        long duplicates() {
            return deliveries.values().stream().mapToLong(n -> n - 1).sum();
        }
    }

    /** A seed from the test's seed, a payload token and a generation (0: the token's seed from before redrives). */
    private static long mix(long seed, byte[] payload, int generation) {
        long token = Long.parseLong(new String(payload, UTF_8));
        return seed ^ (token * 0x9E3779B97F4A7C15L) ^ (generation * 0xBF58476D1CE4E5B9L);
    }

    /**
     * The dead-letter hook: per letter, drawn from its payload token and {@code redrives()} (so a replay decides the
     * same), keep, discard, redrive or redrive at now + 100 ms, a quarter each. {@link #last} holds each message's last
     * decision (a letter may be handed again); {@link #redrives} its redrive decisions, by which {@link FlakyHandler}
     * decides whether a redriven message fails permanently again.
     */
    private static final class DecidingHook implements OutboxDeadLetterHandler {

        private final long seed;
        final Map<UUID, DeadLetterDecision> last = new ConcurrentHashMap<>();
        final Map<UUID, Integer> redrives = new ConcurrentHashMap<>();
        /** Letters handed, counted before deciding: one handed while a drain ran shows as a change. */
        final AtomicLong handed = new AtomicLong();
        /**
         * Letters whose {@code redrives()} exceeds the redrive decisions recorded for their message: every redrive here
         * is one, recorded before it commits, so the count may only over-count. Asserted at the end: a hook that threw
         * would only leave its letters pending.
         */
        final List<String> redrivesAhead = new CopyOnWriteArrayList<>();

        DecidingHook(long seed) {
            this.seed = seed;
        }

        @Override
        public List<DeadLetterDecision> handle(List<DeadLetter> letters) {
            var decisions = new ArrayList<DeadLetterDecision>(letters.size());
            for (var letter : letters) {
                handed.incrementAndGet();
                int decided = redrives.getOrDefault(letter.messageId(), 0);
                if (letter.redrives() > decided) {
                    redrivesAhead.add(letter.messageId() + " redrives " + letter.redrives() + ", decided " + decided);
                }
                // ~seed: a stream of its own, not the permanent failure's of the same token and generation
                var random = new SplittableRandom(mix(~seed, letter.payload(), letter.redrives()));
                var decision = switch (random.nextInt(4)) {
                    case 0 -> DeadLetterDecision.keep();
                    case 1 -> DeadLetterDecision.discard();
                    case 2 -> DeadLetterDecision.redrive();
                    default -> DeadLetterDecision.redriveAt(Instant.now().plusMillis(100));
                };
                if (!(decision instanceof DeadLetterDecision.Keep || decision instanceof DeadLetterDecision.Discard)) {
                    redrives.merge(letter.messageId(), 1, Integer::sum);
                }
                last.put(letter.messageId(), decision);
                decisions.add(decision);
            }
            return decisions;
        }

        Set<UUID> lastDecided(Predicate<DeadLetterDecision> which) {
            return last.entrySet().stream().filter(e -> which.test(e.getValue())).map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
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
