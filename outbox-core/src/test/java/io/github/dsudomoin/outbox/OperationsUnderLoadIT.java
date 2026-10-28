package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.Cursors;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OrphanRestore;
import io.github.dsudomoin.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PartitionDdl;
import io.github.dsudomoin.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.outbox.internal.PgInbox;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.internal.PgOutboxAdmin;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.internal.Ticks;
import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
 * {@link Inbox#tryAccept}, its maintenance running every 50 ms. In the outbox tests a business transaction sends
 * {@value #HELD} and stays open across the operation ({@link #holdKey}), so the operation meets a key held for longer
 * than {@code swapLockTimeout}.
 */
class OperationsUnderLoadIT {

    private static final int WRITERS = 8;
    private static final int KEYS = 200;
    /** The key the holder keeps; no row of a restore's first chunk has it, so its rows lie in later ones. */
    private static final String HELD = "k7";
    /** Three restore chunks, the first without {@link #HELD}. */
    private static final int ORPHAN_ROWS = 2500;
    private static final int CHUNK = OrphanRestore.CHUNK;
    private static final long ORPHAN_IDS = 1_000_000_000L;
    private static final Duration SWAP_LOCK_TIMEOUT = Duration.ofMillis(200);
    /**
     * A writer of a key a restore chunk holds waits for the chunk, which waits for no key lock (it leaves a held key's
     * rows) and for any other lock at most {@code swapLockTimeout}, as the tail swap may stall one; the margin covers
     * the chunk's own work and a loaded machine, as in {@link TailSwapIT}.
     */
    private static final Duration WRITER_BOUND = SWAP_LOCK_TIMEOUT.plusMillis(500);
    /** A restore that meets {@link #HELD} moves the other rows and gives up within this: it waits for no key lock. */
    private static final Duration FIRST_FAILURE_CAP = Duration.ofSeconds(1);
    /** For every other wait on the library, and every loop of calls. */
    private static final Duration BOUND = Duration.ofSeconds(60);
    private static final long HOUR = Duration.ofHours(1).toMillis();
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String NO_PARTITION = "23514";

    private final JdbcDatabase db = TestDb.newDatabase(30);
    // ranges of 30 000 xids: the relay lags the counter by far less, so the range an orphan is made of is the only one
    // relayed below the pinned horizon (relayedRange); premake 16: the extension is due with 8 left, 240 000 xids, so
    // the seconds maintenance is paused never let rows reach the tail
    private final TestOutboxConfig config = new TestOutboxConfig()
            .xidsPerPartition(30_000)
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
    /** Ends the holder's transaction. */
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicReference<Sent> holderSent = new AtomicReference<>();
    /** Writers do not send {@link #HELD} meanwhile: they would queue behind the holder. */
    private volatile boolean skipHeld;
    /** Writers' sends of {@link #HELD} in progress, counted before they read {@link #skipHeld}. */
    private final AtomicInteger heldSends = new AtomicInteger();
    private TableNames names;
    private PgOutbox outbox;
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
        release.countDown();
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
        long firstFailure;
        try (var pause = TestDb.connect(); var c = TestDb.connect(); var pin = TestDb.connect()) {
            maintenanceLock(pause, "pg_advisory_lock"); // after the run in progress, if any
            var range = relayedRange(c, pin);
            // as dropProcessed marks and detaches a range, by a run killed before its drop
            execute(c, "insert into " + names.dropping() + " (table_name) values ('" + names.unqualified(range.name())
                    + "')");
            execute(c, "alter table " + names.base() + " detach partition " + range.name() + " concurrently");
            // far above the counter: the horizon stays below them
            orphan = fill(c, range, queryLong(c, "select pg_current_xact_id()::text::bigint") + 100_000_000L);
            var holder = holdKey();
            restoreStarted = System.nanoTime();
            maintenanceLock(pause, "pg_advisory_unlock");

            // a run moves every row but those of keys an open send holds, the holder's and maybe a writer's, and
            // gives up; the next runs move the writers' ones, so only the holder's stay
            var failed = failedRuns(orphan);
            await(FIRST_FAILURE_CAP, "a maintenance run giving up on " + HELD,
                    () -> failed.matcher(log.text()).results().findAny().isPresent());
            firstFailure = System.nanoTime() - restoreStarted;
            await(BOUND, "only the rows of " + HELD + " left", () -> remaining(orphan) == heldRows(orphan));
            release.countDown();
            holder.join(BOUND.toMillis());
            skipHeld = false;
            await(BOUND, "the orphan restored", () -> !exists(orphan.table()));
            assertEachOnceInTheOutbox(orphan);
        }
        var failed = failedRuns(orphan).matcher(log.text()).results().map(m -> m.group(1)).toList();
        assertThat(failed).as("the runs that gave up").isNotEmpty().containsOnly(LOCK_NOT_AVAILABLE);
        assertThat(occurrences(log.text(), "could not move the rows of " + orphan.table())).as("every failed run")
                .isEqualTo(failed.size());
        await(BOUND, "the orphan gauge back to 0", () -> gauge("outbox.partitions.orphaned") == 0);
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + names.dropping() + " where table_name = '"
                    + names.unqualified(orphan.table()) + "'")).as("its <base>_dropping marker").isZero();
        }

        var delivered = finish("oursOrphanIsRestoredUnderLoad", failed.size() + " maintenance runs gave up, the first "
                + TimeUnit.NANOSECONDS.toMillis(firstFailure) + " ms in, leaving the rows of held keys, then one"
                + " restored the rest of " + orphan.rows().size() + " rows");

        assertThat(Duration.ofNanos(maxLatency.get())).as("the slowest writer transaction").isLessThan(WRITER_BOUND);
        assertPutBackInOrder(delivered, orphan.rows().stream().collect(Collectors.toMap(r -> r, r -> restoreStarted)));
        assertHeldRowsAfterTheHolder(delivered, orphan);
        assertThat(delivered).as("below the horizon: gone with the table").doesNotContainKey(orphan.below());
    }

    @Test
    void foreignOrphanIsRestoredByTheAdminUnderLoad() throws Exception {
        start();
        var admin = new PgOutboxAdmin(db, names, config, maintenance, relay::wake);
        Orphan orphan;
        long restoreStarted;
        long firstFailure = 0;
        int calls = 0;
        int failedCalls = 0;
        try (var pause = TestDb.connect(); var c = TestDb.connect(); var pin = TestDb.connect()) {
            maintenanceLock(pause, "pg_advisory_lock");
            var range = relayedRange(c, pin);
            execute(c, "alter table " + names.base() + " detach partition " + range.name() + " concurrently");
            // its own rows fill its bounds and were delivered, so no cut inside them parts delivered from undelivered:
            // the undelivered rows sit from its upper bound on, the fromXid the call is given, and the cut re-sends
            // exactly them; an operator with a real foreign orphan finds the cut by comparing with what was delivered
            orphan = fill(c, range, Long.parseLong(range.to()));
            maintenanceLock(pause, "pg_advisory_unlock");
            await(BOUND, "maintenance keeping it", () -> gauge("outbox.partitions.orphaned") == 1);

            var holder = holdKey();
            restoreStarted = System.nanoTime();
            // a call moves every row but those of keys an open send holds, the holder's and maybe a writer's, and gives
            // up: call until only the holder's stay
            long held = heldRows(orphan);
            while (true) {
                calls++;
                var call = CompletableFuture.supplyAsync(() -> admin.restoreOrphan(orphan.table(), orphan.from()));
                Throwable failure = null;
                try {
                    call.get(FIRST_FAILURE_CAP.toNanos(), TimeUnit.NANOSECONDS);
                } catch (ExecutionException e) {
                    failure = e.getCause();
                } catch (TimeoutException e) {
                    throw new AssertionError("restoreOrphan did not give up on " + HELD + " within "
                            + FIRST_FAILURE_CAP + ", call " + calls, e);
                }
                if (calls == 1) {
                    firstFailure = System.nanoTime() - restoreStarted;
                }
                assertThat(failure).as("call %d, while %s is held", calls, HELD).isInstanceOfSatisfying(
                        RuntimeSqlException.class, OperationsUnderLoadIT::assertLockNotAvailable);
                failedCalls++;
                long left = remaining(orphan);
                assertThat(left).as("rows left after call %d", calls).isGreaterThanOrEqualTo(held);
                if (left == held) {
                    break;
                }
                assertThat(System.nanoTime() - restoreStarted).as("calls within %s", BOUND).isLessThan(BOUND.toNanos());
            }
            release.countDown();
            holder.join(BOUND.toMillis());
            skipHeld = false;
            long deadline = System.nanoTime() + BOUND.toNanos();
            while (exists(orphan.table())) {
                assertThat(System.nanoTime()).as("restored within %s, %d calls", BOUND, calls).isLessThan(deadline);
                calls++;
                try {
                    admin.restoreOrphan(orphan.table(), orphan.from());
                } catch (RuntimeSqlException e) { // a chunk met a key a writer held: rolled back, the next call goes on
                    assertLockNotAvailable(e);
                    failedCalls++;
                }
            }
            assertEachOnceInTheOutbox(orphan);
        }
        await(BOUND, "the orphan gauge back to 0", () -> gauge("outbox.partitions.orphaned") == 0);

        var delivered = finish("foreignOrphanIsRestoredByTheAdminUnderLoad", calls + " restoreOrphan calls, "
                + failedCalls + " gave up (the first " + TimeUnit.NANOSECONDS.toMillis(firstFailure)
                + " ms in), restored " + orphan.rows().size() + " rows");

        assertThat(Duration.ofNanos(maxLatency.get())).as("the slowest writer transaction").isLessThan(WRITER_BOUND);
        assertPutBackInOrder(delivered, orphan.rows().stream().collect(Collectors.toMap(r -> r, r -> restoreStarted)));
        assertHeldRowsAfterTheHolder(delivered, orphan);
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
        var detachIssued = new AtomicLong(Long.MAX_VALUE); // just before the detach statement
        var attachedSeen = new AtomicLong(Long.MAX_VALUE); // when the test saw it attached again
        var tolerated = new AtomicLong();
        for (int i = 0; i < WRITERS; i++) {
            run("inbox-writer-" + i, () -> {
                var random = ThreadLocalRandom.current();
                while (!stopping.get()) {
                    int range = random.nextInt(2);
                    var pool = pools.get(range);
                    boolean redelivered = random.nextBoolean() && !pool.isEmpty();
                    var id = redelivered ? pool.get(random.nextInt(pool.size())) : v7(range == 0 ? current : detached);
                    long started = System.nanoTime();
                    var issued = new long[1];
                    try {
                        boolean accepted = db.inTx((SqlFunction0<Boolean>) () -> {
                            issued[0] = System.nanoTime(); // its insert, which the detach makes fail
                            return inbox.tryAccept(id);
                        });
                        commits.incrementAndGet();
                        if (accepted == redelivered) {
                            errors.add(new AssertionError((redelivered ? "a recorded id accepted again: "
                                    : "a new id taken as a duplicate: ") + id));
                        } else if (accepted) {
                            pool.add(id);
                        }
                    } catch (RuntimeSqlException e) {
                        // no partition takes an id of the detached range (documented) until the attach
                        if (range == 1 && issued[0] >= detachIssued.get() && issued[0] < attachedSeen.get()
                                && e.getCause() instanceof SQLException s && NO_PARTITION.equals(s.getSQLState())) {
                            tolerated.incrementAndGet();
                        } else {
                            errors.add(e);
                        }
                    } catch (RuntimeException e) {
                        errors.add(e);
                    } finally {
                        maxLatency.accumulateAndGet(System.nanoTime() - started, Math::max);
                    }
                }
            });
        }
        await(BOUND, "inbox load", () -> commits.get() >= 1000);

        try (var pause = TestDb.connect(); var c = TestDb.connect()) {
            var lock = "(" + TableNames.of(table).resolve(c).internalNamespace() + ", "
                    + TableNames.LOCK_INBOX_MAINTENANCE + ")";
            execute(pause, "select pg_advisory_lock" + lock); // after the run in progress, if any
            detachIssued.set(System.nanoTime());
            execute(c, "alter table " + table + " detach partition " + detached.name() + " concurrently");
            assertThat(PartitionDdl.listRanges(c, table)).doesNotContain(detached);
            long during = commits.get();
            await(BOUND, "ids recorded while it is detached", () -> commits.get() >= during + 1000);
            execute(pause, "select pg_advisory_unlock" + lock);
        }
        await(BOUND, "the range attached back", () -> inboxRanges(table).contains(detached));
        attachedSeen.set(System.nanoTime());
        long attached = commits.get();
        await(BOUND, "load after the attach", () -> commits.get() >= attached + 1000);
        stopLoad();
        System.out.printf("OperationsUnderLoadIT inboxOrphanIsAttachedBackUnderLoad: %d writer commits, %d ids"
                + " recorded, %d inserts into the detached range failed (23514) until the attach, the slowest %.1f"
                + " ms%n", commits.get(), pools.get(0).size() + pools.get(1).size(), tolerated.get(),
                maxLatency.get() / 1e6);

        assertThat(errors).as("writer failures").isEmpty();
        assertThat(tolerated).as("writers that met the detached range").hasPositiveValue();
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
        long heldLetter = letters.entrySet().stream().filter(e -> e.getValue().key().equals(HELD)).findFirst()
                .orElseThrow().getKey();
        var filter = new DeadLetterFilter("t", null, null, null, null, null);
        var redriven = new HashMap<Row, Long>(); // when the call that redrove it began
        var returned = new ArrayList<Long>();

        var holder = holdKey();
        // writers hold keys for their own sends too, so a call skips those as well: the next call takes them
        long deadline = System.nanoTime() + BOUND.toNanos();
        while (deadIds().size() > 1) {
            assertThat(System.nanoTime()).as("redriven within %s: %s", BOUND, returned).isLessThan(deadline);
            returned.add(redrive(admin, filter, letters, redriven));
        }
        assertThat(deadIds()).as("the letter of the held key, skipped").containsExactly(heldLetter);
        assertThat(redriven).as("redriven while " + HELD + " is held, never waiting for it").hasSize(KEYS - 1);
        release.countDown();
        holder.join(BOUND.toMillis());
        assertThat(redrive(admin, filter, letters, redriven)).as("once the holder committed").isOne();
        skipHeld = false;

        var delivered = finish("adminRedriveByFilterUnderLoad", "redriveDeadLetters returned " + returned + " while "
                + HELD + " was held, then 1");

        assertPutBackInOrder(delivered, redriven); // the held letter: after the holder's message
    }

    /**
     * One {@code redriveDeadLetters(filter)} call; records when it began for each letter it redrove, checked against
     * the dead table. Returns what it returned, 0 when its insert waited for a tail swap and rolled back (55P03).
     */
    private long redrive(PgOutboxAdmin admin, DeadLetterFilter filter, Map<Long, Row> letters,
            Map<Row, Long> redriven) throws SQLException {
        var dead = deadIds();
        long started = System.nanoTime();
        long done;
        try {
            done = admin.redriveDeadLetters(filter);
        } catch (RuntimeSqlException e) {
            assertLockNotAvailable(e);
            done = 0;
        }
        dead.removeAll(deadIds());
        assertThat(dead).as("the letters the call says it redrove").hasSize((int) done);
        dead.forEach(id -> redriven.put(letters.get(id), started));
        return done;
    }

    /** Bootstraps the outbox, starts the relay with its maintenance and the writers, and waits for 1000 commits. */
    private void start() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
        maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));
        relay = new OutboxRelay(db, config, schema, recording, new OutboxMetrics(null), null, maintenance,
                new DeadLetterPass(db, names, config, null, new OutboxMetrics(null)));
        outbox = new PgOutbox(db, schema, config.shards(), relay::wake, new OutboxMetrics(null));
        relay.init();
        for (int i = 0; i < WRITERS; i++) {
            run("writer-" + i, () -> {
                while (!stopping.get()) {
                    var key = "k" + ThreadLocalRandom.current().nextInt(KEYS);
                    boolean held = key.equals(HELD);
                    if (held) {
                        heldSends.incrementAndGet(); // first: holdKey sees this send, or this writer sees skipHeld
                        if (skipHeld) {
                            heldSends.decrementAndGet();
                            continue;
                        }
                    }
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
                    } finally {
                        if (held) {
                            heldSends.decrementAndGet();
                        }
                    }
                }
            });
        }
        await(BOUND, "outbox load", () -> commits.get() >= 1000);
    }

    /**
     * Sends {@link #HELD} in a transaction that stays open until {@link #release}, as a business transaction that sent
     * the key and goes on working; writers skip the key from now on, and it starts once none is sending it. Returns
     * once it holds the key lock. Its {@link Sent} is recorded after its commit; its wait is no writer's latency.
     */
    private Thread holdKey() throws Exception {
        skipHeld = true;
        await(BOUND, "writers done with their sends of " + HELD, () -> heldSends.get() == 0);
        var holding = new CountDownLatch(1);
        var holder = run("key-holder", () -> {
            var id = db.inTx((SqlFunction0<UUID>) () -> {
                var sentId = outbox.send(OutboxMessage.of("t", HELD, HELD.getBytes(UTF_8)));
                holding.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return sentId;
            });
            var held = new Sent(id, HELD, System.nanoTime());
            holderSent.set(held);
            sent.add(held);
        });
        assertThat(holding.await(BOUND.toMillis(), TimeUnit.MILLISECONDS)).as("the holder sent " + HELD).isTrue();
        return holder;
    }

    /**
     * Stops the writers, checks that none failed and that a drain delivers every committed message, per key first
     * deliveries in rising outbox id (writers' and put-back rows together); prints the slowest writer transaction.
     * Returns each delivered message id's first position in delivery order.
     */
    private Map<UUID, Integer> finish(String test, String operation) throws Exception {
        stopLoad();
        System.out.printf("OperationsUnderLoadIT %s: %s; %d writer commits, the slowest %.1f ms%n", test, operation,
                commits.get(), maxLatency.get() / 1e6);

        assertThat(errors).as("writer failures").isEmpty();
        relay.drain();
        var delivered = new HashMap<UUID, Integer>();
        var lastId = new HashMap<String, Long>();
        var unordered = new ArrayList<String>();
        var records = recording.delivered();
        for (int i = 0; i < records.size(); i++) {
            var r = records.get(i);
            if (delivered.putIfAbsent(r.messageId(), i) == null && r.key() != null) {
                var key = new String(r.key(), UTF_8);
                var last = lastId.put(key, r.id());
                if (last != null && last >= r.id()) {
                    unordered.add(key + ": id " + last + ", then id " + r.id() + " at " + i);
                }
            }
        }
        assertThat(sent.stream().map(Sent::id).filter(id -> !delivered.containsKey(id)).toList())
                .as("committed, never delivered").isEmpty();
        assertThat(unordered).as("per key, first deliveries in rising outbox id").isEmpty();
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

    /** The restored rows of {@link #HELD}, left while the holder's send was open: delivered after its message. */
    private void assertHeldRowsAfterTheHolder(Map<UUID, Integer> delivered, Orphan orphan) {
        int holder = delivered.get(holderSent.get().id());
        assertThat(orphan.rows().stream().filter(r -> HELD.equals(r.key())).toList()).as("restored rows of " + HELD)
                .isNotEmpty()
                .allSatisfy(r -> assertThat(delivered.get(r.id())).as("after the holder's message")
                        .isGreaterThan(holder));
    }

    /**
     * The lowest range at or below the slowest shard's horizon, once there is one; maintenance must be paused. Then
     * {@code pin} opens a transaction with an xid, which holds every later tick's xmin, so the horizon, at or below
     * it: maintenance drops no range the restored rows go to until the test counted them, nor any other range, whose
     * detach would wait for the key holder's open transaction.
     */
    private Bound relayedRange(Connection c, Connection pin) throws Exception {
        long deadline = System.nanoTime() + BOUND.toNanos();
        while (true) {
            long latest = new Ticks(names, config.shards()).latest(c).id();
            long tick = Cursors.busyTick(new Cursors(names, config.shards()).positions(c, latest), latest);
            long horizon = new Ticks(names, config.shards()).get(c, tick).snapshot().xmin();
            var range = PartitionDdl.listRanges(c, names.base()).stream()
                    .filter(b -> b.to() != null && Long.parseLong(b.to()) <= horizon)
                    .min(Comparator.comparingLong(b -> Long.parseLong(b.from())));
            if (range.isPresent()) {
                pin.setAutoCommit(false);
                long pinned = queryLong(pin, "select pg_current_xact_id()::text::bigint");
                assertThat(PartitionDdl.listRanges(c, names.base())).as("relayed by the pinned horizon %d", pinned)
                        .filteredOn(b -> b.to() != null && Long.parseLong(b.to()) <= pinned)
                        .containsExactly(range.get());
                return range.get();
            }
            assertThat(System.nanoTime()).as("the relay passed a range within %s", BOUND).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    /**
     * Gives the standalone {@code range} one row below {@code from} and {@value #ORPHAN_ROWS} from it on, which the
     * relay never saw: on a stale shard, over the writers' keys and some unkeyed, as {@code MaintenanceIT} makes them,
     * but none of {@link #HELD} in the first chunk.
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
        int held = Integer.parseInt(HELD.substring(1));
        execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic, key, payload)"
                + " select " + ORPHAN_IDS + " + s, 999, (" + from + " + s % 7)::text::xid8, gen_random_uuid(), 't',"
                + " case when s % 10 = 0 or (s <= " + CHUNK + " and s % " + KEYS + " = " + held + ") then null"
                + " else convert_to('k' || s % " + KEYS + ", 'UTF8') end, convert_to('p' || s, 'UTF8')"
                + " from generate_series(1, " + ORPHAN_ROWS + ") s");
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

    /** The rows from {@code from} on still in the orphan; waits for a chunk in progress, which locks it. */
    private static long remaining(Orphan orphan) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + orphan.table() + " where xid >= '" + orphan.from()
                    + "'::xid8");
        }
    }

    /** The orphan's rows of {@link #HELD}, which stay while the holder's send is open. */
    private static long heldRows(Orphan orphan) {
        return orphan.rows().stream().filter(r -> HELD.equals(r.key())).count();
    }

    /** Each restored message is in the outbox once: no chunk moved twice, none lost (the pin keeps every range). */
    private void assertEachOnceInTheOutbox(Orphan orphan) throws SQLException {
        try (var c = TestDb.connect(); var ps = c.prepareStatement("select count(*), count(distinct message_id) from "
                + names.base() + " where message_id = any(?)")) {
            ps.setArray(1, c.createArrayOf("uuid", orphan.rows().stream().map(Row::id).toArray()));
            try (var rs = ps.executeQuery()) {
                rs.next();
                assertThat(List.of(rs.getLong(1), rs.getLong(2))).as("rows, distinct message ids")
                        .containsExactly((long) ORPHAN_ROWS, (long) ORPHAN_ROWS);
            }
        }
    }

    /** The WARN of a maintenance run that could not move the rows of {@code orphan}, its SQLState captured. */
    private static Pattern failedRuns(Orphan orphan) {
        return Pattern.compile("could not move the rows of " + Pattern.quote(orphan.table())
                + ", .*?\\(SQLState (\\w+)\\)");
    }

    private static void assertLockNotAvailable(RuntimeSqlException e) {
        assertThat(e.getCause()).isInstanceOfSatisfying(SQLException.class,
                s -> assertThat(s.getSQLState()).as(s.toString()).isEqualTo(LOCK_NOT_AVAILABLE));
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

    private Thread run(String name, Body body) {
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
        return t;
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

    private static void await(Duration timeout, String what, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(what + ": not within " + timeout);
            }
            Thread.sleep(5);
        }
    }
}
