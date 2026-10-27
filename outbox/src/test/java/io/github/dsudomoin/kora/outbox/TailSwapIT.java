package io.github.dsudomoin.kora.outbox;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl;
import io.github.dsudomoin.kora.outbox.internal.PgOutbox;
import io.github.dsudomoin.kora.outbox.internal.TableNames;
import io.github.dsudomoin.kora.outbox.internal.XidPartitions;
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
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The tail swap under load: {@value #WRITERS} writers send one keyed message per transaction through
 * {@link Outbox#send} in {@code inTx} for the whole test, keys over {@value #KEYS} values, while a running relay
 * delivers and its maintenance runs every 50 ms; xids are burnt to force extensions.
 */
class TailSwapIT {

    private static final int WRITERS = 8;
    private static final int KEYS = 200;
    private static final Duration SWAP_LOCK_TIMEOUT = Duration.ofMillis(200);
    /**
     * A writer stalls behind a swap for at most its wait for ACCESS EXCLUSIVE on the parent, {@code swapLockTimeout},
     * plus the milliseconds it holds it; the margin covers the hold and a loaded machine. Not the edge: a session that
     * holds a lock on the tail itself (it queried {@code <base>_tail} directly, not through the parent) makes the
     * swap's detach wait a second {@code swapLockTimeout} while the parent's ACCESS EXCLUSIVE is already held, since
     * {@code lock_timeout} bounds each lock wait and {@code statement_timeout} each statement, not the transaction: a
     * stall of up to 2 × {@code swapLockTimeout} + the hold. These writers reach the tail only through the parent, and
     * the test queries the tail only while maintenance is paused, so one wait bounds them.
     */
    private static final Duration WRITER_BOUND = SWAP_LOCK_TIMEOUT.plusMillis(500);
    /** {@link XidPartitions}'s INFO line of a swap: table, whether the tail kept its rows, hold and wait in ms. */
    private static final Pattern SWAPPED = Pattern.compile("swapped the partition tail of (\\S+): (attached it with"
            + " its rows as )?.*?held ACCESS EXCLUSIVE on \\S+ for ([\\d.]+) ms, after waiting ([\\d.]+) ms for it");
    private static final Duration SLOW_CALL = Duration.ofSeconds(3);
    /** Business transactions open 1 s after their send, at once: about this many a second. */
    private static final int LONG_WRITERS = 24;

    private final JdbcDatabase db = TestDb.newDatabase(30);
    private final TestOutboxConfig config = new TestOutboxConfig()
            .xidsPerPartition(2000)
            .premake(4)
            .swapLockTimeout(SWAP_LOCK_TIMEOUT)
            .maintenanceInterval(Duration.ofMillis(50));
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final RecordingHandler recording = new RecordingHandler();
    /** Runs before the next handler call, once. */
    private final AtomicReference<Runnable> beforeNextCall = new AtomicReference<>();
    private final CapturedStderr log = new CapturedStderr();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final List<Thread> threads = new ArrayList<>();
    private final Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
    private final Set<UUID> committed = ConcurrentHashMap.newKeySet();
    private final AtomicLong commits = new AtomicLong();
    private final AtomicLong maxLatency = new AtomicLong();
    private final AtomicLong samples = new AtomicLong();
    /** {@code outbox.partitions.xids_left} values other than NaN: a sample that found the tail not attached. */
    private final Queue<Double> xidsLeft = new ConcurrentLinkedQueue<>();
    private volatile boolean burning;
    /** Xids the burner burns every 10 ms while {@link #burning}. */
    private int burnBatch = 100;
    /** How long each writer sleeps between its transactions; zero: they run flat out. */
    private Duration writerPause = Duration.ZERO;
    private long loadStarted;
    private long xidAtStart;
    private TableNames names;
    private OutboxSchema schema;
    private OutboxRelay relay;

    @AfterEach
    void release() throws InterruptedException {
        stopLoad();
        if (relay != null) {
            relay.release();
        }
        db.release();
        log.close();
    }

    @Test
    void emptyTailSwapsUnderLoad() throws Exception {
        // every swap must find the tail empty, so the margin left when an extension is due, 2 ranges = 4000 xids,
        // must outlast a maintenance interval and a run on a loaded machine: paced writers and a slow burner keep
        // the xid rate near 1500/s, over 2 s of margin; flat out (about 15 000/s here) a run slowed to a quarter of a
        // second lets rows reach the tail, and the swap rightly takes the hot path
        writerPause = Duration.ofMillis(20);
        burnBatch = 10;
        bootstrap();
        start();
        burning = true;
        await(Duration.ofSeconds(60), () -> swaps().size() >= 3);
        burning = false;

        var swaps = finish("emptyTailSwapsUnderLoad");

        assertThat(swaps).hasSizeGreaterThanOrEqualTo(3)
                .allSatisfy(swap -> assertThat(swap.group(2)).as("an empty tail: %s", swap.group()).isNull());
        assertThat(gauge("outbox.partitions.overflow")).isZero();
    }

    @Test
    void overflowedTailSwapsUnderLoad() throws Exception {
        bootstrap();
        start();
        try (var pause = TestDb.connect(); var c = TestDb.connect()) {
            maintenanceLock(pause, "pg_advisory_lock"); // after the run in progress, if any
            burnUntil(() -> queryLong(c, "select count(*) from (select from " + names.tail() + " limit 1) t") == 1);
            maintenanceLock(pause, "pg_advisory_unlock");
        }
        await(Duration.ofSeconds(30), () -> !swaps().isEmpty());
        assertThat(swaps().get(0).group(2)).as("the tail became a range with its rows").isNotNull();
        burning = true;
        await(Duration.ofSeconds(60), () -> swaps().size() >= 3);
        burning = false;

        var swaps = finish("overflowedTailSwapsUnderLoad");

        assertThat(swaps).hasSizeGreaterThanOrEqualTo(3);
        assertThat(log.text()).contains("outbox rows went to the partition tail " + names.tail());
        assertThat(gauge("outbox.partitions.overflow")).as("healed").isZero();
    }

    @Test
    void swapWaitsBehindASlowHandlerWithoutBlockingWriters() throws Exception {
        // the margin left when the extension is due, 2 ranges = 4000 xids, must outlast the wait: flat-out writers
        // (about 10k transactions a second here) fill it in under a second, so the swap would find rows in the tail
        // and leave it to the hot path of the next run
        writerPause = Duration.ofMillis(20);
        bootstrap();
        start();
        var entered = new CountDownLatch(1);
        var began = new AtomicLong();
        var returned = new AtomicLong();
        var commitsAtReturn = new AtomicLong();
        var swapsAtReturn = new AtomicInteger(-1);
        try (var pause = TestDb.connect(); var c = TestDb.connect()) {
            maintenanceLock(pause, "pg_advisory_lock");
            var partitions = new XidPartitions(names, config.partitions());
            burnUntil(() -> partitions.premadeAhead(c) <= config.premake() / 2); // the extension is due
            int before = swaps().size();
            beforeNextCall.set(() -> {
                began.set(System.nanoTime());
                entered.countDown();
                try {
                    Thread.sleep(SLOW_CALL.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                commitsAtReturn.set(commits.get());
                swapsAtReturn.set(swaps().size()); // the step has not committed yet: no swap may have run
                returned.set(System.nanoTime());
            });
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("a step calls the slow handler").isTrue();
            maintenanceLock(pause, "pg_advisory_unlock"); // the extension starts while that step runs

            // the budget runs from the slow call's start, not from the unpause: the wait must be seen within the call
            await(Duration.ofNanos(began.get() + SLOW_CALL.toNanos() - System.nanoTime()), () -> quiesceWaits(c));
            long waitSeen = System.nanoTime();
            long waiting = commits.get();
            assertThat(returned).as("the step still runs").hasValue(0);
            await(Duration.ofSeconds(10), () -> swaps().size() > before);
            long swapped = System.nanoTime();
            System.out.printf("TailSwapIT: the swap waited for the slow step at least %d ms%n",
                    TimeUnit.NANOSECONDS.toMillis(swapped - waitSeen));

            assertThat(swapsAtReturn).as("no swap while the step ran").hasValue(before);
            assertThat(swaps().get(before).group(2)).as("the extension that waited swapped the empty tail").isNull();
            assertThat(commitsAtReturn.get() - waiting).as("writers committed while the swap waited").isPositive();
        }

        finish("swapWaitsBehindASlowHandlerWithoutBlockingWriters");
    }

    /**
     * Business transactions that stay open 1 s after their send, 24 at a time, so they overlap all the time: the swap
     * never gets swapLockTimeout without one of them, and fails on every due run. Not a promise of a swap: the signals
     * (each 55P03 WARN naming the holders and their age, then the ERROR naming the cause and the remedies) and that
     * nothing is lost.
     */
    @Test
    void writersHoldingTheLockPastTheSendGetTheSwapAnError() throws Exception {
        writerPause = Duration.ofMillis(20);
        bootstrap();
        start();
        var longDb = TestDb.newDatabase(LONG_WRITERS + 2);
        try {
            var longOutbox = new PgOutbox(longDb, schema, config.shards(), relay::wake, new OutboxMetrics(null));
            try (var pause = TestDb.connect(); var c = TestDb.connect()) {
                maintenanceLock(pause, "pg_advisory_lock");
                var partitions = new XidPartitions(names, config.partitions());
                burnUntil(() -> partitions.premadeAhead(c) <= config.premake() / 2); // the extension is due
                for (int i = 0; i < LONG_WRITERS; i++) {
                    run("long-writer-" + i, () -> {
                        while (!stopping.get()) {
                            var key = "long" + ThreadLocalRandom.current().nextInt(KEYS);
                            try {
                                committed.add(longDb.inTx((SqlFunction0<UUID>) () -> {
                                    var id = longOutbox.send(OutboxMessage.of("t", key, key.getBytes(UTF_8)));
                                    try {
                                        Thread.sleep(1000); // a downstream call inside the business transaction
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new IllegalStateException(e);
                                    }
                                    return id;
                                }));
                            } catch (RuntimeException e) {
                                errors.add(e);
                            }
                        }
                    });
                    Thread.sleep(1000 / LONG_WRITERS); // spread over the second, so they always overlap
                }
                maintenanceLock(pause, "pg_advisory_unlock");
            }
            var stuck = Pattern.compile("ERROR .*outbox partition tail swap of " + Pattern.quote(names.base())
                    + " timed out waiting for writers' locks on (\\S+) on (\\d+) due runs in a row, (\\d+) ranges"
                    + " ahead of the current xid: .* swapLockTimeout (\\d+) ms after their send, and overlap, so the"
                    + " swap never gets its gap \\((\\d+) transactions hold a lock on (\\S+) right after, the oldest:"
                    + " (pid \\d+ \\(.*?\\) in a transaction open [\\d.]+ s)");
            await(Duration.ofSeconds(60), () -> stuck.matcher(log.text()).find());

            var text = log.text();
            assertThat(text).contains("failed (SQLState 55P03)").contains("transactions hold a lock on "
                    + names.base() + " right after, the oldest: pid ").contains("in a transaction open ");
            var error = stuck.matcher(text);
            assertThat(error.find()).isTrue();
            assertThat(List.of(names.base(), names.tail())).as("the table waited on").contains(error.group(1));
            assertThat(error.group(6)).as("the holders' table").isEqualTo(error.group(1));
            int runs = Integer.parseInt(error.group(2));
            int ahead = Integer.parseInt(error.group(3));
            assertThat(ahead).as("ranges ahead, the extension due").isLessThanOrEqualTo(config.premake() / 2);
            assertThat(runs >= 10 || ahead < config.premake() / 2) // XidPartitions.STUCK_SWAPS
                    .as("%d runs in a row, %d ahead: the ERROR's condition", runs, ahead).isTrue();
            assertThat(Long.parseLong(error.group(4))).isEqualTo(SWAP_LOCK_TIMEOUT.toMillis());
            assertThat(Integer.parseInt(error.group(5))).as("holders").isPositive();
            assertThat(error.group()).contains("Send last in the transaction, or raise swapLockTimeout");
            stopLoad();
            assertThat(errors).as("writer failures").isEmpty();
            relay.drain();
            var missing = new HashSet<>(committed);
            missing.removeAll(recording.delivered().stream().map(OutboxRecord::messageId).collect(Collectors.toSet()));
            assertThat(missing).as("committed, never delivered").isEmpty();
        } finally {
            stopLoad();
            longDb.release();
        }
    }

    /**
     * The swap at the library's default {@code xidsPerPartition} and {@code premake}, once premake / 2 ranges were used
     * up since the last extension: it attaches premake / 2 new ranges. For the README's lock-hold figure.
     */
    @Test
    void swapAtTheDefaultPartitionSizes() throws Exception {
        var defaults = new OutboxConfig.PartitionsConfig() {};
        config.xidsPerPartition(defaults.xidsPerPartition()).premake(defaults.premake());
        bootstrap();
        try (var pause = TestDb.connect(); var c = TestDb.connect()) {
            maintenanceLock(pause, "pg_advisory_lock");
            var ranges = PartitionDdl.listRanges(c, names.base()).stream().filter(b -> b.to() != null)
                    .sorted(Comparator.comparingLong(b -> Long.parseLong(b.from()))).toList();
            var used = ranges.subList(ranges.size() - config.premake() / 2, ranges.size());
            execute(c, "alter table " + names.base() + " detach partition " + names.tail());
            for (var range : used) {
                execute(c, "drop table " + range.name());
            }
            execute(c, "alter table " + names.base() + " attach partition " + names.tail() + " for values from ('"
                    + used.get(0).from() + "') to (maxvalue)");
            assertThat(new XidPartitions(names, config.partitions()).premadeAhead(c)).isEqualTo(config.premake() / 2);
            start();
            await(Duration.ofSeconds(10), () -> commits.get() > 1000);
            maintenanceLock(pause, "pg_advisory_unlock");
        }
        await(Duration.ofSeconds(60), () -> !swaps().isEmpty());

        var swaps = finish("swapAtTheDefaultPartitionSizes (xidsPerPartition " + config.xidsPerPartition()
                + ", premake " + config.premake() + ")");

        assertThat(swaps).singleElement().satisfies(swap -> assertThat(swap.group()).contains("attached "
                + config.premake() / 2 + " ranges"));
    }

    private void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
    }

    /**
     * Starts the writers, a 10 ms sampler of {@code outbox.partitions.xids_left}, the xid burner (idle until
     * {@link #burning}) and the relay with its maintenance.
     */
    private void start() throws SQLException {
        xidAtStart = currentXid();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));
        relay = new OutboxRelay(db, config, schema, new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                recording.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                var before = beforeNextCall.getAndSet(null);
                if (before != null) {
                    before.run();
                }
                return recording.handleBatch(records);
            }
        }, new OutboxMetrics(null), null, maintenance, new DeadLetterPass(db, names, config, null,
                new OutboxMetrics(null)));
        var outbox = new PgOutbox(db, schema, config.shards(), relay::wake, new OutboxMetrics(null));
        for (int i = 0; i < WRITERS; i++) {
            run("tail-swap-writer-" + i, () -> {
                while (!stopping.get()) {
                    var key = "k" + ThreadLocalRandom.current().nextInt(KEYS);
                    long started = System.nanoTime();
                    try {
                        var id = db.inTx((SqlFunction0<UUID>) () ->
                                outbox.send(OutboxMessage.of("t", key, key.getBytes(UTF_8))));
                        maxLatency.accumulateAndGet(System.nanoTime() - started, Math::max);
                        committed.add(id);
                        commits.incrementAndGet();
                    } catch (RuntimeException e) {
                        errors.add(e);
                    }
                    if (!writerPause.isZero()) {
                        Thread.sleep(writerPause.toMillis());
                    }
                }
            });
        }
        run("tail-swap-sampler", () -> {
            while (!stopping.get()) {
                double left = gauge("outbox.partitions.xids_left");
                if (!Double.isNaN(left)) {
                    xidsLeft.add(left);
                }
                samples.incrementAndGet();
                Thread.sleep(10);
            }
        });
        run("tail-swap-burner", () -> {
            try (var c = TestDb.connect()) {
                while (!stopping.get()) {
                    if (burning) {
                        burn(c, burnBatch);
                    }
                    Thread.sleep(10);
                }
            }
        });
        loadStarted = System.nanoTime();
        relay.init();
    }

    /**
     * Stops the load and checks what every test checks: no writer failed, none waited the bound, the tail was attached
     * at every sample and a drain delivers every committed message. Prints the swaps' lock hold first; returns them.
     */
    private List<MatchResult> finish(String test) throws Exception {
        stopLoad();
        long loadMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - loadStarted);
        long xids = currentXid() - xidAtStart;
        var swaps = swaps();
        System.out.printf("TailSwapIT %s: %d swaps, ACCESS EXCLUSIVE held %s ms after waiting %s ms for it; %d writer"
                        + " commits in %d ms, %d xids/s, the slowest %.1f ms (bound %d ms)%n", test, swaps.size(),
                swaps.stream().map(swap -> swap.group(3)).toList(), swaps.stream().map(swap -> swap.group(4)).toList(),
                commits.get(), loadMillis, xids * 1000 / Math.max(1, loadMillis), maxLatency.get() / 1e6,
                WRITER_BOUND.toMillis());

        assertThat(errors).as("writer failures").isEmpty();
        assertThat(Duration.ofNanos(maxLatency.get())).as("the slowest writer transaction").isLessThan(WRITER_BOUND);
        assertThat(samples.get()).as("samples in %d ms, one per 10 ms", loadMillis).isGreaterThan(loadMillis / 40);
        assertThat(xidsLeft).as("outbox.partitions.xids_left at every sample: NaN, the tail attached").isEmpty();
        relay.drain();
        var missing = new HashSet<>(committed);
        missing.removeAll(recording.delivered().stream().map(OutboxRecord::messageId).collect(Collectors.toSet()));
        assertThat(missing).as("committed, never delivered").isEmpty();
        return swaps;
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

    /** The swaps of this test's table so far, from their INFO line. */
    private List<MatchResult> swaps() {
        return SWAPPED.matcher(log.text()).results().filter(swap -> swap.group(1).equals(names.base())).toList();
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    /** {@code function(internalNs, LOCK_MAINTENANCE)}: maintenance runs skip while this session holds it. */
    private void maintenanceLock(Connection c, String function) throws SQLException {
        execute(c, "select " + function + "(" + names.internalNamespace() + ", " + TableNames.LOCK_MAINTENANCE + ")");
    }

    /** The extension's quiesce waits for {@code LOCK_SWAP}, which relay steps in flight hold shared. */
    private boolean quiesceWaits(Connection c) throws SQLException {
        return queryLong(c, "select count(*) from pg_locks where locktype = 'advisory' and classid::bigint = "
                + Integer.toUnsignedLong(names.internalNamespace()) + " and objid = " + TableNames.LOCK_SWAP
                + " and objsubid = 2 and mode = 'ExclusiveLock' and not granted") > 0;
    }

    private void burnUntil(Condition condition) throws Exception {
        try (var c = TestDb.connect()) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!condition.holds()) {
                assertThat(System.nanoTime()).as("burnt enough within 30 s").isLessThan(deadline);
                burn(c, 100);
            }
        }
    }

    private static long currentXid() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
        }
    }

    private static void burn(Connection c, int xids) throws SQLException {
        try (var st = c.createStatement()) {
            for (int i = 0; i < xids; i++) {
                st.execute("select pg_current_xact_id()");
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
