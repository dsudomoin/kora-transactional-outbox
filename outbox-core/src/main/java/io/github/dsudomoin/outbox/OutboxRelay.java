package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.Backoff;
import io.github.dsudomoin.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.outbox.internal.Cursors.Position;
import io.github.dsudomoin.outbox.internal.Cursors;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Heartbeat;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.ShardProcessor.ShardResult;
import io.github.dsudomoin.outbox.internal.ShardProcessor.ShardStatus;
import io.github.dsudomoin.outbox.internal.ShardProcessor;
import io.github.dsudomoin.outbox.internal.Ticks;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.application.graph.Lifecycle;

/**
 * The background relay: {@code relay.workers} daemon threads {@code outbox-relay-N}, each repeatedly making a tick when
 * due and stepping groups of candidate shards, a {@link ShardProcessor} step of each group in its own transaction
 * ({@link Round}: the workers share one read of the cursors per latest tick); plus maintenance right after start and
 * then every {@code partitions.maintenanceInterval} on the daemon thread {@code outbox-maintenance}. Neither starts
 * when {@code relay.enabled} is false; {@link #drain()} works either way. Idle workers park until {@link #wake()}, a
 * new tick made by this relay, or the next tick attempt is due, so an idle relay does not spin; an idle attempt makes a
 * tick only when the newest tick of any instance is about {@code idlePollInterval} old, so an idle cluster makes about
 * one tick per {@code idlePollInterval}. A shard is a candidate only when a tick after its effective tick holds its
 * rows (each tick records the shards that gained rows, see {@link Ticks}), its batch is in progress or a retry head or
 * parked row is due, so a shard without new rows costs no step: maintenance's checkpoint moves its effective tick on
 * (see {@link Cursors}), which keeps tick cleanup and partition drops going. Every tick this relay makes wakes all its
 * workers, which spread the tick's shards with new rows between them, in groups. A wake-up that cannot make a tick yet
 * ({@code minTickInterval}) only brings the tick forward, so under a high commit rate the workers read cursors per
 * tick, not per commit. While {@link Maintenance#halted()} the workers take no shards. Dead letters go to the
 * {@link DeadLetterPass} on the daemon thread {@code outbox-dead-letters} (also not started when the relay is disabled;
 * {@link #drain()} ends with one run of it): a run right after start, then after each committed step that moved a row
 * to the dead table, when the earliest letter is due, and at least every {@code maintenanceInterval}; so a slow or
 * failing hook never holds up a shard step.
 */
public final class OutboxRelay implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final AtomicInteger THREADS = new AtomicInteger();
    private static final long WARN_INTERVAL = TimeUnit.SECONDS.toNanos(10);
    /** How long interrupted workers get to roll back after {@code shutdownTimeout}. */
    private static final long INTERRUPT_GRACE = TimeUnit.SECONDS.toNanos(1);
    /** The most shards one step takes: a group's statements bind arrays of this size. */
    private static final int MAX_GROUP = 256;

    private final OutboxTransactions db;
    private final OutboxConfig config;
    private final OutboxConfig.RelayConfig relay;
    private final Maintenance maintenance;
    private final Ticks ticks;
    private final Cursors cursors;
    private final ShardProcessor processor;
    private final DeadLetterPass deadLetters;
    private final Heartbeat heartbeat;
    private final long idleNanos;
    private final long minTickNanos;
    /**
     * An idle tick attempt inserts only when the newest tick, any instance's, is older than this: so a cluster makes
     * about one idle tick per {@code idlePollInterval} (at most 4 per 3 intervals with many instances), not one per
     * instance. Three quarters of the interval, not all of it: this instance's own previous tick is an interval old
     * less its insert delay (connection, lock and statement round trips) at the next attempt, so with the full
     * interval jitter in that delay would skip about every other attempt and double the latency of other instances'
     * commits, which only idle ticks cover.
     */
    private final Duration idleTickAge;
    /** Incremented by every {@link #wake()}. */
    private final AtomicLong wakeups = new AtomicLong();
    /** Incremented by every tick this relay inserts; parked workers wake on it. */
    private final AtomicLong ticksMade = new AtomicLong();
    /** Incremented by every committed step that dead-lettered a row; the dead-letter thread wakes on it. */
    private final AtomicLong deadLettered = new AtomicLong();
    /** Per shard, when a worker of this relay last started a step on it ({@code nanoTime}). */
    private final AtomicLongArray stepStarted;
    /**
     * Per shard, 1 after a step of it failed (an exception, the transaction rolled back), until a step of it alone
     * succeeds: it is stepped alone meanwhile, so a shard that stays stuck (e.g. its effective tick deleted by hand)
     * rolls back no neighbour's step. In memory, per relay instance.
     */
    private final AtomicIntegerArray isolated;
    /**
     * Per shard, the rows its next step is expected to read: {@code batchSize} while its batch is unfinished and after
     * a step that left it untouched (unread behind a cut, or not attempted); else the rows its last batch delivered (0
     * before it delivered any). A group
     * takes shards until their rows would pass {@code batchSize}, which a group's read and call share, so a busy or
     * slow shard is stepped alone (a cut read leaves the shards after it untouched and its own rest to be read again)
     * while many shards with a few rows each share one step. In memory, per relay instance; {@link #batchRows}: what
     * the batch in progress delivered so far.
     */
    private final AtomicIntegerArray expected;
    private final AtomicIntegerArray batchRows;
    /**
     * Per shard, 1 while a worker of this relay steps it (from {@link Round#take} to the end of its step): another
     * round passes it over and sets {@link #wanted}, and the step then puts it into the round current at its end.
     */
    private final AtomicIntegerArray inFlight;
    private final AtomicIntegerArray wanted;
    /** Per shard, the latest tick of the step of it that {@link #stepStarted} times. */
    private final AtomicLongArray steppedTick;
    /** The candidates of the latest tick a worker read them for; replaced under {@link #roundLock}. */
    @Nullable
    private volatile Round round;
    private final ReentrantLock roundLock = new ReentrantLock();
    /** Incremented by every step that put shards back into a round; parked workers wake on it. */
    private final AtomicLong requeued = new AtomicLong();
    /** The workers parked in {@link Worker#awaitWork}: a put-back wakes one of them, not one blocked elsewhere. */
    private final Set<Thread> awaiting = ConcurrentHashMap.newKeySet();

    // tick scheduling, shared by the workers: at most one attempt per minTickInterval for wake-ups, per idle period else
    private final Object tickState = new Object();
    private long tickedWakeups;
    private long lastTickAttempt;
    /** A wake-up's commit is not yet covered by a tick; {@code wakeAt}: when the latest such wake-up was seen. */
    private boolean wakePending;
    private long wakeAt;

    private volatile Thread[] workers = new Thread[0];
    @Nullable
    private volatile Thread deadLetterThread;
    @Nullable
    private volatile ScheduledExecutorService scheduler;
    private volatile boolean started;
    private volatile boolean stopping;

    /**
     * {@code deadLetters}: the pass this relay runs, and closes on {@link #release()}; its hook, or none, decides the
     * fate of the dead letters.
     */
    public OutboxRelay(OutboxTransactions db, OutboxConfig config, OutboxSchema schema, OutboxHandler handler,
            OutboxTelemetry telemetry, Maintenance maintenance, DeadLetterPass deadLetters) {
        OutboxSchema.validate(config);
        this.db = db;
        this.config = config;
        this.relay = config.relay();
        this.maintenance = maintenance;
        this.ticks = new Ticks(schema.names(), config.shards());
        this.cursors = new Cursors(schema.names(), config.shards());
        this.deadLetters = deadLetters;
        this.processor = new ShardProcessor(schema.names(), config, handler, telemetry, deadLetters.active());
        // the pass runs only where the relay does
        this.heartbeat = new Heartbeat(db, schema, config, relay.enabled() && deadLetters.active());
        this.idleNanos = relay.idlePollInterval().toNanos();
        this.minTickNanos = relay.minTickInterval().toNanos();
        this.idleTickAge = Duration.ofNanos(Math.max(minTickNanos, idleNanos - idleNanos / 4));
        this.lastTickAttempt = System.nanoTime() - idleNanos; // the first pass ticks
        this.stepStarted = new AtomicLongArray(config.shards());
        this.isolated = new AtomicIntegerArray(config.shards());
        this.expected = new AtomicIntegerArray(config.shards());
        this.batchRows = new AtomicIntegerArray(config.shards());
        this.inFlight = new AtomicIntegerArray(config.shards());
        this.wanted = new AtomicIntegerArray(config.shards());
        this.steppedTick = new AtomicLongArray(config.shards());
        long created = System.nanoTime();
        for (int i = 0; i < config.shards(); i++) {
            stepStarted.set(i, created);
        }
    }

    /** Starts the workers, the dead-letter thread and maintenance, unless {@code relay.enabled} is false. */
    @Override
    public void init() {
        heartbeat.start(); // with the relay disabled too: the reshard guard counts writers
        if (!relay.enabled()) {
            log.info("outbox relay disabled (outbox.relay.enabled = false); messages are delivered only by drain()");
            return;
        }
        maintenance.checkHalt(); // a halt recorded before this instance started holds before a worker takes a shard
        var threads = new Thread[relay.workers()];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Worker(), "outbox-relay-" + THREADS.incrementAndGet());
            threads[i].setDaemon(true);
        }
        workers = threads; // before start, so wake() reaches every worker
        started = true;
        for (var t : threads) {
            t.start();
        }
        var dead = new Thread(this::runDeadLetters, "outbox-dead-letters");
        dead.setDaemon(true);
        deadLetterThread = dead;
        dead.start();
        var maintenanceInterval = config.partitions().maintenanceInterval().toNanos();
        var s = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "outbox-maintenance");
            t.setDaemon(true);
            return t;
        });
        s.scheduleWithFixedDelay(this::runMaintenance, 0, maintenanceInterval, TimeUnit.NANOSECONDS);
        scheduler = s;
        log.info("outbox relay started: {} workers over {} shards", threads.length, config.shards());
    }

    /**
     * Graceful shutdown: workers take no further shards, in-flight batches get {@code shutdownTimeout} to
     * finish, then the workers are interrupted and their uncommitted batches are redelivered later. A dead-letter hook
     * call in flight is interrupted at once ({@link DeadLetterPass#close()}): its letters roll back, costing no
     * attempt, and are handed again later. A dead-letter index build in flight is cancelled
     * ({@link Maintenance#close}); the next one drops what it left.
     */
    @Override
    public void release() {
        stopping = true;
        wakeAll();
        deadLetters.close();
        var dead = deadLetterThread;
        if (dead != null) {
            dead.interrupt(); // also stops it waiting for a hook that ignores the interrupt
        }
        var s = scheduler;
        if (s != null) {
            s.shutdown();
        }
        boolean interrupted = false;
        long deadline = System.nanoTime() + relay.shutdownTimeout().toNanos();
        try {
            if (!awaitWorkers(deadline)) {
                log.warn("outbox relay workers still busy after shutdownTimeout {}: interrupting them, their"
                        + " uncommitted batches are redelivered later", relay.shutdownTimeout());
                for (var t : workers) {
                    t.interrupt();
                }
                awaitWorkers(System.nanoTime() + INTERRUPT_GRACE);
            }
            if (dead != null) {
                TimeUnit.NANOSECONDS.timedJoin(dead, deadline - System.nanoTime());
                if (dead.isAlive()) {
                    log.warn("outbox dead-letter thread still busy after shutdownTimeout {}; it ends with its current"
                            + " statement, its letters are handed again later", relay.shutdownTimeout());
                }
            }
            if (s != null && !s.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                s.shutdownNow();
            }
        } catch (InterruptedException e) {
            interrupted = true;
            for (var t : workers) {
                t.interrupt();
            }
            if (s != null) {
                s.shutdownNow();
            }
        } finally {
            // after the scheduler, and on an interrupted release too: no maintenance run starts a build any more
            maintenance.close(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            processor.close();
            heartbeat.stop();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (started) {
            log.info("outbox relay stopped");
        }
    }

    /** Wakes idle workers to tick and relay now; called after a transaction that sent messages commits. */
    public void wake() {
        wakeups.incrementAndGet();
        wakeAll();
    }

    /** Started by {@link #init()} with {@code relay.enabled} and not yet released. */
    public boolean running() {
        return started && !stopping;
    }

    /**
     * Makes a tick D, then processes shards until none is {@link Position#behind() behind} D (a shard without rows up
     * to D is not, so it is not stepped), none has a batch in progress and no retry head or parked row is due (one due
     * later does not hold it up); returns the number of messages this call delivered. Works whether or not the relay
     * runs: it contends with the workers for shard locks. Handler failures are outcomes (see {@link OutboxHandler}); a
     * database failure of a step propagates. Ends with one run of the dead-letter pass, so the hook has had the letters
     * this call made, whether or not the relay runs; letters it redrives are new messages, after D. A database failure
     * of that run propagates too, a lock timeout while it writes the hook's decisions included. It holds one connection
     * at a time: the pass runs once the shards' is closed, after the dead-letter thread's run in progress, if any.
     *
     * @throws IllegalStateException when no shard advanced for {@code 2 × (batchTimeout + 5 s)}, twice the longest a
     *     tail swap waits for relay steps in flight, while work remains (e.g. a shard paused by a down target), or
     *     while maintenance has halted the relay (xid regression)
     */
    public int drain() {
        int delivered = drainShards();
        if (deadLetters.runOnce() > 0) {
            wake();
        }
        return delivered;
    }

    /** {@link #drain()} but its dead-letter run, on one connection of its own. */
    private int drainShards() {
        maintenance.checkHalt(); // the recorded halt: maintenance does not run while the relay is disabled
        requireNotHalted();
        // a swap keeps every step out while it waits for those in flight: no progress for that long is no stall
        var stallAfter = Maintenance.quiesceTimeout(config).multipliedBy(2);
        long stall = stallAfter.toNanos();
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            long since = System.nanoTime();
            Ticks.Made made;
            while ((made = ticks.tick(c, Duration.ZERO)) != Ticks.Made.INSERTED) { // the 1 s tick lock timeout
                if (made == Ticks.Made.REGRESSED) { // the halt it recorded; trying again would only burn xids
                    maintenance.checkHalt();
                    requireNotHalted();
                }
                if (System.nanoTime() - since > stall) {
                    throw new IllegalStateException("outbox drain could not make a tick for " + stallAfter);
                }
            }
            long target = ticks.latest(c).id();
            int delivered = 0;
            List<Cursor> positions = null;
            long progressed = System.nanoTime();
            while (true) {
                requireNotHalted();
                var now = Instant.now().truncatedTo(ChronoUnit.MICROS);
                var pending = cursors.positions(c, target).stream().filter(p -> !drained(p, now))
                        .map(Position::cursor).toList();
                if (pending.isEmpty()) {
                    return delivered;
                }
                // a position change of a pending shard, by this call or a worker; a pause or due time alone is no
                // progress (a requeue moves parked_due_at on every step), and drained shards moving under live traffic
                // must not hide a stuck one
                var current = pending.stream()
                        .map(cursor -> cursor.cleared().withRetryDueAt(null).withParkedDueAt(null)).toList();
                if (!current.equals(positions)) {
                    positions = current;
                    progressed = System.nanoTime();
                } else if (System.nanoTime() - progressed > stall) {
                    throw new IllegalStateException("outbox drain made no progress for " + stallAfter
                            + " towards tick " + target + "; pending: " + pending);
                }
                boolean worked = false;
                for (var group : groups(pending.stream().map(Cursor::shard).toList())) {
                    var results = processInOwnTransaction(c, group, target, now);
                    learn(group, results);
                    for (var result : results) {
                        delivered += result.delivered();
                        worked |= result.status() == ShardStatus.PROGRESS || result.status() == ShardStatus.PAUSED;
                        if (result.delivered() > 0) {
                            progressed = System.nanoTime();
                        }
                    }
                }
                if (!worked) {
                    TimeUnit.NANOSECONDS.sleep(idleNanos); // locked by workers, or paused
                }
            }
        } catch (SQLException e) {
            throw db.translate(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("outbox drain interrupted", e);
        }
    }

    private void requireNotHalted() {
        if (maintenance.halted()) {
            throw new IllegalStateException(maintenance.haltReason());
        }
    }

    private static boolean drained(Position p, Instant now) {
        var c = p.cursor();
        return !p.behind() && c.targetTickId() == null && !due(c.retryDueAt(), now) && !due(c.parkedDueAt(), now);
    }

    private static boolean due(@Nullable Instant at, Instant now) {
        return at != null && !at.isAfter(now);
    }

    /**
     * One step of the group of {@code shards} in its own transaction on {@code c}; leaves {@code c} in autocommit,
     * rolled back on failure. A step that moved a row of any of them to the dead table wakes the dead-letter thread
     * once committed.
     */
    private List<ShardResult> processInOwnTransaction(Connection c, List<Integer> shards, long latest, Instant now)
            throws SQLException, InterruptedException {
        try {
            ShardProcessor.prepareTransaction(c, relay.batchTimeout());
            var results = processor.tryProcess(c, shards, latest, now);
            c.commit();
            c.setAutoCommit(true);
            // without a hook the letter is kept already
            if (deadLetters.active() && results.stream().anyMatch(ShardResult::deadLettered)) {
                deadLettered.incrementAndGet();
                var dead = deadLetterThread;
                if (dead != null) {
                    LockSupport.unpark(dead);
                }
            }
            return results;
        } catch (Throwable t) {
            rollback(c, t);
            throw t;
        }
    }

    /** {@code shards} in groups of at most {@link #MAX_GROUP} whose {@link #expected} rows fit in {@code batchSize}. */
    private List<List<Integer>> groups(List<Integer> shards) {
        var groups = new ArrayList<List<Integer>>();
        var group = new ArrayList<Integer>();
        long rows = 0;
        for (var shard : shards) {
            if (!group.isEmpty() && (group.size() == MAX_GROUP || busy(group.get(group.size() - 1)) || busy(shard)
                    || rows + expected.get(shard) > relay.batchSize())) {
                groups.add(group);
                group = new ArrayList<>();
                rows = 0;
            }
            group.add(shard);
            rows += expected.get(shard);
        }
        if (!group.isEmpty()) {
            groups.add(group);
        }
        return groups;
    }

    /**
     * Expected to read more than {@code batchSize / 16} rows: such a shard steps alone. Its step's own statements cost
     * little next to its rows, while a group of a few such shards would share one {@code batchSize} read, be cut, and
     * read the rest of its shards again (bulk at 32 shards, about 600 rows a shard per batch, lagged several ticks
     * behind stepping shards alone); grouping pays off for many shards with a few rows each.
     */
    private boolean busy(int shard) {
        return expected.get(shard) > relay.batchSize() / 16;
    }

    /** Untouched by its step: it has work, and the step delivered none of it (unread behind a cut, not attempted). */
    private static boolean untouched(ShardResult result) {
        return result.status() == ShardStatus.PROGRESS && result.delivered() == 0 && !result.caughtUp();
    }

    /** Updates {@link #expected} from the results of one step of {@code group}. */
    private void learn(List<Integer> group, List<ShardResult> results) {
        for (int i = 0; i < group.size(); i++) {
            var result = results.get(i);
            int shard = group.get(i);
            int rows = batchRows.addAndGet(shard, result.delivered());
            if (result.status() == ShardStatus.PROGRESS && !result.batchFinished()) {
                expected.set(shard, relay.batchSize()); // more rows than one read took
            } else if (result.status() == ShardStatus.PROGRESS && rows > 0) {
                expected.set(shard, rows); // its batch ended
                batchRows.set(shard, 0);
            } // else nothing new about its rows: nothing to do, paused or locked elsewhere
            if (untouched(result)) {
                // unread behind a cut or not attempted: alone next, not in a group cut again; a shard that kept the
                // call or the read from shards after it ends its batch or moves to the latest tick, so they sort ahead
                // of it in the next round
                expected.set(shard, relay.batchSize());
            }
        }
    }

    private void wakeAll() {
        for (var t : workers) {
            LockSupport.unpark(t);
        }
    }

    /** Unparks one worker parked for work ({@link Worker#awaitWork}), if any. */
    private void wakeOne() {
        for (var t : awaiting) {
            LockSupport.unpark(t);
            return;
        }
    }

    /** Joins the workers until {@code deadline}; returns whether all have finished. */
    private boolean awaitWorkers(long deadline) throws InterruptedException {
        boolean finished = true;
        for (var t : workers) {
            TimeUnit.NANOSECONDS.timedJoin(t, deadline - System.nanoTime()); // returns at once when <= 0
            finished &= !t.isAlive();
        }
        return finished;
    }

    /**
     * The dead-letter thread: a {@link DeadLetterPass#runOnce} (a run that redrove letters wakes the workers, so they
     * are ticked at once), then a park until the earliest letter is due, {@code maintenanceInterval} passes or a step
     * dead-letters a row. A failed run is retried after {@code relay.retry.initialDelay}, doubled after each failure
     * in a row up to {@code maintenanceInterval} and reset by a run that succeeds; wake-ups do not cut it short: a run
     * that fails after its hook call (its decisions could not be written) calls the hook again each time.
     */
    private void runDeadLetters() {
        var interval = config.partitions().maintenanceInterval();
        int failures = 0;
        while (!stopping) {
            long seen = deadLettered.get();
            try {
                if (deadLetters.runOnce() > 0) {
                    wake();
                }
                var due = deadLetters.untilNextDue();
                failures = 0;
                var wait = due == null || due.compareTo(interval) > 0 ? interval : due;
                park(System.nanoTime() + wait.toNanos(), () -> deadLettered.get() != seen);
            } catch (VirtualMachineError e) {
                log.error("outbox dead-letter thread stopped by a fatal error", e);
                throw e;
            } catch (Throwable t) {
                if (stopping) { // e.g. the pass closed under it
                    log.debug("outbox dead-letter pass failed while stopping", t);
                    return;
                }
                Thread.interrupted(); // not stopping: a stray interrupt is one more failure
                var backoff = Backoff.delay(failures++, relay.retry().initialDelay(), interval);
                log.warn("outbox dead-letter pass failed, retrying in {}", backoff, t);
                park(System.nanoTime() + backoff.toNanos(), () -> false);
            }
        }
    }

    private void runMaintenance() {
        try {
            maintenance.runOnce();
        } catch (VirtualMachineError e) { // rethrown, the scheduler cancels all later runs without a word
            log.error("outbox maintenance stopped by a fatal error; no further runs until restart", e);
            throw e;
        } catch (Throwable t) {
            log.error("outbox maintenance failed; next run in {}", config.partitions().maintenanceInterval(), t);
        }
    }

    /** Whether {@code failure} lost {@code c}: a connection-class SQL state, or the connection no longer valid. */
    private static boolean connectionLost(Connection c, Throwable failure) {
        for (var t = failure; t != null; t = t.getCause()) {
            if (t instanceof SQLException e && e.getSQLState() != null && e.getSQLState().startsWith("08")) {
                return true;
            }
        }
        try {
            return c.isClosed() || !c.isValid(1);
        } catch (SQLException e) {
            return true;
        }
    }

    private static void rollback(Connection c, Throwable cause) {
        try {
            c.rollback();
        } catch (SQLException | RuntimeException e) {
            cause.addSuppressed(e);
        }
    }

    /**
     * Parks until {@code deadline}, release, or (when {@code wakeable}) a wake-up after {@code seen}, a tick of this
     * relay after {@code seenTicks} or shards put back into a round after {@code seenRequeued}.
     */
    private void park(long deadline, boolean wakeable, long seen, long seenTicks, long seenRequeued) {
        park(deadline, () -> wakeable && (wakeups.get() != seen || ticksMade.get() != seenTicks
                || requeued.get() != seenRequeued));
    }

    /** Parks until {@code deadline}, release, or {@code woken}, which a waker makes true before it unparks. */
    private void park(long deadline, BooleanSupplier woken) {
        while (!stopping && !woken.getAsBoolean()) {
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                return;
            }
            LockSupport.parkNanos(this, left);
            Thread.interrupted(); // a stray interrupt must not turn parking into a spin; release() sets stopping
        }
    }

    /**
     * The candidates of one read of every shard's cursor for the latest tick {@link #latest}, shared by the workers of
     * this relay whose pass sees that tick: each takes the next group of shards in candidate order
     * ({@link Worker#candidates}, {@link #take}). A shard a step left with work (not caught up to the step's tick)
     * goes back to the end of the round current when the step ends, those the step left untouched ahead of the others
     * and alone ({@link #expected}). A shard that kept a group's call or read to itself (a slow call wound down at the
     * soft deadline, a hot shard filling the read, the default handler's stop after 3 failing keys) is behind them:
     * it ends its batch or moves on, so in the next round, which sorts by effective tick afresh, the shards it left
     * unread sort ahead of it. A new tick ends the round: the next pass reads the cursors again.
     */
    private final class Round {

        final long latest;
        /** When the cursors were read ({@code nanoTime}). */
        final long read;
        /** {@code min(MAX_GROUP, max(1, ceil(candidates / workers)))}. */
        final int groupSize;
        private final ArrayDeque<Integer> queue;
        /** In {@link #queue}. */
        private final BitSet queued = new BitSet();
        /** Put back by a step after the read: not stale however recently it was stepped. */
        private final BitSet back = new BitSet();

        Round(long latest, long read, List<Integer> candidates) {
            this.latest = latest;
            this.read = read;
            this.groupSize = Math.min(MAX_GROUP, Math.max(1,
                    (candidates.size() + relay.workers() - 1) / relay.workers()));
            this.queue = new ArrayDeque<>(candidates);
            candidates.forEach(queued::set);
        }

        /**
         * The next group, empty when the round is done: at most {@link #groupSize} shards, and no more than fit in
         * {@code batchSize} by their {@link #expected} rows (at least one; a {@link #busy} shard alone); each is
         * {@link #inFlight} until its step
         * ends. A shard another worker steps is passed over and {@link #wanted}: that step puts it into the round
         * current then. A shard a sibling stepped after the read at this round's tick or a newer one is skipped: its
         * candidacy is stale, and that step put it back if it left work. A shard {@link #isolated} is a group of its
         * own.
         */
        synchronized List<Integer> take() {
            var group = new ArrayList<Integer>(Math.min(groupSize, queue.size()));
            long rows = 0;
            while (group.size() < groupSize && !queue.isEmpty()) {
                int shard = queue.peekFirst();
                if (inFlight.get(shard) == 1) {
                    wanted.set(shard, 1); // a want left over when the step ended meanwhile costs one spare put-back
                    if (inFlight.get(shard) == 1) { // else its step ended meanwhile, maybe without seeing wanted
                        drop(shard);
                        continue;
                    }
                }
                if (!back.get(shard) && stepStarted.get(shard) - read > 0 && steppedTick.get(shard) >= latest) {
                    drop(shard);
                    continue;
                }
                boolean alone = isolated.get(shard) == 1 || busy(shard);
                if (!group.isEmpty() && (alone || rows + expected.get(shard) > relay.batchSize())) {
                    break;
                }
                if (!inFlight.compareAndSet(shard, 0, 1)) { // taken from another round just now: the check above
                    continue;
                }
                drop(shard);
                group.add(shard);
                rows += expected.get(shard);
                if (alone) {
                    break;
                }
            }
            return group;
        }

        private void drop(int shard) {
            queue.pollFirst();
            queued.clear(shard);
            back.clear(shard);
        }

        /**
         * Puts {@code shards} back at the end, in their order, as put back by a step (not stale); a shard still queued
         * keeps its place.
         */
        synchronized void putBack(List<Integer> shards) {
            for (var shard : shards) {
                back.set(shard);
                if (!queued.get(shard)) {
                    queue.addLast(shard);
                    queued.set(shard);
                }
            }
        }
    }

    private final class Worker implements Runnable {

        private long lastWarn = System.nanoTime() - WARN_INTERVAL;
        private int suppressed;

        @Override
        public void run() {
            while (!stopping) {
                long seen = wakeups.get();
                long seenTicks = ticksMade.get();
                long seenRequeued = requeued.get();
                if (maintenance.halted()) { // xid regression: rows would be skipped, take no shards
                    park(System.nanoTime() + idleNanos, false, seen, seenTicks, seenRequeued);
                    continue;
                }
                boolean worked;
                try {
                    worked = pass(seen);
                } catch (VirtualMachineError e) {
                    log.error("outbox relay worker {} stopped by a fatal error", Thread.currentThread().getName(), e);
                    throw e;
                } catch (Throwable t) { // outside a shard step (tick, cursor read, connection)
                    if (stopping) {
                        log.debug("outbox relay worker failed while stopping", t);
                        if (t instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        return;
                    }
                    Thread.interrupted(); // not stopping: a stray interrupt is one more failure
                    warn("outbox relay worker failed, retrying in " + relay.idlePollInterval(), t);
                    // wake-ups do not cut it short
                    park(System.nanoTime() + idleNanos, false, seen, seenTicks, seenRequeued);
                    continue;
                }
                if (!worked) {
                    awaitWork(seen, seenTicks, seenRequeued);
                }
            }
        }

        /**
         * Parks after a pass without work until release, a tick of this relay, shards put back into a round, the next
         * tick attempt being due, or a wake-up that can make that attempt now. A wake-up within
         * {@code minTickInterval} of the last attempt only registers its commit (the attempt comes after
         * {@code minTickInterval}, so its latency stays within {@code 2 × minTickInterval}): no tick of this relay came
         * since, so a pass would find the round of the tick this worker just saw, and a high commit rate would turn
         * into one pass per commit and worker.
         */
        private void awaitWork(long seen, long seenTicks, long seenRequeued) {
            var self = Thread.currentThread();
            awaiting.add(self);
            try {
                awaitWork0(seen, seenTicks, seenRequeued);
            } finally {
                awaiting.remove(self);
            }
        }

        private void awaitWork0(long seen, long seenTicks, long seenRequeued) {
            while (true) {
                park(nextTickDue(), true, seen, seenTicks, seenRequeued);
                long woken = wakeups.get();
                if (stopping || ticksMade.get() != seenTicks || requeued.get() != seenRequeued || woken == seen
                        || tickAttemptDue(woken)) {
                    return;
                }
                seen = woken;
            }
        }

        /**
         * Steps groups of the current round until it has none left (one cursor read per latest tick, see
         * {@link #readRound}); returns whether a step did work (progress or a pause).
         */
        private boolean pass(long seen) throws Exception {
            try (var c = db.newConnection()) {
                c.setAutoCommit(true);
                tickIfDue(c, seen);
                readRound(c);
                boolean worked = false;
                while (!stopping && !maintenance.halted()) {
                    // the tick attempts of a long pass (a backlog keeps its round from running out), each followed by
                    // a look at the latest tick: without them ticks would wait for the pass, and each would hold ever
                    // more rows, read again in full by every chunk of a batch
                    if (tickIfDue(c, wakeups.get())) {
                        readRound(c);
                    }
                    var r = round; // a sibling may have read a newer tick: its round is the one to work on
                    var group = r.take();
                    if (group.isEmpty()) {
                        break;
                    }
                    worked |= step(c, r, group);
                }
                return worked;
            }
        }

        /**
         * Makes sure {@link #round} is of the latest tick: reads the latest tick and, when the round is of another, the
         * cursors, under {@link #roundLock}, so the workers of this relay read the cursors once per tick and never
         * replace a round with one of an older tick.
         */
        private void readRound(Connection c) throws SQLException, InterruptedException {
            roundLock.lockInterruptibly();
            try {
                long latest = ticks.latest(c).id();
                var r = round;
                if (r == null || r.latest != latest) {
                    var selected = Instant.now().truncatedTo(ChronoUnit.MICROS);
                    long read = System.nanoTime();
                    round = new Round(latest, read, candidates(cursors.positions(c, latest), selected));
                }
            } finally {
                roundLock.unlock();
            }
        }

        /**
         * One step of {@code group}, taken from a round ({@link Round#take}): {@link #stepHeld}, then the shards it
         * left with work go into the round current now (not caught up to the step's tick, untouched ones first), with
         * any a newer round passed over meanwhile ({@link #wanted}), and one parked worker is woken for them.
         */
        private boolean step(Connection c, Round r, List<Integer> group) throws Exception {
            var back = new ArrayList<Integer>();
            try {
                return stepHeld(c, r, group, back);
            } finally {
                for (var shard : group) {
                    inFlight.set(shard, 0);
                }
                for (var shard : group) { // after inFlight: a take that saw it in flight set wanted before
                    if (wanted.getAndSet(shard, 0) == 1 && !back.contains(shard)) {
                        back.add(shard);
                    }
                }
                if (!back.isEmpty()) {
                    round.putBack(back);
                    requeued.incrementAndGet();
                    wakeOne();
                }
            }
        }

        /**
         * One step of {@code group} in its own transaction; adds the shards it left with work to {@code back}. A step
         * of several shards that fails rolls back for all of them, so every shard of the group is stepped again and the
         * records its handler call delivered are delivered again (at least once, as after any rolled-back step); it is
         * redone at once with each shard alone, so a shard that keeps failing holds up no neighbour. One that fails
         * alone is {@link #isolated}, logged, and retried in a later pass. A failure of the connection ends the pass,
         * as a failure outside a step does.
         */
        private boolean stepHeld(Connection c, Round r, List<Integer> group, List<Integer> back) throws Exception {
            long started = System.nanoTime();
            for (var shard : group) {
                stepStarted.set(shard, started);
                steppedTick.set(shard, r.latest);
            }
            // a fresh clock per step: a long pass must not pause shards or schedule retries from a stale one; a later
            // clock keeps a selected shard unpaused or due (a clock stepped back only yields NO_WORK)
            var now = Instant.now().truncatedTo(ChronoUnit.MICROS);
            List<ShardResult> results;
            try {
                results = processInOwnTransaction(c, group, r.latest, now);
            } catch (InterruptedException | VirtualMachineError e) {
                throw e;
            } catch (Throwable e) { // rolled back
                if (stopping || connectionLost(c, e)) { // run() logs it (at debug while stopping)
                    throw e;
                }
                if (group.size() == 1) {
                    isolated.set(group.get(0), 1);
                    warn("outbox shard " + group.get(0) + " step failed, retried in a later pass", e);
                    return false;
                }
                log.debug("outbox step of shards {} failed; stepping each alone", group, e);
                boolean worked = false;
                for (var shard : group) {
                    worked |= stepHeld(c, r, List.of(shard), back);
                }
                return worked;
            }
            learn(group, results);
            boolean worked = false;
            var progressed = new ArrayList<Integer>();
            for (int i = 0; i < group.size(); i++) {
                var result = results.get(i);
                boolean did = result.status() == ShardStatus.PROGRESS || result.status() == ShardStatus.PAUSED;
                worked |= did;
                if (group.size() == 1 && did) {
                    isolated.set(group.get(0), 0);
                }
                if (result.status() == ShardStatus.PROGRESS && !result.caughtUp()) {
                    (untouched(result) ? back : progressed).add(group.get(i));
                }
            }
            back.addAll(progressed);
            return worked;
        }

        /**
         * {@link Position#behind() Behind} the latest tick (a tick after its effective tick holds its rows), mid-batch
         * or with a due retry head or parked row, and not paused; lowest effective tick first, ties from a random
         * shard on, so no shard is always last. All of it comes from the database, so every instance sees the same
         * candidates, whichever instance made the ticks.
         */
        private List<Integer> candidates(List<Position> all, Instant now) {
            int shards = config.shards();
            int offset = ThreadLocalRandom.current().nextInt(shards);
            return all.stream()
                    .filter(p -> p.behind() || p.cursor().targetTickId() != null
                            || due(p.cursor().retryDueAt(), now) || due(p.cursor().parkedDueAt(), now))
                    .filter(p -> p.cursor().pausedUntil() == null || !p.cursor().pausedUntil().isAfter(now))
                    .sorted(Comparator.comparingLong(Position::effectiveTickId)
                            .thenComparingInt(p -> Math.floorMod(p.cursor().shard() - offset, shards)))
                    .map(p -> p.cursor().shard())
                    .toList();
        }

        private void warn(String what, Throwable t) {
            long now = System.nanoTime();
            if (now - lastWarn < WARN_INTERVAL) {
                suppressed++;
                return;
            }
            log.warn("{} ({} failures since the previous report not logged)", what, suppressed, t);
            lastWarn = now;
            suppressed = 0;
        }
    }

    /**
     * A tick attempt when {@code idlePollInterval} passed since the last one, or when a wake-up's commit is
     * not yet covered and {@code minTickInterval} passed since the last attempt (so under load at most one attempt per
     * {@code minTickInterval}, a wake-up waiting at most that long). The attempt skips its insert when the newest tick
     * is younger than {@code minTickInterval} with a wake-up pending, else younger than {@link #idleTickAge}: an idle
     * tick of another instance then stands for this one's. An inserted tick covers the commits seen before the
     * attempt; one {@link Ticks#make} skipped for a younger tick covers only those seen {@code minTickInterval} before
     * it, since that younger tick may predate the rest, which therefore get another attempt {@code minTickInterval}
     * later. Each inserted tick wakes all workers; one refused for an xid regression refreshes the halt at once.
     * Returns whether it made the attempt.
     */
    private boolean tickIfDue(Connection c, long seen) throws SQLException {
        long attempt;
        Duration youngest;
        synchronized (tickState) {
            long now = System.nanoTime();
            if (!attemptDue(seen, now)) {
                return false;
            }
            lastTickAttempt = attempt = now;
            youngest = wakePending ? relay.minTickInterval() : idleTickAge;
        }
        var made = ticks.tick(c, youngest);
        if (made == Ticks.Made.REGRESSED) {
            maintenance.checkHalt(); // the halt it recorded: the workers stop now, not at the next maintenance run
        }
        boolean inserted = made == Ticks.Made.INSERTED;
        synchronized (tickState) {
            if (wakePending && wakeAt - attempt <= (inserted ? 0 : -minTickNanos)) {
                wakePending = false;
            }
        }
        if (inserted) {
            ticksMade.incrementAndGet();
            wakeAll();
        }
        return true;
    }

    /** Registers the wake-ups up to {@code seen}; whether a tick attempt is due now. */
    private boolean tickAttemptDue(long seen) {
        synchronized (tickState) {
            return attemptDue(seen, System.nanoTime());
        }
    }

    /** Under {@code tickState}. */
    private boolean attemptDue(long seen, long now) {
        if (seen > tickedWakeups) { // committed before now
            tickedWakeups = seen;
            wakePending = true;
            wakeAt = now;
        }
        long elapsed = now - lastTickAttempt;
        return elapsed >= idleNanos || (wakePending && elapsed >= minTickNanos);
    }

    /** When the next tick attempt is due without a further wake-up. */
    private long nextTickDue() {
        synchronized (tickState) {
            return lastTickAttempt + (wakePending ? Math.min(minTickNanos, idleNanos) : idleNanos);
        }
    }
}
