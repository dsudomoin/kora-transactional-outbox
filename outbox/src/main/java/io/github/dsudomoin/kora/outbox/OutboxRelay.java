package io.github.dsudomoin.kora.outbox;

import io.github.dsudomoin.kora.outbox.internal.Backoff;
import io.github.dsudomoin.kora.outbox.internal.Cursors;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Position;
import io.github.dsudomoin.kora.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.ShardProcessor;
import io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardResult;
import io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus;
import io.github.dsudomoin.kora.outbox.internal.Ticks;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.application.graph.Lifecycle;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * The background relay: {@code relay.workers} daemon threads {@code outbox-relay-N}, each repeatedly making
 * a tick when due, reading every shard's cursor once and running one {@link ShardProcessor} step per candidate shard,
 * each in its own transaction; plus maintenance right after start and then every
 * {@code partitions.maintenanceInterval} on the daemon thread {@code outbox-maintenance}. Neither starts when
 * {@code relay.enabled} is false; {@link #drain()} works either way. Idle workers park until {@link #wake()}, a new tick
 * made by this relay, or the next tick attempt is due, so an idle relay does not spin; an idle attempt makes a tick
 * only when the newest tick of any instance is about {@code idlePollInterval} old, so an idle cluster makes about one
 * tick per {@code idlePollInterval}. A shard is a candidate only when a tick after its effective tick holds its rows
 * (each tick records the shards that gained rows, see {@link Ticks}), its batch is in progress or a retry head or
 * parked row is due, so a shard without new rows costs no step: maintenance's checkpoint moves its effective tick on
 * (see {@link Cursors}), which keeps tick cleanup and partition drops going. Every tick this relay makes wakes all its
 * workers, which spread the tick's shards
 * with new rows between them. A wake-up that cannot make a tick yet ({@code minTickInterval}) only brings the tick
 * forward, so under a high commit rate the workers read cursors per tick, not per commit. While
 * {@link Maintenance#halted()} the workers take no shards. Dead letters go to the {@link DeadLetterPass} on the daemon
 * thread {@code outbox-dead-letters} (also not started when the relay is disabled; {@link #drain()} ends with one run
 * of it): a run right after start, then after each committed step that moved a row to the dead table, when the
 * earliest letter is due, and at least every {@code maintenanceInterval}; so a slow or failing hook never holds up a
 * shard step.
 */
public final class OutboxRelay implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final AtomicInteger THREADS = new AtomicInteger();
    private static final long WARN_INTERVAL = TimeUnit.SECONDS.toNanos(10);
    /** How long interrupted workers get to roll back after {@code shutdownTimeout}. */
    private static final long INTERRUPT_GRACE = TimeUnit.SECONDS.toNanos(1);

    private final JdbcConnectionFactory db;
    private final OutboxConfig config;
    private final OutboxConfig.RelayConfig relay;
    private final Maintenance maintenance;
    private final Ticks ticks;
    private final Cursors cursors;
    private final ShardProcessor processor;
    private final DeadLetterPass deadLetters;
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
    public OutboxRelay(JdbcConnectionFactory db, OutboxConfig config, OutboxSchema schema, OutboxHandler handler,
            OutboxMetrics metrics, @Nullable Tracer tracer, Maintenance maintenance, DeadLetterPass deadLetters) {
        OutboxSchema.validate(config);
        this.db = db;
        this.config = config;
        this.relay = config.relay();
        this.maintenance = maintenance;
        this.ticks = new Ticks(schema.names(), config.shards());
        this.cursors = new Cursors(schema.names(), config.shards());
        this.deadLetters = deadLetters;
        this.processor = new ShardProcessor(schema.names(), config, handler, metrics, tracer, deadLetters.active());
        this.idleNanos = relay.idlePollInterval().toNanos();
        this.minTickNanos = relay.minTickInterval().toNanos();
        this.idleTickAge = Duration.ofNanos(Math.max(minTickNanos, idleNanos - idleNanos / 4));
        this.lastTickAttempt = System.nanoTime() - idleNanos; // the first pass ticks
        this.stepStarted = new AtomicLongArray(config.shards());
        long created = System.nanoTime();
        for (int i = 0; i < config.shards(); i++) {
            stepStarted.set(i, created);
        }
    }

    /** Starts the workers, the dead-letter thread and maintenance, unless {@code relay.enabled} is false. */
    @Override
    public void init() {
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
     * attempt, and are handed again later.
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
        try {
            long deadline = System.nanoTime() + relay.shutdownTimeout().toNanos();
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
            processor.close();
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
            while (!ticks.make(c, Duration.ZERO)) { // false only on the 1 s tick lock timeout
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
                for (var cursor : pending) {
                    var result = processInOwnTransaction(c, cursor.shard(), target, now);
                    delivered += result.delivered();
                    worked |= result.status() == ShardStatus.PROGRESS || result.status() == ShardStatus.PAUSED;
                    if (result.delivered() > 0) {
                        progressed = System.nanoTime();
                    }
                }
                if (!worked) {
                    TimeUnit.NANOSECONDS.sleep(idleNanos); // locked by workers, or paused
                }
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("outbox drain interrupted", e);
        }
    }

    private void requireNotHalted() {
        if (maintenance.halted()) {
            throw new IllegalStateException("outbox halted: maintenance found the xid counter below a tick's xmax"
                    + " (see its ERROR log and README \"xid regression recovery\")");
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
     * One shard step in its own transaction on {@code c}; leaves {@code c} in autocommit, rolled back on failure. A
     * step that moved a row to the dead table wakes the dead-letter thread once committed.
     */
    private ShardResult processInOwnTransaction(Connection c, int shard, long latest, Instant now)
            throws SQLException, InterruptedException {
        try {
            ShardProcessor.prepareTransaction(c, relay.batchTimeout());
            var result = processor.tryProcess(c, shard, latest, now);
            c.commit();
            c.setAutoCommit(true);
            if (result.deadLettered() && deadLetters.active()) { // without a hook the letter is kept already
                deadLettered.incrementAndGet();
                var dead = deadLetterThread;
                if (dead != null) {
                    LockSupport.unpark(dead);
                }
            }
            return result;
        } catch (Throwable t) {
            rollback(c, t);
            throw t;
        }
    }

    private void wakeAll() {
        for (var t : workers) {
            LockSupport.unpark(t);
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

    private static void rollback(Connection c, Throwable cause) {
        try {
            c.rollback();
        } catch (SQLException | RuntimeException e) {
            cause.addSuppressed(e);
        }
    }

    /**
     * Parks until {@code deadline}, release, or (when {@code wakeable}) a wake-up after {@code seen} or a tick of this
     * relay after {@code seenTicks}.
     */
    private void park(long deadline, boolean wakeable, long seen, long seenTicks) {
        park(deadline, () -> wakeable && (wakeups.get() != seen || ticksMade.get() != seenTicks));
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

    private final class Worker implements Runnable {

        /** Rotates shards of equal tick so the workers start on different shards. */
        private final int offset = ThreadLocalRandom.current().nextInt(config.shards());
        private long lastWarn = System.nanoTime() - WARN_INTERVAL;
        private int suppressed;

        @Override
        public void run() {
            while (!stopping) {
                long seen = wakeups.get();
                long seenTicks = ticksMade.get();
                if (maintenance.halted()) { // xid regression: rows would be skipped, take no shards
                    park(System.nanoTime() + idleNanos, false, seen, seenTicks);
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
                    park(System.nanoTime() + idleNanos, false, seen, seenTicks); // wake-ups do not cut it short
                    continue;
                }
                if (!worked) {
                    awaitWork(seen, seenTicks);
                }
            }
        }

        /**
         * Parks after a pass without work until release, a tick of this relay, the next tick attempt being due, or a
         * wake-up that can make that attempt now. A wake-up within {@code minTickInterval} of the last attempt only
         * registers its commit (the attempt comes after {@code minTickInterval}, so its latency stays within
         * {@code 2 × minTickInterval}): a pass would re-read the cursors this worker just read, as no tick of this
         * relay came since, and a high commit rate would turn into one cursor read per commit and worker.
         */
        private void awaitWork(long seen, long seenTicks) {
            while (true) {
                park(nextTickDue(), true, seen, seenTicks);
                long woken = wakeups.get();
                if (stopping || ticksMade.get() != seenTicks || woken == seen || tickAttemptDue(woken)) {
                    return;
                }
                seen = woken;
            }
        }

        /**
         * Runs one shard step per candidate of one cursor read, each in its own transaction; returns whether a step
         * did work (progress or a pause).
         */
        private boolean pass(long seen) throws Exception {
            try (var c = db.newConnection()) {
                c.setAutoCommit(true);
                tickIfDue(c, seen);
                long latest = ticks.latest(c).id();
                var selected = Instant.now().truncatedTo(ChronoUnit.MICROS);
                long read = System.nanoTime();
                boolean worked = false;
                for (var shard : candidates(cursors.positions(c, latest), selected)) {
                    if (stopping) {
                        break;
                    }
                    if (stepStarted.get(shard) - read > 0) {
                        // a sibling stepped it after this cursor read, so its candidacy is stale; the sibling's pass
                        // worked (or failed) and re-reads, so any work left there is not lost
                        continue;
                    }
                    stepStarted.set(shard, System.nanoTime());
                    // a fresh clock per step: a long pass must not pause shards or schedule retries from a stale one;
                    // a later clock keeps a selected shard unpaused or due (a clock stepped back only yields NO_WORK)
                    var now = Instant.now().truncatedTo(ChronoUnit.MICROS);
                    try {
                        var status = processInOwnTransaction(c, shard, latest, now).status();
                        worked |= status == ShardStatus.PROGRESS || status == ShardStatus.PAUSED;
                    } catch (InterruptedException | VirtualMachineError e) {
                        throw e;
                    } catch (Throwable e) { // rolled back; a stuck shard sorts first and must not starve the rest
                        if (stopping) { // e.g. the processor closed under it: run() logs it at debug
                            throw e;
                        }
                        warn("outbox shard " + shard + " step failed, retried in a later pass", e);
                    }
                }
                return worked;
            }
        }

        /**
         * {@link Position#behind() Behind} the latest tick (a tick after its effective tick holds its rows), mid-batch
         * or with a due retry head or parked row, and not paused; lowest effective tick first. All of it comes from the
         * database, so every instance sees the same candidates, whichever instance made the ticks.
         */
        private List<Integer> candidates(List<Position> all, Instant now) {
            int shards = config.shards();
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
     * later. Each inserted tick wakes all workers.
     */
    private void tickIfDue(Connection c, long seen) throws SQLException {
        long attempt;
        Duration youngest;
        synchronized (tickState) {
            long now = System.nanoTime();
            if (!attemptDue(seen, now)) {
                return;
            }
            lastTickAttempt = attempt = now;
            youngest = wakePending ? relay.minTickInterval() : idleTickAge;
        }
        boolean inserted = ticks.make(c, youngest);
        synchronized (tickState) {
            if (wakePending && wakeAt - attempt <= (inserted ? 0 : -minTickNanos)) {
                wakePending = false;
            }
        }
        if (inserted) {
            ticksMade.incrementAndGet();
            wakeAll();
        }
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
