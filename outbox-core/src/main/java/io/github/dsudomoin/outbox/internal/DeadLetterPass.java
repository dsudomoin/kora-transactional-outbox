package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.DeadLetterDecision;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.internal.Sql.SqlFunction1;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hands pending dead letters to the {@link OutboxDeadLetterHandler} and redrives scheduled ones, on its own connection
 * and the hook's own daemon thread {@code outbox-dead-letter-hook}, never a relay thread or transaction. Each batch is
 * one transaction (read committed, {@code idle_in_transaction_session_timeout} = hook timeout + 30 s): claim up to
 * {@code deadLetters.batchSize} due letters {@code for update skip locked}, so no two passes on any instance hand one
 * letter at once; call the hook, bounded by {@code deadLetters.timeout}, while the claim holds them; commit its
 * decisions, or, after a failed call, a backoff for every letter of it. That transaction takes no key lock: a
 * {@code redrive} commits as {@code redriveAt(now)}, so a decision is never lost and the hook is called once for it.
 * Scheduled letters due are then redriven, a batch per transaction ({@link Resender}, the {@code <redriven>} count,
 * the dead rows deleted). Their key locks are taken without waiting ({@code pg_try_advisory_xact_lock}): a letter
 * whose key is held, by an open {@code send} of it, is due again after {@code relay.retry.initialDelay} while the
 * others of its batch go on, and the phase ends with that batch. Right after the claim the batch's
 * {@code lock_timeout} becomes {@link XidPartitions#lockWait}: it bounds each lock wait of the batch, not their
 * sum, and while one lasts the writers of the keys the batch holds wait too, so a batch that meets several held
 * table locks stalls them for that many waits. A batch that fails after its claim rolls back, and those of its
 * letters still due are then due again after {@code initialDelay} (none another pass decided meanwhile), so they hold
 * back no letter behind them. In the redrive phase a lock timeout (55P03) is a WARN, at most once a minute, and ends
 * the phase until the next pass; any other failure, and any failure of a hand-over (a lock timeout writing the hook's
 * decisions included: the hook is called again for them), propagates. Hook failures have a WARN of their own, at
 * most once a minute too. Until {@code <dead>_due} exists (an upgraded dead table with rows gets it from
 * {@link Maintenance}) the claim and the other {@code due_at} queries are as correct, only slower: each pass scans the
 * whole dead table.
 */
public final class DeadLetterPass {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPass.class);
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    /** How long a call cut at the timeout gets to end on its interrupt before the hook counts as ignoring it. */
    private static final Duration INTERRUPT_GRACE = Duration.ofSeconds(1);

    private final OutboxTransactions db;
    @Nullable
    private final OutboxDeadLetterHandler hook;
    private final boolean active;
    private final OutboxTelemetry telemetry;
    private final DeadLetters rows;
    private final String dead;
    private final int batchSize;
    private final Duration timeout;
    private final Duration initialDelay;
    private final Duration maxDelay;
    private final String claim;
    private final String keep;
    private final String schedule;
    private final String fail;
    private final String defer;
    private final String keepPending;
    private final String untilNextDue;
    private final ThreadPoolExecutor executor;
    /** One {@link #runOnce} at a time; not {@code synchronized}, which pins a virtual thread's carrier (JDK 21+). */
    private final ReentrantLock running = new ReentrantLock();
    private final Warnings hookFailures = new Warnings();
    private final Warnings lockTimeouts = new Warnings();
    private volatile boolean closed;
    /** A cut call outlived {@link #INTERRUPT_GRACE} and its ERROR is out; cleared once a call returns in time. */
    private final AtomicBoolean ignoresInterrupts = new AtomicBoolean();

    /** A committed hand-over: the letters claimed; the call's failure, or the decisions it committed. */
    private record Batch(int claimed, @Nullable Throwable failure, List<DeadLetterDecision> decisions) {}

    public DeadLetterPass(OutboxTransactions db, TableNames names, OutboxConfig config,
            @Nullable OutboxDeadLetterHandler hook, OutboxTelemetry telemetry) {
        OutboxSchema.validate(config);
        this.db = db;
        this.hook = hook;
        this.active = hook != null && hook.active(); // read once: the relay's steps write by it
        this.telemetry = telemetry;
        this.rows = new DeadLetters(names, config);
        this.dead = names.dead();
        this.batchSize = config.deadLetters().batchSize();
        this.timeout = config.deadLetters().timeout();
        this.initialDelay = config.relay().retry().initialDelay();
        this.maxDelay = config.relay().retry().maxDelay();
        claim = "select " + DeadLetters.COLUMNS + " from " + dead + " where status = ? and due_at <= now()"
                + " order by due_at, id limit ? for update skip locked";
        keep = "update " + dead + " set status = 'kept', due_at = null where id = any(?)";
        schedule = "update " + dead + " set status = 'scheduled',"
                + " due_at = least(greatest(?, now()), now() + interval '7 days') where id = ?";
        // from the failure, not the claim (now()): a call cut at the timeout took that long already
        fail = "update " + dead + " set handler_attempts = handler_attempts + 1, last_handler_error = ?,"
                + " due_at = clock_timestamp() + make_interval(secs => ?) where id = ?";
        // only letters still due: one another pass decided since the batch rolled back keeps its decision
        defer = "update " + dead + " set due_at = now() + make_interval(secs => ?) where id = any(?)"
                + " and due_at <= now()";
        // due_at is set on every pending letter: the partial index finds them, not a scan of all the kept ones. None
        // while an instance with an active hook is live: a rolling deploy that adds one leaves them to it
        keepPending = "update " + dead + " set status = 'kept', due_at = null where id in (select id from " + dead
                + " where status = 'pending' and due_at is not null for update skip locked) and not exists (select 1"
                + " from " + names.instance() + " where hook_active and expires_at > now())";
        untilNextDue = "select (extract(epoch from min(due_at) - now()) * 1000000)::bigint from " + dead
                + " where due_at > now()";
        this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
            var t = new Thread(r, "outbox-dead-letter-hook");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * A hook is set and {@link OutboxDeadLetterHandler#active()}, as read once when this pass was built; otherwise
     * dead letters are written kept, with no call.
     */
    public boolean active() {
        return active;
    }

    /**
     * With an active hook, hands due pending letters to it batch after batch, until a batch is not full or a call fails
     * (its backoff then holds the hook back, not just its letters); without one, makes every pending letter kept,
     * unless an instance with an active hook is live: its {@link Heartbeat} row has not expired. Then
     * redrives the due scheduled letters, those just decided {@code redrive} included. Returns the letters this run
     * redrove: the caller wakes the relay when there are any. One run at a time per pass, so a second caller's hook
     * call never waits for the first one's inside its own timeout; the wait for it is interruptible. A run that starts
     * after {@link #close()}, as one queued behind another may, does nothing and returns 0.
     */
    public int runOnce() {
        try {
            running.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("outbox dead-letter pass interrupted", e);
        }
        try {
            return closed ? 0 : run();
        } finally {
            running.unlock();
        }
    }

    /** One run under {@link #running}. */
    private int run() {
        try (var c = db.newConnection()) {
            if (active()) {
                handOver(c);
            } else {
                inTx(c, tx -> {
                    try (var st = tx.createStatement()) {
                        return st.executeUpdate(keepPending);
                    }
                });
            }
            return redriveScheduled(c);
        } catch (SQLException e) {
            throw db.translate(e);
        }
    }

    /**
     * How long until the earliest {@code due_at} still ahead, by the database clock (which every {@code due_at} is
     * set by), so a skewed local clock does not turn the wait into a spin; null when none. A letter due already was
     * handled by the last run, is held by another pass, which decides it or sets its backoff, or waits behind a
     * batch that deferred a held key, which is due again within {@code initialDelay}.
     */
    @Nullable
    public Duration untilNextDue() {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            try (var st = c.createStatement(); var rs = st.executeQuery(untilNextDue)) {
                rs.next();
                long micros = rs.getLong(1);
                return rs.wasNull() ? null : Duration.of(micros, ChronoUnit.MICROS);
            }
        } catch (SQLException e) {
            throw db.translate(e);
        }
    }

    /**
     * Stops the hook thread, interrupting a call in progress: its letters roll back, without a failed attempt, and
     * those still due are due again after {@code initialDelay}. A run in progress stops after its batch; a later
     * {@link #runOnce} does nothing.
     */
    public void close() {
        closed = true;
        executor.shutdownNow();
    }

    private void handOver(Connection c) throws SQLException {
        while (!closed) {
            var ids = new ArrayList<Long>();
            Batch batch;
            try {
                batch = inTx(c, tx -> handOverBatch(tx, ids));
            } catch (SQLException | RuntimeException e) {
                // even a lock timeout: the hook may have answered, so it is called again, and the caller backs off
                deferStillDue(c, ids, e);
                throw e;
            }
            if (batch.failure() != null) {
                telemetry.deadLetterHookFailed();
                hookFailures.warn("outbox dead-letter hook failed on " + batch.claimed() + " letters of " + dead
                        + "; they stay pending and are handed to it again after a backoff", batch.failure());
                return;
            }
            for (var decision : batch.decisions()) {
                telemetry.deadLetterDecision(name(decision));
            }
            if (batch.claimed() < batchSize) {
                return;
            }
        }
    }

    /** {@code ids} gets the claimed letters' ids, for {@link #deferStillDue} should the transaction fail later. */
    private Batch handOverBatch(Connection tx, List<Long> ids) throws SQLException {
        var claimed = claim(tx, "pending");
        claimed.forEach(c -> ids.add(c.letter().id()));
        if (claimed.isEmpty()) {
            return new Batch(0, null, List.of());
        }
        List<DeadLetterDecision> decisions;
        try {
            decisions = call(claimed.stream().map(DeadLetters.Claimed::letter).toList());
        } catch (ExecutionException | TimeoutException e) {
            if (closed) { // cut by close(), most likely by its interrupt: no failure of the hook
                throw new IllegalStateException("outbox dead-letter pass closed during a hook call", e);
            }
            if (e instanceof ExecutionException x) {
                return failed(tx, claimed, x.getCause());
            }
            return failed(tx, claimed, new TimeoutException("outbox dead-letter hook exceeded"
                    + " outbox.deadLetters.timeout " + timeout));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("outbox dead-letter pass interrupted", e);
        }
        var kept = new ArrayList<Long>();
        var discarded = new ArrayList<Long>();
        int scheduled = 0;
        try (var ps = tx.prepareStatement(schedule)) {
            for (int i = 0; i < claimed.size(); i++) {
                var decision = decisions.get(i);
                var letter = claimed.get(i);
                if (decision instanceof DeadLetterDecision.Keep) {
                    kept.add(letter.letter().id());
                } else if (decision instanceof DeadLetterDecision.Discard) {
                    discarded.add(letter.letter().id());
                } else {
                    // the database clamps it to [now, now + 7 days], so redrive = redriveAt(EPOCH) = redriveAt(now);
                    // Postgres' range first, so Instant.MAX is no failing batch
                    var at = decision instanceof DeadLetterDecision.RedriveAt r ? r.at() : Instant.EPOCH;
                    var ceiling = Instant.now().plus(8, ChronoUnit.DAYS);
                    ps.setObject(1, (at.isAfter(ceiling) ? ceiling : at.isBefore(Instant.EPOCH) ? Instant.EPOCH : at)
                            .atOffset(ZoneOffset.UTC));
                    ps.setLong(2, letter.letter().id());
                    ps.addBatch();
                    scheduled++;
                }
            }
            if (scheduled > 0) {
                ps.executeBatch();
            }
        }
        DeadLetters.byIds(tx, keep, kept);
        rows.delete(tx, discarded);
        return new Batch(claimed.size(), null, decisions);
    }

    /**
     * Due scheduled letters, batch after batch until one is not full or deferred a held key's letter, which is due
     * again within {@code initialDelay}: looping on would claim it again and again while the key is held. Returns how
     * many were redriven.
     */
    private int redriveScheduled(Connection c) throws SQLException {
        int redriven = 0;
        while (!closed) {
            var ids = new ArrayList<Long>();
            DeadLetters.Split split;
            try {
                split = inTx(c, tx -> redriveBatch(tx, ids));
            } catch (SQLException | RuntimeException e) {
                deferStillDue(c, ids, e);
                if (e instanceof SQLException s && blocked(s)) {
                    // no key lock is waited for, so it was a table's lock, which the batches after it would wait for
                    return redriven;
                }
                throw e;
            }
            redriven += split.ready().size();
            if (ids.size() < batchSize || !split.held().isEmpty()) {
                return redriven;
            }
        }
        return redriven;
    }

    /**
     * One batch of due scheduled letters, but those whose key is held, which are deferred; {@code ids} as claimed. Its
     * waits are bounded by {@link XidPartitions#lockWait} from right after the claim: from the key locks on, writers of
     * those keys wait with it.
     */
    private DeadLetters.Split redriveBatch(Connection tx, List<Long> ids) throws SQLException {
        var claimed = claim(tx, "scheduled");
        claimed.forEach(c -> ids.add(c.letter().id()));
        if (claimed.isEmpty()) {
            return new DeadLetters.Split(List.of(), List.of());
        }
        rows.limitLockWaits(tx);
        var split = rows.tryKeyLocks(tx, claimed);
        defer(tx, split.held().stream().map(c -> c.letter().id()).toList());
        rows.redrive(tx, split.ready()); // its key locks are ours already: Resender takes them again without waiting
        return split;
    }

    /**
     * After a batch rolled back on {@code failure}: its claimed letters, if any, that are still due are due again
     * after {@code initialDelay}, set in a fresh transaction, so they hold back no letter behind them and
     * {@link #untilNextDue} finds them; one another pass has decided since the rollback keeps that decision. A failure
     * of that is added to {@code failure}, which the caller handles.
     */
    private void deferStillDue(Connection c, List<Long> ids, Exception failure) {
        if (ids.isEmpty()) {
            return;
        }
        try {
            inTx(c, tx -> {
                defer(tx, ids);
                return null;
            });
        } catch (SQLException | RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    private void defer(Connection tx, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        try (var ps = tx.prepareStatement(defer)) {
            ps.setDouble(1, initialDelay.toNanos() / 1e9);
            ps.setArray(2, tx.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        }
    }

    /**
     * Calls the hook on its thread within {@code timeout}; the answer is read and checked there, so a lazy list runs
     * hook code inside the deadline too and an invalid one fails as the hook's {@link ExecutionException}. A call cut
     * at the timeout that still runs {@link #INTERRUPT_GRACE} after its interrupt holds the one hook thread: an ERROR,
     * once until a call returns in time, as no letter is decided on this instance meanwhile.
     */
    private List<DeadLetterDecision> call(List<DeadLetter> letters)
            throws InterruptedException, ExecutionException, TimeoutException {
        Future<List<DeadLetterDecision>> call;
        var ended = new CountDownLatch(1);
        try {
            call = executor.submit(() -> {
                try {
                    var answer = hook.handle(letters);
                    var decisions = answer == null ? null : Arrays.asList(answer.toArray(new DeadLetterDecision[0]));
                    if (decisions == null || decisions.size() != letters.size() || decisions.contains(null)) {
                        throw new IllegalStateException("outbox dead-letter hook returned " + (decisions == null
                                ? "null" : decisions.size() + " decisions" + (decisions.contains(null)
                                        ? " with a null" : "")) + " for " + letters.size() + " letters");
                    }
                    return decisions;
                } finally {
                    telemetry.releaseThread(); // nothing leaks into the next call on this thread
                    ended.countDown();
                }
            });
        } catch (RejectedExecutionException e) {
            throw new IllegalStateException("outbox dead-letter pass is closed", e);
        }
        List<DeadLetterDecision> decisions;
        try {
            decisions = call.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException | InterruptedException e) {
            call.cancel(true);
            executor.purge(); // a hook ignoring the interrupt holds the thread: calls cut behind it do not pile up
            // never started, as one queued behind a call still running, counts too: that call holds the thread
            if (e instanceof TimeoutException && !ended.await(INTERRUPT_GRACE.toNanos(), TimeUnit.NANOSECONDS)
                    && ignoresInterrupts.compareAndSet(false, true)) {
                log.error("outbox dead-letter hook ignores interrupts: its call cut at outbox.deadLetters.timeout {}"
                        + " still runs {} later and holds the hook thread outbox-dead-letter-hook, so dead letters are"
                        + " not decided on this instance until it returns or the instance restarts (each pass waits"
                        + " out the timeout and counts a failed call). The hook must honour Thread.interrupt(): use"
                        + " interruptible calls or timeouts of its own. This ERROR is logged once until a call returns"
                        + " in time", timeout, INTERRUPT_GRACE);
            }
            throw e;
        }
        ignoresInterrupts.set(false);
        return decisions;
    }

    /** Every letter of a failed call stays pending, due after a backoff by its failed calls so far. */
    private Batch failed(Connection tx, List<DeadLetters.Claimed> claimed, Throwable failure) throws SQLException {
        var error = RetryStore.errorText(failure.toString());
        try (var ps = tx.prepareStatement(fail)) {
            for (var c : claimed) {
                ps.setString(1, error);
                ps.setDouble(2, Backoff.delay(c.letter().handlerAttempts(), initialDelay, maxDelay).toNanos() / 1e9);
                ps.setLong(3, c.letter().id());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return new Batch(claimed.size(), failure, List.of());
    }

    private List<DeadLetters.Claimed> claim(Connection tx, String status) throws SQLException {
        try (var ps = tx.prepareStatement(claim)) {
            ps.setString(1, status);
            ps.setInt(2, batchSize);
            try (var rs = ps.executeQuery()) {
                var claimed = new ArrayList<DeadLetters.Claimed>();
                while (rs.next()) {
                    claimed.add(DeadLetters.read(rs));
                }
                return claimed;
            }
        }
    }

    /** {@code body} in one transaction on {@code c}, committed, or rolled back on any failure. */
    private <T> T inTx(Connection c, SqlFunction1<Connection, T> body) throws SQLException {
        ShardProcessor.prepareTransaction(c, timeout); // the claim stays idle while the hook runs
        try {
            T result = body.apply(c);
            c.commit();
            c.setAutoCommit(true);
            return result;
        } catch (Throwable t) {
            try {
                c.rollback();
            } catch (SQLException | RuntimeException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    /** A lock timeout (55P03), logged; its batch rolled back and is due again after {@code initialDelay}. */
    private boolean blocked(SQLException e) {
        if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
            return false;
        }
        lockTimeouts.warn("outbox dead-letter pass on " + dead + " timed out waiting for a lock: its batch rolled back"
                + " and is due again after outbox.relay.retry.initialDelay; " + e.getMessage(), null);
        return true;
    }

    private static String name(DeadLetterDecision decision) {
        if (decision instanceof DeadLetterDecision.Keep) {
            return "keep";
        } else if (decision instanceof DeadLetterDecision.Discard) {
            return "discard";
        } else if (decision instanceof DeadLetterDecision.Redrive) {
            return "redrive";
        }
        return "redrive_at";
    }

    /** One kind of WARN of this pass, at most once a minute, so a frequent kind never hides another. */
    private static final class Warnings {

        /** When this WARN was last logged ({@code nanoTime}), and how many went unlogged since. */
        private final AtomicLong warned = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
        private final AtomicInteger suppressed = new AtomicInteger();

        void warn(String what, @Nullable Throwable error) {
            long now = System.nanoTime();
            long last = warned.get();
            if (now - last < WARN_INTERVAL || !warned.compareAndSet(last, now)) {
                suppressed.incrementAndGet();
                return;
            }
            if (error == null) {
                log.warn("{} ({} more since the previous report not logged)", what, suppressed.getAndSet(0));
            } else {
                log.warn("{} ({} more since the previous report not logged)", what, suppressed.getAndSet(0), error);
            }
        }
    }
}
