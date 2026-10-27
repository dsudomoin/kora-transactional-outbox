package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxPermanentException;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Action;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Follower;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Head;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Item;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Keep;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Park;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Unpark;
import io.github.dsudomoin.kora.outbox.internal.BatchReader.BatchRow;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.Ticks.Tick;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One relay step for one shard, inside the caller's transaction: take the shard lock and the tail swap's lock shared
 * ({@link #tryLock}), re-read the cursor, deliver due retry heads and due parked rows, then the next chunk of the batch
 * {@code (tickId, target]} with the parked rows of its keys first in its call, classify and append the new cursor. A
 * failed attempt halves the rows the shard's next step reads and a delivery doubles them (up to {@code batchSize}), so
 * a record that fails its whole call deep in a chunk is soon read alone; a requeue is neither. After
 * {@code headFailuresBeforeDivert} failed attempts in a row the shard probes: it reads the head row alone and merges no
 * parked row into its call, so a slow or poison head is diverted as a single failing key, and a down target loses at
 * most one row to the retry table per (backed-off) probe. Handler calls run on this processor's own bounded pool of
 * daemon threads, each bounded by {@code batchTimeout}. A call that throws (an Error included) or returns an invalid
 * outcome list fails each of its records transiently, so a poison record takes that same path and no handler failure
 * reaches the caller. The caller commits on return and rolls back on an exception (SQL), so a failed step is
 * redelivered, and a record it parked stays where it was.
 */
public final class ShardProcessor implements AutoCloseable {

    /**
     * {@code IDLE}: nothing to read up to the latest tick and no retry work; the cursor stays on its older tick
     * without a write.
     */
    public enum ShardStatus { NOT_LOCKED, NO_WORK, IDLE, PROGRESS, PAUSED }

    /**
     * {@code batchFinished}: no batch is in progress on the shard after the call (false when not locked or paused), as
     * the cursor stands: a step that handed only parked rows and left the chunk of a fresh batch unread stores no
     * target, so it reports true. {@code deadLettered}: the step moved a row to the dead table, from any of its passes;
     * once it commits, the dead-letter pass has a new letter.
     */
    public record ShardResult(ShardStatus status, int delivered, boolean batchFinished, boolean deadLettered) {

        public ShardResult(ShardStatus status, int delivered, boolean batchFinished) {
            this(status, delivered, batchFinished, false);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ShardProcessor.class);
    private static final AtomicInteger THREADS = new AtomicInteger();
    /** The handler call running on this thread. */
    private static final ThreadLocal<HandlerCall> CALL = new ThreadLocal<>();
    private static final long BUG_REPORT_INTERVAL = TimeUnit.SECONDS.toNanos(10);
    /**
     * How long an idle shard's cursor may stay on its tick without an append (an idle step writes nothing, and the
     * relay steps a shard without new rows only once its tick is this old, see {@link Cursors.Position#behind()}): an
     * idle relay then burns about {@code shards / 5} transaction ids a second for cursors, plus its idle ticks, and the
     * lag gauge, tick cleanup and partition drops trail by up to 5 s at idle.
     */
    static final Duration IDLE_APPEND_AFTER = Duration.ofSeconds(5);
    /**
     * The outcome of each record of a call that found every handler thread still busy (calls that outlived
     * {@code batchTimeout} and ignore interrupts hold them): not attempted, so it costs no attempt, counts as no
     * failure and diverts nothing. A private instance, told apart from a handler's own NotAttempted by identity.
     */
    private static final Outcome BUSY = new Outcome.NotAttempted(null);

    private final int internalNamespace;
    private final OutboxConfig.RelayConfig relay;
    private final OutboxConfig.RetryConfig retry;
    private final OutboxConfig.RequeueConfig requeue;
    private final OutboxHandler handler;
    private final OutboxMetrics metrics;
    @Nullable
    private final Tracer tracer;
    private final Ticks ticks;
    private final Cursors cursors;
    private final BatchReader reader;
    private final RetryStore retries;
    private final ParkedStore parked;
    private final String hasRetryRows;
    /** Failed attempts that divert a single key: {@code headFailuresBeforeDivert}, at most {@code maxAttempts}. */
    private final int divertAt;
    /** Ids a new batch may span, all shards together: {@code batchSize × shards}. */
    private final long batchIds;
    /**
     * Per shard, the rows the next main-pass step reads, halved by a failed call and doubled by a delivery (AIMD,
     * between 1 and {@code batchSize}): a record failing its whole call is alone after about log2 of its depth in the
     * chunk such calls. In memory, so a shard new to this processor starts at {@code batchSize}.
     */
    private final AtomicIntegerArray limits;
    private final ExecutorService executor;
    /** Per shard, when a step last checked the retry table in full ({@code nanoTime}). */
    private final AtomicLongArray fullChecked;
    /**
     * Per shard, 1 while its retry pass probes: hands each due head alone (see {@link #handOver}). In memory, per
     * relay instance: each instance learns of a hang on its own, so with several instances a hanging head can cost one
     * hung call of all heads per instance before all of them probe; a restart costs one.
     */
    private final AtomicIntegerArray probing;
    /**
     * Per shard, the steps in a row that found no free handler thread of this processor (calls that outlived
     * {@code batchTimeout} hold them all), and until when (epoch microseconds of the callers' clock) this processor
     * skips the shard for it: {@code initialDelay} doubled that many times, up to {@code maxDelay}. A fact about this
     * relay instance, not the shard: the shared cursor gets at most an {@code initialDelay} pause, so other instances
     * go on with the shard. A step that hands a call to a handler thread resets both; one that calls nothing (an empty
     * read, a chunk of blocked rows) says nothing about the threads and keeps them.
     */
    private final AtomicIntegerArray busyPauses;
    private final AtomicLongArray busyUntil;
    private final long fullCheckInterval;
    /** When a handler bug was last logged ({@code nanoTime}), and how many went unlogged since. */
    private final AtomicLong bugReported = new AtomicLong(System.nanoTime() - BUG_REPORT_INTERVAL);
    private final AtomicInteger bugsSuppressed = new AtomicInteger();

    /** {@code deadLetterHook}: a dead-letter hook is active, so every dead row is written pending for it. */
    public ShardProcessor(TableNames names, OutboxConfig config, OutboxHandler handler, OutboxMetrics metrics,
            @Nullable Tracer tracer, boolean deadLetterHook) {
        OutboxSchema.validate(config);
        this.relay = config.relay();
        this.retry = relay.retry();
        this.requeue = relay.requeue();
        this.internalNamespace = names.internalNamespace();
        this.handler = handler;
        this.metrics = metrics;
        this.tracer = tracer;
        this.ticks = new Ticks(names, config.shards());
        this.cursors = new Cursors(names);
        this.reader = new BatchReader(names);
        this.retries = new RetryStore(names, deadLetterHook);
        this.parked = new ParkedStore(names, retries);
        this.hasRetryRows = "select exists (select 1 from " + names.retry() + " where shard = ?)";
        this.divertAt = retry.maxAttempts() > 0
                ? Math.min(relay.headFailuresBeforeDivert(), retry.maxAttempts())
                : relay.headFailuresBeforeDivert();
        this.batchIds = (long) relay.batchSize() * config.shards();
        this.fullCheckInterval = config.partitions().maintenanceInterval().toNanos();
        this.fullChecked = new AtomicLongArray(config.shards());
        this.probing = new AtomicIntegerArray(config.shards());
        this.busyPauses = new AtomicIntegerArray(config.shards());
        this.busyUntil = new AtomicLongArray(config.shards());
        long created = System.nanoTime();
        for (int i = 0; i < config.shards(); i++) {
            fullChecked.set(i, created - fullCheckInterval); // a shard's first step checks in full
        }
        var full = new int[config.shards()];
        Arrays.fill(full, relay.batchSize());
        this.limits = new AtomicIntegerArray(full);
        this.executor = new ThreadPoolExecutor(0, Math.max(4, 2 * relay.workers()), 60, TimeUnit.SECONDS,
                new SynchronousQueue<>(), r -> {
                    var t = new Thread(r, "outbox-handler-" + THREADS.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * Opens the shard transaction: autocommit off, then read committed as its first statement, so
     * the cursor re-read after the shard lock sees the previous holder's commit even under a repeatable-read session
     * default; the idle timeout frees the lock of a frozen worker. No {@code transaction_timeout} (PG 17+): a role's
     * default would end a step waiting on a slow handler and lose the pause it writes. A step runs a statement between
     * any two handler calls, so the idle timeout never ends one waiting on a call that keeps to {@code batchTimeout}.
     */
    public static void prepareTransaction(Connection c, Duration batchTimeout) throws SQLException {
        c.setAutoCommit(false);
        try (var st = c.createStatement()) {
            st.execute("set transaction isolation level read committed;"
                    + " set local idle_in_transaction_session_timeout = '"
                    + batchTimeout.plusSeconds(30).toMillis() + "ms';"
                    + " set local lock_timeout = '5s'; set local plan_cache_mode = force_custom_plan"
                    + Ticks.noTransactionTimeout(c));
        }
    }

    /**
     * {@code inTx} is prepared by {@link #prepareTransaction}; {@code latestTickId} is the relay's latest tick. An idle
     * step (no due retry head or parked row, no row read, no cursor change but its tick) writes nothing and returns
     * {@code IDLE} while the cursor's tick is less than {@link #IDLE_APPEND_AFTER} older than the latest, so an
     * idle relay burns no transaction ids; {@code draining} steps always append, so the cursor reaches the drain's
     * tick. {@code NO_WORK} may also mean the shard is skipped here, without its lock or cursor: every handler thread
     * of this processor was busy at a recent step (see {@link #busyPauses}), and other relay instances go on with it.
     */
    public ShardResult tryProcess(Connection inTx, int shard, long latestTickId, Instant now, boolean draining)
            throws SQLException, InterruptedException {
        long started = System.nanoTime();
        now = now.truncatedTo(ChronoUnit.MICROS); // as Postgres stores it: due times compare exactly
        if (ChronoUnit.MICROS.between(Instant.EPOCH, now) < busyUntil.get(shard)) {
            return new ShardResult(ShardStatus.NO_WORK, 0, false); // no free handler thread here a moment ago
        }
        if (!tryLock(inTx, shard)) {
            return new ShardResult(ShardStatus.NOT_LOCKED, 0, false);
        }
        var cursor = cursors.latest(inTx, shard); // a statement after the lock: sees the previous holder's commit
        if (cursor.pausedUntil() != null && now.isBefore(cursor.pausedUntil())) {
            return new ShardResult(ShardStatus.NO_WORK, 0, cursor.targetTickId() == null);
        }
        Tick prev = null;
        Tick target = null;
        if (cursor.targetTickId() != null) {
            // never re-chosen mid-batch: rows of a long transaction below lastId committing after the old target
            // would be skipped
            target = ticks.get(inTx, cursor.targetTickId());
        } else if (latestTickId > cursor.tickId()) {
            prev = ticks.get(inTx, cursor.tickId());
            var candidates = ticks.between(inTx, cursor.tickId(),
                    Math.min(latestTickId, cursor.tickId() + relay.maxTicksPerBatch()));
            // a gap in tick ids (e.g. a rolled-back tick insert) must not stop the shard
            target = candidates.isEmpty()
                    ? ticks.oldestAfter(inTx, cursor.tickId())
                    : chooseTarget(prev.maxId(), candidates, batchIds);
        }
        // Retry rows of a shard are written only under its lock (divert, retry pass), and each (topic, key) group in
        // the retry table always has exactly one head, the only rows with next_attempt_at; every step that writes
        // there stores min(next_attempt_at) as the cursor's retry_due_at. So retry_due_at == null means no retry rows,
        // and the table is read only when it is due or the step wrote to it, not on every step: under a held xmin
        // horizon each probe walks every dead version a mass divert left. Once per maintenanceInterval the step
        // checks in full, for rows changed outside the relay.
        boolean full = fullCheckDue(shard);
        var retryDue = cursor.retryDueAt();
        var heads = full || retryDue != null && !retryDue.isAfter(now)
                ? retries.dueHeads(inTx, shard, now, relay.batchSize())
                : List.<OutboxRecord>of();
        // parked_due_at works as retry_due_at: every step that writes to the parked table stores min(due_at), so
        // null means no parked rows, and a step looks up parked rows only then or on its full check
        var parkedDue = full ? parked.minDue(inTx, shard) : cursor.parkedDueAt();
        var dueParked = parkedDue != null && !parkedDue.isAfter(now)
                ? parked.due(inTx, shard, now, relay.batchSize() - heads.size())
                : List.<OutboxRecord>of();
        if (target == null && heads.isEmpty() && dueParked.isEmpty()) {
            var checked = cursor.withRetryDueAt(full ? retries.minDue(inTx, shard) : retryDue)
                    .withParkedDueAt(parkedDue);
            if (!checked.equals(cursor)) {
                cursors.append(inTx, checked);
            }
            return new ShardResult(ShardStatus.NO_WORK, 0, true);
        }

        var pass = retryPass(inTx, shard, heads, dueParked, now);
        int delivered = pass.delivered;
        boolean deadLettered = pass.deadLettered;
        // the retry pass writes to the table of the rows it hands, and moves rows between the two
        boolean wroteRetry = !heads.isEmpty() || pass.toRetry;
        boolean wroteParked = !dueParked.isEmpty() || pass.fromRetry;
        boolean busy = pass.busy; // no handler thread free: the main pass would find none either
        boolean called = pass.called; // a handler thread was free
        var status = ShardStatus.PROGRESS;
        var next = cursor;
        boolean idle = false;
        if (target != null && !busy) {
            // probe mode: after headFailuresBeforeDivert failed attempts in a row, the head row alone
            boolean probe = cursor.failures() >= relay.headFailuresBeforeDivert();
            int limit = probe ? 1 : limits.get(shard);
            if (prev == null) {
                prev = ticks.get(inTx, cursor.tickId());
            }
            // a parked row the retry pass made a retry head blocks its key here too
            var rows = reader.read(inTx, shard, prev.snapshot(), target.snapshot(), cursor.lastId(), limit,
                    full ? hasRetryRows(inTx, shard) : retryDue != null || wroteRetry);
            // (P, C] read nothing, so the next step's (P, C'] reads (C, C'] exactly: the cursor may stay on P; not
            // when the target is below the latest tick, or maxTicksPerBatch ticks within IDLE_APPEND_AFTER keep it on P
            idle = !draining && rows.isEmpty() && target.id() == latestTickId && young(prev, target);
            // a parked record comes back right before the next record of its key: the oldest limit parked rows of
            // the chunk's keys go first in its call (a key blocked in the retry table hands no record, so none); a
            // probe merges none, so the head it isolates is the only record of its call
            var parkedRows = probe || parkedDue == null && !wroteParked ? List.<OutboxRecord>of()
                    : parked.ofKeys(inTx, shard, rows.stream()
                            .filter(row -> !row.blocked() && row.record().key() != null).map(BatchRow::record)
                            .toList(), limit);
            var items = deliverBatch(parkedRows, rows, now);
            busy = items.stream().anyMatch(item -> item.outcome() == BUSY); // nothing handed: nothing read, no items
            called |= !busy && items.stream().anyMatch(item -> !item.dbBlocked());
            var chunk = items.subList(parkedRows.size(), items.size());
            int read = busy ? 0
                    : read(chunk, items.subList(0, parkedRows.size()).stream().anyMatch(this::leavesParked));
            items = items.subList(0, busy ? 0 : parkedRows.size() + read);
            // the shard's failures may stem from other rows than a probed head: it is diverted with divertAt attempts
            var decision = BatchClassifier.classify(items, Math.min(cursor.failures(), divertAt - 1), divertAt, now,
                    retry.initialDelay(), retry.maxDelay(), requeue.delay(), requeue.maxRequeues());
            if (!decision.pause() && !busy) {
                var parkedIds = new HashSet<Long>();
                parkedRows.forEach(row -> parkedIds.add(row.id()));
                wroteParked |= apply(inTx, shard, decision.actions(), parkedIds);
                wroteRetry |= decision.actions().stream()
                        .anyMatch(action -> action instanceof Head || action instanceof Follower);
                deadLettered |= decision.actions().stream().anyMatch(action -> action instanceof Dead);
                delivered += recordOutcomes(items, decision.actions(), shard, now);
                if (read == rows.size() && rows.size() < limit) {
                    next = cursor.withProgress(target.id(), null, 0);
                } else if (read > 0) {
                    next = cursor.withProgress(cursor.tickId(), target.id(), rows.get(read - 1).record().id());
                } // else only parked rows read: the position stays, as on a pause
            } // else no progress: a batch in progress keeps its target, a fresh one is not stored (lastId is still 0)
            if (failedAttempt(items)) { // paused, or a single failing key diverted: either way back off
                limits.set(shard, Math.max(1, limit / 2));
                var backoff = Backoff.delay(cursor.failures(), retry.initialDelay(), retry.maxDelay());
                var until = now.plus(Duration.ofNanos(System.nanoTime() - started)).plus(backoff)
                        .truncatedTo(ChronoUnit.MICROS); // from the decision, not from the start of a slow call
                next = next.withPause(until, cursor.failures() + 1);
                status = ShardStatus.PAUSED;
                metrics.shardPaused();
                log.warn("outbox shard {} paused until {}: nothing delivered, {} consecutive failed attempts ({})",
                        shard, until, next.failures(), firstError(items));
            } else if (items.stream().anyMatch(item -> item.outcome() instanceof Outcome.Delivered)) {
                limits.set(shard, limit > relay.batchSize() / 2 ? relay.batchSize() : 2 * limit); // no int overflow
                next = next.cleared();
            } // else only blocked, dead or requeued rows (a requeue is no failure): nothing about the target, the
            // backoff and the limit stay as they are
        }
        if (busy) { // calls that outlived batchTimeout hold every handler thread: no attempt and no failure (so no
            // probe, no divert); from the rejection, the shard pauses initialDelay for all relay instances, and this
            // one, whose threads are held, skips it for longer each step in a row (busyUntil)
            var rejected = now.plus(Duration.ofNanos(System.nanoTime() - started)).truncatedTo(ChronoUnit.MICROS);
            var skipUntil = rejected.plus(Backoff.delay(busyPauses.getAndIncrement(shard), retry.initialDelay(),
                    retry.maxDelay()));
            busyUntil.set(shard, ChronoUnit.MICROS.between(Instant.EPOCH, skipUntil));
            next = next.withPause(rejected.plus(retry.initialDelay()), cursor.failures());
            status = ShardStatus.PAUSED;
            metrics.shardPaused();
            log.warn("outbox shard {}: every handler thread of this relay is still busy; skipped here until {}",
                    shard, skipUntil);
        } else if (called && busyPauses.get(shard) != 0) {
            busyPauses.set(shard, 0);
            busyUntil.set(shard, 0); // a wall clock stepping back would meet the old skip again
        }
        if (full || wroteRetry) {
            next = next.withRetryDueAt(retries.minDue(inTx, shard));
        }
        if (full || wroteParked) {
            next = next.withParkedDueAt(wroteParked ? parked.minDue(inTx, shard) : parkedDue);
        }
        // after retry_due_at and parked_due_at are settled: a full check that corrects one is a change, so the step
        // appends; an idle step read no row, so its main pass handed no parked row either
        if (idle && heads.isEmpty() && dueParked.isEmpty()
                && next.equals(cursor.withProgress(next.tickId(), cursor.targetTickId(), cursor.lastId()))) {
            return new ShardResult(ShardStatus.IDLE, 0, true);
        }
        cursors.append(inTx, next);
        metrics.batch(Duration.ofNanos(System.nanoTime() - started));
        return new ShardResult(status, delivered, status == ShardStatus.PROGRESS && next.targetTickId() == null,
                deadLettered);
    }

    /** {@code target} made less than {@link #IDLE_APPEND_AFTER} after {@code prev} (both database clock). */
    private static boolean young(Tick prev, Tick target) {
        var age = Duration.between(prev.createdAt(), target.createdAt());
        return !age.isNegative() && age.compareTo(IDLE_APPEND_AFTER) < 0; // a clock stepped back appends
    }

    /**
     * The target of a new batch, given the ticks after the cursor's tick P in id order (at least
     * one): the newest whose id high-water mark is at most {@code batchIds} past P's, so a batch stays a few chunk
     * reads however far the relay lags; always at least the first tick, so one huge tick is still one batch. An
     * unknown mark (a tick made before {@code max_id} existed) ends the merge.
     */
    static Tick chooseTarget(@Nullable Long prevMaxId, List<Tick> candidates, long batchIds) {
        var target = candidates.get(0);
        if (prevMaxId == null) {
            return target;
        }
        for (var tick : candidates.subList(1, candidates.size())) {
            if (tick.maxId() == null || tick.maxId() - prevMaxId > batchIds) {
                break;
            }
            target = tick;
        }
        return target;
    }

    /** Stops the handler threads, interrupting running calls. */
    @Override
    public void close() {
        executor.shutdownNow();
    }

    /** Whether the shard's last full retry-table check was {@code maintenanceInterval} ago; if so it counts as done. */
    private boolean fullCheckDue(int shard) {
        long now = System.nanoTime();
        long last = fullChecked.get(shard);
        return now - last >= fullCheckInterval && fullChecked.compareAndSet(shard, last, now);
    }

    /**
     * {@code LOCK_SWAP} shared and the shard's lock, both until the step's commit, or the step does not run: the tail
     * swap ({@link XidPartitions#ensurePremade}) takes {@code LOCK_SWAP} exclusively, so it waits for the steps in
     * flight, and steps skip while it holds or awaits it. Every step of every caller (workers, {@code drain()}) starts
     * here, before its first read. Either lock taken alone is released at the step's commit, which is immediate.
     */
    private boolean tryLock(Connection c, int shard) throws SQLException {
        try (var ps = c.prepareStatement(
                "select pg_try_advisory_xact_lock_shared(?, ?), pg_try_advisory_xact_lock(?, ?)")) {
            ps.setInt(1, internalNamespace);
            ps.setInt(2, TableNames.LOCK_SWAP);
            ps.setInt(3, internalNamespace);
            ps.setInt(4, TableNames.LOCK_SHARD_BASE + shard);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1) & rs.getBoolean(2);
            }
        }
    }

    private boolean hasRetryRows(Connection c, int shard) throws SQLException {
        try (var ps = c.prepareStatement(hasRetryRows)) {
            ps.setShort(1, (short) shard);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /** Nothing delivered and a transient failure: the shard pauses, whether the classifier diverted or not. */
    private static boolean failedAttempt(List<Item> items) {
        boolean failed = false;
        for (var item : items) {
            if (item.outcome() instanceof Outcome.Delivered) {
                return false;
            }
            failed |= item.outcome() instanceof Outcome.Retry || item.outcome() instanceof Outcome.NotAttempted;
        }
        return failed;
    }

    /**
     * Each due head with its followers, in id order, in one handler call together with the other due heads and, in the
     * first round, the due parked rows ({@code dueParked}, a group per key), at most {@code batchSize} rows in all; the
     * followers of keyed heads share what the heads and parked rows leave of it. Per key the delivered rows are
     * deleted, a permanent failure goes to dead and a requeued row is parked (past {@code maxRequeues}: dead, attempts
     * kept); the first row that fails transiently (an attempt, backoff) or is not attempted (attempts kept) becomes the
     * head and the rows after it stay followers. A key whose handed rows are all done promotes its next row, due now,
     * to a further round of the pass, which hands retry heads only. A head that used up {@code maxAttempts} goes to
     * dead without another attempt and its followers go on. Parked rows are settled as {@link #resolveParked} says. A
     * call that fails as a whole charges no one, and no call starts once the pass has spent {@code batchTimeout}: see
     * {@link #handOver}. A pass that finds no free handler thread ends there ({@link Pass#busy}; the step then pauses
     * the shard). A Retry counts as {@code failed{outcome=retry}}, as diverted heads of a batch do; NotAttempted is no
     * attempt and no failure.
     */
    private Pass retryPass(Connection c, int shard, List<OutboxRecord> heads, List<OutboxRecord> dueParked,
            Instant now) throws SQLException, InterruptedException {
        int budget = relay.batchSize() - dueParked.size();
        var pass = new Pass(System.nanoTime() + relay.batchTimeout().toNanos());
        boolean probe = probing.get(shard) == 1; // the first round only: later rounds hand promoted rows
        var parkedGroups = parkedGroups(dueParked);
        while (!heads.isEmpty() || !parkedGroups.isEmpty()) {
            budget -= heads.size();
            long keyed = heads.stream().filter(head -> head.key() != null).count();
            int perKey = keyed == 0 ? 0 : (int) (budget / keyed);
            var followers = perKey == 0 ? Map.<Long, List<OutboxRecord>>of() : retries.followers(c, heads, perKey);
            var groups = new ArrayList<Group>(heads.size() + parkedGroups.size());
            var handed = new HashSet<Long>();
            for (var head : heads) {
                if (!handed.add(head.id())) {
                    continue; // a second head of its key (written outside the relay), handed as an earlier one's row
                }
                var rows = new ArrayList<OutboxRecord>();
                if (exhausted(head.attempts())) {
                    dead(c, pass, head, head.attempts(), null);
                } else {
                    rows.add(head);
                }
                var after = followers.getOrDefault(head.id(), List.of());
                budget -= probe ? 0 : after.size(); // a probe hands heads alone: their followers wait for a round
                rows.addAll(after);
                after.forEach(row -> handed.add(row.id()));
                groups.add(new Group(head, rows, false));
            }
            groups.addAll(parkedGroups); // after the retry groups, which handOver settles first
            parkedGroups = List.of();
            boolean promoted = handOver(c, shard, groups, now, pass, probe);
            if (probe && !pass.failed && !pass.busy) {
                probing.set(shard, 0); // no call of the probe failed as a whole: batch again
            }
            probe = false;
            if (!promoted || budget <= 0 || pass.outOfTime() || pass.busy) {
                break; // nothing promoted, or out of budget or time: promoted rows are due at now for the next step
            }
            heads = retries.dueHeads(c, shard, now, budget); // promoted rows are due at now
        }
        // in the step's transaction, before the main pass reads the parked table
        var parkedIds = new HashSet<Long>();
        dueParked.forEach(row -> parkedIds.add(row.id()));
        parked.apply(c, shard, pass.parkedWrites, parkedIds);
        return pass;
    }

    /** One group per (topic, key) of the parked {@code rows} (one per unkeyed row), each in id order. */
    private static List<Group> parkedGroups(List<OutboxRecord> rows) {
        var byKey = new LinkedHashMap<Object, List<OutboxRecord>>();
        for (var row : rows) {
            byKey.computeIfAbsent(BatchClassifier.orderingKey(row), key -> new ArrayList<>()).add(row);
        }
        return byKey.values().stream().map(group -> new Group(group.get(0), group, true)).toList();
    }

    /**
     * The rows handed for one key, in id order: a due retry head (unless out of attempts) and its followers, or, when
     * {@code parked}, the key's due parked rows, the first of them as {@code head}.
     */
    private record Group(OutboxRecord head, List<OutboxRecord> rows, boolean parked) {}

    /**
     * The retry pass's progress: it starts no handler call once {@code deadline} (start + {@code batchTimeout}) has
     * passed, except its very first call, and while it has settled no row yet, one call of a single record.
     */
    private static final class Pass {

        final long deadline;
        boolean settled;
        int delivered;
        /** A call failed as a whole. */
        boolean failed;
        /** A call found every handler thread busy: no call starts after it. */
        boolean busy;
        /** A call ran on a handler thread. */
        boolean called;
        /** A parked row became a retry head. */
        boolean toRetry;
        /** A retry row was parked. */
        boolean fromRetry;
        /** A row went to the dead table. */
        boolean deadLettered;
        /** Re-parks, keeps and unparks of handed parked rows, written as JDBC batches when the pass ends. */
        final List<Action> parkedWrites = new ArrayList<>();

        Pass(long deadline) {
            this.deadline = deadline;
        }

        boolean outOfTime() {
            return System.nanoTime() - deadline >= 0;
        }
    }

    /**
     * One handler call of every group's rows in id order, settled in SQL; returns whether a key promoted its next row.
     * A call of several records that fails as a whole ({@link FailedCall}) says nothing about any one of them and
     * counts no attempt: each group is called again on its own, and a group of several rows failing as a whole once
     * more is called with its first row alone, which then bears the attempt. Out of time, only that single-record call
     * may still run, and only while the pass has settled nothing; the groups not called again get no outcome and wait
     * for the next pass, uncharged. Such a pass, out of time, makes the shard probe ({@link #probing}): its next pass
     * starts without the call of all groups and hands each head alone, in id order, as many as its time allows; once a
     * probe passes without a call failing as a whole, the shard batches again. So a head that hangs behind healthy
     * followers, or behind fewer healthy heads than single calls fit in {@code batchTimeout}, is charged in the next
     * step. A call that finds every handler thread busy is not attempted (its heads keep their attempts) and no call
     * follows it in the pass. Each call is settled, or a statement run, before the next one starts: between two
     * statements of a step there is at most one handler call (at most {@code batchTimeout}), well under the step's
     * {@code idle_in_transaction_session_timeout} ({@code batchTimeout + 30 s}, see {@link #prepareTransaction}).
     * Groups called one by one are settled in their order, those not called included, so the retry groups (first in
     * {@code groups}) are settled before the parked ones, as {@link #settle} needs.
     */
    private boolean handOver(Connection c, int shard, List<Group> groups, Instant now, Pass pass, boolean probe)
            throws SQLException, InterruptedException {
        if (!probe) {
            var outcomes = call(groups, now, pass);
            if (!(outcomes.whole() && outcomes.byId().size() > 1)) {
                return settle(c, shard, groups, outcomes.byId(), now, pass);
            }
            pass.failed = true;
        }
        boolean several = !probe && groups.stream().filter(group -> !group.rows().isEmpty()).count() > 1;
        boolean promoted = false;
        for (var group : groups) {
            var rows = group.rows();
            if (rows.isEmpty() || pass.busy || pass.outOfTime() && pass.settled) {
                promoted |= settle(c, shard, List.of(group), Map.of(), now, pass); // a head moved to dead promotes
                continue;
            }
            keepAlive(c);
            if (several && !pass.outOfTime()) {
                var outcomes = call(List.of(group), now, pass);
                if (!(outcomes.whole() && rows.size() > 1)) {
                    promoted |= settle(c, shard, List.of(group), outcomes.byId(), now, pass);
                    continue;
                }
                if (pass.outOfTime() && pass.settled) {
                    promoted |= settle(c, shard, List.of(group), Map.of(), now, pass);
                    continue;
                }
                keepAlive(c);
            }
            var outcomes = call(List.of(new Group(group.head(), rows.subList(0, 1), group.parked())), now, pass);
            pass.failed |= outcomes.whole();
            promoted |= settle(c, shard, List.of(group), outcomes.byId(), now, pass);
        }
        if (!probe && pass.outOfTime()) {
            probing.set(shard, 1);
        }
        return promoted;
    }

    /** The outcome of each handed row by id; {@code whole}: the call failed as a whole ({@link FailedCall}). */
    private record Call(Map<Long, Outcome> byId, boolean whole) {}

    /** One handler call of the groups' rows in id order; notes in {@code pass} when no handler thread was free. */
    private Call call(List<Group> groups, Instant now, Pass pass) throws InterruptedException {
        var records = new ArrayList<OutboxRecord>();
        for (var group : groups) {
            records.addAll(group.rows());
        }
        records.sort(Comparator.comparingLong(OutboxRecord::id));
        var byId = new HashMap<Long, Outcome>();
        if (records.isEmpty()) {
            return new Call(byId, false);
        }
        var outcomes = deliver(records, now);
        for (int i = 0; i < records.size(); i++) {
            byId.put(records.get(i).id(), outcomes.get(i));
        }
        boolean busy = outcomes.get(0) == BUSY;
        pass.busy |= busy;
        pass.called |= !busy;
        return new Call(byId, outcomes instanceof FailedCall);
    }

    /**
     * Applies the outcomes to the retry groups ({@link #resolve}), deletes their delivered rows and promotes the next
     * row of each key that goes on, then applies them to the parked groups ({@link #resolveParked}); returns whether a
     * row was promoted. Parked groups come last: a parked row made the retry head of its key there is the lowest retry
     * row of that key, which a promote of the key would make due at once.
     */
    private boolean settle(Connection c, int shard, List<Group> groups, Map<Long, Outcome> outcomes, Instant now,
            Pass pass) throws SQLException {
        var done = new ArrayList<OutboxRecord>();
        var goOn = new ArrayList<OutboxRecord>();
        for (var group : groups) {
            if (!group.parked() && resolve(c, group, outcomes, done, now, pass)) {
                goOn.add(group.head());
            }
        }
        retries.delete(c, done);
        boolean promoted = false;
        for (var head : goOn) {
            promoted |= retries.promote(c, shard, head, now);
        }
        for (var group : groups) {
            if (group.parked()) {
                resolveParked(c, shard, group, outcomes, done, now, pass);
            }
        }
        for (var row : done) {
            metrics.delivered(row.topic(), delay(row, now));
        }
        pass.delivered += done.size();
        pass.settled |= !outcomes.isEmpty();
        return promoted;
    }

    /** A statement between two handler calls whose first left nothing to write. */
    private static void keepAlive(Connection c) throws SQLException {
        try (var ps = c.prepareStatement("select 1")) {
            ps.execute();
        }
    }

    /**
     * Applies the outcomes of one key's retry rows in id order (rows not handed keep theirs): delivered rows join
     * {@code done}, a requeued row is parked ({@code requeues + 1}; past {@code maxRequeues} dead, attempts kept), a
     * permanent failure (or a transient one out of attempts) goes to dead, and the first transient failure or
     * not-attempted row becomes the head, the rest staying followers. Returns whether the key goes on, i.e. its next
     * row is to be promoted; a head that was not handed at all stays as it is.
     */
    private boolean resolve(Connection c, Group group, Map<Long, Outcome> outcomes, List<OutboxRecord> done,
            Instant now, Pass pass) throws SQLException {
        for (var row : group.rows()) {
            var outcome = outcomes.get(row.id());
            int attempts = row.attempts() + 1;
            if (outcome == null) { // not handed: the next row of the key, unless the head itself was not (out of time)
                return row != group.head();
            } else if (outcome instanceof Outcome.Delivered) {
                done.add(row);
            } else if (outcome instanceof Outcome.Requeue q) { // the rows of its key before it have all left the table
                if (BatchClassifier.requeueLimitReached(row, requeue.maxRequeues())) {
                    dead(c, pass, row, row.attempts(), BatchClassifier.REQUEUE_LIMIT);
                } else {
                    parked.fromRetry(c, row, row.requeues() + 1, requeueDue(q.notBefore(), now));
                    metrics.requeued(row.topic());
                    pass.fromRetry = true;
                }
            } else if (outcome instanceof Outcome.Permanent p) {
                dead(c, pass, row, attempts, p.error().toString());
            } else if (outcome instanceof Outcome.Retry r) {
                if (exhausted(attempts)) {
                    dead(c, pass, row, attempts, r.error().toString());
                    continue;
                }
                var due = now.plus(Backoff.delay(row.attempts(), retry.initialDelay(), retry.maxDelay()));
                retries.reschedule(c, row, attempts, BatchClassifier.dueAt(due, r.notBefore(), now),
                        r.error().toString());
                metrics.failed(row.topic(), "retry");
                return false;
            } else if (outcome instanceof Outcome.NotAttempted n) { // attempts and last error stay
                var due = BatchClassifier.dueAt(now.plus(retry.initialDelay()), n.notBefore(), now);
                retries.reschedule(c, row, row.attempts(), due, null);
                return false;
            }
        }
        return true;
    }

    /**
     * Applies the outcomes of one key's parked rows in id order, as the main pass settles parked rows: delivered rows
     * join {@code delivered} and leave the table; a requeued row is parked again ({@code requeues + 1}; past
     * {@code maxRequeues} dead, attempts kept); a permanent failure, or a transient one out of attempts, goes to dead;
     * a not-attempted row (a call that found no free handler thread included) stays parked, due no earlier than after
     * the requeue delay. None of these blocks the rows after it. The first other transient failure becomes the retry
     * head of the key and takes its newer parked rows along as followers ({@link ParkedStore#toRetry}), which ends the
     * group, as does a row not handed. Never promotes. Unparks, re-parks and keeps go to {@link Pass#parkedWrites}:
     * such a row precedes the group's row that moves, if any, and a move carries only the key's newer parked rows,
     * while no other group of the pass moves a parked row of its key (a retry group writes the parked table only
     * through {@link ParkedStore#fromRetry}, which inserts its own row), so the row is still parked when the pass
     * writes it.
     */
    private void resolveParked(Connection c, int shard, Group group, Map<Long, Outcome> outcomes,
            List<OutboxRecord> delivered, Instant now, Pass pass) throws SQLException {
        for (var row : group.rows()) {
            var outcome = outcomes.get(row.id());
            int attempts = row.attempts() + 1;
            if (outcome == null) {
                return;
            } else if (outcome instanceof Outcome.Delivered) {
                delivered.add(row);
                pass.parkedWrites.add(new Unpark(row));
            } else if (outcome instanceof Outcome.Requeue q) {
                if (BatchClassifier.requeueLimitReached(row, requeue.maxRequeues())) {
                    deadFromParked(c, pass, row, row.attempts(), BatchClassifier.REQUEUE_LIMIT);
                } else {
                    pass.parkedWrites.add(new Park(row, row.requeues() + 1, requeueDue(q.notBefore(), now)));
                    metrics.requeued(row.topic());
                }
            } else if (outcome instanceof Outcome.Permanent p) {
                deadFromParked(c, pass, row, attempts, p.error().toString());
            } else if (outcome instanceof Outcome.Retry r) {
                if (exhausted(attempts)) {
                    deadFromParked(c, pass, row, attempts, r.error().toString());
                    continue;
                }
                var due = now.plus(Backoff.delay(row.attempts(), retry.initialDelay(), retry.maxDelay()));
                parked.toRetry(c, shard, row, attempts, BatchClassifier.dueAt(due, r.notBefore(), now),
                        r.error().toString());
                metrics.failed(row.topic(), "retry");
                pass.toRetry = true;
                return;
            } else if (outcome instanceof Outcome.NotAttempted n) { // attempts and requeues stay
                pass.parkedWrites.add(new Keep(row, requeueDue(n.notBefore(), now)));
            }
        }
    }

    /** When a record requeued at {@code now} with {@code notBefore}, or a parked one not attempted, is due. */
    private Instant requeueDue(@Nullable Instant notBefore, Instant now) {
        return BatchClassifier.dueAt(now.plus(requeue.delay()), notBefore, now);
    }

    private boolean exhausted(int attempts) {
        return retry.maxAttempts() > 0 && attempts >= retry.maxAttempts();
    }

    /** A null {@code error}: maxAttempts used up, the stored last error stays. */
    private void dead(Connection c, Pass pass, OutboxRecord row, int attempts, @Nullable String error)
            throws SQLException {
        retries.deadFromRetry(c, row, attempts, error);
        pass.deadLettered = true;
        metrics.failed(row.topic(), "dead");
        logDead(row, attempts, error != null ? error : "maxAttempts reached");
    }

    private void deadFromParked(Connection c, Pass pass, OutboxRecord row, int attempts, String error)
            throws SQLException {
        parked.toDead(c, row, attempts, error);
        pass.deadLettered = true;
        metrics.failed(row.topic(), "dead");
        logDead(row, attempts, error);
    }

    /**
     * Hands {@code parkedRows}, then the non-blocked rows, to the handler in one call: the items of both, in that
     * order; blocked rows get no outcome.
     */
    private List<Item> deliverBatch(List<OutboxRecord> parkedRows, List<BatchRow> rows, Instant now)
            throws InterruptedException {
        var records = new ArrayList<OutboxRecord>(parkedRows.size() + rows.size());
        records.addAll(parkedRows);
        for (var row : rows) {
            if (!row.blocked()) {
                records.add(row.record());
            }
        }
        var outcomes = records.isEmpty() ? List.<Outcome>of() : deliver(records, now);
        var items = new ArrayList<Item>(parkedRows.size() + rows.size());
        int i = 0;
        for (var row : parkedRows) {
            items.add(new Item(row, false, outcomes.get(i++), true));
        }
        for (var row : rows) {
            items.add(row.blocked()
                    ? new Item(row.record(), true, null)
                    : new Item(row.record(), false, outcomes.get(i++)));
        }
        return items;
    }

    /**
     * Writes a main-pass decision: the retry and dead rows of main-stream records through {@link RetryStore#apply},
     * every {@link Park} and every action of a parked row through {@link ParkedStore#apply}, which runs no statement
     * when there is none. Returns whether the parked table was written.
     */
    private boolean apply(Connection c, int shard, List<Action> actions, Set<Long> parkedIds) throws SQLException {
        var mainStream = new ArrayList<Action>(actions.size());
        boolean toParked = false;
        for (var action : actions) {
            if (action instanceof Park || parkedIds.contains(action.r().id())) {
                toParked = true;
            } else {
                mainStream.add(action);
            }
        }
        retries.apply(c, shard, mainStream);
        if (toParked) {
            parked.apply(c, shard, actions, parkedIds);
        }
        return toParked;
    }

    /** Metrics and logs of an applied batch decision; returns the number delivered, parked rows included. */
    private int recordOutcomes(List<Item> items, List<BatchClassifier.Action> actions, int shard, Instant now) {
        int delivered = 0;
        for (var item : items) {
            if (item.outcome() instanceof Outcome.Delivered) {
                metrics.delivered(item.record().topic(), delay(item.record(), now));
                delivered++;
            }
        }
        Head firstHead = null;
        int heads = 0;
        for (var action : actions) {
            if (action instanceof Head h) {
                metrics.failed(h.r().topic(), "retry");
                firstHead = firstHead == null ? h : firstHead;
                heads++;
            } else if (action instanceof Dead d) {
                metrics.failed(d.r().topic(), "dead");
                logDead(d.r(), d.attempts(), d.error());
            } else if (action instanceof Park p) {
                metrics.requeued(p.r().topic());
            }
        }
        if (firstHead != null) {
            log.warn("outbox shard {}: {} (topic, key) streams diverted to retry, first {} id {}: {}", shard, heads,
                    firstHead.r().topic(), firstHead.r().id(), firstHead.error());
        }
        return delivered;
    }

    /**
     * Runs {@code handleBatch} on a handler thread, bounded by {@code batchTimeout} (see {@link #callHandler}). With a
     * tracer each record gets a delivery span, parented by its stored trace, and reaches the handler carrying that
     * span's trace headers; the stored record keeps its own.
     */
    private List<Outcome> deliver(List<OutboxRecord> records, Instant now) throws InterruptedException {
        var spans = tracer == null ? null : new Span[records.size()];
        var handed = records;
        if (spans != null) {
            handed = new ArrayList<>(records.size());
            for (int i = 0; i < records.size(); i++) {
                var r = records.get(i);
                spans[i] = tracer.spanBuilder("outbox deliver " + r.topic())
                        .setSpanKind(SpanKind.PRODUCER)
                        .setParent(OutboxTracing.extract(r.headers()))
                        .setAttribute("messaging.destination.name", r.topic())
                        .setAttribute("outbox.message_id", r.messageId().toString())
                        .setAttribute("outbox.attempt", r.attempts() + 1L)
                        .setAttribute("outbox.delay_ms", delay(r, now).toMillis())
                        .startSpan();
                var ctx = io.opentelemetry.context.Context.root().with(spans[i]);
                handed.add(r.withHeaders(OutboxTracing.replaceTraceHeaders(r.headers(), ctx)));
            }
        }
        List<Outcome> outcomes;
        try {
            outcomes = callHandler(handed);
        } catch (Throwable t) { // interrupted, or the processor is closed: the caller rolls back
            if (spans != null) {
                for (var span : spans) {
                    span.recordException(t);
                    span.setStatus(StatusCode.ERROR);
                    span.end();
                }
            }
            throw t;
        }
        if (spans != null) {
            for (int i = 0; i < spans.length; i++) {
                end(spans[i], outcomes.get(i));
            }
        }
        return outcomes;
    }

    /**
     * At the soft deadline (80 % of {@code batchTimeout}) the call is asked to wind down (a flag the
     * handler sees through {@link OutboxHandler#windDownRequested()}, plus an interrupt), not cancelled: the default
     * handleBatch starts no further record, and the call's real outcomes count. A call still running at
     * {@code batchTimeout} is cancelled: nothing is known about any of its records, each a transient failure. A call
     * that finds no free handler thread does not run: each record {@link #BUSY}.
     */
    private List<Outcome> callHandler(List<OutboxRecord> records) throws InterruptedException {
        var readOnly = Collections.unmodifiableList(records);
        var task = new HandlerCall(() -> {
            try {
                var outcomes = handler.handleBatch(readOnly);
                // read here, inside the deadline: a lazy view runs handler code; nulls are checked on return
                return outcomes == null ? null : Arrays.asList(outcomes.toArray(new Outcome[0]));
            } finally {
                ru.tinkoff.kora.common.Context.clear(); // nothing leaks into the next batch on this thread
            }
        });
        Future<List<Outcome>> call;
        try {
            call = executor.submit(task);
        } catch (RejectedExecutionException e) {
            if (executor.isShutdown()) {
                throw new IllegalStateException("outbox shard processor is closed", e);
            }
            return Collections.nCopies(records.size(), BUSY);
        }
        long timeout = relay.batchTimeout().toNanos();
        long soft = timeout - timeout / 5;
        List<Outcome> outcomes;
        try {
            try {
                outcomes = call.get(soft, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                task.interrupt();
                outcomes = call.get(timeout - soft, TimeUnit.NANOSECONDS);
            }
        } catch (TimeoutException e) {
            task.interrupt(); // its wind-down flag before the cancel: a handler may have swallowed both interrupts
            call.cancel(true);
            return new FailedCall(records.size(),
                    new TimeoutException("outbox handler exceeded batchTimeout " + relay.batchTimeout()));
        } catch (InterruptedException e) {
            task.interrupt();
            call.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            return failedCall(records, e.getCause(), e.getCause() instanceof Error);
        }
        if (outcomes == null || outcomes.size() != records.size()) {
            return failedCall(records, new IllegalStateException("outbox handler returned "
                    + (outcomes == null ? null : outcomes.size()) + " outcomes for " + records.size() + " records"),
                    true);
        }
        Throwable error = null;
        for (var outcome : outcomes) {
            if (outcome == null) {
                return failedCall(records, new IllegalStateException("outbox handler returned a null outcome"), true);
            }
            if (error == null && outcome instanceof Outcome.Retry r && r.error() instanceof Error) {
                error = r.error(); // e.g. thrown by handle(): the default handleBatch retries its record alone
            }
        }
        if (error != null) {
            handlerBug("outbox handler failed a record with an Error; that record is retried", error);
        }
        return outcomes;
    }

    /**
     * Chunk rows the step has read: all of them, except the NotAttempted rows without {@code notBefore} after the last
     * attempted row (a call cut at the soft deadline, a handler that stopped early), with any blocked rows among them:
     * the next step reads those again. A NotAttempted with {@code notBefore} is the handler's decision (e.g. a Kafka
     * partition cooling down) and counts as attempted. No chunk row attempted: every row is read, the batch fails as a
     * whole, unless a parked row of the call leaves the parked table ({@code parkedLeft}, see {@link #leavesParked});
     * then none is. Parked rows that were only parked again or kept are no progress: a call of those and an
     * unattempted chunk fails as a whole too, or a handler cut after them would hand the same call on every step.
     */
    private static int read(List<Item> chunk, boolean parkedLeft) {
        int read = chunk.size();
        boolean unread = false;
        while (read > 0 && !attempted(chunk.get(read - 1))) {
            unread |= !chunk.get(--read).dbBlocked();
        }
        return unread && (read > 0 || parkedLeft) ? read : chunk.size();
    }

    /**
     * A parked item whose outcome takes it out of the parked table, as {@link BatchClassifier#classify} settles it
     * unless the call pauses: delivered (unparked), retried (to the retry table), permanent or past the requeue limit
     * (to dead). A requeue below the limit (parked again) and a NotAttempted (kept) leave it parked.
     */
    private boolean leavesParked(Item item) {
        var outcome = item.outcome();
        return outcome instanceof Outcome.Delivered || outcome instanceof Outcome.Retry
                || outcome instanceof Outcome.Permanent || outcome instanceof Outcome.Requeue
                && BatchClassifier.requeueLimitReached(item.record(), requeue.maxRequeues());
    }

    /** Handed and not a NotAttempted without {@code notBefore}. */
    private static boolean attempted(Item item) {
        return !item.dbBlocked() && !(item.outcome() instanceof Outcome.NotAttempted n && n.notBefore() == null);
    }

    /**
     * The outcomes of a call that threw {@code error} or returned an invalid list: a transient failure of every record
     * (pause, halved chunks, probe and divert then isolate the failing one), except a single record failing
     * permanently. A {@code bug} (an Error, an invalid list) is logged at ERROR, at most once per 10 s.
     */
    private List<Outcome> failedCall(List<OutboxRecord> records, Throwable error, boolean bug) {
        if (records.size() == 1 && error instanceof OutboxPermanentException) {
            return List.of(Outcome.permanent(error));
        }
        if (bug) {
            handlerBug("outbox handler failed with an Error or returned an invalid outcome list; every record of the"
                    + " call is retried", error);
        }
        return new FailedCall(records.size(), error);
    }

    /** The outcomes of a call that failed as a whole, not record by record: each record {@code Retry(error)}. */
    private static final class FailedCall extends AbstractList<Outcome> implements RandomAccess {

        private final int size;
        private final Outcome retry;

        FailedCall(int size, Throwable error) {
            this.size = size;
            this.retry = Outcome.retry(error);
        }

        @Override
        public Outcome get(int index) {
            Objects.checkIndex(index, size);
            return retry;
        }

        @Override
        public int size() {
            return size;
        }
    }

    /** A handler bug at ERROR, at most one per 10 s for this processor. */
    private void handlerBug(String what, Throwable error) {
        long now = System.nanoTime();
        long last = bugReported.get();
        if (now - last < BUG_REPORT_INTERVAL || !bugReported.compareAndSet(last, now)) {
            bugsSuppressed.incrementAndGet();
            return;
        }
        log.error("{} ({} more since the previous report not logged)", what, bugsSuppressed.getAndSet(0), error);
    }

    /** {@link OutboxHandler#windDownRequested()} without the interrupt flag: the call on this thread was cut. */
    public static boolean windDownRequested() {
        var call = CALL.get();
        return call != null && call.windDown;
    }

    /**
     * A handler call that can be asked to wind down at the soft deadline without being cancelled, so its real outcomes
     * still come back. The request is a flag the call sees through {@link OutboxHandler#windDownRequested()} even when
     * its handler swallows the interrupt that comes with it. The interrupt never reaches a later task of the pooled
     * thread: the call drops its thread and clears the interrupt status under the lock that {@link #interrupt} holds.
     */
    static final class HandlerCall implements Callable<List<Outcome>> {

        private final Callable<List<Outcome>> body;
        @Nullable
        private Thread thread; // under this
        private volatile boolean windDown;

        HandlerCall(Callable<List<Outcome>> body) {
            this.body = body;
        }

        @Override
        public List<Outcome> call() throws Exception {
            synchronized (this) {
                thread = Thread.currentThread();
            }
            CALL.set(this);
            try {
                return body.call();
            } finally {
                CALL.remove();
                synchronized (this) {
                    thread = null;
                    Thread.interrupted();
                }
            }
        }

        /** Asks the call to wind down, and interrupts it while it runs (not before it starts or after it returns). */
        synchronized void interrupt() {
            windDown = true;
            if (thread != null) {
                thread.interrupt();
            }
        }
    }

    private static void end(Span span, Outcome outcome) {
        Throwable error = null;
        String name;
        if (outcome instanceof Outcome.Retry r) {
            name = "retry";
            error = r.error();
        } else if (outcome instanceof Outcome.Permanent p) {
            name = "permanent";
            error = p.error();
        } else if (outcome instanceof Outcome.NotAttempted) {
            name = "not_attempted";
        } else if (outcome instanceof Outcome.Requeue) {
            name = "requeued";
        } else {
            name = "delivered";
        }
        span.setAttribute("outbox.outcome", name);
        if (error != null) {
            span.recordException(error);
            span.setStatus(StatusCode.ERROR);
        }
        span.end();
    }

    private static void logDead(OutboxRecord r, int attempts, String error) {
        log.error("outbox message {} (topic {}, id {}) moved to the dead table after {} attempts: {}",
                r.messageId(), r.topic(), r.id(), attempts, error);
    }

    @Nullable
    private static String firstError(List<Item> items) {
        for (var item : items) {
            if (item.outcome() instanceof Outcome.Retry r) {
                return r.error().toString();
            }
        }
        return null;
    }

    /** {@code now - createdAt}, zero when the clocks disagree. */
    private static Duration delay(OutboxRecord r, Instant now) {
        var delay = Duration.between(r.createdAt(), now);
        return delay.isNegative() ? Duration.ZERO : delay;
    }
}
