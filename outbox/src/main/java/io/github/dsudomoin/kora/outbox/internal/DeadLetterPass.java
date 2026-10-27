package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.DeadLetterDecision;
import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.OutboxDeadLetterHandler;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction1;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

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
 * others go on. Any other lock the insert waits for is waited for at most {@code partitions.swapLockTimeout} each:
 * while it waits, writers of the keys it holds wait too. A batch that fails after its claim rolls back, and its
 * letters are then due again after {@code initialDelay}, so they hold back no letter behind them; a lock timeout
 * (55P03) is a WARN, at most once a minute, and ends the phase until the next pass; any other failure propagates.
 */
public final class DeadLetterPass {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPass.class);
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String COLUMNS = "id, message_id, topic, key, payload, headers, created_at, attempts,"
            + " failed_at, error, status, redrives, handler_attempts, due_at";

    private final JdbcConnectionFactory db;
    @Nullable
    private final OutboxDeadLetterHandler hook;
    private final OutboxMetrics metrics;
    private final Resender resender;
    private final String dead;
    private final int batchSize;
    private final Duration timeout;
    private final Duration initialDelay;
    private final Duration maxDelay;
    private final int keyNamespace;
    private final String redriveLockTimeout;
    private final String claim;
    private final String keep;
    private final String schedule;
    private final String fail;
    private final String delete;
    private final String defer;
    private final String heldKeys;
    private final String remember;
    private final String keepPending;
    private final String nextDue;
    private final ThreadPoolExecutor executor;
    /** When a WARN was last logged ({@code nanoTime}), and how many went unlogged since. */
    private final AtomicLong warned = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    private final AtomicInteger suppressed = new AtomicInteger();

    /** A claimed letter with its headers in the stored encoding, as a redrive inserts them. */
    private record Claimed(DeadLetter letter, @Nullable byte[] headers) {}

    /** A committed hand-over: the letters claimed; the call's failure, or the decisions it committed. */
    private record Batch(int claimed, @Nullable Throwable failure, List<DeadLetterDecision> decisions) {}

    public DeadLetterPass(JdbcConnectionFactory db, TableNames names, OutboxConfig config,
            @Nullable OutboxDeadLetterHandler hook, OutboxMetrics metrics) {
        OutboxSchema.validate(config);
        this.db = db;
        this.hook = hook;
        this.metrics = metrics;
        this.resender = new Resender(names, config.shards());
        this.dead = names.dead();
        this.batchSize = config.deadLetters().batchSize();
        this.timeout = config.deadLetters().timeout();
        this.initialDelay = config.relay().retry().initialDelay();
        this.maxDelay = config.relay().retry().maxDelay();
        this.keyNamespace = names.keyNamespace();
        this.redriveLockTimeout = "set local lock_timeout = '"
                + config.partitions().swapLockTimeout().toMillis() + "ms'";
        claim = "select " + COLUMNS + " from " + dead + " where status = ? and due_at <= now() order by due_at, id"
                + " limit ? for update skip locked";
        keep = "update " + dead + " set status = 'kept', due_at = null where id = any(?)";
        schedule = "update " + dead + " set status = 'scheduled',"
                + " due_at = least(greatest(?, now()), now() + interval '7 days') where id = ?";
        fail = "update " + dead + " set handler_attempts = handler_attempts + 1, last_handler_error = ?,"
                + " due_at = now() + make_interval(secs => ?) where id = ?";
        delete = "delete from " + dead + " where id = any(?)";
        defer = "update " + dead + " set due_at = now() + make_interval(secs => ?) where id = any(?)";
        // one row whatever the driver's fetch size, so every hash is tried (see Resender.LOCK_ALL); never waits
        heldKeys = "select array_agg(h) filter (where not pg_try_advisory_xact_lock(?, h)) from unnest(?::int[]) h";
        // one row per message, should two of its letters be redriven together
        remember = "insert into " + names.redriven() + " (message_id, redrives)"
                + " select m, max(r) from unnest(?::uuid[], ?::int[]) u(m, r) group by m"
                + " on conflict (message_id) do update set redrives = excluded.redrives, at = now()";
        // due_at is set on every pending letter: the partial index finds them, not a scan of all the kept ones
        keepPending = "update " + dead + " set status = 'kept', due_at = null where id in (select id from " + dead
                + " where status = 'pending' and due_at is not null for update skip locked)";
        nextDue = "select min(due_at) from " + dead + " where due_at > now()";
        this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
            var t = new Thread(r, "outbox-dead-letter-hook");
            t.setDaemon(true);
            return t;
        });
    }

    /** A hook is set: not null and not {@link OutboxDeadLetterHandler#NONE}. */
    public boolean active() {
        return hook != null && hook != OutboxDeadLetterHandler.NONE;
    }

    /**
     * With a hook, hands due pending letters to it batch after batch, until a batch is not full or a call fails (its
     * backoff then holds the hook back, not just its letters); without one, makes every pending letter kept. Then
     * redrives the due scheduled letters, those just decided {@code redrive} included. Returns the letters this run
     * redrove: the caller wakes the relay when there are any. One run at a time per pass, so a second caller's hook
     * call never waits for the first one's inside its own timeout.
     */
    public synchronized int runOnce() {
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
            throw new RuntimeSqlException(e);
        }
    }

    /**
     * The earliest {@code due_at} still ahead, null when none: a letter due already was handled by the last run or is
     * held by another pass, which decides it or sets its backoff.
     */
    @Nullable
    public Instant nextDue() {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            try (var st = c.createStatement(); var rs = st.executeQuery(nextDue)) {
                rs.next();
                var at = rs.getObject(1, OffsetDateTime.class);
                return at == null ? null : at.toInstant();
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    /** Stops the hook thread, interrupting a call in progress; a later {@link #runOnce} with letters fails. */
    public void close() {
        executor.shutdownNow();
    }

    private void handOver(Connection c) throws SQLException {
        while (true) {
            var ids = new ArrayList<Long>();
            Batch batch;
            try {
                batch = inTx(c, tx -> handOverBatch(tx, ids));
            } catch (SQLException | RuntimeException e) {
                recover(c, ids, e);
                return;
            }
            if (batch.failure() != null) {
                metrics.deadHookFailure();
                warn("outbox dead-letter hook failed on " + batch.claimed() + " letters of " + dead
                        + "; they stay pending and are handed to it again after a backoff", batch.failure());
                return;
            }
            for (var decision : batch.decisions()) {
                metrics.deadDecision(name(decision));
            }
            if (batch.claimed() < batchSize) {
                return;
            }
        }
    }

    /** {@code ids} gets the claimed letters' ids, for {@link #recover} should the transaction fail after the claim. */
    private Batch handOverBatch(Connection tx, List<Long> ids) throws SQLException {
        var claimed = claim(tx, "pending");
        claimed.forEach(c -> ids.add(c.letter().id()));
        if (claimed.isEmpty()) {
            return new Batch(0, null, List.of());
        }
        List<DeadLetterDecision> decisions;
        try {
            decisions = call(claimed.stream().map(Claimed::letter).toList());
        } catch (ExecutionException e) {
            return failed(tx, claimed, e.getCause());
        } catch (TimeoutException e) {
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
        byIds(tx, keep, kept);
        byIds(tx, delete, discarded);
        return new Batch(claimed.size(), null, decisions);
    }

    /** Due scheduled letters, batch after batch until one is not full; returns how many were redriven. */
    private int redriveScheduled(Connection c) throws SQLException {
        int redriven = 0;
        while (true) {
            var ids = new ArrayList<Long>();
            try {
                redriven += inTx(c, tx -> redriveBatch(tx, ids));
            } catch (SQLException | RuntimeException e) {
                recover(c, ids, e);
                return redriven;
            }
            if (ids.size() < batchSize) {
                return redriven;
            }
        }
    }

    /** One batch of due scheduled letters, but those whose key is held, which are deferred; {@code ids} as claimed. */
    private int redriveBatch(Connection tx, List<Long> ids) throws SQLException {
        var claimed = claim(tx, "scheduled");
        claimed.forEach(c -> ids.add(c.letter().id()));
        var hashes = claimed.stream()
                .map(c -> c.letter().key() == null ? null : Hashing.keyHash(c.letter().topic(), c.letter().key()))
                .toList();
        var held = held(tx, hashes);
        var ready = new ArrayList<Claimed>();
        var deferred = new ArrayList<Long>();
        for (int i = 0; i < claimed.size(); i++) {
            if (hashes.get(i) != null && held.contains(hashes.get(i))) {
                deferred.add(claimed.get(i).letter().id());
            } else {
                ready.add(claimed.get(i));
            }
        }
        defer(tx, deferred);
        redrive(tx, ready); // its own key locks are ours already: Resender takes them again without waiting
        return ready.size();
    }

    /** Tries the key lock of each distinct hash without waiting; returns the hashes another transaction holds. */
    private Set<Integer> held(Connection tx, List<Integer> hashes) throws SQLException {
        var distinct = hashes.stream().filter(Objects::nonNull).distinct().toArray(Integer[]::new);
        if (distinct.length == 0) {
            return Set.of();
        }
        try (var ps = tx.prepareStatement(heldKeys)) {
            ps.setInt(1, keyNamespace);
            ps.setArray(2, tx.createArrayOf("int4", distinct));
            try (var rs = ps.executeQuery()) {
                rs.next();
                var held = rs.getArray(1);
                return held == null ? Set.of() : Set.of((Integer[]) held.getArray());
            }
        }
    }

    /**
     * After a batch rolled back on {@code failure}: its claimed letters, if any, are due again after
     * {@code initialDelay}, set in a fresh transaction, so they hold back no letter behind them and {@link #nextDue}
     * finds them. Returns after a lock timeout (a WARN), which ends the phase: no key lock is waited for, so it was a
     * table's lock, which the batches after it would wait for too. Rethrows any other failure.
     */
    private void recover(Connection c, List<Long> ids, Exception failure) throws SQLException {
        if (!ids.isEmpty()) {
            try {
                inTx(c, tx -> {
                    defer(tx, ids);
                    return null;
                });
            } catch (SQLException | RuntimeException e) {
                failure.addSuppressed(e);
            }
        }
        if (failure instanceof SQLException s) {
            if (blocked(s)) {
                return;
            }
            throw s;
        }
        throw (RuntimeException) failure; // the callers catch SQLException | RuntimeException only
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
     * hook code inside the deadline too and an invalid one fails as the hook's {@link ExecutionException}.
     */
    private List<DeadLetterDecision> call(List<DeadLetter> letters)
            throws InterruptedException, ExecutionException, TimeoutException {
        Future<List<DeadLetterDecision>> call;
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
                    ru.tinkoff.kora.common.Context.clear(); // nothing leaks into the next call on this thread
                }
            });
        } catch (RejectedExecutionException e) {
            throw new IllegalStateException("outbox dead-letter pass is closed", e);
        }
        try {
            return call.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException | InterruptedException e) {
            call.cancel(true);
            executor.purge(); // a hook ignoring the interrupt holds the thread: calls cut behind it do not pile up
            throw e;
        }
    }

    /** Every letter of a failed call stays pending, due after a backoff by its failed calls so far. */
    private Batch failed(Connection tx, List<Claimed> claimed, Throwable failure) throws SQLException {
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

    /** Back into the outbox, the redrive counted in {@code <redriven>}, the dead rows deleted. */
    private void redrive(Connection tx, List<Claimed> letters) throws SQLException {
        if (letters.isEmpty()) {
            return;
        }
        try (var st = tx.createStatement()) {
            st.execute(redriveLockTimeout); // after the claim: the insert waits for a lock no longer than the swap
        }
        resender.insertAll(tx, letters.stream().map(c -> new Resender.Row(c.letter().messageId(),
                c.letter().topic(), c.letter().key(), c.letter().payload(), c.headers())).toList());
        try (var ps = tx.prepareStatement(remember)) {
            ps.setArray(1, tx.createArrayOf("uuid", letters.stream().map(c -> c.letter().messageId()).toArray()));
            ps.setArray(2, tx.createArrayOf("int4", letters.stream().map(c -> c.letter().redrives() + 1).toArray()));
            ps.executeUpdate();
        }
        byIds(tx, delete, letters.stream().map(c -> c.letter().id()).toList());
    }

    private List<Claimed> claim(Connection tx, String status) throws SQLException {
        try (var ps = tx.prepareStatement(claim)) {
            ps.setString(1, status);
            ps.setInt(2, batchSize);
            try (var rs = ps.executeQuery()) {
                var claimed = new ArrayList<Claimed>();
                while (rs.next()) {
                    claimed.add(claimed(rs));
                }
                return claimed;
            }
        }
    }

    private static Claimed claimed(ResultSet rs) throws SQLException {
        var headers = rs.getBytes(6);
        var dueAt = rs.getObject(14, OffsetDateTime.class);
        return new Claimed(new DeadLetter(
                rs.getLong(1),
                rs.getObject(2, UUID.class),
                rs.getString(3),
                rs.getBytes(4),
                rs.getBytes(5),
                HeaderCodec.decode(headers),
                rs.getObject(7, OffsetDateTime.class).toInstant(),
                rs.getInt(8),
                rs.getObject(9, OffsetDateTime.class).toInstant(),
                rs.getString(10),
                DeadLetter.Status.valueOf(rs.getString(11).toUpperCase(Locale.ROOT)),
                rs.getInt(12),
                rs.getInt(13),
                dueAt == null ? null : dueAt.toInstant()), headers);
    }

    private static void byIds(Connection tx, String sql, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        try (var ps = tx.prepareStatement(sql)) {
            ps.setArray(1, tx.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
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
        warn("outbox dead-letter pass on " + dead + " timed out waiting for a lock: its batch rolled back and is"
                + " due again after outbox.relay.retry.initialDelay; " + e.getMessage(), null);
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

    /** A WARN at most once a minute for this pass. */
    private void warn(String what, @Nullable Throwable error) {
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
