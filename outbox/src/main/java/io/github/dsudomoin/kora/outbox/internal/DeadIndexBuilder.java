package io.github.dsudomoin.kora.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * Builds the dead table's {@code <dead>_due} when bootstrap left it out (a dead table with rows) or an interrupted
 * build left it INVALID, off the maintenance lock and thread: a concurrent build waits for every transaction with an
 * older snapshot, a session queued for {@code LOCK_MAINTENANCE} included, so holding that lock would deadlock with it,
 * and the scan may take hours. {@link #startIfDue} only checks, from the maintenance step; the build runs on one daemon
 * thread {@code outbox-dead-index} per instance, on its own autocommit pool connection, under the session lock
 * {@code pg_try_advisory_lock(internalNs, LOCK_DEAD_INDEX)}: an instance that does not get it leaves the build to the
 * holder. Holding it, the thread reads the catalog again, drops an INVALID index concurrently and builds it
 * concurrently: SHARE UPDATE EXCLUSIVE, which dead inserts pass; no {@code statement_timeout}, each lock wait (the wait
 * for older transactions included) under {@code lockTimeout}, the session's own values given back. Any failure is a
 * WARN and backs the next try off by 1, 2, 4… maintenance intervals, at most {@link #MAX_BACKOFF}; a success resets
 * it. The backoff is in memory: a restart tries at its first run. {@link #close} cancels a statement in flight; the
 * thread then aborts its connection, which the pool evicts rather than reuse with the build's timeouts, and the next
 * build drops the INVALID leftover. An {@link Error} is logged at ERROR and ends the thread.
 */
final class DeadIndexBuilder {

    /** The longest backoff after a failed build. */
    static final Duration MAX_BACKOFF = Duration.ofHours(24);
    static final String THREAD_NAME = "outbox-dead-index";

    private static final Logger log = LoggerFactory.getLogger(DeadIndexBuilder.class);
    private static final String STATE = "select indisvalid from pg_index where indexrelid = to_regclass(?)";

    private final JdbcConnectionFactory db;
    private final TableNames names;
    private final String index;
    private final Duration interval;
    private final Duration lockTimeout;
    /** Guards {@link #closed}, {@link #thread} and {@link #inDdl}, so a close either sees the DDL or prevents it. */
    private final Object monitor = new Object();
    private boolean closed;
    @Nullable
    private Thread thread;
    /** The builder's backend pid while a drop or build runs on it, 0 otherwise: what {@link #close} cancels. */
    private int inDdl;
    private volatile int failures;
    private volatile long retryAt = System.nanoTime();

    DeadIndexBuilder(JdbcConnectionFactory db, TableNames names, Duration interval, Duration lockTimeout) {
        this.db = db;
        this.names = names;
        this.index = names.dead() + "_due";
        this.interval = interval;
        this.lockTimeout = lockTimeout;
    }

    /** From the maintenance step, on its connection {@code c}: starts the thread when the index needs it and may. */
    void startIfDue(Connection c) throws SQLException {
        synchronized (monitor) {
            if (closed || (thread != null && thread.isAlive()) || System.nanoTime() - retryAt < 0) {
                return;
            }
        }
        if (Boolean.TRUE.equals(state(c))) {
            return;
        }
        synchronized (monitor) {
            if (closed || (thread != null && thread.isAlive())) {
                return;
            }
            var t = new Thread(this::build, THREAD_NAME);
            t.setDaemon(true);
            thread = t;
            t.start();
        }
    }

    /** True while the thread runs. */
    boolean running() {
        synchronized (monitor) {
            return thread != null && thread.isAlive();
        }
    }

    /** Stops starting builds, cancels a drop or build in flight and waits for the thread up to {@code timeout}. */
    void close(Duration timeout) {
        Thread t;
        synchronized (monitor) {
            closed = true;
            t = thread;
            // under the monitor, so the thread cannot begin another DDL statement meanwhile
            // ponytail: a cancel landing just after its DDL ends hits PartitionDdl's restore instead; the thread then
            // aborts the connection (the pool evicts it), so a cancelled build may cost one pooled connection
            if (inDdl != 0) {
                try (var c = db.newConnection(); var ps = c.prepareStatement("select pg_cancel_backend(?)")) {
                    ps.setInt(1, inDdl);
                    ps.execute();
                } catch (SQLException e) {
                    log.warn("outbox could not cancel the build of {} at shutdown: {}", index, e.getMessage());
                }
            }
        }
        if (t == null) {
            return;
        }
        try {
            TimeUnit.NANOSECONDS.timedJoin(t, timeout.toNanos());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            log.warn("outbox thread {} still busy {} after shutdown began; it ends with its statement", THREAD_NAME,
                    timeout);
        }
    }

    private void build() {
        long start = System.nanoTime();
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            int pid = pid(c);
            if (!advisory(c, "pg_try_advisory_lock")) {
                return; // another instance builds it
            }
            boolean aborted = false;
            Exception failure = null;
            try {
                var valid = state(c);
                if (Boolean.TRUE.equals(valid)) {
                    return;
                }
                if (valid != null) {
                    var drop = "drop index concurrently if exists " + index;
                    if (!ddl(c, pid, lockTimeout.plus(PartitionDdl.DDL_MARGIN), drop)) {
                        return;
                    }
                    log.info("outbox dropped {}, left invalid by an interrupted concurrent build", index);
                }
                if (!ddl(c, pid, Duration.ZERO, "create index concurrently if not exists " + names.unqualified(index)
                        + " on " + names.dead() + " (due_at) where due_at is not null")) {
                    return;
                }
            } catch (SQLException | RuntimeException e) {
                failure = e;
                if (isClosed()) { // its timeouts may still be set: the pool evicts it instead of reusing it
                    aborted = abort(c, e);
                }
                throw e;
            } finally {
                if (!aborted) { // an aborted session took its lock with it
                    try {
                        advisory(c, "pg_advisory_unlock");
                    } catch (SQLException e) { // the session died, and its lock with it: keep the cause on top
                        if (failure == null) {
                            throw e;
                        }
                        failure.addSuppressed(e);
                    }
                }
            }
            failures = 0;
            log.info("outbox built {} concurrently in {} ms", index, (System.nanoTime() - start) / 1_000_000);
        } catch (Error e) {
            log.error("outbox thread {} stopped by a fatal error", THREAD_NAME, e);
            throw e;
        } catch (SQLException | RuntimeException e) {
            if (isClosed()) {
                log.info("outbox build of {} cancelled at shutdown; the next one drops what it left", index);
                return;
            }
            int failed = ++failures;
            var backoff = interval.multipliedBy(1L << Math.min(failed - 1, 20));
            if (backoff.compareTo(MAX_BACKOFF) > 0) {
                backoff = MAX_BACKOFF;
            }
            retryAt = System.nanoTime() + backoff.toNanos();
            String state = e instanceof SQLException s ? s.getSQLState() : e.getClass().getSimpleName();
            log.warn("outbox could not build {} concurrently after {} ms ({}: {}); a lock wait past {} ms means a long"
                    + " transaction on the database, see pg_stat_activity, oldest xact_start. Until it exists the"
                    + " dead-letter pass scans all of {} on each claim; next try in {} s (failure {} in a row)", index,
                    (System.nanoTime() - start) / 1_000_000, state, e.getMessage(), lockTimeout.toMillis(),
                    names.dead(), backoff.toSeconds(), failed);
        }
    }

    private boolean isClosed() {
        synchronized (monitor) {
            return closed;
        }
    }

    /** Closes {@code c}'s session at once; false, with the failure added to {@code cause}, when that failed. */
    private static boolean abort(Connection c, Exception cause) {
        try {
            c.abort(Runnable::run);
            return true;
        } catch (SQLException e) {
            cause.addSuppressed(e);
            return false;
        }
    }

    /** Runs {@code sql} unless closed, cancellable by {@link #close}; false when closed. */
    private boolean ddl(Connection c, int pid, Duration statementTimeout, String sql) throws SQLException {
        synchronized (monitor) {
            if (closed) {
                return false;
            }
            inDdl = pid;
        }
        try {
            PartitionDdl.execute(c, lockTimeout, statementTimeout, sql);
        } finally {
            synchronized (monitor) {
                inDdl = 0;
            }
        }
        return true;
    }

    private static int pid(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select pg_backend_pid()")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Nullable
    private Boolean state(Connection c) throws SQLException {
        try (var ps = c.prepareStatement(STATE)) {
            ps.setString(1, index);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getBoolean(1) : null;
            }
        }
    }

    private boolean advisory(Connection c, String function) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_DEAD_INDEX);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }
}
