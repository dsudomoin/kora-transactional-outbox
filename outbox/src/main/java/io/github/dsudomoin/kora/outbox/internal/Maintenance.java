package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.Ticks.Tick;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * Partition maintenance and log cleanup. Every run first checks the xid counter ({@link #halted()}),
 * then reads, from {@code <base>_maintenance}, when a run last completed every step on any instance
 * ({@code outbox.maintenance.since_success}, seconds; unless halted, a WARN at most once a minute once that is over 5 ×
 * {@code maintenanceInterval}); neither needs a lock, so every instance does both, and a failed read is a WARN. The
 * rest needs a connection that keeps one server session: behind a transaction-pooling proxy
 * ({@link PartitionDdl#backendChanges}) the run stops there, with an ERROR at most once a minute and
 * {@code outbox.maintenance.transaction_pooling} at 1 (0 otherwise). It runs on at most one instance at a time: it
 * holds the session lock {@code pg_try_advisory_lock(internalNs, LOCK_MAINTENANCE)} on one autocommit connection with
 * {@code idle_session_timeout = 60s}, so Postgres ends the session of a frozen holder and frees the lock; the setting
 * is reset and the lock released before the connection goes back to the pool. Steps, each on its own (a failure is
 * logged and the next step still runs): finish a pending detach, premade partitions (the extension swaps the tail in
 * one transaction, a tail with rows becoming a range in it, the library's one ACCESS EXCLUSIVE on the parent: writers
 * stall for its lock wait, at most {@code outbox.partitions.swapLockTimeout}, and the milliseconds it holds the lock;
 * before it, a wait of up to {@code batchTimeout} + 5 s for relay steps in flight blocks no writer), drop of processed
 * partitions, drop of ranges left detached but not dropped: by a crash of this library's maintenance once relayed
 * (one still holding rows at or above the horizon first has those moved back into the outbox as new messages,
 * {@link OrphanRestore}, unless {@code outbox.partitions.restoreOrphans} is off), by anyone else only when empty
 * ({@code outbox.partitions.orphaned} counts the ones kept, with an ERROR each run, as their rows lie outside the
 * outbox), tick cleanup, cursor cleanup, gauges; when none failed, the run is recorded as the last success. One
 * long-lived instance per outbox table: it registers the gauges {@code outbox.shard.lag},
 * {@code outbox.retry.size}, {@code outbox.dead.size}, {@code outbox.parked.size}, {@code outbox.partitions.premade},
 * {@code outbox.partitions.overflow}, {@code outbox.partitions.xids_left} (xids before inserts fail while the tail is
 * not attached, with an ERROR each run; NaN while it is), {@code outbox.maintenance.transaction_pooling} and
 * {@code outbox.maintenance.since_success} (NaN until the first run).
 */
public final class Maintenance {

    private static final Logger log = LoggerFactory.getLogger(Maintenance.class);
    private static final long LOG_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    /** Maintenance not completing for this many intervals is a WARN. */
    private static final int STALE_INTERVALS = 5;

    private final JdbcConnectionFactory db;
    private final TableNames names;
    private final int shards;
    private final Duration detachLockTimeout;
    /** How long the tail swap waits for relay steps in flight: a step's handler call runs up to batchTimeout. */
    private final Duration quiesceTimeout;
    private final XidPartitions partitions;
    private final Ticks ticks;
    private final Cursors cursors;
    private final RetryStore retries;
    private final ParkedStore parked;
    final OrphanRestore orphanRestore; // for a test's seam
    private final boolean restoreOrphans;
    final String xidCheck; // for a test of its plan
    private final String haltInsert;
    private final String sinceSuccess;
    private final String succeeded;
    private final Duration staleAfter;
    private final AtomicLong poolingLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong staleLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private volatile boolean halted;
    private long haltLogged; // under this
    private volatile boolean stepFailed; // in the current run, under LOCK_MAINTENANCE

    private volatile double lag = Double.NaN;
    private volatile double retrySize = Double.NaN;
    private volatile double deadSize = Double.NaN;
    private volatile double parkedSize = Double.NaN;
    private volatile double premade = Double.NaN;
    private volatile double xidsLeft = Double.NaN;
    private volatile double orphaned = Double.NaN;
    private volatile double transactionPooling = Double.NaN;
    /** {@link System#nanoTime()} of the last success known on any instance, null before the first read. */
    @Nullable
    private volatile Long lastSuccess;

    public Maintenance(JdbcConnectionFactory db, TableNames names, OutboxConfig config, OutboxMetrics metrics) {
        this(db, names, config, metrics, PartitionDdl.DETACH_LOCK_TIMEOUT);
    }

    Maintenance(JdbcConnectionFactory db, TableNames names, OutboxConfig config, OutboxMetrics metrics,
            Duration detachLockTimeout) {
        this.db = db;
        this.names = names;
        this.shards = config.shards();
        this.detachLockTimeout = detachLockTimeout;
        this.quiesceTimeout = config.relay().batchTimeout().plusSeconds(5);
        this.partitions = new XidPartitions(names, config.partitions());
        this.ticks = new Ticks(names, config.shards());
        this.cursors = new Cursors(names);
        this.retries = new RetryStore(names);
        this.parked = new ParkedStore(names);
        this.orphanRestore = new OrphanRestore(names, new Resender(names, config.shards()));
        this.restoreOrphans = config.partitions().restoreOrphans();
        // every tick, not just the newest: workers keep ticking after a regression, and their new ticks look fine; the
        // highest xmax comes from the index <tick>_xmax, as ticks pile up while a paused shard holds cleanup back
        this.xidCheck = "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint,"
                + " (select pg_snapshot_xmax(snapshot)::text::bigint from " + names.tick()
                + " order by pg_snapshot_xmax(snapshot) desc limit 1),"
                + " (select max_tick_xmax::text::bigint from " + names.halt() + ")";
        this.haltInsert = "insert into " + names.halt() + " as h (max_tick_xmax) values (?::text::xid8)"
                + " on conflict (id) do update set max_tick_xmax = greatest(h.max_tick_xmax, excluded.max_tick_xmax)";
        // the database clock for both ends, so instances with skewed clocks agree
        this.sinceSuccess = "select extract(epoch from clock_timestamp() - succeeded_at)::float8 from "
                + names.maintenance();
        this.succeeded = "insert into " + names.maintenance() + " (succeeded_at) values (clock_timestamp())"
                + " on conflict (id) do update set succeeded_at = excluded.succeeded_at";
        this.staleAfter = config.partitions().maintenanceInterval().multipliedBy(STALE_INTERVALS);
        metrics.gauge("outbox.shard.lag", () -> lag);
        metrics.gauge("outbox.retry.size", () -> retrySize);
        metrics.gauge("outbox.dead.size", () -> deadSize);
        metrics.gauge("outbox.parked.size", () -> parkedSize);
        metrics.gauge("outbox.partitions.premade", () -> premade);
        metrics.gauge("outbox.partitions.overflow", () -> partitions.overflow() ? 1 : 0);
        metrics.gauge("outbox.partitions.xids_left", () -> xidsLeft);
        metrics.gauge("outbox.partitions.orphaned", () -> orphaned);
        metrics.gauge("outbox.maintenance.transaction_pooling", () -> transactionPooling);
        metrics.gauge("outbox.maintenance.since_success", () -> {
            var at = lastSuccess;
            return at == null ? Double.NaN : (System.nanoTime() - at) / 1e9;
        });
    }

    /**
     * As of the last check: true while the xid-regression halt is recorded in {@code <base>_halt}. A check that finds
     * the counter below a tick's xmax (dump/restore or logical migration: relaying would skip new rows) records it with
     * the highest tick xmax; relay workers then take no shards and {@code drain()} fails, on every instance and across
     * restarts. The counter catching up does not end it (cursors still sit on pre-regression ticks and would skip the
     * rows written meanwhile): only ticks repaired below the recorded xmax with a consistent counter, or an operator
     * deleting the row, do (README "xid regression recovery").
     */
    public boolean halted() {
        return halted;
    }

    /** The xid-regression check alone, without the maintenance lock: refreshes {@link #halted()} and returns it. */
    public boolean checkHalt() {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            return checkXidCounter(c);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    /**
     * One run; false without maintaining anything when another session holds the maintenance lock or the connection
     * does not keep one server session. While halted only the reads and the xid check run (true).
     */
    public boolean runOnce() {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            boolean halted = checkXidCounter(c); // first: nothing may keep it from running
            readLastSuccess(c, halted);
            return halted || (sessionStable(c) && maintain(c));
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    /** While halted, the halt ERROR says why no run completes, so the stale WARN keeps quiet. */
    private void readLastSuccess(Connection c, boolean halted) {
        double age;
        try (var st = c.createStatement(); var rs = st.executeQuery(sinceSuccess)) {
            if (!rs.next()) {
                return; // the row is made at bootstrap; an operator deleted it
            }
            age = rs.getDouble(1);
        } catch (SQLException e) { // the run goes on; outbox.maintenance.since_success grows from the last read
            if (due(staleLogged)) {
                log.warn("outbox maintenance of {} cannot read its last success from {} (SQLState {}): {}; the run"
                        + " goes on, a restart makes the table again", names.base(), names.maintenance(),
                        e.getSQLState(), e.getMessage());
            }
            return;
        }
        lastSuccess = System.nanoTime() - (long) (age * 1e9);
        if (!halted && age > staleAfter.toMillis() / 1e3 && due(staleLogged)) {
            log.warn("outbox maintenance of {} has not completed a run on any instance for {} s, over {} ×"
                    + " outbox.partitions.maintenanceInterval: partitions are not extended or dropped and ticks pile"
                    + " up meanwhile. Earlier WARN or ERROR lines name failing steps; if runs never start, a session"
                    + " holds the maintenance lock: select pid from pg_locks where locktype = 'advisory' and classid"
                    + " = {} and objid = {} and objsubid = 2", names.base(), Math.round(age), STALE_INTERVALS,
                    Integer.toUnsignedLong(names.internalNamespace()), TableNames.LOCK_MAINTENANCE);
        }
    }

    private boolean sessionStable(Connection c) throws SQLException {
        var pids = PartitionDdl.backendChanges(c);
        transactionPooling = pids == null ? 0 : 1;
        if (pids != null && due(poolingLogged)) {
            log.error("outbox maintenance of {} {}: pg_backend_pid() was {} in consecutive statements on one"
                    + " connection, so its session lock and settings would leak into other clients' sessions; runs are"
                    + " skipped (gauge outbox.maintenance.transaction_pooling) until the DataSource reaches Postgres"
                    + " directly or through a session-mode pool; this ERROR repeats at most once a minute",
                    names.base(), PartitionDdl.SESSION_POOLING_REQUIRED, pids);
        }
        return pids == null;
    }

    /** True at most once per {@code LOG_INTERVAL} for {@code logged}. */
    private static boolean due(AtomicLong logged) {
        long now = System.nanoTime();
        long last = logged.get();
        return now - last >= LOG_INTERVAL && logged.compareAndSet(last, now);
    }

    private boolean maintain(Connection c) throws SQLException {
        // before the lock, so a holder frozen right after taking it loses it too
        execute(c, "set idle_session_timeout = '60s'");
        boolean locked = false;
        Throwable failure = null;
        try {
            locked = advisory(c, "pg_try_advisory_lock");
            if (locked && run(c)) {
                execute(c, succeeded);
                lastSuccess = System.nanoTime();
            }
            return locked;
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            try {
                if (locked) {
                    advisory(c, "pg_advisory_unlock");
                }
                execute(c, "reset idle_session_timeout");
            } catch (SQLException e) { // the session died, and its lock and setting with it: keep the cause on top
                if (failure == null) {
                    throw e;
                }
                failure.addSuppressed(e);
            }
        }
    }

    /** True when every step succeeded. */
    private boolean run(Connection c) {
        stepFailed = false;
        step("finish pending detach", () -> {
            partitions.finishPendingDetach(c, detachLockTimeout);
            return null;
        });
        step("partition extension", () -> {
            partitions.ensurePremade(c, detachLockTimeout, quiesceTimeout);
            return null;
        });
        var slowest = step("cursor read", () -> slowestTick(c));
        orphaned = Double.NaN; // unknown unless the orphan step completes
        if (slowest != null) {
            step("partition drop", () -> partitions.dropProcessed(c, slowest.snapshot().xmin(), detachLockTimeout));
            step("orphan drop", () -> {
                long horizon = slowest.snapshot().xmin();
                int left = 0;
                for (var orphan : partitions.dropOrphans(c, horizon, detachLockTimeout)) {
                    if (orphan.ours() && restoreOrphans) {
                        long moved = orphanRestore.restore(c, orphan.table(), horizon, detachLockTimeout);
                        log.info("outbox table {}, a range of {} detached as relayed, held rows the relay could not"
                                + " deliver: moved {} rows at or above xid {}, the slowest shard's horizon, back into"
                                + " the outbox with new ids and xids, so they are new messages, delivered after later"
                                + " messages of their keys; rows below it went with the table, which is dropped",
                                orphan.table(), names.base(), moved, horizon);
                    } else if (orphan.ours()) {
                        left++;
                        // the columns send writes: moved rows are new messages, with new ids and xids
                        log.error("outbox table {} is a range of {} detached as relayed, yet it holds rows at or above"
                                + " xid {}, the slowest shard's horizon: they lie outside the outbox, so the relay"
                                + " cannot deliver them. It is kept (gauge outbox.partitions.orphaned); move them back"
                                + " with insert into {} (shard, message_id, topic, key, payload, headers) select shard,"
                                + " message_id, topic, key, payload, headers from {} where xid >= '{}'::xid8 (they get"
                                + " new ids, so they are delivered after later messages of their keys), then drop it",
                                orphan.table(), names.base(), horizon, names.base(), orphan.table(), horizon);
                    } else {
                        left++;
                        log.error("outbox table {} holds rows and is a range detached from {}, but not by this"
                                + " library's maintenance: rows of a range detached before the relay reached them were"
                                + " never delivered. It is kept (gauge outbox.partitions.orphaned); compare its rows"
                                + " with what was delivered, then move the undelivered ones back with"
                                + " OutboxAdmin.restoreOrphan(\"{}\", fromXid) or POST"
                                + " /kora-outbox/admin/v1/orphans/{}/restore?fromXid=<xid>: rows at or above fromXid"
                                + " get new ids, so they are delivered after later messages of their keys, and the rest"
                                + " go with the table, which is dropped", orphan.table(), names.base(), orphan.table(),
                                orphan.table());
                    }
                }
                orphaned = left;
                return null;
            });
            step("tick cleanup", () -> ticks.deleteBelow(c, slowest.id()));
        }
        step("cursor cleanup", () -> cursors.deleteSuperseded(c));
        step("gauges", () -> {
            refreshGauges(c, slowest);
            return null;
        });
        return !stepFailed;
    }

    /** The tick of the slowest shard; shards mid-batch count by tick id, as their target is not processed yet. */
    private Tick slowestTick(Connection c) throws SQLException {
        long minTickId = cursors.latestAll(c, shards).stream().mapToLong(Cursor::tickId).min()
                .orElseThrow(() -> new IllegalStateException("no cursors in " + names.cursor()));
        return ticks.get(c, minTickId);
    }

    private void refreshGauges(Connection c, @Nullable Tick slowest) throws SQLException {
        lag = slowest == null ? Double.NaN
                : Duration.between(slowest.createdAt(), ticks.latest(c).createdAt()).toNanos() / 1e9;
        retrySize = retries.size(c);
        deadSize = retries.deadSize(c);
        parkedSize = parked.size(c);
        int ahead = partitions.premadeAhead(c);
        premade = ahead;
        var left = partitions.xidsLeft(c);
        xidsLeft = left == null ? Double.NaN : left;
        if (left != null) {
            log.error("outbox partition tail {} is not attached, so no partition takes rows above the last range:"
                    + " inserts into {} fail once {} more xids are used. Each maintenance run tries to attach it back"
                    + " or make a new one; a long transaction or snapshot (pg_dump, a repeatable-read report) blocks"
                    + " that until it ends: see pg_stat_activity, oldest xact_start with backend_xmin set",
                    names.tail(), names.base(), left);
        } else if (ahead < 2) {
            log.error("only {} premade xid partitions of {} left; inserts reach the tail soon,"
                    + " see README \"tail overflow\"", ahead, names.base());
        }
    }

    /** One step; a failure is logged (the next run retries) and yields null, so the remaining steps still run. */
    @Nullable
    private <T> T step(String name, SqlFunction0<T> step) {
        try {
            return step.apply();
        } catch (SQLException e) {
            log.warn("outbox maintenance step '{}' on {} failed (SQLState {}); the next run retries it: {}", name,
                    names.base(), e.getSQLState(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("outbox maintenance step '{}' on {} failed; the next run retries it", name, names.base(), e);
        }
        stepFailed = true;
        return null;
    }

    /**
     * The xid-regression check (README "xid regression recovery") against the recorded halt; sets {@link #halted()}
     * and returns it.
     */
    private synchronized boolean checkXidCounter(Connection c) throws SQLException {
        long current;
        long tickXmax;
        Long recorded;
        try (var st = c.createStatement(); var rs = st.executeQuery(xidCheck)) {
            rs.next();
            current = rs.getLong(1);
            tickXmax = rs.getLong(2); // 0 without ticks
            recorded = rs.getObject(3, Long.class);
        }
        boolean consistent = current >= tickXmax;
        if (consistent && recorded != null && tickXmax < recorded) { // ticks repaired: the regressed ones are gone
            execute(c, "delete from " + names.halt());
            recorded = null;
        }
        if (consistent && recorded == null) {
            if (halted) {
                halted = false;
                log.info("outbox relay resumed: no xid-regression halt recorded in {} and the xid counter is above"
                        + " every tick", names.halt());
            }
            return false;
        }
        if (recorded == null || tickXmax > recorded) {
            try (var ps = c.prepareStatement(haltInsert)) {
                ps.setLong(1, tickXmax);
                ps.executeUpdate();
            }
        }
        long now = System.nanoTime();
        if (!halted || now - haltLogged >= LOG_INTERVAL) {
            log.error("outbox relay halted: xid counter went backwards (current xid {}, highest tick xmax {} in {};"
                    + " restored from a dump or logically migrated?): relaying would skip rows; it stays halted, on"
                    + " every instance and across restarts, while {} holds a row,"
                    + " see README \"xid regression recovery\"", current, tickXmax, names.tick(), names.halt());
            haltLogged = now;
        }
        halted = true;
        return true;
    }

    private boolean advisory(Connection c, String function) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_MAINTENANCE);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }
}
