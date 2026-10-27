package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Position;
import io.github.dsudomoin.kora.outbox.internal.Ticks.Tick;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * Partition maintenance and log cleanup. Every run first checks the xid counter ({@link #halted()}), then reads, from
 * {@code <base>_maintenance}, when a run last completed every step on any instance
 * ({@code outbox.maintenance.since_success}, seconds; unless halted, a WARN at most once a minute once that is over 5 ×
 * {@code maintenanceInterval}); neither needs a lock, so every instance does both, and a failed read is a WARN. The
 * rest needs a connection that keeps one server session: behind a transaction-pooling proxy
 * ({@link PartitionDdl#backendChanges}) the run stops there, with an ERROR at most once a minute and
 * {@code outbox.maintenance.transaction_pooling} at 1 (0 otherwise). It runs on at most one instance at a time: it
 * holds the session lock {@code pg_try_advisory_lock(internalNs, LOCK_MAINTENANCE)} on one autocommit connection with
 * {@code idle_session_timeout = 60s}, so Postgres ends the session of a frozen holder and frees the lock; the setting
 * is reset and the lock released before the connection goes back to the pool. While halted the one step under the
 * lock is the recovery ({@link XidRecovery#autoRecover}), with {@code outbox.recovery.xidRegression = auto}; with
 * {@code halt} the run takes no lock. Otherwise the steps, each on its own (a failure is logged and the next step
 * still runs): finish a pending detach, premade partitions (the extension swaps the tail in one transaction, a tail
 * with rows becoming a range in it, the library's one ACCESS EXCLUSIVE on the parent besides the recovery's: writers
 * stall for its lock wait, at most {@code outbox.partitions.swapLockTimeout}, and the milliseconds it holds the lock;
 * before it, a wait of up to {@code batchTimeout} + 5 s for relay steps in flight, which blocks no writer but pauses
 * every shard; a step running longer, as one may, defers the swap to the next run), the checkpoint ({@link Cursors}:
 * the latest tick and the shards busy at it, by which a shard without new rows moves on without a cursor row; the
 * drops and the tick cleanup go by its effective ticks, and a run that fails to write it does neither; a checkpoint
 * whose tick is gone or was made after it, as tick ids going backwards in a partial restore leave it, counts for
 * nothing and is replaced, with an ERROR at most once a minute), drop of
 * processed partitions, drop of ranges left detached but not dropped: by a crash of this library's maintenance once
 * relayed (one still holding rows at or above the horizon first has those moved back into the outbox as new messages,
 * {@link OrphanRestore}, unless {@code outbox.partitions.restoreOrphans} is off), by anyone else only when empty
 * (and ranges the xid-regression recovery set aside, {@code <base>_o<digits>}, likewise:
 * {@code outbox.partitions.orphaned} counts the ones kept, with an ERROR each run, as their rows lie outside the
 * outbox), tick cleanup, cursor cleanup, deletion of kept dead letters older than
 * {@code outbox.deadLetters.retention} (by {@code failed_at}; none when it is 0, pending or scheduled ones never) and
 * of redrive counts older than {@code outbox.deadLetters.redriveHistory}, the start of the dead table's
 * {@code <dead>_due} build when it is missing or invalid ({@link DeadIndexBuilder}: on its own thread, off the
 * maintenance lock; never a failed step), gauges; when none failed, the run is recorded
 * as the last success. One long-lived instance per outbox table: it registers the gauges {@code outbox.shard.lag},
 * {@code outbox.retry.size}, {@code outbox.dead.size}, {@code outbox.dead.pending} (letters waiting for the hook),
 * {@code outbox.dead.pending.oldest} (seconds since the oldest of them failed, 0 when none),
 * {@code outbox.parked.size}, {@code outbox.partitions.premade},
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
    private final Duration detachLockTimeout;
    /**
     * How long the tail swap waits for relay steps in flight, every shard paused meanwhile. No bound on a step: its
     * retry pass may run about 2 × batchTimeout and its main pass's call another batchTimeout. A step running longer
     * defers the swap to the next run, with premake / 2 ranges of xids left before rows reach the tail.
     */
    private final Duration quiesceTimeout;
    private final XidPartitions partitions;
    private final Ticks ticks;
    private final Cursors cursors;
    private final RetryStore retries;
    private final ParkedStore parked;
    final OrphanRestore orphanRestore; // for a test's seam
    private final boolean restoreOrphans;
    final XidRecovery recovery; // for a test of its check's plan
    private final boolean autoRecovery;
    private final String sinceSuccess;
    private final String succeeded;
    private final Duration deadRetention;
    private final Duration redriveHistory;
    private final String deleteOldKept;
    private final String deleteOldRedrives;
    private final String deadPending;
    private final Duration staleAfter;
    private final AtomicLong poolingLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong staleLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong haltLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong checkpointLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong strandedLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private volatile boolean halted;
    private volatile boolean stepFailed; // in the current run, under LOCK_MAINTENANCE
    final DeadIndexBuilder deadIndex; // for tests

    private volatile double lag = Double.NaN;
    private volatile double retrySize = Double.NaN;
    private volatile double deadSize = Double.NaN;
    private volatile double deadPendingCount = Double.NaN;
    private volatile double deadPendingOldest = Double.NaN;
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
        this.detachLockTimeout = detachLockTimeout;
        this.quiesceTimeout = quiesceTimeout(config);
        this.partitions = new XidPartitions(names, config.partitions());
        this.ticks = new Ticks(names, config.shards());
        this.cursors = new Cursors(names, config.shards());
        this.retries = new RetryStore(names, false); // counts rows only, writes no dead row
        this.parked = new ParkedStore(names, retries);
        this.orphanRestore = new OrphanRestore(
                names, new Resender(names, config.shards()), config.partitions().swapLockTimeout());
        this.restoreOrphans = config.partitions().restoreOrphans();
        this.recovery = new XidRecovery(names, config);
        this.autoRecovery = config.recovery().xidRegression().equals("auto");
        // the database clock for both ends, so instances with skewed clocks agree
        this.sinceSuccess = "select extract(epoch from clock_timestamp() - succeeded_at)::float8 from "
                + names.maintenance();
        this.succeeded = "insert into " + names.maintenance() + " (succeeded_at) values (clock_timestamp())"
                + " on conflict (id) do update set succeeded_at = excluded.succeeded_at";
        this.staleAfter = config.partitions().maintenanceInterval().multipliedBy(STALE_INTERVALS);
        this.deadIndex = new DeadIndexBuilder(db, names, config.partitions().maintenanceInterval(), detachLockTimeout);
        this.deadRetention = config.deadLetters().retention();
        this.redriveHistory = config.deadLetters().redriveHistory();
        // ponytail: one statement per run, a scan of the dead table; chunk it if a first retention ever meets millions
        this.deleteOldKept = "delete from " + names.dead() + " where status = 'kept'"
                + " and failed_at < now() - make_interval(secs => ?)";
        this.deleteOldRedrives = "delete from " + names.redriven() + " where at < now() - make_interval(secs => ?)";
        // due_at is set on every pending letter: the partial index finds them, not a scan of all the kept ones
        this.deadPending = "select count(*), coalesce(extract(epoch from now() - min(failed_at)), 0)::float8 from "
                + names.dead() + " where status = 'pending' and due_at is not null";
        metrics.gauge("outbox.shard.lag", () -> lag);
        metrics.gauge("outbox.retry.size", () -> retrySize);
        metrics.gauge("outbox.dead.size", () -> deadSize);
        metrics.gauge("outbox.dead.pending", () -> deadPendingCount);
        metrics.gauge("outbox.dead.pending.oldest", () -> deadPendingOldest);
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
     * Cancels a dead-letter index build in flight and waits for its thread up to {@code timeout}; no build starts
     * afterwards. The relay calls it on release.
     */
    public void close(Duration timeout) {
        deadIndex.close(timeout);
    }

    /** How long the tail swap waits for relay steps in flight, which it keeps out meanwhile: batchTimeout + 5 s. */
    public static Duration quiesceTimeout(OutboxConfig config) {
        return config.relay().batchTimeout().plusSeconds(5);
    }

    /**
     * As of the last check: true while the xid-regression halt is recorded in {@code <base>_halt}. A check that finds
     * the counter below a tick's xmax (dump/restore or logical migration: relaying would skip new rows) records it with
     * the highest tick xmax; relay workers then take no shards and {@code drain()} fails, on every instance and across
     * restarts. The counter catching up does not end it (cursors still sit on pre-regression ticks and would skip the
     * rows written meanwhile): only ticks repaired below the recorded xmax with a consistent counter do, as the
     * recovery ({@link XidRecovery}) leaves them, or an operator deleting the row. While halted an ERROR at most once a
     * minute says why no recovery ran.
     */
    public boolean halted() {
        return halted;
    }

    /**
     * The xid-regression check alone, without the maintenance lock: refreshes {@link #halted()} and returns it; never
     * recovers, and leaves the ERROR to the run that says why it did not.
     */
    public boolean checkHalt() {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            return checkXidCounter(c) != null;
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    /**
     * One run; false without maintaining anything when another session holds the maintenance lock or the connection
     * does not keep one server session. While halted only the reads, the xid check and, in {@code auto} mode, the
     * recovery run (true with {@code halt}: no lock is taken).
     */
    public boolean runOnce() {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            var check = checkXidCounter(c); // first: nothing may keep it from running
            readLastSuccess(c, check != null);
            if (check != null && !autoRecovery) {
                logHalt(check, XidRecovery.HALT_MODE);
                return true;
            }
            return sessionStable(c) && maintain(c, check);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    /** While halted, the halt ERROR says why no run completes, so the stale WARN keeps quiet. */
    private void readLastSuccess(Connection c, boolean halted) {
        Double age;
        try {
            age = secondsSinceSuccess(c);
            if (age == null) {
                return; // the row is made at bootstrap; an operator deleted it
            }
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

    /**
     * Seconds since a run last completed every step on any instance, by the database clock; null without the row.
     * Also for {@link PgOutboxAdmin#status()}.
     */
    @Nullable
    Double secondsSinceSuccess(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(sinceSuccess)) {
            return rs.next() ? rs.getDouble(1) : null;
        }
    }

    /** {@code outbox.shard.lag} read now, for {@link PgOutboxAdmin#status()}. */
    double lagSeconds(Connection c) throws SQLException {
        return read(c).lagSeconds();
    }

    /**
     * One read of every shard's position at the latest tick L. {@code slowest}: the tick of the lowest effective tick
     * of the shards busy at L ({@link Cursors#busyTick}), L when none: the lowest effective tick of all once the
     * checkpoint of this read is written, by which the drops and tick cleanup go; null when {@code stranded} is not
     * empty. {@code stranded}: the shards whose effective tick has no row, by shard, with that tick; a step of such a
     * shard fails, so the relay is stopped for it (see {@link #logStranded}). {@code lagFrom}: the tick
     * {@code outbox.shard.lag} measures from, {@code slowest}, or the oldest tick left when some shards are stranded.
     * {@code ignoredCheckpointTick}: the newest checkpoint's tick before this run's when that checkpoint counts for
     * nothing ({@link Cursors#ignoredCheckpointTick}), else null.
     */
    private record Read(Tick latest, List<Position> positions, @Nullable Tick slowest, Map<Integer, Long> stranded,
            Tick lagFrom, @Nullable Long ignoredCheckpointTick) {

        /** {@code outbox.shard.lag}: from the slowest busy shard's effective tick to L, 0 when no shard is busy. */
        double lagSeconds() {
            return Duration.between(lagFrom.createdAt(), latest.createdAt()).toNanos() / 1e9;
        }
    }

    private Read read(Connection c) throws SQLException {
        var latest = ticks.latest(c);
        var positions = cursors.positions(c, latest.id());
        var wanted = new TreeSet<Long>();
        for (var p : positions) {
            if (Cursors.busy(p) && p.effectiveTickId() != latest.id()) {
                wanted.add(p.effectiveTickId());
            }
        }
        var found = ticks.get(c, wanted);
        var stranded = new TreeMap<Integer, Long>();
        for (var p : positions) {
            if (Cursors.busy(p) && wanted.contains(p.effectiveTickId()) && !found.containsKey(p.effectiveTickId())) {
                stranded.put(p.cursor().shard(), p.effectiveTickId());
            }
        }
        long slowestId = Cursors.busyTick(positions, latest.id());
        var slowest = !stranded.isEmpty() ? null : slowestId == latest.id() ? latest : found.get(slowestId);
        return new Read(latest, positions, slowest, stranded, slowest != null ? slowest : ticks.oldestAfter(c, 0),
                cursors.ignoredCheckpointTick(c));
    }

    /**
     * The ERROR for shards whose effective tick has no row: their batches cannot be read from it, so every step of
     * theirs fails and the relay is stopped for them, and maintenance drops no partitions and deletes no ticks while
     * any is, as the horizon they need is unknown. Their rows stay in the outbox. Nothing moves them on by itself:
     * which rows their lost tick had seen is gone with it.
     */
    private void logStranded(Read read) {
        log.error("outbox shards {} of {} start from ticks {} that no longer exist: deleted by hand, or by cleanup"
                + " while a checkpoint moved those shards past them, a checkpoint that now counts for nothing (from"
                + " another history: restoring {} without {}?). Their batches cannot be read, so the relay is stopped"
                + " for them (each step fails with «its effective tick was deleted»), and maintenance drops no"
                + " partitions and deletes no ticks until they move; their rows stay in the outbox, and"
                + " outbox.shard.lag counts from the oldest tick left. To recover, save their unrelayed rows and reset"
                + " the cursors as the README describes for a restore («Восстановление после регрессии xid»); this"
                + " ERROR repeats at most once a minute", read.stranded().keySet(), names.base(),
                read.stranded().values().stream().distinct().sorted().toList(), names.tick(), names.checkpoint());
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

    /** {@code check}: the halt found by this run's check, null when none. */
    private boolean maintain(Connection c, @Nullable XidRecovery.Check check) throws SQLException {
        // before the lock, so a holder frozen right after taking it loses it too
        execute(c, "set idle_session_timeout = '60s'");
        boolean locked = false;
        Throwable failure = null;
        try {
            locked = advisory(c, "pg_try_advisory_lock");
            if (!locked && check != null) {
                logHalt(check, XidRecovery.LOCK_BUSY);
            } else if (check != null) { // halted: the one step is the recovery
                var why = recovery.autoRecover(c);
                var still = checkXidCounter(c); // resumes once recovered, by this run or by another instance
                if (still != null && why != null) {
                    logHalt(still, why);
                }
            } else if (locked && run(c)) {
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
        var read = step("cursor read", () -> read(c));
        if (read != null && read.ignoredCheckpointTick() != null && due(checkpointLogged)) {
            // the read went by the cursors alone (Cursors ignores such a checkpoint); the checkpoint below replaces it
            log.error("outbox checkpoint of {} was on tick {}, which is gone or was made after the checkpoint (the"
                    + " newest tick is {}): it belongs to another history, as tick ids went backwards under it"
                    + " (restoring {} without {}?). It counted for nothing and is replaced by this run's; this ERROR"
                    + " repeats at most once a minute", names.base(), read.ignoredCheckpointTick(), read.latest().id(),
                    names.tick(), names.checkpoint());
        }
        if (read != null && !read.stranded().isEmpty() && due(strandedLogged)) {
            logStranded(read);
        }
        // the drops and tick cleanup go by the effective ticks of this run's checkpoint: without it, none this run.
        // The checkpoint is written with stranded shards too: it moves the others on, and keeps them busy at their own
        // ticks (the lowest of which is busy_tick_id), as they were
        var slowest = read == null ? null : step("checkpoint", () -> {
            cursors.checkpoint(c, read.latest().id(), read.positions());
            return read.slowest();
        });
        orphaned = Double.NaN; // unknown unless the orphan step completes
        if (slowest != null) {
            step("partition drop", () -> partitions.dropProcessed(c, slowest.snapshot().xmin(), detachLockTimeout));
            step("orphan drop", () -> {
                long horizon = slowest.snapshot().xmin();
                int left = 0;
                for (var orphan : partitions.dropOrphans(c, horizon, detachLockTimeout)) {
                    if (orphan.ours() && restoreOrphans) {
                        if (!restored(c, orphan.table(), horizon)) {
                            left++;
                        }
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
                        log.error("outbox table {} holds rows and is a range {}: rows of a range detached before the"
                                + " relay reached them were never delivered. It is kept (gauge"
                                + " outbox.partitions.orphaned); compare its rows with what was delivered, then move"
                                + " the undelivered ones back with OutboxAdmin.restoreOrphan(\"{}\", fromXid) or POST"
                                + " /kora-outbox/admin/v1/orphans/{}/restore?fromXid=<xid>: rows at or above fromXid"
                                + " get new ids, so they are delivered after later messages of their keys, and the rest"
                                + " go with the table, which is dropped", orphan.table(), orphan.setAside()
                                ? "the xid-regression recovery of " + names.base() + " set aside, as it was not"
                                        + " attached to it"
                                : "detached from " + names.base() + ", but not by this library's maintenance",
                                orphan.table(), orphan.table());
                    }
                }
                orphaned = left;
                return null;
            });
            step("tick cleanup", () -> ticks.deleteBelow(c, slowest.id()));
        }
        step("cursor cleanup", () -> cursors.deleteSuperseded(c));
        if (!deadRetention.isZero()) {
            step("dead-letter retention", () -> deleteOlder(c, deleteOldKept, deadRetention));
        }
        step("redrive history cleanup", () -> deleteOlder(c, deleteOldRedrives, redriveHistory));
        step("dead-letter index", () -> {
            deadIndex.startIfDue(c);
            return null;
        });
        step("gauges", () -> {
            refreshGauges(c, read);
            return null;
        });
        return !stepFailed;
    }

    /**
     * Moves the rows of an orphan of ours back from {@code horizon} ({@link OrphanRestore}); false when that failed,
     * which is logged and fails the step, but not the other orphans': the table is kept, and the next run resumes.
     */
    private boolean restored(Connection c, String table, long horizon) {
        try {
            long moved = orphanRestore.restore(c, table, horizon, detachLockTimeout);
            log.info("outbox table {}, a range of {} detached as relayed, held rows the relay could not deliver: moved"
                    + " {} rows at or above xid {}, the slowest shard's horizon, back into the outbox with new ids and"
                    + " xids, so they are new messages, delivered after later messages of their keys; rows below it"
                    + " went with the table, which is dropped", table, names.base(), moved, horizon);
            return true;
        } catch (SQLException e) {
            log.warn("outbox maintenance could not move the rows of {}, a range of {} detached as relayed, back into"
                    + " the outbox (SQLState {}): {}. It is kept (gauge outbox.partitions.orphaned) and the next run"
                    + " goes on from the rows moved so far", table, names.base(), e.getSQLState(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("outbox maintenance could not move the rows of {}, a range of {} detached as relayed, back into"
                    + " the outbox. It is kept (gauge outbox.partitions.orphaned) and the next run goes on from the"
                    + " rows moved so far", table, names.base(), e);
        }
        stepFailed = true; // no success recorded while rows lie outside the outbox
        return false;
    }

    /** {@code delete} with the one parameter: {@code age} in seconds; returns the rows deleted. */
    private static int deleteOlder(Connection c, String delete, Duration age) throws SQLException {
        try (var ps = c.prepareStatement(delete)) {
            ps.setDouble(1, age.toNanos() / 1e9);
            return ps.executeUpdate();
        }
    }

    private void refreshGauges(Connection c, @Nullable Read read) throws SQLException {
        lag = read == null ? Double.NaN : read.lagSeconds();
        retrySize = retries.size(c);
        deadSize = retries.deadSize(c);
        try (var st = c.createStatement(); var rs = st.executeQuery(deadPending)) {
            rs.next();
            deadPendingCount = rs.getLong(1);
            deadPendingOldest = rs.getDouble(2);
        }
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
     * The xid-regression check against the recorded halt: sets {@link #halted()} and returns what it found while
     * halted, else null. A halt found is recorded, with the highest tick xmax; one whose ticks were repaired is lifted.
     */
    @Nullable
    private synchronized XidRecovery.Check checkXidCounter(Connection c) throws SQLException {
        var check = recovery.check(c);
        if (!check.halted()) {
            if (check.recorded() != null) { // ticks repaired: the regressed ones are gone
                execute(c, "delete from " + names.halt());
            }
            if (halted) {
                halted = false;
                haltLogged.set(System.nanoTime() - LOG_INTERVAL); // a later halt is logged at once
                log.info("outbox relay resumed: no xid-regression halt recorded in {} and the xid counter is above"
                        + " every tick", names.halt());
            }
            return null;
        }
        if (check.recorded() == null || check.tickXmax() > check.recorded()) {
            recovery.recordHalt(c, check.tickXmax());
        }
        halted = true;
        return check;
    }

    /** The halt ERROR, at most once a minute. */
    private void logHalt(XidRecovery.Check check, String why) {
        if (due(haltLogged)) {
            log.error(recovery.halted(check, why));
        }
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
