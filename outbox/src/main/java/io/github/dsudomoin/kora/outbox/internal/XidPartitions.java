package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Attachment;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * Keeps xid ranges {@code <base>_x<lower>} = {@code [lower, lower + xidsPerPartition)} premade above the current xid,
 * followed by the tail {@code [horizon, MAXVALUE)} that catches rows if maintenance falls behind (README "tail
 * overflow") until the next extension turns it into a range. The current xid is
 * {@code pg_snapshot_xmax(pg_current_snapshot())}, the next xid Postgres will assign. All methods need an autocommit
 * connection (but {@link #ensurePremade} for a missing tail, which {@link XidRecovery} runs in its transaction: it
 * only creates and attaches), and callers serialise them under {@code LOCK_MAINTENANCE}: bootstrap takes it after
 * {@code LOCK_SCHEMA}; {@link Maintenance} takes it with {@code pg_try_advisory_lock} and never takes
 * {@code LOCK_SCHEMA}. Besides the xid-regression recovery, the extension's tail swap ({@link #ensurePremade}) is the
 * one place the library takes ACCESS EXCLUSIVE on the parent, for milliseconds after a wait of at most
 * {@code outbox.partitions.swapLockTimeout}; range drops detach concurrently.
 */
public final class XidPartitions {

    private static final Logger log = LoggerFactory.getLogger(XidPartitions.class);

    /**
     * A transaction open this long with a lock on the parent is taken as one that may outlast the swap's lock wait:
     * the swap would stall writers for {@code swapLockTimeout} and time out.
     */
    private static final Duration LONG_TRANSACTION = Duration.ofSeconds(20);
    /** A swap statement's {@code statement_timeout} past {@code swapLockTimeout}: room for its own work. */
    private static final Duration SWAP_MARGIN = Duration.ofSeconds(2);
    /**
     * Who would hold the swap's ACCESS EXCLUSIVE back: a description of the first, else null; and the virtual
     * transactions of lock holders whose age is hidden. Snapshots do not count: a detach that is not concurrent waits
     * for no snapshot. A relay step (it holds {@code LOCK_SWAP} shared) does not count either: the quiesce waits for
     * it, without blocking writers. Of the rest, short holders are fine, the swap waits them out; a lock awaited on
     * the parent counts at any age (the swap would queue behind it), as do a prepared transaction's and a pg_dump's.
     * A visible holder counts once its transaction is older than the threshold; one without {@code xact_start} was
     * between transactions when {@code pg_stat_activity} was read, so the lock seen is in a transaction that has
     * ended or begun since: young either way. A holder logged in as another role (without {@code pg_read_all_stats})
     * or with {@code track_activities = off} has its {@code state} and age hidden: it is returned for a second sample
     * instead ({@link #swapBlocker}). {@code pg_locks} is read once, so a step's two locks are seen together.
     */
    private static final String SWAP_BLOCKERS = """
            with p as materialized (select * from pg_locks
                                     where database = (select oid from pg_database where datname = current_database())),
                 l as (select * from p
                        where p.locktype = 'relation' and p.relation = to_regclass(?)
                          and p.pid is distinct from pg_backend_pid()
                          and not exists (select 1 from p s where s.locktype = 'advisory' and s.pid = p.pid
                                  and s.classid::bigint = ? and s.objid::bigint = ? and s.objsubid = 2
                                  and s.mode = 'ShareLock' and s.granted))
            select (select format('%s %s %s on %s',
                             case when l.pid is null then 'a prepared transaction'
                                  else format('pid %s (%s)', l.pid, a.application_name) end,
                             case when l.granted then 'holds' else 'waits for' end, l.mode, ?)
                      from l left join pg_stat_activity a on a.pid = l.pid
                     where not l.granted or l.pid is null or a.application_name like 'pg_dump%'
                        or a.state <> 'disabled' and a.xact_start < now() - make_interval(secs => ?)
                     limit 1),
                   h.vxids, h.holders
              from (select array_agg(l.virtualtransaction order by l.virtualtransaction) vxids,
                           array_agg(format('pid %s, which has held %s on %s', l.pid, l.mode, ?)
                                     order by l.virtualtransaction) holders
                      from l
                     where l.granted and exists (select 1 from pg_stat_activity a where a.pid = l.pid
                             and coalesce(a.application_name, '') not like 'pg_dump%'
                             and (a.state is null or a.state = 'disabled'))) h""";
    /** One {@link #SWAP_BLOCKERS} read: who defers at once, and the lock holders whose age is hidden. */
    private record Sample(@Nullable String blocker, List<String> hidden, List<String> holders) {}
    private static final long LOG_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    /** The tail's bound check as Postgres prints it, its bounds [H, U) captured; any other form is not ours. */
    private static final String BOUND = "^\\(\\(xid >= '(\\d+)'::xid8\\) AND \\(xid < '(\\d+)'::xid8\\)\\)$";
    /** A row of a standalone range at or above the bound, shard by shard through the (shard, xid) index it keeps. */
    private static final String ROW_AT_OR_ABOVE = """
            with recursive s(shard) as (
              select min(shard) from {table}
              union all
              select (select min(o.shard) from {table} o where o.shard > s.shard) from s where s.shard is not null)
            select 1 from s where s.shard is not null
              and exists (select 1 from {table} o where o.shard = s.shard and o.xid >= ?::text::xid8)
            limit 1""";

    /**
     * A standalone range {@link #orphans} lists and {@link #dropOrphans} kept; {@code ours} when maintenance detached
     * it as relayed, and then {@link Maintenance} moves its rows back ({@link OrphanRestore}) unless
     * {@code restoreOrphans} is off; {@code setAside} when the xid-regression recovery set it aside
     * ({@link XidRecovery}).
     */
    record Orphan(String table, boolean ours, boolean setAside) {}

    private final TableNames names;
    private final String markDropping;
    private final String unmarkDropping;
    private final String deleteStaleMarkers;
    private final long size;
    private final int premake;
    private final Duration swapLockTimeout;
    private final String quiesce;
    private final String unquiesce;
    private final Duration longTransaction;
    private final boolean bootstrap;
    /** The check a hot tail gets before its swap, {@link #convert}; not schema-qualified, as constraint names are. */
    private final String bounded;
    private final AtomicLong deferLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong overflowLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong strangersLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private volatile boolean overflow;
    /** For tests: runs inside the swap transaction right after the tail's detach, to fail the swap there. */
    @Nullable
    Runnable inSwap;
    /** For tests: the warnings of a hot tail's attach as a range, its DEBUG1 lines once the session asks for them. */
    @Nullable
    Consumer<SQLWarning> attachWarnings;

    public XidPartitions(TableNames names, OutboxConfig.PartitionsConfig cfg) {
        this(names, cfg, LONG_TRANSACTION, false);
    }

    /** {@code longTransaction} replaces {@link #LONG_TRANSACTION}, for tests. */
    XidPartitions(TableNames names, OutboxConfig.PartitionsConfig cfg, Duration longTransaction) {
        this(names, cfg, longTransaction, false);
    }

    /**
     * For bootstrap: a lock holder whose age is hidden defers the extension to maintenance at once, rather than after a
     * second sample, which would hold the schema lock, and every other instance's startup, for the threshold; and a
     * hot tail is left to maintenance, as turning it into a range scans it ({@link #convert}).
     */
    static XidPartitions forBootstrap(TableNames names, OutboxConfig.PartitionsConfig cfg) {
        return new XidPartitions(names, cfg, LONG_TRANSACTION, true);
    }

    private XidPartitions(
            TableNames names, OutboxConfig.PartitionsConfig cfg, Duration longTransaction, boolean bootstrap) {
        this.names = names;
        this.markDropping = "insert into " + names.dropping() + " (table_name) values (?) on conflict do nothing";
        this.unmarkDropping = "delete from " + names.dropping() + " where table_name = ?";
        this.deleteStaleMarkers = "delete from " + names.dropping() + " d where not exists (select 1 from pg_class c"
                + " where c.relname = d.table_name"
                + " and c.relnamespace = (select p.relnamespace from pg_class p where p.oid = to_regclass(?)))";
        this.size = cfg.xidsPerPartition();
        this.premake = cfg.premake();
        this.swapLockTimeout = cfg.swapLockTimeout();
        this.quiesce = "select pg_advisory_lock(" + names.internalNamespace() + ", " + TableNames.LOCK_SWAP + ")";
        // only when held, so a quiesce that timed out costs no "you don't own a lock" WARNING in the server log
        this.unquiesce = "select pg_advisory_unlock(" + names.internalNamespace() + ", " + TableNames.LOCK_SWAP + ")"
                + " from pg_locks where locktype = 'advisory' and pid = pg_backend_pid() and mode = 'ExclusiveLock'"
                + " and classid::bigint = " + Integer.toUnsignedLong(names.internalNamespace()) + " and objid = "
                + TableNames.LOCK_SWAP + " and objsubid = 2";
        this.longTransaction = longTransaction;
        this.bootstrap = bootstrap;
        this.bounded = names.unqualified(names.tail()) + "_bounded";
    }

    /**
     * Extends once half of the premade ranges are used up ({@link #premadeAhead} ≤ {@code premake / 2}), until
     * {@code premake} whole ranges lie above the one holding the current xid, by swapping the attached tail for those
     * ranges and a new tail above them in one short transaction ({@link #swap}), so the tail is never left
     * detach-pending. First, under no lock that writers wait for, it creates the new ranges as standalone tables (each
     * skipped when it exists: a failed swap leaves them for the next attempt, and {@link #dropOrphans} leaves them be).
     * Not while ({@link #swapBlocker}) a transaction open for 20 s holds a lock on the parent, or a lock on the parent
     * is awaited: the swap would stall writers for {@code swapLockTimeout} and time out. The run then leaves the
     * extension to the next one with a WARN; if the margin runs out first, rows go to the attached tail. Otherwise it
     * makes {@code <base>_tail_next} anew and waits up to {@code quiesceTimeout} for relay steps in flight, which hold
     * {@code LOCK_SWAP} shared, by taking it exclusively ({@code pg_advisory_lock(internalNs, LOCK_SWAP)}, released
     * after the swap). Writers stall while the swap waits for its lock, at most {@code swapLockTimeout}, and the
     * milliseconds it holds it. A hot tail (it holds rows, which sets {@link #overflow()}, or the current xid is within
     * one range of its lower bound) becomes a range itself, rows and all ({@link #convert}); bootstrap leaves that to
     * maintenance. A tail left detached, detach-pending (an operator's {@code DETACH CONCURRENTLY}) or missing is
     * finished first: dropped when empty, attached back when it holds rows; ranges and a new tail are then attached
     * without a swap, nothing being attached above the last range. Lock timeouts surface as {@link SQLException}
     * (SQLState 55P03), the tail still attached; the next run retries.
     */
    public void ensurePremade(Connection autocommit) throws SQLException {
        ensurePremade(autocommit, PartitionDdl.DETACH_LOCK_TIMEOUT, PartitionDdl.DETACH_LOCK_TIMEOUT);
    }

    void ensurePremade(Connection c, Duration lockTimeout, Duration quiesceTimeout) throws SQLException {
        var s = state(c);
        long floor = Math.floorDiv(s.current, size) * size; // lower bound of the range holding the current xid
        long target = floor + (premake + 1) * size; // premake whole ranges above the current xid's
        if (s.tail == Attachment.ATTACHED) {
            if (s.ahead > premake / 2) {
                overflow = false;
                return;
            }
            boolean rows = PartitionDdl.hasRows(c, names.tail());
            if (rows) {
                overflowed();
            } else {
                overflow = false;
            }
            if (!rows && s.current + size <= s.tailLower) {
                extend(c, s, target, quiesceTimeout);
            } else if (!bootstrap) { // rows may reach the tail any moment: it becomes a range, holding them
                convert(c, s, floor + Math.max(2, premake / 2) * size, quiesceTimeout);
            } // bootstrap leaves that to maintenance: the validate's scan would hold the schema lock and other startups
            return;
        }
        if (s.tail != Attachment.MISSING && !dropOrAttachBack(c, s, lockTimeout)) {
            return;
        }
        overflow = false;
        long from = s.upper == null ? floor : s.upper;
        if (from < floor) { // ranges ended below the current xid (maintenance was down): bridge the gap in one range
            create(from, floor, c);
            from = floor;
        }
        for (; from < target; from += size) {
            create(from, from + size, c);
        }
        PartitionDdl.createAndAttach(
                c, names.base(), names.tail(), Long.toString(from), null, PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    /** The swap path of {@link #ensurePremade} for a tail that is not hot: ranges from its lower bound until target. */
    private void extend(Connection c, State s, long target, Duration quiesceTimeout) throws SQLException {
        long lower = s.tailLower;
        long top = precreate(c, lower, target);
        // none to add, as after xidsPerPartition shrank: the swap would only replace the tail by one alike
        if (top == lower || deferred(c, s)) {
            return;
        }
        nextTail(c);
        if (!quiesced(c, quiesceTimeout, () -> swap(c, lower, null, top))) {
            overflowed();
        }
    }

    /**
     * The hot path of {@link #ensurePremade}: the tail {@code [H, MAXVALUE)} becomes the range {@code [H, U)} with its
     * rows, and ranges from U and a new tail are attached as for an empty tail. So that the swap does not scan the tail
     * under its ACCESS EXCLUSIVE, the tail first gets the check {@code <tail>_bounded (xid >= 'H' and xid < 'U')},
     * which Postgres takes as proof that the rows fit the range: added {@code not valid} ({@link #addBound}), then
     * validated, a scan under SHARE UPDATE EXCLUSIVE on the tail, which writers pass. The check applies to new rows at
     * once, so an insert routed to the tail at or above U fails until the swap: U is {@code bound}, k = max(2,
     * premake / 2) ranges above the current xid's lower bound. That margin must outlast the longest lock holder on the
     * tail: a pg_dump that starts after the check is added holds back both the swap and the check's drop, so it is the
     * sizing rule premake already follows for long snapshots (README: premake / 2 × xidsPerPartition ≥ peak xid/s ×
     * longest snapshot). The cost: {@code [H, U)} spans up to premake / 2 ranges and is dropped only once the horizon
     * passes U. A check already there is replaced once the current xid comes within half a range of its U, even while
     * a blocker defers the swap, and kept otherwise, for the next attempt after a failed swap.
     * Ranges a failed empty-tail swap pre-created below U are dropped: {@code [H, U)} covers them, and the first would
     * take its name. A swap that finds the current xid at or above U drops the check with an ERROR, as inserts routed
     * to the tail at or above U may have failed, and the next run adds one higher up.
     */
    private void convert(Connection c, State s, long bound, Duration quiesceTimeout) throws SQLException {
        long lower = s.tailLower;
        Long kept = boundOf(c, lower);
        if (kept != null && s.current + size / 2 >= kept) {
            addBound(c, lower, bound, true, quiesceTimeout);
            kept = bound;
        }
        long from = kept != null ? kept : bound;
        for (long l = lower; l < from; l += size) {
            var range = names.partitionPrefix() + l;
            if (PartitionDdl.attachment(c, names.base(), range) == Attachment.STANDALONE
                    && !PartitionDdl.hasRows(c, range)) {
                PartitionDdl.drop(c, range, PartitionDdl.CREATE_LOCK_TIMEOUT);
            }
        }
        long top = precreate(c, from, from + premake * size); // premake whole ranges above [H, U)
        if (deferred(c, s)) {
            return;
        }
        if (kept == null) {
            addBound(c, lower, bound, false, quiesceTimeout);
        }
        PartitionDdl.validate(c, names.tail(), bounded);
        nextTail(c);
        boolean swapped = quiesced(c, quiesceTimeout, () -> {
            if (swap(c, lower, from, top)) {
                return true;
            }
            PartitionDdl.execute(c, swapLockTimeout, "alter table " + names.tail() + " drop constraint " + bounded);
            // the next xid reached U; one at U may not have been assigned yet, so "may"
            log.error("outbox partition tail {} was not swapped: the current xid reached {}, the upper bound of its"
                    + " check {}, so inserts routed to it with an xid at or above that may have failed meanwhile"
                    + " (SQLState 23514). The check is dropped; the next run adds one higher up", names.tail(), from,
                    bounded);
            return false;
        });
        if (swapped) {
            overflow = false;
        }
    }

    /**
     * U of the tail's bound check, or null when it has none; 0, so that it is replaced, when the check is not
     * {@code [lower, U)} in the form {@link #addBound} gives it.
     */
    @Nullable
    private Long boundOf(Connection c, long lower) throws SQLException {
        try (var ps = c.prepareStatement("select regexp_match(pg_get_expr(conbin, conrelid), ?) from pg_constraint"
                + " where conrelid = to_regclass(?) and conname = ?")) {
            ps.setString(1, BOUND);
            ps.setString(2, names.tail());
            ps.setString(3, bounded);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                var bounds = strings(rs.getArray(1));
                return bounds.size() == 2 && Long.parseLong(bounds.get(0)) == lower ? Long.parseLong(bounds.get(1)) : 0;
            }
        }
    }

    /**
     * Adds the tail's bound check {@code [lower, u)} not valid, in place of the one there when {@code replace}: ACCESS
     * EXCLUSIVE on the tail alone, waited for at most {@code swapLockTimeout} (as long as writers routed to the tail
     * wait) under the quiesce, so that no relay step reading the tail holds it back.
     */
    private void addBound(Connection c, long lower, long u, boolean replace, Duration quiesceTimeout)
            throws SQLException {
        quiesced(c, quiesceTimeout, () -> {
            PartitionDdl.execute(c, swapLockTimeout, "alter table " + names.tail()
                    + (replace ? " drop constraint " + bounded + "," : "") + " add constraint " + bounded
                    + " check (xid >= '" + lower + "' and xid < '" + u + "') not valid");
            return null;
        });
        log.info("{} check {} (xid >= {} and xid < {}) on the partition tail {}, so that the swap attaches it as a"
                + " range without a scan; inserts routed to it at or above {} fail until then",
                replace ? "replaced" : "added", bounded, lower, u, names.tail(), u);
    }

    /** Creates the ranges from {@code from} until {@code target} standalone, each skipped when it exists; their top. */
    private long precreate(Connection c, long from, long target) throws SQLException {
        for (; from < target; from += size) {
            PartitionDdl.create(c, names.base(), names.partitionPrefix() + from, PartitionDdl.CREATE_LOCK_TIMEOUT);
        }
        return from;
    }

    /** Makes {@code <base>_tail_next} anew: one a failed swap left may lack a column the parent gained since. */
    private void nextTail(Connection c) throws SQLException {
        PartitionDdl.execute(c, PartitionDdl.CREATE_LOCK_TIMEOUT, "drop table if exists " + names.tailNext());
        PartitionDdl.create(c, names.base(), names.tailNext(), PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    /** True when {@link #swapBlocker} finds who would hold the swap back, with a WARN at most once a minute. */
    private boolean deferred(Connection c, State s) throws SQLException {
        var blocker = swapBlocker(c);
        if (blocker != null && due(deferLogged)) { // never forced: the swap would stall writers and time out
            log.warn("outbox partition extension of {} deferred to the next run, {} ranges ahead: {}, so the tail swap"
                    + " would wait for it, stalling writers for outbox.partitions.swapLockTimeout, and time out."
                    + " Long transactions on the outbox table (pg_dump of it, long reports or batch jobs) hold the"
                    + " extension back; outbox.partitions.premake must outlast the longest. This WARN repeats at most"
                    + " once a minute",
                    names.base(), s.ahead, blocker);
        }
        return blocker != null;
    }

    /**
     * Runs {@code action} holding {@code LOCK_SWAP} exclusively, taken within {@code quiesceTimeout}: relay steps in
     * flight, which hold it shared, have ended, and new ones skip until it is released, whatever fails.
     */
    private <T> T quiesced(Connection c, Duration quiesceTimeout, SqlFunction0<T> action) throws SQLException {
        Throwable failure = null;
        try {
            // inside the try: whatever fails once the lock is granted, even an Error, releases it below; a session
            // lock left behind would go back to the pool and keep every relay step out
            PartitionDdl.execute(c, quiesceTimeout, quiesce); // under the session's own timeouts again afterwards
            return action.apply();
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            try {
                execute(c, unquiesce);
            } catch (SQLException e) { // a terminated session, its lock gone with it: keep the cause on top
                if (failure == null) {
                    throw e;
                }
                failure.addSuppressed(e);
            }
        }
    }

    /**
     * The swap, in one transaction under {@code swapLockTimeout} (a statement's {@code statement_timeout}
     * {@link #SWAP_MARGIN} longer, so a shorter role default cannot cut the lock wait short; both {@code SET LOCAL},
     * so the session's own come back at its end): ACCESS EXCLUSIVE on the parent, which waits for every writer's
     * transaction to end, so a row routed to the tail before is committed or gone; detach the tail (not concurrently:
     * it waits for no snapshot and is never left pending). An empty tail ({@code bound} null): if it holds a row, roll
     * back and return false (the hot path); else drop it and attach the pre-created ranges {@code [lower, top)}. A hot
     * one: unless the current xid is still below {@code bound}, roll back first and return false; else rename it to
     * {@code <base>_x<lower>} and attach it as {@code [lower, bound)}, which its validated bound check lets Postgres do
     * without a scan, keeping its indexes, then the pre-created ranges {@code [bound, top)}. Then rename
     * {@code <base>_tail_next} to the tail and attach it at {@code top}. The pre-created ranges are empty, so their
     * attach has nothing to scan and builds the indexes on empty tables. Read committed whatever the session default:
     * the row check takes its snapshot under the lock. One INFO says how long the lock was waited for and held.
     */
    boolean swap(Connection c, long lower, @Nullable Long bound, long top) throws SQLException {
        String base = names.base();
        String former = names.partitionPrefix() + lower;
        long first = bound == null ? lower : bound;
        long asked;
        long locked;
        c.setAutoCommit(false);
        try (var st = c.createStatement()) {
            st.execute("set transaction isolation level read committed"); // first: before any statement of the tx
            st.execute("set local lock_timeout = " + swapLockTimeout.toMillis());
            st.execute("set local statement_timeout = " + swapLockTimeout.plus(SWAP_MARGIN).toMillis());
            asked = System.nanoTime();
            // as the detach would take it, but its own statement: the wait and the hold are timed apart
            st.execute("lock table only " + base + " in access exclusive mode");
            locked = System.nanoTime();
            if (bound != null) { // at or above the bound, the check has been refusing rows routed to the tail
                try (var rs = st.executeQuery(
                        "select pg_snapshot_xmax(pg_current_snapshot()) < '" + bound + "'::xid8")) {
                    rs.next();
                    if (!rs.getBoolean(1)) {
                        c.rollback();
                        return false;
                    }
                }
            }
            st.execute("alter table " + base + " detach partition " + names.tail());
            if (inSwap != null) {
                inSwap.run();
            }
            if (bound != null) {
                st.execute("alter table " + names.tail() + " rename to " + names.unqualified(former));
                st.execute("alter table " + base + " attach partition " + former + " "
                        + PartitionDdl.bounds(Long.toString(lower), Long.toString(bound)));
                if (attachWarnings != null) {
                    attachWarnings.accept(st.getWarnings());
                }
            } else {
                try (var rs = st.executeQuery("select 1 from " + names.tail() + " limit 1")) {
                    if (rs.next()) {
                        c.rollback();
                        return false;
                    }
                }
                st.execute("drop table " + names.tail());
            }
            for (long from = first; from < top; from += size) {
                st.execute("alter table " + base + " attach partition " + names.partitionPrefix() + from + " "
                        + PartitionDdl.bounds(Long.toString(from), Long.toString(from + size)));
            }
            st.execute("alter table " + names.tailNext() + " rename to " + names.unqualified(names.tail()));
            st.execute("alter table " + base + " attach partition " + names.tail() + " "
                    + PartitionDdl.bounds(Long.toString(top), null));
            c.commit();
        } catch (Throwable e) { // an Error too: setAutoCommit(true) below would commit the DDL run so far
            try {
                c.rollback();
            } catch (SQLException cleanup) { // a terminated session, its transaction gone with it
                e.addSuppressed(cleanup);
            }
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
        long done = System.nanoTime();
        log.info("swapped the partition tail of {}: {}attached {} ranges [{}, {}) and the tail at {}; held ACCESS"
                + " EXCLUSIVE on {} for {} ms, after waiting {} ms for it", base,
                bound == null ? "" : "attached it with its rows as " + former + " [" + lower + ", " + bound + "), ",
                (top - first) / size, first, top, top, base, (done - locked) / 100_000 / 10.0,
                (locked - asked) / 100_000 / 10.0);
        return true;
    }

    /**
     * Ranges lying entirely above the current xid (lower bound > current xid). Neither the range holding the current
     * xid nor the tail counts; right after an extension this equals {@code premake}.
     */
    public int premadeAhead(Connection c) throws SQLException {
        return state(c).ahead;
    }

    /**
     * True while the tail holds rows, until a swap turns it into a range: informational, they are relayed as any, and
     * the next extension heals it ({@link #convert}). Also set while an unattached tail holds rows, which are
     * invisible to the relay until it is attached back.
     */
    public boolean overflow() {
        return overflow;
    }

    /**
     * The drop rule: detaches concurrently and drops, lowest first, every range ending at or below
     * {@code minCursorXmin}, the {@code pg_snapshot_xmin} of the slowest shard cursor's tick. Every transaction below
     * it had finished by that tick and the committed ones were visible to it, so every shard has relayed their rows.
     * The tail never qualifies, nor does the range holding the current xid (xmin is at most the current xid). Each
     * range is recorded in {@code <base>_dropping} before its detach and removed from it after its drop, so a run that
     * dies in between leaves {@link #dropOrphans} the proof that the library detached it as relayed. A range already
     * detach-pending is left to {@link #finishPendingDetach} and {@link #dropOrphans}: whoever started that detach
     * may have hidden its rows from the relay before they were delivered. A lock timeout
     * (SQLState 55P03, e.g. a detach waiting for a long transaction that uses the table) surfaces as
     * {@link SQLException} and leaves the rest to the next run. Returns the number of ranges dropped.
     */
    public int dropProcessed(Connection autocommit, long minCursorXmin) throws SQLException {
        return dropProcessed(autocommit, minCursorXmin, PartitionDdl.DETACH_LOCK_TIMEOUT);
    }

    int dropProcessed(Connection c, long minCursorXmin, Duration lockTimeout) throws SQLException {
        // not one already detach-pending: someone else's detach may have hidden its rows from the relay
        var pending = PartitionDdl.pendingDetach(c, names.base());
        var processed = PartitionDdl.listRanges(c, names.base()).stream()
                .filter(b -> b.to() != null && Long.parseLong(b.to()) <= minCursorXmin && !b.name().equals(pending))
                .sorted(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .toList();
        for (var b : processed) {
            var range = names.unqualified(b.name());
            // committed before the detach: what lets dropOrphans finish it if this session dies before the drop
            marker(c, markDropping, range);
            PartitionDdl.detachConcurrentlyAndDrop(c, names.base(), b.name(), lockTimeout);
            marker(c, unmarkDropping, range);
        }
        return processed.size();
    }

    /**
     * Drops the ranges left standalone ({@code <base>_x<lower>} in no partition tree, which {@link #dropProcessed} no
     * longer lists), as a run that died, or whose DROP failed, after its detach leaves them. One recorded in
     * {@code <base>_dropping} was detached by {@link #dropProcessed} as relayed: dropped when no row is at or above
     * {@code minCursorXmin}, checked on the data. Any other was detached by someone else, perhaps before the relay
     * reached its rows: dropped only when empty (as a create whose attach never ran leaves one), as is a range the
     * xid-regression recovery set aside ({@code <base>_o<digits>}). Records whose table is gone are deleted first.
     * Ranges at or above the tail's lower bound are not looked at: only the extension makes them there, standalone
     * until its swap, and a swap that failed or was deferred leaves them for the next attempt; set-aside ones are
     * looked at whatever their digits. Returns the ones kept.
     */
    List<Orphan> dropOrphans(Connection c, long minCursorXmin, Duration lockTimeout) throws SQLException {
        try (var ps = c.prepareStatement(deleteStaleMarkers)) { // their table is gone
            ps.setString(1, names.base());
            ps.executeUpdate();
        }
        var kept = new ArrayList<Orphan>();
        for (var orphan : orphans(c)) {
            var table = orphan.table();
            // anyone else's detach may have come before the relay reached its rows: only an empty one goes
            boolean dropped = orphan.ours()
                    ? PartitionDdl.dropUnlessFound(c, table, ROW_AT_OR_ABOVE, Long.toString(minCursorXmin), lockTimeout)
                    : PartitionDdl.dropUnlessFound(c, table, "select 1 from {table} limit 1", null, lockTimeout);
            if (!dropped) {
                kept.add(orphan);
            } else if (orphan.ours()) {
                marker(c, unmarkDropping, names.unqualified(table));
            }
        }
        return kept;
    }

    /**
     * The standalone ranges {@link #dropOrphans} looks at, read only: {@code <base>_x<lower>} in no partition tree but
     * those at or above the tail's lower bound, which the extension makes for its next swap, and set-aside
     * {@code <base>_o<digits>}; {@code ours} when recorded in {@code <base>_dropping}. A name whose digits fit no long
     * is no table of the library's: left out (so never dropped or restored), with a WARN at most once a minute.
     */
    List<Orphan> orphans(Connection c) throws SQLException {
        var marked = new HashSet<String>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select table_name from " + names.dropping())) {
            while (rs.next()) {
                marked.add(rs.getString(1));
            }
        }
        long tailLower = PartitionDdl.listRanges(c, names.base()).stream()
                .filter(b -> b.to() == null)
                .mapToLong(b -> Long.parseLong(b.from()))
                .findFirst()
                .orElse(Long.MAX_VALUE);
        int prefix = names.unqualified(names.partitionPrefix()).length();
        var setAsidePrefix = names.unqualified(names.setAsidePrefix());
        var orphans = new ArrayList<Orphan>();
        var strangers = new ArrayList<String>();
        for (var table : PartitionDdl.standalone(c, names.base(), "_[xo][0-9]+")) {
            var range = names.unqualified(table);
            var lower = xidOf(range.substring(prefix));
            if (lower == null) {
                strangers.add(table);
                continue;
            }
            boolean setAside = range.startsWith(setAsidePrefix);
            if (!setAside && lower >= tailLower) {
                continue;
            }
            orphans.add(new Orphan(table, !setAside && marked.contains(range), setAside));
        }
        if (!strangers.isEmpty() && due(strangersLogged)) {
            log.warn("outbox tables {} are named like ranges of {}, but their digits fit no bigint, so no table of the"
                    + " library's: left alone, neither dropped nor restorable. This WARN repeats at most once a minute",
                    strangers, names.base());
        }
        return orphans;
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static void marker(Connection c, String sql, String range) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, range);
            ps.executeUpdate();
        }
    }

    /**
     * Finishes a detach that an interrupted run left pending. Postgres allows one pending detach per parent and fails
     * every other {@code DETACH ... CONCURRENTLY} meanwhile (SQLState 55000), so this goes before any other partition
     * DDL. A pending range is finalized, which leaves it standalone, and {@link #dropOrphans} decides on its drop: one
     * {@link #dropProcessed} detached is recorded as relayed, anyone else's is kept unless empty. A pending tail takes
     * the extension's path: dropped when empty, attached back when it holds rows.
     */
    void finishPendingDetach(Connection c, Duration lockTimeout) throws SQLException {
        var pending = PartitionDdl.pendingDetach(c, names.base());
        if (pending == null) {
            return;
        }
        if (PartitionDdl.attachment(c, names.base(), names.tail()) == Attachment.DETACH_PENDING) {
            dropOrAttachBack(c, state(c), lockTimeout);
        } else {
            PartitionDdl.detach(c, names.base(), pending, lockTimeout); // FINALIZE; dropOrphans decides on the drop
        }
    }

    /**
     * Xids left before an insert finds no partition (SQLState 23514), or null while the tail is attached. A tail that
     * is detach-pending, detached or missing takes no rows, so inserts fail once the current xid reaches the top of the
     * highest range.
     */
    @Nullable
    Long xidsLeft(Connection c) throws SQLException {
        var s = state(c);
        return s.tail == Attachment.ATTACHED ? null : s.upper == null ? 0 : Math.max(0, s.upper - s.current);
    }

    /** Whether an insert at the current xid finds a partition: the tail is attached or a range covers the xid. */
    boolean acceptsInserts(Connection c) throws SQLException {
        var s = state(c);
        return s.tail == Attachment.ATTACHED || (s.upper != null && s.current < s.upper);
    }

    /** Finishes a tail that is no longer attached; false when it held rows and was attached back. */
    private boolean dropOrAttachBack(Connection c, State s, Duration lockTimeout) throws SQLException {
        if (PartitionDdl.hasRows(c, names.tail())) {
            overflow = true; // stays set until the tail is attached again, even if the steps below time out
        }
        PartitionDdl.detach(c, names.base(), names.tail(), lockTimeout); // FINALIZE when pending, else no-op
        // a standalone tail has no bound left in the catalog; a tail always starts where the last range ends
        long lower = s.tailLower != null ? s.tailLower
                : s.upper != null ? s.upper
                : Math.floorDiv(s.current, size) * size;
        String tail = names.tail();
        if (PartitionDdl.dropIfEmptyElseAttach(c, names.base(), tail, Long.toString(lower), null, lockTimeout)) {
            return true;
        }
        overflowed();
        return false;
    }

    /**
     * WARNs when this role cannot see how long the transactions of sessions logged in as another role have run (no
     * {@code pg_read_all_stats}), which changes the check before a tail swap ({@link #swapBlocker}).
     */
    void warnIfActivityHidden(Connection c) throws SQLException {
        try (var st = c.createStatement();
                var rs = st.executeQuery("select current_user, pg_has_role('pg_read_all_stats', 'USAGE')")) {
            rs.next();
            if (!rs.getBoolean(2)) {
                log.warn("outbox role {} is not a member of pg_read_all_stats, so Postgres hides how long the"
                        + " transactions of sessions logged in as another role have run. That includes this app's own"
                        + " sessions when they log in as a role other than {} (SET ROLE, options=-c role=...,"
                        + " per-instance credentials). Before swapping the partition tail of {}, maintenance then"
                        + " samples such sessions' locks on it twice, 20 s apart, and defers the extension when one"
                        + " transaction holds a lock across both. Grant pg_read_all_stats (or pg_monitor) to {} to"
                        + " read their age directly, without the 20 s wait",
                        rs.getString(1), rs.getString(1), names.base(), rs.getString(1));
            }
        }
    }

    /**
     * Who would hold the tail swap back, described, or null ({@link #SWAP_BLOCKERS}). A lock holder whose age is
     * hidden counts only when the same transaction still holds a lock on the parent a threshold later: this sleeps for
     * it, between two autocommit statements (no transaction open, only the caller's session locks held), and only
     * when the first sample finds such a holder. A short transaction of the app's own (under another login role)
     * has ended by then.
     */
    @Nullable
    String swapBlocker(Connection c) throws SQLException {
        var first = sample(c);
        if (first.blocker() != null || first.hidden().isEmpty()) {
            return first.blocker();
        }
        if (bootstrap) {
            return "a session whose age is hidden from this role, which lacks pg_read_all_stats, holds a lock on "
                    + names.base() + "; bootstrap leaves the extension to maintenance";
        }
        try {
            Thread.sleep(longTransaction.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "maintenance was interrupted while it waited to sample the locks on " + names.base() + " again";
        }
        // everything again, not only the hidden holders: a pg_dump or a waiter may have come meanwhile
        var second = sample(c);
        if (second.blocker() != null) {
            return second.blocker();
        }
        for (int i = 0; i < second.hidden().size(); i++) {
            if (first.hidden().contains(second.hidden().get(i))) {
                return second.holders().get(i) + " for longer than " + longTransaction.toSeconds() + " s, its age"
                        + " hidden from this role; granting pg_read_all_stats to the outbox role reads it directly,"
                        + " without this second look " + longTransaction.toSeconds() + " s later";
            }
        }
        return null;
    }

    private Sample sample(Connection c) throws SQLException {
        double seconds = longTransaction.toMillis() / 1e3;
        try (var ps = c.prepareStatement(SWAP_BLOCKERS)) {
            ps.setString(1, names.base());
            ps.setLong(2, Integer.toUnsignedLong(names.internalNamespace())); // pg_locks shows the key as an oid
            ps.setLong(3, TableNames.LOCK_SWAP);
            ps.setString(4, names.base());
            ps.setDouble(5, seconds);
            ps.setString(6, names.base());
            try (var rs = ps.executeQuery()) {
                rs.next();
                return new Sample(rs.getString(1), strings(rs.getArray(2)), strings(rs.getArray(3)));
            }
        }
    }

    private static List<String> strings(@Nullable java.sql.Array array) throws SQLException {
        return array == null ? List.of() : List.of((String[]) array.getArray());
    }

    private void create(long from, long to, Connection c) throws SQLException {
        PartitionDdl.createAndAttach(c, names.base(), names.partitionPrefix() + from, Long.toString(from),
                Long.toString(to), PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    private void overflowed() {
        overflow = true;
        if (due(overflowLogged)) {
            log.warn("outbox rows went to the partition tail {}: maintenance fell behind; the next extension turns it"
                    + " into a range (gauge outbox.partitions.overflow). This WARN repeats at most once a minute",
                    names.tail());
        }
    }

    /**
     * The xid the digits of a range name give, or null when they fit no long: no table of the library's, as xids stay
     * far below 2^63.
     */
    @Nullable
    static Long xidOf(String digits) {
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** True at most once per {@link #LOG_INTERVAL} for {@code logged}. */
    private static boolean due(AtomicLong logged) {
        long now = System.nanoTime();
        long last = logged.get();
        return now - last >= LOG_INTERVAL && logged.compareAndSet(last, now);
    }

    private State state(Connection c) throws SQLException {
        long current;
        try (var st = c.createStatement();
                var rs = st.executeQuery("select pg_snapshot_xmax(pg_current_snapshot())::text::bigint")) {
            rs.next();
            current = rs.getLong(1);
        }
        Long upper = null;
        Long tailLower = null;
        int ahead = 0;
        for (var b : PartitionDdl.listRanges(c, names.base())) {
            if (b.to() == null) {
                tailLower = Long.parseLong(b.from());
                continue;
            }
            if (Long.parseLong(b.from()) > current) {
                ahead++;
            }
            long to = Long.parseLong(b.to());
            upper = upper == null ? to : Math.max(upper, to);
        }
        return new State(current, upper, ahead, PartitionDdl.attachment(c, names.base(), names.tail()), tailLower);
    }

    private record State(
            long current, @Nullable Long upper, int ahead, Attachment tail, @Nullable Long tailLower) {}
}
