package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Attachment;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps xid ranges {@code <base>_x<lower>} = {@code [lower, lower + xidsPerPartition)} premade above the current xid,
 * followed by the tail {@code [horizon, MAXVALUE)} that catches rows if maintenance falls behind (README "tail
 * overflow"). The current xid is {@code pg_snapshot_xmax(pg_current_snapshot())}, the next xid Postgres will assign.
 * All methods need an autocommit connection, and callers serialise them under {@code LOCK_MAINTENANCE}: bootstrap
 * takes it after {@code LOCK_SCHEMA}; {@link Maintenance} takes it with {@code pg_try_advisory_lock} and never takes
 * {@code LOCK_SCHEMA}.
 */
public final class XidPartitions {

    private static final Logger log = LoggerFactory.getLogger(XidPartitions.class);

    /**
     * A transaction open this long, with a snapshot or a lock on the parent, is taken as one that may outlast the tail
     * detach's {@link PartitionDdl#DETACH_LOCK_TIMEOUT}.
     */
    private static final Duration LONG_TRANSACTION = Duration.ofSeconds(20);
    /**
     * Over {@code pg_stat_activity a}, one bind parameter (the threshold, seconds): a snapshot that may keep a tail
     * detach pending. {@code FINALIZE} waits for every snapshot older than its own in the database, autovacuum aside.
     * A pg_dump, or a walsender (a logical replication initial sync exports one; {@code xact_start} is always NULL for
     * walsenders), counts at any age; any other once older than the threshold. Without {@code pg_read_all_stats},
     * Postgres hides {@code backend_type} and {@code xact_start} of sessions logged in as another role, so their
     * snapshots count only for pg_dump.
     */
    static final String SNAPSHOT_DEFERS = "a.backend_xmin is not null and (a.application_name like 'pg_dump%'"
            + " or a.backend_type = 'walsender'"
            + " or a.backend_type <> 'autovacuum worker' and a.xact_start < now() - make_interval(secs => ?))";
    /**
     * Who may keep a tail detach pending: a description of the first that does, else null; and the virtual
     * transactions of lock holders whose age is hidden. Phase 2 of {@code DETACH CONCURRENTLY} waits for every
     * transaction holding a lock on the parent; short ones are fine, the detach waits them out. A lock awaited on the
     * parent counts at any age (the detach would queue behind it), as do a prepared transaction's and a pg_dump's. A
     * visible holder counts once its transaction is older than the threshold; one without {@code xact_start} was
     * between transactions when {@code pg_stat_activity} was read, before {@code pg_locks} (the first column reads it
     * first), so its lock is in a transaction begun since. A holder logged in as another role (without
     * {@code pg_read_all_stats}) or with {@code track_activities = off} has its {@code state} and age hidden: it is
     * returned for a second sample instead ({@link #detachBlocker}).
     */
    private static final String DETACH_BLOCKERS = """
            with l as (select * from pg_locks l
                        where l.locktype = 'relation' and l.relation = to_regclass(?)
                          and l.pid is distinct from pg_backend_pid()
                          and l.database = (select oid from pg_database where datname = current_database()))
            select coalesce(
              (select format('pid %s (%s) holds a snapshot in a transaction open since %s', a.pid, a.application_name,
                        coalesce(a.xact_start::text, 'a time hidden from this role'))
                 from pg_stat_activity a
                where a.datname = current_database() and a.pid <> pg_backend_pid() and ${SNAPSHOT_DEFERS}
                order by a.xact_start limit 1),
              (select format('%s %s %s on %s', coalesce('pid ' || l.pid, 'a prepared transaction'),
                        case when l.granted then 'holds' else 'waits for' end, l.mode, ?)
                 from l
                where not l.granted or l.pid is null or exists (select 1 from pg_stat_activity a where a.pid = l.pid
                        and (a.application_name like 'pg_dump%'
                          or a.state <> 'disabled' and a.xact_start < now() - make_interval(secs => ?)))
                limit 1)),
              h.vxids, h.holders
              from (select array_agg(l.virtualtransaction order by l.virtualtransaction) vxids,
                           array_agg(format('pid %s, which has held %s on %s', l.pid, l.mode, ?)
                                     order by l.virtualtransaction) holders
                      from l
                     where l.granted and exists (select 1 from pg_stat_activity a where a.pid = l.pid
                             and coalesce(a.application_name, '') not like 'pg_dump%'
                             and (a.state is null or a.state = 'disabled'))) h"""
            .replace("${SNAPSHOT_DEFERS}", SNAPSHOT_DEFERS);
    /** One {@link #DETACH_BLOCKERS} read: who defers at once, and the lock holders whose age is hidden. */
    private record Sample(@Nullable String blocker, List<String> hidden, List<String> holders) {}
    private static final long LOG_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    /** A row of a standalone range at or above the bound, shard by shard through the (shard, xid) index it keeps. */
    private static final String ROW_AT_OR_ABOVE = """
            with recursive s(shard) as (
              select min(shard) from {table}
              union all
              select (select min(o.shard) from {table} o where o.shard > s.shard) from s where s.shard is not null)
            select 1 from s where s.shard is not null
              and exists (select 1 from {table} o where o.shard = s.shard and o.xid >= ?::text::xid8)
            limit 1""";

    /** A standalone range {@link #dropOrphans} kept; {@code ours} when maintenance detached it as relayed. */
    record Orphan(String table, boolean ours) {}

    private final TableNames names;
    private final String markDropping;
    private final String unmarkDropping;
    private final String deleteStaleMarkers;
    private final long size;
    private final int premake;
    private final Duration longTransaction;
    private final boolean resample;
    private final AtomicLong deferLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private volatile boolean overflow;

    public XidPartitions(TableNames names, OutboxConfig.PartitionsConfig cfg) {
        this(names, cfg, LONG_TRANSACTION, true);
    }

    /** {@code longTransaction} replaces {@link #LONG_TRANSACTION}, for tests. */
    XidPartitions(TableNames names, OutboxConfig.PartitionsConfig cfg, Duration longTransaction) {
        this(names, cfg, longTransaction, true);
    }

    /**
     * For bootstrap: a lock holder whose age is hidden defers the extension to maintenance at once, rather than after a
     * second sample, which would hold the schema lock, and every other instance's startup, for the threshold.
     */
    static XidPartitions forBootstrap(TableNames names, OutboxConfig.PartitionsConfig cfg) {
        return new XidPartitions(names, cfg, LONG_TRANSACTION, false);
    }

    private XidPartitions(
            TableNames names, OutboxConfig.PartitionsConfig cfg, Duration longTransaction, boolean resample) {
        this.names = names;
        this.markDropping = "insert into " + names.dropping() + " (table_name) values (?) on conflict do nothing";
        this.unmarkDropping = "delete from " + names.dropping() + " where table_name = ?";
        this.deleteStaleMarkers = "delete from " + names.dropping() + " d where not exists (select 1 from pg_class c"
                + " where c.relname = d.table_name"
                + " and c.relnamespace = (select p.relnamespace from pg_class p where p.oid = to_regclass(?)))";
        this.size = cfg.xidsPerPartition();
        this.premake = cfg.premake();
        this.longTransaction = longTransaction;
        this.resample = resample;
    }

    /**
     * Extends once half of the premade ranges are used up ({@link #premadeAhead} ≤ {@code premake / 2}): detaches the
     * empty tail concurrently, drops it, creates ranges until {@code premake} whole ranges lie above the current xid,
     * attaches a new tail. The tail is never detached once it is hot (holds rows, or the current xid is within one
     * range of its lower bound): it stays attached and sets {@link #overflow()}. Nor while ({@link #detachBlocker}) a
     * transaction open for 20 s holds a snapshot or a lock on the parent, or a lock on the parent is awaited: such a
     * detach could stay pending (no partition above the last range) until that transaction ends, and inserts fail once
     * the ranges run out. The run then leaves the extension to the next one with a WARN; if the margin runs out first,
     * rows go to the attached tail, the overflow path. A tail left detached or detach-pending by an interrupted run is
     * finished first: dropped when empty, attached back when it holds rows. Lock timeouts surface as
     * {@link SQLException} (SQLState 55P03); the next run retries.
     */
    public void ensurePremade(Connection autocommit) throws SQLException {
        ensurePremade(autocommit, PartitionDdl.DETACH_LOCK_TIMEOUT);
    }

    void ensurePremade(Connection c, Duration lockTimeout) throws SQLException {
        var s = state(c);
        if (s.tail == Attachment.ATTACHED) {
            if (s.ahead > premake / 2) {
                overflow = false;
                return;
            }
            if (PartitionDdl.hasRows(c, names.tail())) {
                overflowed();
                return;
            }
            // a detach races writers only once they can route to the tail, so it needs a full range of margin
            if (s.current + size > s.tailLower) {
                overflow = true;
                log.error("outbox partition horizon reached; maintenance fell behind: {},"
                        + " see README \"tail overflow\"", names.tail());
                return;
            }
            var blocker = detachBlocker(c);
            if (blocker != null) { // never forced: a pending tail is worse than rows in the attached one
                long now = System.nanoTime();
                long last = deferLogged.get();
                if (now - last < LOG_INTERVAL || !deferLogged.compareAndSet(last, now)) {
                    return;
                }
                log.warn("outbox partition extension of {} deferred to the next run, {} ranges ahead: {}, and a tail"
                        + " detach now could stay pending until it ends. Long transactions and snapshots (pg_dump,"
                        + " repeatable-read reports, logical replication initial syncs) hold partition DDL back;"
                        + " outbox.partitions.premake must outlast the longest. This WARN repeats at most once a"
                        + " minute",
                        names.base(), s.ahead, blocker);
                return;
            }
            PartitionDdl.detach(c, names.base(), names.tail(), lockTimeout);
        }
        if (s.tail != Attachment.MISSING && !dropOrAttachBack(c, s, lockTimeout)) {
            return;
        }
        overflow = false;
        long floor = Math.floorDiv(s.current, size) * size; // lower bound of the range holding the current xid
        long from = s.upper == null ? floor : s.upper;
        if (from < floor) { // ranges ended below the current xid (maintenance was down): bridge the gap in one range
            create(from, floor, c);
            from = floor;
        }
        for (long target = floor + (premake + 1) * size; from < target; from += size) {
            create(from, from + size, c);
        }
        PartitionDdl.createAndAttach(
                c, names.base(), names.tail(), Long.toString(from), null, PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    /**
     * Ranges lying entirely above the current xid (lower bound > current xid). Neither the range holding the current
     * xid nor the tail counts; right after an extension this equals {@code premake}.
     */
    public int premadeAhead(Connection c) throws SQLException {
        return state(c).ahead;
    }

    /**
     * True while the tail is hot (holds rows, or the current xid is within one range of it): extension needs README
     * "tail overflow". Also set while an unattached tail holds rows, which are invisible to the relay until then.
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
     * reached its rows: dropped only when empty (as a create whose attach never ran leaves one). Records whose table is
     * gone are deleted first. Returns the ones kept.
     */
    List<Orphan> dropOrphans(Connection c, long minCursorXmin, Duration lockTimeout) throws SQLException {
        try (var ps = c.prepareStatement(deleteStaleMarkers)) { // their table is gone
            ps.setString(1, names.base());
            ps.executeUpdate();
        }
        var marked = new HashSet<String>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select table_name from " + names.dropping())) {
            while (rs.next()) {
                marked.add(rs.getString(1));
            }
        }
        var kept = new ArrayList<Orphan>();
        for (var table : PartitionDdl.standalone(c, names.base(), "_x[0-9]+")) {
            var range = names.unqualified(table);
            boolean ours = marked.contains(range);
            // anyone else's detach may have come before the relay reached its rows: only an empty one goes
            boolean dropped = ours
                    ? PartitionDdl.dropUnlessFound(c, table, ROW_AT_OR_ABOVE, Long.toString(minCursorXmin), lockTimeout)
                    : PartitionDdl.dropUnlessFound(c, table, "select 1 from {table} limit 1", null, lockTimeout);
            if (!dropped) {
                kept.add(new Orphan(table, ours));
            } else if (ours) {
                marker(c, unmarkDropping, range);
            }
        }
        return kept;
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
     * {@code pg_read_all_stats}), which changes the check before a tail detach ({@link #detachBlocker}).
     */
    void warnIfActivityHidden(Connection c) throws SQLException {
        try (var st = c.createStatement();
                var rs = st.executeQuery("select current_user, pg_has_role('pg_read_all_stats', 'USAGE')")) {
            rs.next();
            if (!rs.getBoolean(2)) {
                log.warn("outbox role {} is not a member of pg_read_all_stats, so Postgres hides how long the"
                        + " transactions of sessions logged in as another role have run. That includes this app's own"
                        + " sessions when they log in as a role other than {} (SET ROLE, options=-c role=...,"
                        + " per-instance credentials). Before detaching the partition tail of {}, maintenance then"
                        + " samples such sessions' locks on it twice, 20 s apart, and defers the extension when one"
                        + " transaction holds a lock across both; it sees their snapshots only for pg_dump. Grant"
                        + " pg_read_all_stats (or pg_monitor) to {} to read their age directly, without the 20 s wait",
                        rs.getString(1), rs.getString(1), names.base(), rs.getString(1));
            }
        }
    }

    /**
     * Who may keep a tail detach pending, described, or null ({@link #DETACH_BLOCKERS}). A lock holder whose age is
     * hidden counts only when the same transaction still holds a lock on the parent a threshold later: this sleeps for
     * it, between two autocommit statements (no transaction open, only the caller's session locks held), and only
     * when the first sample finds such a holder. A short transaction of the app's own (under another login role)
     * has ended by then.
     */
    @Nullable
    String detachBlocker(Connection c) throws SQLException {
        var first = sample(c);
        if (first.blocker() != null || first.hidden().isEmpty()) {
            return first.blocker();
        }
        if (!resample) {
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
        try (var ps = c.prepareStatement(DETACH_BLOCKERS)) {
            ps.setString(1, names.base());
            ps.setDouble(2, seconds);
            ps.setString(3, names.base());
            ps.setDouble(4, seconds);
            ps.setString(5, names.base());
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
        log.error("outbox partition tail {} holds rows, so no new xid ranges can be attached; rows are still relayed"
                + " but storage is not reclaimed until README \"tail overflow\" is followed", names.tail());
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
