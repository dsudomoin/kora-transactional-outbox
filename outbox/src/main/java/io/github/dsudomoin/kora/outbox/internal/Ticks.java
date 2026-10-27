package io.github.dsudomoin.kora.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The snapshot log {@code <base>_tick}. A tick records {@code pg_current_snapshot()}; the batch (P, C] of a
 * shard is the rows whose xid is visible in tick C and not in tick P. Ticks are made under the transaction-scoped
 * advisory lock {@code (internalNs, LOCK_TICK)} held to commit, with the snapshot taken in a statement after the lock
 * statement, so tick ids, snapshots and commits are monotonic. The lock-holding transaction runs with a 5 s
 * {@code idle_in_transaction_session_timeout}, so Postgres terminates a frozen holder's session and frees the lock.
 * A tick also records the outbox id sequence's high-water mark, which sizes batches by data volume.
 *
 * <p>A tick C also records in {@code dirty} the shards that gained rows since the newest tick P before it, as a bitmap
 * of {@code outbox.shards} bits (bit s, leftmost 0, set for shard s; 128 bytes at 1024 shards), with no bits at all
 * when no shard gained rows: a superset of the shards whose batch (P, C] is not empty, so the relay steps only those
 * (see {@link Cursors#positions}), and maintenance's checkpoint moves every other shard past C without a write (see
 * {@link Cursors}). Null means every shard: the first tick, a tick made while the table held no tick, and ticks made
 * before the column existed or by an instance without it. Readers take a bitmap of another length for every shard too,
 * except an empty one, which they take for none: instances with different shard counts on one table are not supported
 * (the bootstrap refuses cursors of another count). Why no shard with rows in (P, C] is left out, from the facts the
 * batch reader relies on:
 * <ol>
 *   <li>A row the batch reader returns for (P, C] belongs to a committed transaction T visible in C and not in P (its
 *       {@code xid} column is T's top-level xid). Visible in C: {@code T < C.xmax} and T not in C.xip, so T had ended
 *       when C was taken. Not visible in P: {@code T >= P.xmax}, or T in P.xip. So T is in {@code [P.xmax, C.xmax)}
 *       and not in C.xip (branch 1), or in P.xip and not in C.xip (branch 2): exactly the two candidate sets of the
 *       batch query ({@link BatchReader}).</li>
 *   <li>The probe scans both sets per shard. Branch 1 scans {@code [P.xmax, C.xmax)} less C.xip. Branch 2 scans, per
 *       cluster of E = P.xip \ C.xip (its xids sorted, a new cluster where the gap to the previous xid exceeds a
 *       bound of at most {@link #CLUSTER_GAP}, smaller when the newest ticks show many outbox rows per xid:
 *       {@link #SPLIT_ROWS}), the range {@code [lo, hi]} from the cluster's first xid to its last, keeping the rows
 *       not visible in P. Every xid of E lies in its cluster's range, however E is cut into clusters, so every row of
 *       E is scanned. And a row this branch returns has its xid in {@code [lo, hi]}, within {@code [P.xmin, P.xmax)},
 *       and not visible in P, so in P.xip; the scan sees it, so it committed before C (next item): its xid is in E.
 *       So branch 2 finds the same shards as a probe per xid of E would; the ranges add only rows of transactions
 *       committed before P, of at most {@code CLUSTER_GAP - 1} xids per gap between consecutive xids of a
 *       cluster.</li>
 *   <li>The shard set is computed by the very statement that takes C (the probe, whose row the tick is then written
 *       from under the same lock): {@code pg_current_snapshot()} is that statement's snapshot, which is also the one
 *       its scans read with. T committed before C was taken, so the scans see T's rows, and every shard holding one of
 *       them gets its bit set. Rows of rolled-back or still running transactions are invisible to the scans, so the set
 *       is in fact exact; a larger one would only cost an empty step.</li>
 *   <li>P is the newest tick the statement sees. The tick lock is held from before that statement to commit, and a
 *       tick's commit is visible before its lock is released, so P is the newest tick of all, and no tick can come
 *       between P and C later: ids grow in lock order ({@code CACHE 1}, checked at startup), and cleanup deletes only
 *       ticks below every shard's effective tick, which never falls below the lowest one ({@link Cursors}). So for a
 *       shard at tick P0 the ticks above it chain, each
 *       made right after the one before it; visibility only grows from tick to tick, so the batch (P0, L] is the union
 *       of the batches between consecutive ticks up to L, and a shard with rows in it has its bit set in (or a null
 *       {@code dirty} on) one of those ticks.</li>
 * </ol>
 * A tick deleted by hand from above a shard's effective tick breaks the chain, and leaves a gap in the tick ids:
 * readers take the tick after a gap for one that marks every shard (as for a gap left by a rolled-back tick insert),
 * so the shards of its rows are found by a step of every shard, not lost. When the probe estimates its branch 2 to cost
 * more than
 * {@link #PROBE_BUDGET_MICROS}, it scans neither branch and C gets a null {@code dirty}, every shard: a superset of any
 * set the scans could find, so none of the above depends on the estimate being right.
 *
 * <p>The shard set is an optimisation, the tick is not: when the probing insert fails, the tick is made without it
 * ({@code dirty} null, every shard), so a lock on any partition (an ALTER TABLE, VACUUM FULL or LOCK by hand, even on
 * an empty premade range) or an error in the probes costs the relay its shard filter, never its ticks.
 */
public final class Ticks {

    /**
     * {@code maxId}: the last id the outbox's id sequence had handed out when the tick was made (0 before the first),
     * null for a tick made before the column existed. {@code snapshot}: null only in the ticks of {@link #between},
     * which leaves it unread.
     */
    public record Tick(long id, @Nullable PgSnapshot snapshot, Instant createdAt, @Nullable Long maxId) {}

    private static final Logger log = LoggerFactory.getLogger(Ticks.class);
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String SELECT = "select id, snapshot::text, created_at, max_id from ";
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    /**
     * The widest gap between consecutive ended xids that the probe's branch 2 scans as one range: wide enough that the
     * transactions of a busy moment, xids drawn close together, make one cluster; a long transaction far below them
     * makes its own.
     */
    static final int CLUSTER_GAP = 1024;
    /**
     * A cluster splits where the rows committed between two of its xids would cost more than probing every shard once
     * more: consecutive ended xids a &lt; b go to different clusters when the {@code b - a - 1} xids between them are
     * estimated to hold more than {@code SPLIT_ROWS} rows per shard, at the rows per xid between the two newest ticks
     * (the outbox ids handed out over the xids drawn; while unknown, no split), so when
     * {@code b - a > 1 + shards * SPLIT_ROWS / (rows per xid)}, and whenever {@code b - a > CLUSTER_GAP}. Measured
     * ({@code TickCostBenchmark}, variant {@code interleaved}): a range spends about 0.26 µs on each such row (a heap
     * fetch, as they are too fresh to be all-visible), one more range probe about 0.8 µs per shard.
     */
    static final int SPLIT_ROWS = 3;
    /** The estimated cost of one range probe of one shard, in µs (see {@link #SPLIT_ROWS}). */
    static final double PROBE_MICROS = 0.8;
    /** The estimated cost of one row of a transaction committed before P that a range scans past, in µs. */
    static final double ROW_MICROS = 0.26;
    /**
     * The estimated cost of the probe's branch 2 above which a tick skips both branches and marks every shard: a range
     * probe per shard and cluster ({@link #PROBE_MICROS}) plus, inside the clusters' ranges, the estimated rows of
     * transactions committed before P ({@link #ROW_MICROS} each, at the rows per xid of {@link #SPLIT_ROWS}); with
     * those rows per xid unknown, no estimate and no cap. Many clusters trip it too, with no such rows at all. The
     * cost moves to the relays rather than going away: with every shard marked, each shard steps once, reading its
     * batch through {@link BatchReader}, whose second branch looks up every xid of E ({@code xid = any(...)}), so about
     * shards × |E| index descents in all, roughly the work of the per-xid probe the clusters replaced (about 266 ms at
     * 1024 shards and 400 ended xids), and appending its cursor row: one capped tick costs one sweep of every shard
     * plus one cursor row per shard, outside the tick lock and spread over the relay workers, however far the shards'
     * effective ticks lie behind it, as a fresh batch starts at the last tick before C that does not mark its shard
     * ({@link #lastUnmarked}). The rows
     * inside the ranges are not read: for a shard at P they are no candidates of the batch (P, C], being visible in P.
     * That the heavy load such ranges need has branch 1 mark nearly every shard anyway is an expectation, not a
     * measurement. At 1024 shards a probe estimated just below the budget took about 5 ms (on a scratch Postgres 16,
     * rows too fresh to be all-visible), a capped one 0.3 – 0.5 ms ({@code TickCostBenchmark}).
     */
    static final int PROBE_BUDGET_MICROS = 4000;

    private final TableNames names;
    private final int shards;
    /** Built on the first {@link #make}: they name the id sequence, which only the catalog knows. */
    @Nullable
    private volatile String probe;
    @Nullable
    private volatile String insertUnprobed;
    /** When a failed probe was last logged ({@code nanoTime}), and how many went unlogged since. */
    private final AtomicLong probeWarned = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    private final AtomicInteger probeUnwarned = new AtomicInteger();

    /** {@code shards}: {@code outbox.shards}, the length of a tick's {@code dirty} bitmap. */
    public Ticks(TableNames names, int shards) {
        this.names = names;
        this.shards = shards;
    }

    /**
     * A scalar subquery for the {@code max_id} of a tick made now: the last value of {@code <base>.id}'s identity
     * sequence, 0 while it has handed out none. Ids of rolled-back inserts count, so it can only overestimate.
     */
    static String maxIdQuery(Connection c, TableNames names) throws SQLException {
        String sequence;
        try (var ps = c.prepareStatement("select pg_get_serial_sequence(?, 'id')")) {
            ps.setString(1, names.base());
            try (var rs = ps.executeQuery()) {
                rs.next();
                sequence = rs.getString(1); // quoted and schema-qualified by Postgres
            }
        }
        if (sequence == null) {
            throw new IllegalStateException("no id sequence for " + names.base());
        }
        return "(select case when is_called then last_value else 0 end from " + sequence + ")";
    }

    /**
     * The tick probe: one row of the snapshot C (text), the id high-water mark and the {@code dirty} bitmap of (P, C]
     * for the newest tick P (see the class comment), or no row while the newest tick is younger than the bound
     * interval. It probes each shard's {@code (shard, xid)} index once for branch 1 and, only when that finds nothing,
     * once per cluster of P.xip \ C.xip for branch 2 (a range scan, so the transactions that ended together cost a
     * shard one probe, not one each), each stopping at its first row (a lateral {@code union all ... limit 1}: an
     * {@code exists} there may be planned as one hashed pass over every row of the range, which a busy tick makes
     * slow). Every {@code c.snapshot} is {@code pg_current_snapshot()}, stable, so the same snapshot in the whole
     * statement; as such it prunes the premade partitions above C.xmax when the executor starts (the redundant bound
     * in branch 2 is there for that), and the probes' lower bounds (P.xmax, a cluster's first xid) prune the rest as
     * the probes run. When the estimate of branch 2 goes over {@link #PROBE_BUDGET_MICROS}, the bitmap is null and the
     * statement probes no shard (a CASE that skips the subquery).
     */
    String probe(Connection c) throws SQLException {
        var sql = probe;
        if (sql == null) {
            // a future created_at (the clock stepped back) counts as stale, so a wrong clock cannot stop ticks
            probe = sql = "select c.snapshot::text, " + maxIdQuery(c, names) + ", case when p.snapshot is not null"
                    + " and not x.capped then"
                    // ordinality: the rows come in shard order, and the planner knows it, so the aggregate sorts
                    // nothing
                    // and no bit at all when no shard gained rows (an idle tick stores 0 bytes, not shards / 8)
                    + " (select case when bool_or(d.found is not null) then string_agg(case when d.found is null"
                    + " then '0' else '1' end, '' order by g.i)::varbit else ''::varbit end"
                    + " from generate_series(0, ? - 1) with ordinality g(s, i)"
                    + " left join lateral ((select 1 found from " + names.base() + " o where o.shard = s"
                    + " and o.xid >= pg_snapshot_xmax(p.snapshot) and o.xid < pg_snapshot_xmax(c.snapshot)"
                    + " and o.xid <> all(x.running) limit 1)"
                    + " union all (select 1 from unnest(x.lo, x.hi) k(lo, hi) cross join lateral (select 1 from "
                    + names.base() + " o where o.shard = s and o.xid >= k.lo and o.xid <= k.hi"
                    + " and o.xid < pg_snapshot_xmax(c.snapshot) and not pg_visible_in_snapshot(o.xid, p.snapshot)"
                    + " limit 1) o limit 1) limit 1) d on true) end"
                    + " from (select pg_current_snapshot() snapshot) c"
                    // through text once: a snapshot of a few hundred xids is stored compressed, and each function
                    // given the stored value would decompress it again, for every row a range scans
                    + " left join lateral (select snapshot::text::pg_snapshot snapshot, created_at from "
                    + names.tick() + " order by id desc limit 1) p on true"
                    // the clusters of E = P.xip \ C.xip: lo[k] and hi[k] are the first and the last xid of cluster
                    // k. E is the xids of P.xip visible in C (they are below P.xmax <= C.xmax), and P.xip lists them
                    // ascending (pg_snapshot rejects any other order), so ordinality is xid order and nothing is sorted
                    + " cross join lateral (select array(select pg_snapshot_xip(c.snapshot)) running,"
                    + " array_agg(xid order by i) filter (where first) lo,"
                    + " array_agg(xid order by i) filter (where last) hi,"
                    // the estimate of PROBE_BUDGET_MICROS: a range probe per shard for each cluster, the rows per xid
                    // for each xid inside a range that is not in E; one aggregate, as each costs even when E is empty
                    + " coalesce(sum(case when rho is null then null when first then " + PROBE_MICROS * shards
                    + " else " + ROW_MICROS + " * rho * (n - b - 1) end) > " + PROBE_BUDGET_MICROS + ", false) capped"
                    // a cluster starts after a gap wider than SPLIT_ROWS allows, and ends before one. The two bounds
                    // must stay the same expression: unnest(lo, hi) pairs the k-th start with the k-th end, and the
                    // proof (every xid of E inside its cluster's range) needs each gap to end one cluster exactly
                    // where it starts the next
                    + " from (select xid, i, n, b, rho, n - b > least(" + CLUSTER_GAP + ", 1 + "
                    + (long) shards * SPLIT_ROWS + " / nullif(rho, 0)) is not false first,"
                    + " a - n > least(" + CLUSTER_GAP + ", 1 + " + (long) shards * SPLIT_ROWS
                    + " / nullif(rho, 0)) is not false last"
                    // b and a: the xids before and after; rho: the rows per xid between the two newest ticks (null
                    // unless both have max_id), an uncorrelated subquery, so computed once (and only when E is not
                    // empty)
                    + " from (select xid, i, n, lag(n) over w b, lead(n) over w a, (select case"
                    + " when count(t.max_id) = 2 then (max(t.max_id) - min(t.max_id))"
                    + " / nullif(max(t.xmax) - min(t.xmax), 0) end from (select"
                    + " pg_snapshot_xmax(snapshot)::text::float8 xmax, max_id from " + names.tick()
                    + " order by id desc limit 2) t) rho"
                    + " from (select xid, i, xid::text::bigint n"
                    + " from pg_snapshot_xip(p.snapshot) with ordinality e(xid, i)"
                    + " where pg_visible_in_snapshot(xid, c.snapshot)) e window w as (order by i)) e) e) x"
                    // P younger than the bound: no row (P's own row, so the tick table is read once for it)
                    + " where not (p.created_at > clock_timestamp() - ? * interval '1 microsecond'"
                    + " and p.created_at <= clock_timestamp()) is true";
        }
        return sql;
    }

    /** The tick insert after a failed probe: the snapshot and the id high-water mark, no shard set (every shard). */
    private String insertUnprobed(Connection c) throws SQLException {
        var sql = insertUnprobed;
        if (sql == null) {
            insertUnprobed = sql = "insert into " + names.tick() + " (snapshot, max_id)"
                    + " select pg_current_snapshot(), " + maxIdQuery(c, names)
                    + " where not exists (select 1 from (select created_at from " + names.tick()
                    + " order by id desc limit 1) t"
                    + " where t.created_at > clock_timestamp() - ? * interval '1 microsecond'"
                    + " and t.created_at <= clock_timestamp())";
        }
        return sql;
    }

    /**
     * Makes a tick unless the newest one is younger than {@code minInterval}; returns whether a tick was inserted.
     * Returns false without a tick when the tick lock stays taken for 1 s ({@code lock_timeout} on the lock statement,
     * SQLState 55P03). When the probing insert fails short of a broken connection, the tick is made without its shard
     * set and a WARN names the cause, at most once a minute: a lock it waited for longer than 50 ms (any partition held
     * or awaited in ACCESS EXCLUSIVE mode), a role's {@code statement_timeout}, an unusable index. Runs its own
     * read-committed transaction on an autocommit connection and restores autocommit; all settings are
     * transaction-scoped.
     */
    public boolean make(Connection autocommit, Duration minInterval) throws SQLException {
        var c = autocommit;
        boolean autoCommit = c.getAutoCommit();
        c.setAutoCommit(false);
        boolean inserted;
        try {
            if (!lock(c)) {
                c.rollback();
                c.setAutoCommit(autoCommit);
                return false;
            }
            inserted = insert(c, minInterval);
            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
                c.setAutoCommit(autoCommit);
            } catch (SQLException cleanup) { // a terminated session: keep its cause (e.g. 25P03) on top
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        c.setAutoCommit(autoCommit);
        return inserted;
    }

    /**
     * The tick after the lock statement, its snapshot taken by a new statement so under the lock: the probe runs inside
     * a savepoint with a 50 ms {@code lock_timeout}. It takes ACCESS SHARE on {@code <base>}, every attached partition
     * and the indexes it opens, granted at once unless someone holds or awaits ACCESS EXCLUSIVE on one, while
     * autovacuum truncating a partition yields to a waiter within 20 ms; the wait counts against the tick lock, so a
     * locked partition adds at most 50 ms to a tick. The savepoint is then rolled back and released, which undoes the
     * shorter {@code lock_timeout}, keeps the tick lock, and leaves a failed probe without effect. The tick row is
     * written at the top level, from the probe's row or, after a failure, without a shard set: a write inside the
     * savepoint would take a second transaction id for every tick.
     */
    private boolean insert(Connection c, Duration minInterval) throws SQLException {
        long micros = TimeUnit.MICROSECONDS.convert(minInterval);
        try (var st = c.createStatement()) {
            st.execute("savepoint tick_probe; set local lock_timeout = '50ms'");
        }
        boolean young = false;
        String snapshot = null;
        long maxId = 0;
        String dirty = null;
        SQLException failed = null;
        try (var ps = c.prepareStatement(probe(c))) {
            ps.setInt(1, shards);
            ps.setLong(2, micros);
            try (var rs = ps.executeQuery()) {
                young = !rs.next();
                if (!young) {
                    snapshot = rs.getString(1);
                    maxId = rs.getLong(2);
                    dirty = rs.getString(3);
                }
            }
        } catch (SQLException e) {
            failed = e;
        }
        try (var st = c.createStatement()) {
            st.execute("rollback to savepoint tick_probe; release savepoint tick_probe");
        } catch (SQLException broken) { // a broken connection: the tick fails with it
            if (failed != null) {
                failed.addSuppressed(broken);
                throw failed;
            }
            throw broken;
        }
        if (failed == null) {
            if (young) {
                return false;
            }
            try (var ps = c.prepareStatement("insert into " + names.tick() + " (snapshot, max_id, dirty)"
                    + " values (?::pg_snapshot, ?, ?::varbit)")) {
                ps.setString(1, snapshot);
                ps.setLong(2, maxId);
                ps.setString(3, dirty);
                ps.executeUpdate();
                return true;
            }
        }
        warnProbeFailed(failed);
        try (var ps = c.prepareStatement(insertUnprobed(c))) {
            ps.setLong(1, micros);
            return ps.executeUpdate() == 1;
        }
    }

    private void warnProbeFailed(SQLException e) {
        long now = System.nanoTime();
        long last = probeWarned.get();
        if (now - last < WARN_INTERVAL || !probeWarned.compareAndSet(last, now)) {
            probeUnwarned.incrementAndGet();
            return;
        }
        log.warn("outbox tick made without its shard set, so the relay steps every shard behind it: the shard probe"
                + " failed with SQLState {}: {} ({} more since the previous report not logged)", e.getSQLState(),
                e.getMessage(), probeUnwarned.getAndSet(0));
    }

    /** The first two statements of {@link #make}, for tests that hold the tick lock as a frozen instance would. */
    void lockForTest(Connection inTransaction) throws SQLException {
        lock(inTransaction);
    }

    /** The settings and the tick lock; false when the lock stays taken for 1 s (the transaction is then aborted). */
    private boolean lock(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            // read committed whatever the session default: under repeatable read the lock statement would fix the
            // snapshot before the lock is granted, so the tick would miss commits made while it waited. A generic
            // plan: no parameter of the probe helps plan-time pruning, and a custom plan of it over every partition
            // takes about 1.5 ms to make, against 0.02 ms to reuse the cached generic one
            st.execute("set transaction isolation level read committed;"
                    + " set local idle_in_transaction_session_timeout = '5s'; set local lock_timeout = '1s';"
                    + " set local plan_cache_mode = force_generic_plan" + noTransactionTimeout(c));
        }
        try (var ps = c.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_TICK);
            ps.execute();
        } catch (SQLException e) {
            if (LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                return false;
            }
            throw e;
        }
        return true;
    }

    /**
     * A further {@code SET LOCAL} that switches off a role's or database's {@code transaction_timeout} for the library's
     * own transactions, whose bounds are their own; empty before PG 17, which has no such setting (and would reject it).
     * Disabling it mid-transaction stops the running timer.
     */
    static String noTransactionTimeout(Connection c) throws SQLException {
        return c.getMetaData().getDatabaseMajorVersion() >= 17 ? "; set local transaction_timeout = 0" : "";
    }

    public Tick latest(Connection c) throws SQLException {
        return one(c, " order by id desc limit 1", null);
    }

    public Tick get(Connection c, long id) throws SQLException {
        return one(c, " where id = ?", id);
    }

    /**
     * For a fresh batch of {@code shard} from the tick {@code afterId}: the last tick before the first one after it, up
     * to {@code latestId}, that marks the shard, when that is past {@code afterId}; null when none is. A tick marks the
     * shard when its {@code dirty} has the shard's bit or is null (or of another non-zero length), when it follows a
     * gap in tick ids, or when it is the latest. The ticks skipped mark nothing of the shard and chain (see the class
     * comment), so the shard has no row visible in the tick returned and not in {@code afterId}: its batch can start
     * there, read the same rows and cover its {@code maxTicksPerBatch} ticks from the first that may hold them. One
     * statement, an index scan from {@code afterId} that stops at the first marking tick.
     */
    @Nullable
    public Tick lastUnmarked(Connection c, int shard, long afterId, long latestId) throws SQLException {
        try (var ps = c.prepareStatement(SELECT + names.tick() + " where id = (select max(m.id) from " + names.tick()
                + " m where m.id > ? and m.id < (select n.id from " + names.tick() + " n where n.id > ? and n.id <= ?"
                + " and (n.id = ? or n.dirty is null or not exists (select 1 from " + names.tick() + " g where g.id"
                + " = n.id - 1) or case when length(n.dirty) = 0 then false when length(n.dirty) <> " + shards
                + " then true else get_bit(n.dirty, ?) = 1 end) order by n.id limit 1))")) {
            ps.setLong(1, afterId);
            ps.setLong(2, afterId);
            ps.setLong(3, latestId);
            ps.setLong(4, latestId);
            ps.setInt(5, shard);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? tick(rs) : null;
            }
        }
    }

    /** The ticks with these ids, keyed by id; an id without a tick is absent. One statement. */
    public Map<Long, Tick> get(Connection c, Collection<Long> ids) throws SQLException {
        var result = new HashMap<Long, Tick>();
        if (ids.isEmpty()) {
            return result;
        }
        try (var ps = c.prepareStatement(SELECT + names.tick() + " where id = any(?)")) {
            ps.setArray(1, c.createArrayOf("int8", ids.toArray()));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var tick = tick(rs);
                    result.put(tick.id(), tick);
                }
            }
        }
        return result;
    }

    /**
     * The ticks with {@code afterId < id <= upToId}, in id order, without their snapshots ({@link Tick#snapshot()}
     * null): choosing a batch's target needs only the ids and high-water marks, and a snapshot of a busy moment is a
     * few KB of text; {@link #get} reads the one chosen.
     */
    public List<Tick> between(Connection c, long afterId, long upToId) throws SQLException {
        try (var ps = c.prepareStatement("select id, created_at, max_id from " + names.tick()
                + " where id > ? and id <= ? order by id")) {
            ps.setLong(1, afterId);
            ps.setLong(2, upToId);
            try (var rs = ps.executeQuery()) {
                var ticks = new ArrayList<Tick>();
                while (rs.next()) {
                    ticks.add(new Tick(rs.getLong(1), null, rs.getObject(2, OffsetDateTime.class).toInstant(),
                            rs.getObject(3, Long.class)));
                }
                return ticks;
            }
        }
    }

    /** The oldest tick with {@code id > afterId}: bridges gaps in tick ids. */
    public Tick oldestAfter(Connection c, long afterId) throws SQLException {
        return one(c, " where id > ? order by id limit 1", afterId);
    }

    public int deleteBelow(Connection c, long id) throws SQLException {
        try (var ps = c.prepareStatement("delete from " + names.tick() + " where id < ?")) {
            ps.setLong(1, id);
            return ps.executeUpdate();
        }
    }

    /** Throws {@link IllegalStateException} when no tick matches: callers only ask for ticks that must exist. */
    private Tick one(Connection c, String condition, @Nullable Long id) throws SQLException {
        try (var ps = c.prepareStatement(SELECT + names.tick() + condition)) {
            if (id != null) {
                ps.setLong(1, id);
            }
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("no tick in " + names.tick() + " matching" + condition
                            + (id == null ? "" : " (" + id + ")"));
                }
                return tick(rs);
            }
        }
    }

    private static Tick tick(ResultSet rs) throws SQLException {
        return new Tick(
                rs.getLong(1),
                PgSnapshot.parse(rs.getString(2)),
                rs.getObject(3, OffsetDateTime.class).toInstant(),
                rs.getObject(4, Long.class));
    }
}
