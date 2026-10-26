package io.github.dsudomoin.kora.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
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
 * <p>A tick C also records in {@code shards} the shards that gained rows since the newest tick P before it: a superset
 * of the shards whose batch (P, C] is not empty, so the relay steps only those (see {@link Cursors#positions}). Null
 * means every shard: the first tick, a tick made while the table held no tick, and ticks made before the column
 * existed or by an instance without it. Why no shard with rows in (P, C] is left out, from the facts the batch reader
 * relies on:
 * <ol>
 *   <li>A row the batch reader returns for (P, C] belongs to a committed transaction T visible in C and not in P (its
 *       {@code xid} column is T's top-level xid). Visible in C: {@code T < C.xmax} and T not in C.xip, so T had ended
 *       when C was taken. Not visible in P: {@code T >= P.xmax}, or T in P.xip. So T is in {@code [P.xmax, C.xmax)}
 *       and not in C.xip (branch 1), or in P.xip and not in C.xip (branch 2): exactly the two candidate sets of the
 *       batch query ({@link BatchReader}).</li>
 *   <li>The shard set is computed by the very statement that takes C (the probe, whose row the tick is then written
 *       from under the same lock): {@code pg_current_snapshot()} is that statement's snapshot, which is also the one
 *       its scans read with. T committed before C was taken, so the scans
 *       see T's rows, and every shard holding one of them gets into the set. Rows of rolled-back or still running
 *       transactions are invisible to the scans, so the set is in fact exact; a larger one would only cost an empty
 *       step.</li>
 *   <li>P is the newest tick the statement sees. The tick lock is held from before that statement to commit, and a
 *       tick's commit is visible before its lock is released, so P is the newest tick of all, and no tick can come
 *       between P and C later: ids grow in lock order ({@code CACHE 1}, checked at startup), and cleanup deletes only
 *       ticks below every cursor, while cursors only move up. So for a cursor on tick P0 the ticks above it chain, each
 *       made right after the one before it; visibility only grows from tick to tick, so the batch (P0, L] is the union
 *       of the batches between consecutive ticks up to L, and a shard with rows in it is in the set (or null) of one of
 *       those ticks.</li>
 * </ol>
 * A tick deleted by hand from above a cursor breaks the chain: the shards of its rows are then found only by the idle
 * append ({@link ShardProcessor#IDLE_APPEND_AFTER}), not lost.
 *
 * <p>The shard set is an optimisation, the tick is not: when the probing insert fails, the tick is made without it
 * ({@code shards} null, every shard), so a lock on any partition (an ALTER TABLE, VACUUM FULL or LOCK by hand, even on an
 * empty premade range) or an error in the probes costs the relay its shard filter, never its ticks.
 */
public final class Ticks {

    /**
     * {@code maxId}: the last id the outbox's id sequence had handed out when the tick was made (0 before the first),
     * null for a tick made before the column existed.
     */
    public record Tick(long id, PgSnapshot snapshot, Instant createdAt, @Nullable Long maxId) {}

    private static final Logger log = LoggerFactory.getLogger(Ticks.class);
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String SELECT = "select id, snapshot::text, created_at, max_id from ";
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);

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

    /** {@code shards}: {@code outbox.shards}, the shards a tick's {@code shards} set is drawn from. */
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
     * The tick probe: one row of the snapshot C (text), the id high-water mark and the shards with rows in (P, C] for
     * the newest tick P (see the class comment), or no row while the newest tick is younger than the bound interval.
     * It probes each shard's {@code (shard, xid)} index once for branch 1 and, only when that finds nothing, once per
     * xid of P.xip \ C.xip for branch 2, each stopping at its first row (a lateral {@code union all ... limit 1}: an
     * {@code exists} there may be planned as one hashed pass over every row of the range, which a busy tick makes
     * slow). Every {@code c.snapshot} is {@code pg_current_snapshot()}, stable, so the same snapshot in the whole
     * statement; as such it prunes the premade partitions above C.xmax when the executor starts (the redundant bound
     * in branch 2 is there for that), and the probed xids prune the rest as the probes run.
     */
    String probe(Connection c) throws SQLException {
        var sql = probe;
        if (sql == null) {
            // a future created_at (the clock stepped back) counts as stale, so a wrong clock cannot stop ticks
            probe = sql = "select c.snapshot::text, " + maxIdQuery(c, names) + ", case when p.snapshot is not null then"
                    + " coalesce((select array_agg(s::smallint order by s) from generate_series(0, ? - 1) s"
                    + " cross join lateral ((select 1 from " + names.base() + " o where o.shard = s"
                    + " and o.xid >= pg_snapshot_xmax(p.snapshot) and o.xid < pg_snapshot_xmax(c.snapshot)"
                    + " and o.xid <> all(x.running) limit 1)"
                    + " union all (select 1 from unnest(x.ended) e(xid) cross join lateral (select 1 from "
                    + names.base() + " o where o.shard = s and o.xid = e.xid"
                    + " and o.xid < pg_snapshot_xmax(c.snapshot) limit 1) o limit 1) limit 1) d), '{}') end"
                    + " from (select pg_current_snapshot() snapshot) c"
                    + " left join lateral (select snapshot from " + names.tick() + " order by id desc limit 1) p"
                    + " on true"
                    + " cross join lateral (select array(select pg_snapshot_xip(c.snapshot)) running,"
                    + " array(select pg_snapshot_xip(p.snapshot) except select pg_snapshot_xip(c.snapshot)) ended) x"
                    + " where not exists (select 1 from (select created_at from " + names.tick()
                    + " order by id desc limit 1) t"
                    + " where t.created_at > clock_timestamp() - ? * interval '1 microsecond'"
                    + " and t.created_at <= clock_timestamp())";
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
        String set = null;
        SQLException failed = null;
        try (var ps = c.prepareStatement(probe(c))) {
            ps.setInt(1, shards);
            ps.setLong(2, micros);
            try (var rs = ps.executeQuery()) {
                young = !rs.next();
                if (!young) {
                    snapshot = rs.getString(1);
                    maxId = rs.getLong(2);
                    set = rs.getString(3);
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
            try (var ps = c.prepareStatement("insert into " + names.tick() + " (snapshot, max_id, shards)"
                    + " values (?::pg_snapshot, ?, ?::smallint[])")) {
                ps.setString(1, snapshot);
                ps.setLong(2, maxId);
                ps.setString(3, set);
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

    /** The ticks with {@code afterId < id <= upToId}, in id order. */
    public List<Tick> between(Connection c, long afterId, long upToId) throws SQLException {
        try (var ps = c.prepareStatement(SELECT + names.tick() + " where id > ? and id <= ? order by id")) {
            ps.setLong(1, afterId);
            ps.setLong(2, upToId);
            try (var rs = ps.executeQuery()) {
                var ticks = new ArrayList<Tick>();
                while (rs.next()) {
                    ticks.add(tick(rs));
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
