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
 *   <li>The shard set is computed by the very statement that takes C: {@code pg_current_snapshot()} is that
 *       statement's snapshot, which is also the one its scans read with. T committed before C was taken, so the scans
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
 */
public final class Ticks {

    /**
     * {@code maxId}: the last id the outbox's id sequence had handed out when the tick was made (0 before the first),
     * null for a tick made before the column existed.
     */
    public record Tick(long id, PgSnapshot snapshot, Instant createdAt, @Nullable Long maxId) {}

    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String SELECT = "select id, snapshot::text, created_at, max_id from ";

    private final TableNames names;
    private final int shards;
    /** Built on the first {@link #make}: it names the id sequence, which only the catalog knows. */
    @Nullable
    private volatile String insert;

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
     * The tick insert: the snapshot C, the id high-water mark and the shards with rows in (P, C] for the newest tick P
     * (see the class comment), probing each shard's {@code (shard, xid)} index once for branch 1 and once per xid of
     * P.xip \ C.xip for branch 2. Unless the newest tick is younger than the bound interval: then it inserts and
     * computes nothing. Every {@code c.snapshot} is {@code pg_current_snapshot()}, stable, so the same snapshot in the
     * whole statement; as such it prunes the premade partitions above C.xmax when the executor starts (the redundant
     * bound in branch 2 is there for that), and the probed xids prune the rest as the probes run.
     */
    private String insert(Connection c) throws SQLException {
        var sql = insert;
        if (sql == null) {
            // a future created_at (the clock stepped back) counts as stale, so a wrong clock cannot stop ticks
            insert = sql = "insert into " + names.tick() + " (snapshot, max_id, shards)"
                    + " select c.snapshot, " + maxIdQuery(c, names) + ", case when p.snapshot is not null then"
                    + " coalesce((select array_agg(s::smallint order by s) from generate_series(0, ? - 1) s"
                    + " where exists (select 1 from " + names.base() + " o where o.shard = s"
                    + " and o.xid >= pg_snapshot_xmax(p.snapshot) and o.xid < pg_snapshot_xmax(c.snapshot)"
                    + " and o.xid <> all(x.running))"
                    + " or exists (select 1 from unnest(x.ended) e(xid) cross join lateral (select 1 from "
                    + names.base() + " o where o.shard = s and o.xid = e.xid"
                    + " and o.xid < pg_snapshot_xmax(c.snapshot) limit 1) o)), '{}') end"
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

    /**
     * Makes a tick unless the newest one is younger than {@code minInterval}; returns whether a tick was inserted.
     * Returns false without a tick when the tick lock stays taken for 1 s ({@code lock_timeout}, SQLState 55P03). Runs
     * its own read-committed transaction on an autocommit connection and restores autocommit; all settings are
     * transaction-scoped.
     */
    public boolean make(Connection autocommit, Duration minInterval) throws SQLException {
        var c = autocommit;
        boolean autoCommit = c.getAutoCommit();
        c.setAutoCommit(false);
        boolean inserted;
        try {
            lock(c);
            try (var ps = c.prepareStatement(insert(c))) { // a new statement, so a snapshot taken under the lock
                ps.setInt(1, shards);
                ps.setLong(2, TimeUnit.MICROSECONDS.convert(minInterval));
                inserted = ps.executeUpdate() == 1;
            }
            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
                c.setAutoCommit(autoCommit);
            } catch (SQLException cleanup) { // a terminated session: keep its cause (e.g. 25P03) on top
                e.addSuppressed(cleanup);
            }
            if (LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                return false;
            }
            throw e;
        }
        c.setAutoCommit(autoCommit);
        return inserted;
    }

    /** The first two statements of {@link #make}, for tests that hold the tick lock as a frozen instance would. */
    void lockForTest(Connection inTransaction) throws SQLException {
        lock(inTransaction);
    }

    private void lock(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            // read committed whatever the session default: under repeatable read the lock statement would fix the
            // snapshot before the lock is granted, so the tick would miss commits made while it waited. A generic
            // plan: no parameter of the insert helps plan-time pruning, and a custom plan of it over every partition
            // takes about 1.5 ms to make, against 0.02 ms to reuse the cached generic one
            st.execute("set transaction isolation level read committed;"
                    + " set local idle_in_transaction_session_timeout = '5s'; set local lock_timeout = '1s';"
                    + " set local plan_cache_mode = force_generic_plan" + noTransactionTimeout(c));
        }
        try (var ps = c.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_TICK);
            ps.execute();
        }
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
