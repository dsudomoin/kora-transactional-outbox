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
    /** Built on the first {@link #make}: it names the id sequence, which only the catalog knows. */
    @Nullable
    private volatile String insert;

    public Ticks(TableNames names) {
        this.names = names;
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

    private String insert(Connection c) throws SQLException {
        var sql = insert;
        if (sql == null) {
            // a future created_at (the clock stepped back) counts as stale, so a wrong clock cannot stop ticks
            insert = sql = "insert into " + names.tick() + " (snapshot, max_id)"
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
                ps.setLong(1, TimeUnit.MICROSECONDS.convert(minInterval));
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
            // snapshot before the lock is granted, so the tick would miss commits made while it waited
            st.execute("set transaction isolation level read committed;"
                    + " set local idle_in_transaction_session_timeout = '5s'; set local lock_timeout = '1s'"
                    + noTransactionTimeout(c));
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
