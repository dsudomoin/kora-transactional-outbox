package io.github.dsudomoin.kora.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Pattern;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlConsumer;

/**
 * Moves the rows of a standalone outbox range back into the outbox, then drops the range. Moved rows are new messages
 * ({@link Resender}: new id and xid, the same {@code message_id} and headers, the shard recomputed, through the key
 * locks {@code send} takes), so they are delivered after later messages of their keys.
 */
public final class OrphanRestore {

    /** Rows, so key locks, per chunk transaction: far below {@code max_locks_per_transaction} × connections. */
    public static final int CHUNK = 1000;

    private final TableNames names;
    private final Resender resender;
    private final Duration keyLockTimeout;
    /**
     * {@code <base>_x<digits>}, or {@code <base>_o<digits>} as the xid-regression recovery sets one aside, unqualified
     * (as listed on the search_path) or qualified as the outbox is named.
     */
    private final Pattern range;
    private final String unmarkDropping;
    /** For tests: runs after each chunk's commit, to end the session there. */
    @Nullable
    SqlConsumer<Connection> afterChunk;

    /**
     * {@code keyLockTimeout} bounds each wait for a key lock (and any other lock the insert waits for), so a chunk
     * stalls a writer of a key it holds no longer than the tail swap may stall one: pass
     * {@code outbox.partitions.swapLockTimeout}.
     */
    public OrphanRestore(TableNames names, Resender resender, Duration keyLockTimeout) {
        this.names = names;
        this.resender = resender;
        this.keyLockTimeout = keyLockTimeout;
        var base = names.base();
        var unqualified = names.unqualified(base);
        this.range = Pattern.compile("(?:" + Pattern.quote(base.substring(0, base.length() - unqualified.length()))
                + ")?" + Pattern.quote(unqualified) + "_[xo][0-9]+");
        this.unmarkDropping = "delete from " + names.dropping() + " where table_name = ?";
    }

    /**
     * Moves the rows of {@code table} with an xid at or above {@code fromXid} back into the outbox, then drops it:
     * <b>its rows below {@code fromXid} go with the table</b>. First {@code create index if not exists <table>_id};
     * then chunks of {@link #CHUNK} rows in id order, each one read-committed transaction that holds ACCESS EXCLUSIVE
     * on {@code table} alone, never on the outbox, waited for at most {@code lockTimeout}, idle in it at most
     * {@code lockTimeout} + 30 s ({@code idle_in_transaction_session_timeout}): it inserts the rows and
     * deletes them from {@code table} together, so a restore that dies loses and repeats nothing, and the next call
     * resumes it. Its key locks are waited for at most {@code keyLockTimeout} each: while it waits for one, writers of
     * the keys it already holds wait too. The chunk that finds fewer rows, the last, also drops {@code table} and
     * deletes its {@code <base>_dropping} marker. A lock timeout (SQLState 55P03) rolls back the chunk it hits, rows
     * and locks. Returns the rows moved.
     *
     * @throws IllegalArgumentException before any SQL that names it, unless {@code table} is {@code <base>_x<digits>}
     *     or {@code <base>_o<digits>}, unqualified or qualified as the outbox is named, and a table in the outbox's
     *     schema that is no partition
     */
    public long restore(Connection autocommit, String table, long fromXid, Duration lockTimeout) throws SQLException {
        if (!range.matcher(table).matches() || !standalone(autocommit, table)) {
            throw new IllegalArgumentException(table + " is not a standalone range of the outbox " + names.base()
                    + ": a table " + names.partitionPrefix() + "<digits> or " + names.setAsidePrefix() + "<digits> in"
                    + " its schema and in no partition tree");
        }
        PartitionDdl.execute(autocommit, lockTimeout,
                "create index if not exists " + names.unqualified(table) + "_id on " + table + " (id)");
        String chunk = "select id, message_id, topic, key, payload, headers from " + table
                + " where xid >= ?::text::xid8 and id > ? order by id limit " + CHUNK;
        long moved = 0;
        long after = Long.MIN_VALUE;
        boolean last = false;
        var c = autocommit;
        while (!last) {
            var ids = new ArrayList<Long>(CHUNK);
            var rows = new ArrayList<Resender.Row>(CHUNK);
            c.setAutoCommit(false);
            try (var st = c.createStatement()) {
                // first in the transaction: read committed, so the read takes its snapshot after the lock
                st.execute("set transaction isolation level read committed");
                st.execute("set local lock_timeout = " + lockTimeout.toMillis());
                // as relay steps do: Postgres ends a frozen holder's session, and with it the table's lock and a
                // session lock it holds (the maintenance lock of a restoreOrphan call)
                st.execute("set local idle_in_transaction_session_timeout = "
                        + lockTimeout.plusSeconds(30).toMillis());
                st.execute("set local statement_timeout = " + lockTimeout.plus(PartitionDdl.DDL_MARGIN).toMillis());
                st.execute("lock table " + table + " in access exclusive mode");
                try (var ps = c.prepareStatement(chunk)) {
                    ps.setString(1, Long.toString(fromXid));
                    ps.setLong(2, after);
                    try (var rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getLong(1));
                            rows.add(new Resender.Row(rs.getObject(2, UUID.class), rs.getString(3), rs.getBytes(4),
                                    rs.getBytes(5), rs.getBytes(6)));
                        }
                    }
                }
                // from here on, writers may queue behind this chunk's key locks: wait no longer than the swap may
                st.execute("set local lock_timeout = " + keyLockTimeout.toMillis());
                resender.insertAll(c, rows);
                try (var ps = c.prepareStatement("delete from " + table + " where id = any(?)")) {
                    ps.setArray(1, c.createArrayOf("int8", ids.toArray()));
                    ps.executeUpdate();
                }
                last = rows.size() < CHUNK;
                if (last) { // under the lock still: no row can reach the table between this read and the drop
                    st.execute("drop table " + table);
                    try (var ps = c.prepareStatement(unmarkDropping)) {
                        ps.setString(1, names.unqualified(table));
                        ps.executeUpdate();
                    }
                }
                c.commit();
            } catch (Throwable e) { // an Error too: setAutoCommit(true) would commit the chunk so far
                try {
                    c.rollback();
                    c.setAutoCommit(true);
                } catch (SQLException cleanup) { // a terminated session, its transaction gone with it
                    e.addSuppressed(cleanup);
                }
                throw e;
            }
            c.setAutoCommit(true);
            moved += rows.size();
            if (!ids.isEmpty()) {
                after = ids.get(ids.size() - 1);
            }
            if (afterChunk != null) {
                afterChunk.accept(c);
            }
        }
        return moved;
    }

    private boolean standalone(Connection c, String table) throws SQLException {
        try (var ps = c.prepareStatement("select exists (select 1 from pg_class t where t.oid = to_regclass(?)"
                + " and t.relkind = 'r'"
                + " and t.relnamespace = (select p.relnamespace from pg_class p where p.oid = to_regclass(?))"
                + " and not exists (select 1 from pg_inherits i where i.inhrelid = t.oid))")) {
            ps.setString(1, table);
            ps.setString(2, names.base());
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }
}
