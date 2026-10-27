package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Attachment;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlConsumer;

/**
 * Recovery from an xid regression: the xid counter below the xmax of a tick, as after a logical restore or migration
 * (pg_dump/pg_restore, logical replication into another cluster), where new rows get xids that the old ticks take for
 * relayed. {@link #recover} rebuilds what a fresh table has (ranges above the current xid, a tail, one tick, one cursor
 * per shard) and re-sends, as new messages, every row the relay may not have delivered, in one transaction that
 * writers wait for. Bootstrap ({@link OutboxSchema}) and {@link Maintenance} run it by themselves under
 * {@code outbox.recovery} ({@link #autoRecover}); both hold {@code LOCK_MAINTENANCE}, so one instance recovers.
 */
public final class XidRecovery {

    /** Why a halt stays when {@code outbox.recovery.xidRegression = halt}. */
    static final String HALT_MODE = "outbox.recovery.xidRegression = halt";
    /** Why a halt stays when the maintenance lock was not to be had. */
    static final String LOCK_BUSY =
            "another session holds the maintenance lock; the next maintenance run to take it recovers it";

    private static final Logger log = LoggerFactory.getLogger(XidRecovery.class);
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration STATEMENT_TIMEOUT = Duration.ofMinutes(10);

    /**
     * One read of the xid check: the current xid, the highest tick xmax (0 without ticks) and the recorded halt's
     * xmax (null without one).
     */
    record Check(long current, long tickXmax, @Nullable Long recorded) {

        /**
         * A regression, or a recorded halt that ticks repaired below its xmax have not ended: the counter catching up
         * does not end it, as cursors still sit on pre-regression ticks and would skip the rows written meanwhile.
         */
        boolean halted() {
            return current < tickXmax || recorded != null && tickXmax >= recorded;
        }
    }

    private final TableNames names;
    private final int shards;
    private final long maxRows;
    private final long size;
    private final XidPartitions partitions;
    /**
     * The rows to re-send: per shard, the rows the tick of its latest cursor does not see, plus every row written
     * since T, the {@code created_at} of the tick with the highest xmax (the newest made before the regression) less
     * {@code clockMargin}: written into the restored database, their xids may look relayed. An earlier T only adds
     * duplicates, which consumers drop within their inbox window. A shard without a cursor, or whose tick is gone,
     * keeps every row. Rows the retry or the dead table holds stay there.
     */
    private final String saved;
    final String xidCheck; // for a test of its plan
    private final String haltInsert;
    private final String holding;
    /** For tests: runs once the pending detach is finished, before the transaction. */
    @Nullable
    SqlConsumer<Connection> beforeTransaction;
    /** For tests: runs in {@link #makeRanges} between its snapshot read and {@code ensurePremade}'s. */
    @Nullable
    SqlConsumer<Connection> betweenReads;

    public XidRecovery(TableNames names, OutboxConfig config) {
        this.names = names;
        this.shards = config.shards();
        this.maxRows = config.recovery().maxRows();
        this.size = config.partitions().xidsPerPartition();
        this.partitions = new XidPartitions(names, config.partitions());
        long margin = TimeUnit.MICROSECONDS.convert(config.recovery().clockMargin());
        this.saved = "select o.id, o.shard, o.created_at, o.message_id, o.topic, o.key, o.payload, o.headers from "
                + names.base() + " o"
                + " left join (select distinct on (shard) shard, tick_id from " + names.cursor()
                + " order by shard, seq desc) c on c.shard = o.shard"
                + " left join " + names.tick() + " t on t.id = c.tick_id"
                + " where (t.snapshot is null or not pg_visible_in_snapshot(o.xid, t.snapshot)"
                + " or o.created_at >= (select created_at from " + names.tick()
                + " order by pg_snapshot_xmax(snapshot) desc limit 1) - " + margin + " * interval '1 microsecond')"
                + " and not exists (select 1 from " + names.retry() + " r where r.id = o.id"
                + " and r.message_id = o.message_id)"
                + " and not exists (select 1 from " + names.dead() + " d where d.id = o.id"
                + " and d.message_id = o.message_id)"; // parked rows: added after the requeue merge
        // every tick, not just the newest: workers keep ticking after a regression, and their new ticks look fine; the
        // highest xmax comes from the index <tick>_xmax, as ticks pile up while a paused shard holds cleanup back
        this.xidCheck = "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint,"
                + " (select pg_snapshot_xmax(snapshot)::text::bigint from " + names.tick()
                + " order by pg_snapshot_xmax(snapshot) desc limit 1),"
                + " (select max_tick_xmax::text::bigint from " + names.halt() + ")";
        this.haltInsert = "insert into " + names.halt() + " as h (max_tick_xmax) values (?::text::xid8)"
                + " on conflict (id) do update set max_tick_xmax = greatest(h.max_tick_xmax, excluded.max_tick_xmax)";
        this.holding = names.base() + "_recovery";
    }

    Check check(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(xidCheck)) {
            rs.next();
            return new Check(rs.getLong(1), rs.getLong(2), rs.getObject(3, Long.class));
        }
    }

    /** Records the halt with {@code tickXmax}, keeping a higher one recorded before. */
    void recordHalt(Connection c, long tickXmax) throws SQLException {
        try (var ps = c.prepareStatement(haltInsert)) {
            ps.setLong(1, tickXmax);
            ps.executeUpdate();
        }
    }

    /** How many rows {@link #recover} would re-send now; one scan of the outbox, under no lock. */
    public long rowsToSave(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select count(*) from (" + saved + ") s")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * The automatic recovery, for a caller holding {@code LOCK_MAINTENANCE}: checks again (another instance may have
     * recovered meanwhile), then recovers when at most {@code outbox.recovery.maxRows} rows are to be re-sent, with an
     * ERROR saying how many were. Returns null when nothing is halted any more, else why the relay stays halted,
     * a failed recovery included (rolled back: the next attempt starts over).
     */
    @Nullable
    String autoRecover(Connection autocommit) {
        try {
            var check = check(autocommit);
            if (!check.halted()) {
                return null;
            }
            long rows = rowsToSave(autocommit);
            if (rows > maxRows) {
                return rows + " rows to re-send exceed outbox.recovery.maxRows " + maxRows;
            }
            long sent = recover(autocommit);
            log.error("{}, recovered: {} rows re-sent, the ones the relay may not have delivered: new messages with"
                    + " new ids and xids and the same message ids, so consumers drop within their inbox window the ones"
                    + " delivered before", detected(check), sent);
            return null;
        } catch (SQLException e) {
            return "the recovery failed (SQLState " + e.getSQLState() + "): " + e.getMessage()
                    + "; the next maintenance run retries it";
        } catch (RuntimeException e) {
            return "the recovery failed: " + e + "; the next maintenance run retries it";
        }
    }

    /** The ERROR while halted: what the check found, {@code why} no recovery ran, and the call that runs one. */
    String halted(Check check, String why) {
        return detected(check) + ": relay halted, as relaying would skip rows, on every instance and across restarts"
                + " while " + names.halt() + " holds a row: " + why + ". OutboxAdmin.recoverXidRegression() or POST"
                + " /kora-outbox/admin/v1/recovery/xid-regression runs the recovery, writers pause meanwhile";
    }

    private String detected(Check check) {
        return "outbox xid regression detected on " + names.base() + " (current xid " + check.current()
                + ", highest tick xmax " + check.tickXmax() + " in " + names.tick()
                + "; restored from a dump or logically migrated?)";
    }

    /**
     * Recovers in one read-committed transaction ({@code lock_timeout} 5 s, {@code statement_timeout} 10 min,
     * {@code idle_in_transaction_session_timeout} 5 s, so Postgres ends the session of a frozen holder and writers go
     * on; no {@code transaction_timeout}), for a caller holding {@code LOCK_MAINTENANCE}; returns the rows re-sent, or
     * 0 with no change when there is neither a regression nor a halt row. First, outside it, a detach left pending is
     * finished ({@link XidPartitions#finishPendingDetach}), so its wait for older snapshots stalls no writer. Then:
     * ACCESS EXCLUSIVE on the cursor and tick tables, in the order relay steps read them (a step that read its cursor
     * ends first, one that reads it later sees the new one), then on the outbox, which writers wait for, not fail;
     * the rows to re-send ({@link #saved}) copied into {@code <base>_recovery}; every standalone range, and a tail that
     * is not attached, set aside as {@code <base>_o<digits>} when it holds rows (they lie outside the outbox and are
     * never re-sent by themselves: {@code restoreOrphan} moves them back) and dropped when empty; the id sequence set
     * above every id of the outbox, retry and dead tables; every partition dropped; ticks, cursors, dropping markers
     * and the halt deleted; ranges from the one holding the oldest running xid (a writer waiting for the lock may hold
     * it) and a tail above the current xid ({@link XidPartitions#ensurePremade}, its fresh-table path); one tick and
     * one cursor per shard at it, due for a retry check where the shard holds retry rows; the saved rows inserted in
     * {@code created_at, id} order, which gives the ids, as no {@code send} gets in before the commit.
     *
     * @throws IllegalStateException when {@code outbox.shards} differs from the shards of the cursors: saved rows keep
     *     their shard; or when a detach became pending after the finish before the transaction: the next attempt
     *     finishes it first
     */
    public long recover(Connection autocommit) throws SQLException {
        var c = autocommit;
        partitions.finishPendingDetach(c, LOCK_TIMEOUT);
        if (beforeTransaction != null) {
            beforeTransaction.accept(c);
        }
        long sent;
        c.setAutoCommit(false);
        try (var st = c.createStatement()) {
            st.execute("set transaction isolation level read committed"); // first: before any statement of the tx
            st.execute("set local lock_timeout = " + LOCK_TIMEOUT.toMillis() + "; set local statement_timeout = "
                    + STATEMENT_TIMEOUT.toMillis() + "; set local idle_in_transaction_session_timeout = "
                    + LOCK_TIMEOUT.toMillis() + Ticks.noTransactionTimeout(c));
            st.execute("lock table " + names.cursor() + ", " + names.tick() + " in access exclusive mode");
            st.execute("lock table " + names.base() + " in access exclusive mode");
            st.execute("lock table " + names.dropping() + ", " + names.halt() + " in exclusive mode");
            var check = check(c);
            if (check.current() >= check.tickXmax() && check.recorded() == null) {
                c.rollback();
                return 0;
            }
            long shardsPresent = queryLong(st, "select count(distinct shard) from " + names.cursor());
            if (shardsPresent != 0 && shardsPresent != shards) {
                throw new IllegalStateException("outbox.shards = " + shards + " but " + names.cursor() + " holds"
                        + " cursors for " + shardsPresent + " shards: the recovery keeps the shard of every row it"
                        + " re-sends, so set outbox.shards back to " + shardsPresent);
            }
            st.execute("create unlogged table " + holding + " as " + saved);
            setAside(c, st);
            // parked rows: added after the requeue merge (the parked table's max id joins the greatest())
            try (var ps = c.prepareStatement("select setval(pg_get_serial_sequence(?, 'id'), greatest("
                    + "(select coalesce(max(id), 0) from " + names.base() + "),"
                    + " (select coalesce(max(id), 0) from " + names.retry() + "),"
                    + " (select coalesce(max(id), 0) from " + names.dead() + "), 1))")) {
                ps.setString(1, names.base());
                ps.execute();
            }
            for (var partition : partitionsOf(c)) {
                st.execute("drop table " + partition);
            }
            st.execute("delete from " + names.tick() + "; delete from " + names.cursor() + "; delete from "
                    + names.dropping() + "; delete from " + names.halt());
            makeRanges(c);
            long tick = insertTick(c);
            // parked rows: added after the requeue merge
            try (var ps = c.prepareStatement("insert into " + names.cursor() + " (shard, tick_id, retry_due_at)"
                    + " select s, ?, case when exists (select 1 from " + names.retry() + " r where r.shard = s)"
                    + " then now() end from generate_series(0, ? - 1) s")) {
                ps.setLong(1, tick);
                ps.setInt(2, shards);
                ps.executeUpdate();
            }
            sent = st.executeUpdate("insert into " + names.base() + " (shard, message_id, topic, key, payload, headers)"
                    + " select shard, message_id, topic, key, payload, headers from " + holding
                    + " order by created_at, id");
            st.execute("drop table " + holding);
            c.commit();
        } catch (Throwable e) { // an Error too: setAutoCommit(true) below would commit the work so far
            try {
                c.rollback();
            } catch (SQLException cleanup) { // a terminated session, its transaction gone with it
                e.addSuppressed(cleanup);
            }
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
        return sent;
    }

    /**
     * The tables outside the outbox's partition tree whose rows the parent does not show: every standalone range (but
     * one whose digits fit no long, no table of the library's) and a standalone tail is set aside. A detach begun
     * since the finish before the transaction rolls it back instead: the parent hides the rows of a pending one, which
     * a drop would lose, and a FINALIZE here would wait for older snapshots, writers queued behind the outbox lock
     * among them.
     */
    private void setAside(Connection c, Statement st) throws SQLException {
        var pending = PartitionDdl.pendingDetach(c, names.base());
        if (pending != null) {
            throw new IllegalStateException("the detach of " + pending + " became pending after the recovery of "
                    + names.base() + " finished the one before it: rolled back, the next attempt finishes it first");
        }
        int prefix = names.unqualified(names.partitionPrefix()).length();
        for (var table : PartitionDdl.standalone(c, names.base(), "_x[0-9]+")) {
            var digits = names.unqualified(table).substring(prefix);
            if (XidPartitions.xidOf(digits) != null) {
                setAside(st, table, digits);
            }
        }
        if (PartitionDdl.attachment(c, names.base(), names.tail()) == Attachment.STANDALONE) {
            setAside(st, names.tail(), null);
        }
    }

    /** Drops {@code table} when empty, else renames it {@code <base>_o<digits>}, by default its lowest xid. */
    private void setAside(Statement st, String table, @Nullable String digits) throws SQLException {
        st.execute("lock table " + table + " in access exclusive mode");
        String lowest;
        try (var rs = st.executeQuery("select min(xid)::text from (select xid from " + table + " limit "
                + (digits == null ? "all" : "1") + ") t")) {
            rs.next();
            lowest = rs.getString(1);
        }
        if (lowest == null) {
            st.execute("drop table " + table);
            return;
        }
        var name = names.unqualified(names.setAsidePrefix()) + (digits != null ? digits : lowest);
        st.execute("alter table " + table + " rename to " + name);
        log.warn("outbox xid-regression recovery of {} set {} aside as {}: it holds rows outside the outbox, which"
                + " are not re-sent by themselves; OutboxAdmin.restoreOrphan moves them back", names.base(), table,
                name);
    }

    private ArrayList<String> partitionsOf(Connection c) throws SQLException {
        var tables = new ArrayList<String>();
        try (var ps = c.prepareStatement(
                "select inhrelid::regclass::text from pg_inherits where inhparent = to_regclass(?)")) {
            ps.setString(1, names.base());
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
        }
        return tables;
    }

    /**
     * From the range holding the oldest running xid, as a writer waiting for the outbox lock may have taken an xid
     * below the current one's range, one range until the current one's, or the oldest's own when they are one, then
     * the premade ranges and the tail.
     */
    private void makeRanges(Connection c) throws SQLException {
        long xmin;
        long current;
        try (var st = c.createStatement(); var rs = st.executeQuery("select pg_snapshot_xmin(s)::text::bigint,"
                + " pg_snapshot_xmax(s)::text::bigint from (select pg_current_snapshot() s) q")) {
            rs.next();
            xmin = rs.getLong(1);
            current = rs.getLong(2);
        }
        if (betweenReads != null) {
            betweenReads.accept(c);
        }
        long from = Math.floorDiv(xmin, size) * size;
        // never none: ensurePremade reads the current xid again and, without a range, starts at that one's range, so
        // a counter crossing a boundary in between would leave the range of xids taken meanwhile uncreated
        long to = Math.max(Math.floorDiv(current, size) * size, from + size);
        PartitionDdl.createAndAttach(c, names.base(), names.partitionPrefix() + from, Long.toString(from),
                Long.toString(to), PartitionDdl.CREATE_LOCK_TIMEOUT);
        partitions.ensurePremade(c, LOCK_TIMEOUT, LOCK_TIMEOUT); // the tail is missing: no swap, nothing that commits
    }

    /**
     * The tick, its snapshot taken by this statement: {@code pg_current_snapshot()} leaves this transaction's own xid
     * out of the in-progress list, so a tick made here would take the rows re-sent here for relayed whenever a later
     * xid has ended. The xid is listed in its snapshot instead: the rows are invisible to it, and visible to the first
     * tick after the commit. No {@code dirty} (null): the relay steps every shard behind it.
     */
    private long insertTick(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("insert into " + names.tick() + " (snapshot,"
                + " max_id) select case when pg_snapshot_xmax(s) <= x then s else (least(pg_snapshot_xmin(s), x)"
                + " || ':' || pg_snapshot_xmax(s) || ':' || array_to_string(array(select i from (select"
                + " pg_snapshot_xip(s) i union all select x) u order by i), ','))::pg_snapshot end, "
                + Ticks.maxIdQuery(c, names) + " from (select pg_current_snapshot() s, pg_current_xact_id() x) q"
                + " returning id")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long queryLong(Statement st, String sql) throws SQLException {
        try (var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
