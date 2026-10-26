package io.github.dsudomoin.kora.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Range-partition DDL shared by the outbox (xid8 bounds) and the inbox (uuid bounds). Every method needs an autocommit
 * connection ({@code DETACH ... CONCURRENTLY} cannot run in a transaction block) and runs each DDL statement under its
 * own {@code lock_timeout} and a {@code statement_timeout} {@link #DDL_MARGIN} longer, so a shorter role or database
 * {@code statement_timeout} cannot end the lock wait first (57014). Both are given back afterwards on every path: the
 * session's own value, or a reset when it had none. Bound values are unquoted text ({@code "1000"}); a {@code null} upper
 * bound is {@code MAXVALUE}. Names are interpolated, so they and the values are checked against a strict charset.
 */
public final class PartitionDdl {

    public static final Duration CREATE_LOCK_TIMEOUT = Duration.ofSeconds(2);
    public static final Duration DETACH_LOCK_TIMEOUT = Duration.ofSeconds(30);
    /** A DDL statement's {@code statement_timeout} past its {@code lock_timeout}: room for its own work. */
    static final Duration DDL_MARGIN = Duration.ofSeconds(60);
    /** The two timeouts as this session set them, null where it did not (the role, database or server default). */
    private static final String SESSION_TIMEOUTS = "select max(setting) filter (where name = 'lock_timeout'),"
            + " max(setting) filter (where name = 'statement_timeout') from pg_settings"
            + " where name in ('lock_timeout', 'statement_timeout') and source = 'session'";
    /** What bootstrap and maintenance need of their connection, see {@link #backendChanges}. */
    static final String SESSION_POOLING_REQUIRED =
            "needs a direct or session-pooled connection (transaction-pooling proxy detected)";

    private static final Logger log = LoggerFactory.getLogger(PartitionDdl.class);
    private static final Pattern NAME = Pattern.compile("(?:[A-Za-z_][A-Za-z0-9_]*\\.)?[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern VALUE = Pattern.compile("[0-9A-Za-z:.+ -]+");
    private static final Pattern BOUNDS = Pattern.compile("FOR VALUES FROM \\((.+)\\) TO \\((.+)\\)");

    /** One partition; {@code name} is usable in DDL as is, {@code to == null} is {@code MAXVALUE}. */
    public record Bound(String name, String from, @Nullable String to) {}

    /** How a table relates to a partitioned parent. */
    enum Attachment {
        ATTACHED,
        /** {@code DETACH CONCURRENTLY} was interrupted: no new rows can be routed to it and its rows are invisible. */
        DETACH_PENDING,
        /** The table exists but is not a partition of the parent. */
        STANDALONE,
        MISSING
    }

    private PartitionDdl() {}

    public static void createAndAttach(
            Connection c, String parent, String partition, String from, @Nullable String to, Duration lockTimeout)
            throws SQLException {
        boolean existed = exists(c, partition);
        if (!existed) {
            execute(c, lockTimeout, "create table " + name(partition) + " (like " + name(parent)
                    + " including defaults)");
        }
        String bounds = bounds(from, to);
        execute(c, lockTimeout, "alter table " + name(parent) + " attach partition " + name(partition) + " " + bounds);
        log.info(existed ? "attached existing table {} to {} {}" : "created partition {} of {} {}",
                partition, parent, bounds);
    }

    public static void detachConcurrentlyAndDrop(Connection c, String parent, String partition, Duration lockTimeout)
            throws SQLException {
        detach(c, parent, partition, lockTimeout);
        drop(c, partition, lockTimeout);
    }

    /** Partitions of {@code parent} in no particular order, including ones whose detach is still pending. */
    public static List<Bound> listRanges(Connection c, String parent) throws SQLException {
        var bounds = new ArrayList<Bound>();
        try (var ps = c.prepareStatement("select c.oid::regclass::text, pg_get_expr(c.relpartbound, c.oid)"
                + " from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = to_regclass(?)")) {
            ps.setString(1, parent);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var m = BOUNDS.matcher(rs.getString(2));
                    if (!m.matches()) {
                        throw new IllegalStateException("not a range partition: " + rs.getString(1));
                    }
                    bounds.add(new Bound(rs.getString(1), value(m.group(1)), value(m.group(2))));
                }
            }
        }
        return bounds;
    }

    /** The partition of {@code parent} whose detach is pending, or null: Postgres allows at most one per parent. */
    @Nullable
    static String pendingDetach(Connection c, String parent) throws SQLException {
        try (var ps = c.prepareStatement("select inhrelid::regclass::text from pg_inherits"
                + " where inhparent = to_regclass(?) and inhdetachpending")) {
            ps.setString(1, parent);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    static Attachment attachment(Connection c, String parent, String table) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?) is not null, (select inhdetachpending from pg_inherits"
                + " where inhrelid = to_regclass(?) and inhparent = to_regclass(?))")) {
            ps.setString(1, table);
            ps.setString(2, table);
            ps.setString(3, parent);
            try (var rs = ps.executeQuery()) {
                rs.next();
                if (!rs.getBoolean(1)) {
                    return Attachment.MISSING;
                }
                boolean pending = rs.getBoolean(2);
                return rs.wasNull() ? Attachment.STANDALONE
                        : pending ? Attachment.DETACH_PENDING : Attachment.ATTACHED;
            }
        }
    }

    /**
     * {@code DETACH ... CONCURRENTLY}, or {@code FINALIZE} when an earlier detach was interrupted (lock timeout,
     * crash) and left the partition pending; no-op when the partition is not attached.
     */
    static void detach(Connection c, String parent, String partition, Duration lockTimeout) throws SQLException {
        var attachment = attachment(c, parent, partition);
        if (attachment == Attachment.ATTACHED || attachment == Attachment.DETACH_PENDING) {
            execute(c, lockTimeout, "alter table " + name(parent) + " detach partition " + name(partition)
                    + (attachment == Attachment.DETACH_PENDING ? " finalize" : " concurrently"));
            log.info("detached partition {} from {}", partition, parent);
        }
    }

    static void drop(Connection c, String table, Duration lockTimeout) throws SQLException {
        if (exists(c, table)) {
            execute(c, lockTimeout, "drop table " + name(table));
            log.info("dropped partition {}", table);
        }
    }

    /**
     * For a table that is no longer a partition: in one transaction under {@code ACCESS EXCLUSIVE}, drops it when
     * empty, otherwise attaches it back {@code FOR VALUES FROM (from) TO (to)}. The lock waits for a writer that was
     * routed to the table before its detach completed, so a row committed by that writer is seen, never dropped.
     * Returns true when the table was dropped.
     */
    static boolean dropIfEmptyElseAttach(
            Connection c, String parent, String table, String from, @Nullable String to, Duration lockTimeout)
            throws SQLException {
        boolean empty;
        c.setAutoCommit(false);
        try (var st = c.createStatement()) {
            st.execute("set local lock_timeout = " + lockTimeout.toMillis());
            st.execute("set local statement_timeout = " + lockTimeout.plus(DDL_MARGIN).toMillis());
            st.execute("lock table " + name(table) + " in access exclusive mode");
            try (var rs = st.executeQuery("select 1 from " + name(table) + " limit 1")) {
                empty = !rs.next();
            }
            st.execute(empty
                    ? "drop table " + name(table)
                    : "alter table " + name(parent) + " attach partition " + name(table) + " " + bounds(from, to));
            c.commit();
        } catch (SQLException | RuntimeException e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
        if (empty) {
            log.info("dropped partition {}", table);
        } else {
            log.info("attached {} back to {} {}: it holds rows", table, parent, bounds(from, to));
        }
        return empty;
    }

    /**
     * Tables in {@code parent}'s schema named {@code <parent><suffix>} ({@code suffix} a regex) that are no partition:
     * what a run leaves when it dies, or its DROP fails, after its detach went through. Usable in DDL as is.
     */
    static List<String> standalone(Connection c, String parent, String suffix) throws SQLException {
        var tables = new ArrayList<String>();
        try (var ps = c.prepareStatement("select c.oid::regclass::text from pg_class c where c.relkind = 'r'"
                + " and c.relnamespace = (select p.relnamespace from pg_class p where p.oid = to_regclass(?))"
                + " and c.relname ~ ? and not exists (select 1 from pg_inherits i where i.inhrelid = c.oid)"
                + " order by c.relname")) {
            ps.setString(1, parent);
            ps.setString(2, "^" + parent.substring(parent.indexOf('.') + 1) + suffix + "$");
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
        }
        return tables;
    }

    /**
     * Drops {@code table} unless {@code find} ({@code {table}} for the table, one bind parameter: {@code bound}, none
     * when null) returns a row; the lock and statement timeouts of every other DDL statement. {@code find} runs first
     * under {@code ACCESS SHARE}, so a table that stays (someone may be reading it) is never locked exclusively; only
     * a table to drop is, and {@code find} runs again under that lock in the same transaction, so nothing written
     * between the two is lost: the transaction is read committed whatever the session default, so that second read
     * takes a new snapshot. Taking the exclusive lock over the shared one can deadlock with someone who reads the
     * table and then alters or drops it in one transaction; Postgres then aborts one side (40P01), and a failed
     * maintenance step is retried next run. Returns true when dropped.
     */
    static boolean dropUnlessFound(
            Connection c, String table, String find, @Nullable String bound, Duration lockTimeout)
            throws SQLException {
        boolean found;
        c.setAutoCommit(false);
        try (var st = c.createStatement()) {
            // first: a repeatable-read default would fix the snapshot at the first read, and the second would miss rows
            st.execute("set transaction isolation level read committed");
            st.execute("set local lock_timeout = " + lockTimeout.toMillis());
            st.execute("set local statement_timeout = " + lockTimeout.plus(DDL_MARGIN).toMillis());
            found = finds(c, table, find, bound);
            if (!found) {
                st.execute("lock table " + name(table) + " in access exclusive mode");
                found = finds(c, table, find, bound);
            }
            if (!found) {
                st.execute("drop table " + name(table));
            }
            c.commit();
        } catch (SQLException | RuntimeException e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
        if (!found) {
            log.info("dropped partition {}, detached earlier but never dropped", table);
        }
        return !found;
    }

    private static boolean finds(Connection c, String table, String find, @Nullable String bound) throws SQLException {
        try (var ps = c.prepareStatement(find.replace("{table}", name(table)))) {
            if (bound != null) {
                ps.setString(1, bound);
            }
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static boolean exists(Connection c, String table) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?) is not null")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    static boolean hasRows(Connection c, String table) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select 1 from " + name(table) + " limit 1")) {
            return rs.next();
        }
    }

    /**
     * Null when three consecutive statements on the autocommit connection {@code c} ran in one server session, else
     * their {@code pg_backend_pid()}s. Bootstrap and maintenance need one session: they hold session advisory locks
     * and set session settings around autocommit DDL. Behind a transaction-pooling proxy (pgbouncer, Odyssey) each
     * statement may run in another server session, where such a lock or setting outlives the statement and reaches
     * other clients. A proxy that happens to keep one server session for these three statements is not detected.
     */
    @Nullable
    static String backendChanges(Connection c) throws SQLException {
        var pids = new int[3];
        try (var st = c.createStatement()) {
            for (int i = 0; i < pids.length; i++) {
                try (var rs = st.executeQuery("select pg_backend_pid()")) {
                    rs.next();
                    pids[i] = rs.getInt(1);
                }
            }
        }
        return pids[0] == pids[1] && pids[1] == pids[2] ? null : Arrays.toString(pids);
    }

    /** Fails {@code what} with {@link IllegalStateException} when {@link #backendChanges} finds several sessions. */
    static void requireSessionStable(Connection c, String what) throws SQLException {
        var pids = backendChanges(c);
        if (pids != null) {
            throw new IllegalStateException(what + " " + SESSION_POOLING_REQUIRED + ": pg_backend_pid() was " + pids
                    + " in consecutive statements on one connection, so its session lock and settings would leak into"
                    + " other clients' sessions; point the DataSource at Postgres directly or at a session-mode pool");
        }
    }

    private static void execute(Connection c, Duration lockTimeout, String sql) throws SQLException {
        String lock;
        String statement;
        try (var st = c.createStatement(); var rs = st.executeQuery(SESSION_TIMEOUTS)) {
            rs.next();
            lock = rs.getString(1);
            statement = rs.getString(2);
        }
        try (var st = c.createStatement()) {
            st.execute("set lock_timeout = " + lockTimeout.toMillis());
            st.execute("set statement_timeout = " + lockTimeout.plus(DDL_MARGIN).toMillis());
            st.execute(sql);
        } catch (SQLException | RuntimeException e) {
            try {
                restore(c, "lock_timeout", lock);
                restore(c, "statement_timeout", statement);
            } catch (SQLException cleanup) { // a terminated session: keep its cause on top
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        restore(c, "lock_timeout", lock);
        restore(c, "statement_timeout", statement);
    }

    private static void restore(Connection c, String setting, @Nullable String sessionValue) throws SQLException {
        if (sessionValue == null) {
            try (var st = c.createStatement()) {
                st.execute("reset " + setting);
            }
            return;
        }
        try (var ps = c.prepareStatement("select set_config(?, ?, false)")) {
            ps.setString(1, setting);
            ps.setString(2, sessionValue);
            ps.execute();
        }
    }

    private static String bounds(String from, @Nullable String to) {
        return "for values from (" + literal(from) + ") to (" + (to == null ? "maxvalue" : literal(to)) + ")";
    }

    /** {@code 'text'} → {@code text}; {@code MINVALUE}/{@code MAXVALUE} → null. */
    @Nullable
    private static String value(String expr) {
        return expr.startsWith("'") ? expr.substring(1, expr.length() - 1) : null;
    }

    private static String literal(String value) {
        if (!VALUE.matcher(value).matches()) {
            throw new IllegalArgumentException("unsafe partition bound: " + value);
        }
        return "'" + value + "'";
    }

    private static String name(String name) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("unsafe table name: " + name);
        }
        return name;
    }
}
