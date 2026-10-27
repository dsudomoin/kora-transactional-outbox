package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.DeadLetterFilter;
import io.github.dsudomoin.kora.outbox.DeadLetterPage;
import io.github.dsudomoin.kora.outbox.OutboxAdmin;
import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.OutboxStatus;
import io.github.dsudomoin.kora.outbox.OutboxStatus.DeadLetterCount;
import io.github.dsudomoin.kora.outbox.OutboxStatus.OrphanKind;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Attachment;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlConsumer;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction1;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * {@link OutboxAdmin} on Postgres. A bulk call acts on the letters dead when it began: it first reads the highest id
 * and the time, then claims chunks of {@link #CHUNK} letters matching it with an id up to that one and a
 * {@code failed_at} up to that time (a redriven letter dying again comes back with a new, higher id; an older message
 * dying meanwhile, with a later {@code failed_at}), in id order, each above the last chunk's,
 * {@code for update skip locked}, in a read-committed transaction of its own. A redrive chunk then tries their key
 * locks without waiting ({@link DeadLetters#tryKeyLocks}) and redrives the ready ones as the dead-letter pass does
 * ({@link DeadLetters#redrive}), so no writer waits for one of its key locks. Filters are bound parameters, never
 * interpolated; user text is logged with control characters escaped ({@link #printable}). {@code restoreOrphan} and
 * {@code recoverXidRegression} hold the maintenance lock for their whole run, so maintenance's steps never run beside
 * them.
 */
public final class PgOutboxAdmin implements OutboxAdmin {

    private static final Logger log = LoggerFactory.getLogger(PgOutboxAdmin.class);
    /** Letters, so key locks, per transaction of a bulk call; also the largest page. */
    static final int CHUNK = 1000;
    private static final String LOCK_NOT_AVAILABLE = "55P03";

    private final JdbcConnectionFactory db;
    private final TableNames names;
    private final int shards;
    private final Maintenance maintenance;
    private final Runnable wakeRelay;
    private final DeadLetters rows;
    private final XidPartitions partitions;
    private final RetryStore retries;
    private final XidRecovery recovery;
    final OrphanRestore orphanRestore; // for a test's seam
    private final String dead;
    /** How long a call under the maintenance lock waits for a maintenance run in progress; shorter in tests. */
    Duration maintenanceWait = Duration.ofSeconds(60);
    /** For tests: runs after each bulk chunk's commit. */
    @Nullable
    SqlConsumer<Connection> afterChunk;

    /** A chunk's claim: the letters claimed, the last one's id, and how many it acted on. */
    private record Chunk(int claimed, long lastId, long done) {}

    /** One chunk in {@code tx}: up to {@link #CHUNK} letters matching {@code where} with an id above {@code after}. */
    private interface Step {
        Chunk run(Connection tx, Where where, long after) throws SQLException;
    }

    public PgOutboxAdmin(JdbcConnectionFactory db, TableNames names, OutboxConfig config, Maintenance maintenance,
            Runnable wakeRelay) {
        this.db = db;
        this.names = names;
        this.shards = config.shards();
        this.maintenance = maintenance;
        this.wakeRelay = wakeRelay;
        this.rows = new DeadLetters(names, config);
        this.partitions = new XidPartitions(names, config.partitions());
        this.retries = new RetryStore(names);
        this.recovery = new XidRecovery(names, config);
        this.orphanRestore = new OrphanRestore(
                names, new Resender(names, config.shards()), config.partitions().swapLockTimeout());
        this.dead = names.dead();
    }

    @Override
    public OutboxStatus status() {
        requireOwnTransactions();
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            return new OutboxStatus(
                    queryBoolean(c, "select exists (select 1 from " + names.halt() + ")"),
                    maintenance.lagSeconds(c),
                    retries.size(c),
                    parkedSize(c),
                    deadLetterCounts(c),
                    partitions.orphans(c).stream().map(o -> new OutboxStatus.Orphan(o.table(), o.setAside()
                            ? OrphanKind.SET_ASIDE : o.ours() ? OrphanKind.DETACHED_BY_LIBRARY : OrphanKind.FOREIGN))
                            .toList(),
                    PartitionDdl.attachment(c, names.base(), names.tail()) != Attachment.MISSING
                            && PartitionDdl.hasRows(c, names.tail()),
                    partitions.premadeAhead(c),
                    partitions.xidsLeft(c),
                    maintenance.secondsSinceSuccess(c),
                    shards);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    @Override
    public DeadLetterPage findDeadLetters(DeadLetterFilter filter, @Nullable Long afterId, int limit) {
        requireOwnTransactions();
        if (limit < 1 || limit > CHUNK) {
            throw new IllegalArgumentException("limit must be within 1.." + CHUNK + ": " + limit);
        }
        var where = Where.of(filter);
        try (var c = db.newConnection();
                var ps = c.prepareStatement("select " + DeadLetters.COLUMNS + " from " + dead + " where "
                        + where.sql() + " and id > ? order by id limit ?")) {
            c.setAutoCommit(true);
            int next = where.bind(ps);
            ps.setLong(next, afterId == null ? Long.MIN_VALUE : afterId);
            ps.setInt(next + 1, limit);
            var letters = new ArrayList<DeadLetter>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    letters.add(DeadLetters.read(rs).letter());
                }
            }
            return new DeadLetterPage(letters, letters.size() < limit ? null : letters.get(letters.size() - 1).id());
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    @Override
    public long countDeadLetters(DeadLetterFilter filter) {
        requireOwnTransactions();
        var where = Where.of(filter);
        try (var c = db.newConnection();
                var ps = c.prepareStatement("select count(*) from " + dead + " where " + where.sql())) {
            c.setAutoCommit(true);
            where.bind(ps);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    @Override
    public long redriveDeadLetters(Collection<Long> ids) {
        requireOwnTransactions();
        return bulk("redriveDeadLetters(" + ids + ")", Where.ids(ids), true, this::redrive);
    }

    @Override
    public long redriveDeadLetters(DeadLetterFilter filter) {
        requireOwnTransactions();
        return bulk("redriveDeadLetters(" + filter + ")", Where.of(nonEmpty(filter)), true, this::redrive);
    }

    @Override
    public long discardDeadLetters(Collection<Long> ids) {
        requireOwnTransactions();
        return bulk("discardDeadLetters(" + ids + ")", Where.ids(ids), false, this::discard);
    }

    @Override
    public long discardDeadLetters(DeadLetterFilter filter) {
        requireOwnTransactions();
        return bulk("discardDeadLetters(" + filter + ")", Where.of(nonEmpty(filter)), false, this::discard);
    }

    /**
     * Under the maintenance lock ({@link #underMaintenanceLock}), so the tail's lower bound, which tells an orphan from
     * a range the extension made ahead of its swap, holds still: {@code table} must be one {@link XidPartitions#orphans}
     * lists, then {@link OrphanRestore} validates it again and moves its rows.
     */
    @Override
    public long restoreOrphan(String table, long fromXid) {
        requireOwnTransactions();
        var call = printable("restoreOrphan(" + table + ", " + fromXid + ")");
        long moved;
        var restoring = new AtomicBoolean();
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            moved = underMaintenanceLock(c, call, () -> {
                var name = names.unqualified(table);
                if (partitions.orphans(c).stream().noneMatch(o -> names.unqualified(o.table()).equals(name))) {
                    throw new IllegalArgumentException(table + " is not an orphan of the outbox " + names.base()
                            + " (OutboxAdmin.status() lists them): a table " + names.partitionPrefix() + "<digits>"
                            + " below the partition tail's lower bound, or " + names.setAsidePrefix() + "<digits>, in"
                            + " its schema and in no partition tree");
                }
                restoring.set(true);
                return orphanRestore.restore(c, table, fromXid, PartitionDdl.DETACH_LOCK_TIMEOUT);
            });
        } catch (SQLException | RuntimeException e) {
            // refused, nothing changed: OrphanRestore validates before any SQL that names the table
            boolean refused = e instanceof IllegalArgumentException
                    || e instanceof IllegalStateException && !restoring.get();
            if (!refused) {
                log.warn("outbox admin {} on {} failed; rows moved so far stay moved, and calling again goes on: {}",
                        call, names.base(), printable(e.toString()));
            }
            if (restoring.get() && !(e instanceof IllegalArgumentException)) {
                wakeRelay.run(); // for the chunks it may have moved before it failed
            }
            throw e instanceof SQLException s ? new RuntimeSqlException(s) : (RuntimeException) e;
        }
        log.info("outbox admin {} on {} returned {}", call, names.base(), moved);
        if (moved > 0) {
            wakeRelay.run();
        }
        return moved;
    }

    /**
     * Under the maintenance lock ({@link #underMaintenanceLock}), the recovery maintenance runs
     * ({@link XidRecovery#recover}), whatever {@code outbox.recovery} says. Then this instance's halt is refreshed
     * ({@link Maintenance#checkHalt}), so the relay woken resumes at once; other instances resume at their next
     * maintenance run.
     */
    @Override
    public long recoverXidRegression() {
        requireOwnTransactions();
        long sent;
        var recovering = new AtomicBoolean();
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            sent = underMaintenanceLock(c, "recoverXidRegression()", () -> {
                var check = recovery.check(c);
                if (check.current() >= check.tickXmax() && check.recorded() == null) {
                    throw new IllegalStateException("no xid regression to recover: the xid counter of " + names.base()
                            + " (" + check.current() + ") is at or above every tick's xmax (" + check.tickXmax()
                            + ") and " + names.halt() + " holds no row");
                }
                recovering.set(true);
                return recovery.recover(c);
            });
        } catch (SQLException | RuntimeException e) {
            if (recovering.get()) {
                log.warn("outbox admin recoverXidRegression() on {} failed, its transaction rolled back; calling again"
                        + " retries: {}", names.base(), printable(e.toString()));
            }
            throw e instanceof SQLException s ? new RuntimeSqlException(s) : (RuntimeException) e;
        }
        log.info("outbox admin recoverXidRegression() on {} returned {}, the rows re-sent", names.base(), sent);
        maintenance.checkHalt();
        wakeRelay.run();
        return sent;
    }

    /**
     * {@code body} on {@code c} under the maintenance lock, waited for at most {@link #maintenanceWait}, so no
     * maintenance run goes on beside it. The session gets {@code idle_session_timeout = 60s} first, as
     * {@link Maintenance} does, so Postgres ends the session of a frozen holder and frees the lock; reset, and the lock
     * released, before the connection goes back to the pool.
     */
    private <T> T underMaintenanceLock(Connection c, String call, SqlFunction0<T> body) throws SQLException {
        PartitionDdl.requireSessionStable(c, "outbox admin " + call + " on " + names.base());
        // before the lock, so a holder frozen right after taking it loses it too
        execute(c, "set idle_session_timeout = '60s'");
        boolean locked = false;
        Throwable failure = null;
        try {
            lockMaintenance(c);
            locked = true;
            return body.apply();
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            try {
                if (locked) {
                    advisory(c, "pg_advisory_unlock");
                }
                execute(c, "reset idle_session_timeout");
            } catch (SQLException e) { // the session died, and its lock and setting with it: keep the cause
                if (failure == null) {
                    throw e;
                }
                failure.addSuppressed(e);
            }
        }
    }

    /**
     * Chunk after chunk until one claims fewer than {@link #CHUNK}, among the letters dead when the call began; one
     * INFO line, or a WARN on a failure. {@code putsBack}: the relay is woken when it put rows back into the outbox,
     * also when it failed after some chunks did.
     */
    private long bulk(String call, Where filter, boolean putsBack, Step step) {
        call = printable(call);
        long done = 0;
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            Where where;
            try (var st = c.createStatement(); var rs = st.executeQuery("select max(id), now() from " + dead)) {
                rs.next();
                long last = rs.getLong(1);
                where = filter.and("id <= ?", rs.wasNull() ? Long.MIN_VALUE : last)
                        .and("failed_at <= ?", rs.getObject(2, OffsetDateTime.class));
            }
            long after = Long.MIN_VALUE;
            while (true) {
                long above = after;
                var chunk = inTx(c, tx -> step.run(tx, where, above));
                done += chunk.done();
                if (afterChunk != null) {
                    afterChunk.accept(c);
                }
                if (chunk.claimed() < CHUNK) {
                    break;
                }
                after = chunk.lastId();
            }
        } catch (SQLException | RuntimeException e) {
            log.warn("outbox admin {} on {} failed after {} letters, which stay done; calling again goes on: {}",
                    call, dead, done, printable(e.toString()));
            if (putsBack && done > 0) {
                wakeRelay.run();
            }
            throw e instanceof SQLException s ? new RuntimeSqlException(s) : (RuntimeException) e;
        }
        log.info("outbox admin {} on {} returned {}", call, dead, done);
        if (putsBack && done > 0) {
            wakeRelay.run();
        }
        return done;
    }

    /** Redrives the claimed letters but those whose key another transaction holds, skipped without waiting. */
    private Chunk redrive(Connection tx, Where where, long after) throws SQLException {
        var claimed = new ArrayList<DeadLetters.Claimed>();
        try (var ps = tx.prepareStatement("select " + DeadLetters.COLUMNS + " from " + dead + " where " + where.sql()
                + " and id > ? order by id limit " + CHUNK + " for update skip locked")) {
            ps.setLong(where.bind(ps), after);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    claimed.add(DeadLetters.read(rs));
                }
            }
        }
        var ready = rows.tryKeyLocks(tx, claimed).ready();
        rows.redrive(tx, ready);
        return new Chunk(claimed.size(),
                claimed.isEmpty() ? after : claimed.get(claimed.size() - 1).letter().id(), ready.size());
    }

    private Chunk discard(Connection tx, Where where, long after) throws SQLException {
        var ids = new ArrayList<Long>();
        try (var ps = tx.prepareStatement("delete from " + dead + " where id in (select id from " + dead + " where "
                + where.sql() + " and id > ? order by id limit " + CHUNK + " for update skip locked) returning id")) {
            ps.setLong(where.bind(ps), after);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
        }
        return new Chunk(ids.size(), ids.stream().mapToLong(Long::longValue).max().orElse(after), ids.size());
    }

    /** {@code body} in one read-committed transaction on {@code c}, committed, or rolled back on any failure. */
    private static <T> T inTx(Connection c, SqlFunction1<Connection, T> body) throws SQLException {
        ShardProcessor.prepareTransaction(c, Duration.ZERO); // never idle: the 30 s idle timeout is ample
        try {
            T result = body.apply(c);
            c.commit();
            c.setAutoCommit(true);
            return result;
        } catch (Throwable t) {
            try {
                c.rollback();
            } catch (SQLException | RuntimeException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    /**
     * {@code text} with control characters (CR, LF, tab, any other C0 or C1 one) and the Unicode line and paragraph
     * separators escaped, so text a caller supplied cannot forge log lines.
     */
    static String printable(String text) {
        var out = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '\n') {
                out.append("\\n");
            } else if (ch == '\r') {
                out.append("\\r");
            } else if (ch == '\t') {
                out.append("\\t");
            } else if (Character.isISOControl(ch) || ch == '\u2028' || ch == '\u2029') {
                out.append(String.format("\\u%04x", (int) ch));
            } else {
                out.append(ch);
            }
        }
        return out.toString();
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    /** Rows in {@code <base>_parked}, which a later version makes: 0 while it does not exist. */
    private long parkedSize(Connection c) throws SQLException {
        var parked = names.base() + "_parked";
        try (var ps = c.prepareStatement("select to_regclass(?) is null")) {
            ps.setString(1, parked);
            try (var rs = ps.executeQuery()) {
                rs.next();
                if (rs.getBoolean(1)) {
                    return 0;
                }
            }
        }
        try (var st = c.createStatement(); var rs = st.executeQuery("select count(*) from " + parked)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private List<DeadLetterCount> deadLetterCounts(Connection c) throws SQLException {
        var counts = new ArrayList<DeadLetterCount>();
        try (var st = c.createStatement(); var rs = st.executeQuery(
                "select topic, status, count(*) from " + dead + " group by topic, status order by topic, status")) {
            while (rs.next()) {
                counts.add(new DeadLetterCount(rs.getString(1),
                        DeadLetter.Status.valueOf(rs.getString(2).toUpperCase(Locale.ROOT)), rs.getLong(3)));
            }
        }
        return counts;
    }

    /** {@code pg_advisory_lock(internalNs, LOCK_MAINTENANCE)} within {@link #maintenanceWait}. */
    private void lockMaintenance(Connection c) throws SQLException {
        try {
            PartitionDdl.execute(c, maintenanceWait, "select pg_advisory_lock(" + names.internalNamespace() + ", "
                    + TableNames.LOCK_MAINTENANCE + ")");
        } catch (SQLException e) {
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                throw e;
            }
            throw new IllegalStateException("maintenance is running, retry: outbox maintenance of " + names.base()
                    + " held its lock pg_advisory_lock(" + names.internalNamespace() + ", "
                    + TableNames.LOCK_MAINTENANCE + ") for longer than " + maintenanceWait.toMillis() / 1e3 + " s", e);
        }
    }

    private void advisory(Connection c, String function) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_MAINTENANCE);
            ps.execute();
        }
    }

    private void requireOwnTransactions() {
        if (db.currentConnectionContext() != null) {
            throw new IllegalStateException("OutboxAdmin manages its own transactions: call it outside"
                    + " JdbcConnectionFactory.inTx(...)");
        }
    }

    private static DeadLetterFilter nonEmpty(DeadLetterFilter filter) {
        if (filter.isEmpty()) {
            throw new IllegalArgumentException("an empty DeadLetterFilter matches every dead letter: name what to"
                    + " act on (topic, key, errorContains, failedFrom, failedTo or status), or pass the ids");
        }
        return filter;
    }

    private static boolean queryBoolean(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    /** A condition on the dead table, each value a bound parameter, never interpolated. */
    private record Where(String sql, List<Object> params) {

        static Where of(DeadLetterFilter f) {
            var sql = new ArrayList<String>();
            var params = new ArrayList<Object>();
            add(sql, params, "topic = ?", f.topic());
            add(sql, params, "key = ?", f.key());
            add(sql, params, "position(? in error) > 0", f.errorContains()); // a substring: no pattern, no escapes
            add(sql, params, "failed_at >= ?", f.failedFrom() == null ? null : f.failedFrom().atOffset(ZoneOffset.UTC));
            add(sql, params, "failed_at < ?", f.failedTo() == null ? null : f.failedTo().atOffset(ZoneOffset.UTC));
            add(sql, params, "status = ?", f.status() == null ? null : f.status().name().toLowerCase(Locale.ROOT));
            return new Where(sql.isEmpty() ? "true" : String.join(" and ", sql), params);
        }

        Where and(String condition, Object value) {
            var all = new ArrayList<>(params);
            all.add(value);
            return new Where(sql + " and " + condition, all);
        }

        static Where ids(Collection<Long> ids) {
            // every chunk binds all the ids: quadratic for huge lists, fine for a page of them (a filter serves bulk)
            return new Where("id = any(?)", List.of(ids.stream().mapToLong(Long::longValue).toArray()));
        }

        private static void add(List<String> sql, List<Object> params, String condition, @Nullable Object value) {
            if (value != null) {
                sql.add(condition);
                params.add(value);
            }
        }

        /** Binds the parameters from index 1; returns the next index. */
        int bind(PreparedStatement ps) throws SQLException {
            int i = 1;
            for (var param : params) {
                ps.setObject(i++, param);
            }
            return i;
        }
    }
}
