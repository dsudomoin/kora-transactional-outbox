package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.internal.InboxPartitions.Window;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.application.graph.Lifecycle;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlRunnable;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * Postgres inbox: {@code <inbox>(message_id uuid, consumer text default '', received_at, primary key
 * (message_id, consumer))} range-partitioned by message id ({@link InboxPartitions}); the primary key holds the
 * partition key, so uniqueness holds across partitions. {@code consumer} is the {@link #scope}, {@code ''} for the
 * unscoped methods. {@link #tryAccept} (and {@link #tryAcceptAll}, one statement for many ids) inserts with
 * {@code ON CONFLICT DO NOTHING} only for ids inside the dedup window, which this instance takes from the partitions
 * that exist ({@link Window}, refreshed at {@link #init()} and after every maintenance run) and from the clock
 * ({@link InboxPartitions#safeFrom}), so an insert never misses a partition.
 *
 * <p>{@link #init()} fails when its connection does not keep one server session (a transaction-pooling proxy,
 * {@link PartitionDdl#backendChanges}), then creates the table and its ranges under
 * {@code pg_advisory_lock(internalNs, LOCK_INBOX_MAINTENANCE)} with a 5 s {@code lock_timeout} and a 65 s
 * {@code statement_timeout}, {@code internalNs} from the table the name resolves to ({@link TableNames#resolve}), and
 * fails when an existing table has no {@code consumer} column (made by an earlier version, with dedup across
 * consumers). It then runs {@link #runMaintenance()} every {@code outbox.partitions.maintenanceInterval} on the daemon
 * thread {@code outbox-inbox-maintenance} until {@link #release()}.
 */
public final class PgInbox implements Inbox, Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(PgInbox.class);
    private static final String NOT_IN_TX =
            "Inbox.tryAccept and tryAcceptAll must be called inside JdbcConnectionFactory.inTx(...)";
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(5);
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    private static final String UNSCOPED = "";
    private static final Pattern CONSUMER = Pattern.compile("[A-Za-z0-9_.:-]{1,64}");

    private final JdbcConnectionFactory db;
    private final OutboxConfig config;
    private final OutboxTelemetry telemetry;
    private final TableNames names;
    private final InboxPartitions partitions;
    private final String insert;
    private final String insertAll;
    private final AtomicLong lastWarn = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    private final AtomicLong poolingLogged = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    private volatile double transactionPooling = Double.NaN;
    private volatile double orphaned = Double.NaN;
    private volatile Window window = Window.EMPTY;
    @Nullable
    private volatile ScheduledExecutorService scheduler;

    public PgInbox(JdbcConnectionFactory db, OutboxConfig config, OutboxTelemetry telemetry) {
        this.db = db;
        this.config = config;
        this.telemetry = telemetry;
        this.names = TableNames.of(config.inbox().table());
        this.partitions = new InboxPartitions(names.base(), config);
        this.insert = "insert into " + names.base() + " (message_id, consumer) values (?, ?)"
                + " on conflict (message_id, consumer) do nothing";
        // rows are inserted, and so take their keys, in the array's order by SQL semantics, not by plan shape
        this.insertAll = "insert into " + names.base() + " (message_id, consumer) select u.id, ?"
                + " from unnest(?::uuid[]) with ordinality u(id, ord) order by u.ord"
                + " on conflict (message_id, consumer) do nothing returning message_id";
        telemetry.gauge(OutboxGauge.INBOX_MAINTENANCE_TRANSACTION_POOLING, () -> transactionPooling);
        telemetry.gauge(OutboxGauge.INBOX_PARTITIONS_ORPHANED, () -> orphaned);
    }

    @Override
    public boolean tryAccept(UUID messageId) {
        return accept(currentConnection(), messageId, UNSCOPED);
    }

    @Override
    public boolean tryAccept(@Nullable Connection c, UUID messageId) {
        return accept(c, messageId, UNSCOPED);
    }

    @Override
    public Set<UUID> tryAcceptAll(Collection<UUID> messageIds) {
        return acceptAll(currentConnection(), messageIds, UNSCOPED);
    }

    @Override
    public Set<UUID> tryAcceptAll(@Nullable Connection c, Collection<UUID> messageIds) {
        return acceptAll(c, messageIds, UNSCOPED);
    }

    @Override
    public Inbox scope(String consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (!CONSUMER.matcher(consumer).matches()) {
            throw new IllegalArgumentException(
                    "inbox consumer must be 1 to 64 characters of A-Z a-z 0-9 _ . : -, got [" + consumer + "]");
        }
        return new Scope(consumer);
    }

    private boolean accept(@Nullable Connection c, UUID messageId, String consumer) {
        Objects.requireNonNull(messageId, "messageId");
        try {
            if (c == null || c.getAutoCommit()) {
                throw new IllegalStateException(NOT_IN_TX);
            }
            if (!dedups(messageId)) {
                acceptedOutside(messageId);
                return true;
            }
            try (var ps = c.prepareStatement(insert)) {
                ps.setObject(1, messageId);
                ps.setString(2, consumer);
                return ps.executeUpdate() == 1;
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    /**
     * {@link #accept} per distinct id, with one insert for the in-window ones (none when there are none), in a fixed
     * order, so transactions accepting overlapping ids wait for each other instead of deadlocking.
     */
    private Set<UUID> acceptAll(@Nullable Connection c, Collection<UUID> messageIds, String consumer) {
        var distinct = new TreeSet<UUID>(); // the fixed order
        for (var id : messageIds) {
            distinct.add(Objects.requireNonNull(id, "messageId"));
        }
        try {
            if (c == null || c.getAutoCommit()) {
                throw new IllegalStateException(NOT_IN_TX);
            }
            var accepted = new HashSet<UUID>();
            var dedup = new ArrayList<UUID>(distinct.size());
            for (var id : distinct) {
                if (dedups(id)) {
                    dedup.add(id);
                } else {
                    acceptedOutside(id);
                    accepted.add(id);
                }
            }
            if (dedup.isEmpty()) {
                return accepted;
            }
            try (var ps = c.prepareStatement(insertAll)) {
                ps.setString(1, consumer);
                ps.setArray(2, c.createArrayOf("uuid", dedup.toArray()));
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        accepted.add(rs.getObject(1, UUID.class));
                    }
                }
            }
            return accepted;
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    @Override
    public void init() throws SQLException {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            PartitionDdl.requireSessionStable(c, "outbox inbox bootstrap of " + names.base());
            execute(c, "set lock_timeout = " + LOCK_TIMEOUT.toMillis());
            // a shorter role or database default would end the lock waits with 57014 instead of 55P03
            execute(c, "set statement_timeout = " + LOCK_TIMEOUT.plus(PartitionDdl.DDL_MARGIN).toMillis());
            try {
                bootstrap(c);
            } finally {
                execute(c, "reset lock_timeout");
                execute(c, "reset statement_timeout");
            }
            if (!OutboxSchema.columnExists(c, names.base(), "consumer")) {
                throw new IllegalStateException("outbox inbox table " + names.base() + " has no consumer column: an"
                        + " earlier version made it, and its dedup spans every consumer of a message; drop the table"
                        + " (the ids it recorded go with it) and restart to have it made with the primary key"
                        + " (message_id, consumer)");
            }
            window = partitions.window(c);
        }
        var w = window;
        log.info("outbox inbox {} ready: ids from {} to {} deduplicated", names.base(),
                Instant.ofEpochMilli(dedupFrom(w)), Instant.ofEpochMilli(w.to()));
        long every = config.partitions().maintenanceInterval().toNanos();
        var s = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "outbox-inbox-maintenance");
            t.setDaemon(true);
            return t;
        });
        // no run at once: init made the ranges
        s.scheduleWithFixedDelay(this::scheduledMaintenance, every, every, TimeUnit.NANOSECONDS);
        scheduler = s;
    }

    /** Stops the maintenance schedule; a run in progress gets {@code outbox.relay.shutdownTimeout} to finish. */
    @Override
    public void release() {
        var s = scheduler;
        if (s == null) {
            return;
        }
        scheduler = null;
        s.shutdown();
        try {
            if (!s.awaitTermination(config.relay().shutdownTimeout().toNanos(), TimeUnit.NANOSECONDS)) {
                s.shutdownNow();
            }
        } catch (InterruptedException e) {
            s.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * One run. On the instance that gets {@code pg_try_advisory_lock(internalNs, LOCK_INBOX_MAINTENANCE)} (on one
     * autocommit connection with {@code idle_session_timeout = 60s}, so a frozen holder loses it): finish a pending
     * detach, drop expired ranges, make ranges up to {@code now + premake} (attaching a table of the next range that
     * exists but is not attached), drop expired ranges a crash left detached but not dropped and, with
     * {@code outbox.inbox.attachOrphans}, attach back the ones still inside the window (the gauge
     * {@code outbox.inbox.partitions.orphaned} counts the ones kept, with an ERROR), each step on its own (a failure is
     * logged and the next step still runs). That part is skipped when the connection does not keep one server session
     * ({@link PartitionDdl#backendChanges}), with an ERROR at most once a minute and the gauge
     * {@code outbox.inbox.maintenance.transaction_pooling} at 1 (0 otherwise). Then, on every instance, the window
     * refresh.
     */
    public void runMaintenance() {
        try (var c = db.newConnection()) {
            c.setAutoCommit(true);
            if (sessionStable(c)) {
                maintain(c);
            }
            step("window refresh", () -> window = partitions.window(c));
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    private void bootstrap(Connection c) throws SQLException {
        int ns = names.resolve(c).internalNamespace();
        try {
            advisory(c, "pg_advisory_lock", ns); // waits for another instance's bootstrap or maintenance run
            try {
                execute(c, "create table if not exists " + names.base() + " (message_id uuid not null,"
                        + " consumer text not null default '', received_at timestamptz not null default now(),"
                        + " primary key (message_id, consumer)) partition by range (message_id)");
                partitions.ensurePremade(c, System.currentTimeMillis());
            } finally {
                advisory(c, "pg_advisory_unlock", ns);
            }
        } catch (SQLException e) {
            // a maintenance run elsewhere holds the lock (a detach waiting for consumer transactions) and extends too
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())
                    || !partitions.window(c).contains(System.currentTimeMillis())) {
                throw e;
            }
            log.warn("outbox inbox {} partitions not extended at startup (lock timeout); maintenance does it: {}",
                    names.base(), e.getMessage());
        }
    }

    private void maintain(Connection c) throws SQLException {
        int ns = names.resolve(c).internalNamespace();
        // before the lock, so a holder frozen right after taking it loses it too
        execute(c, "set idle_session_timeout = '60s'");
        boolean locked = false;
        Throwable failure = null;
        try {
            locked = advisory(c, "pg_try_advisory_lock", ns);
            if (locked) {
                long now = System.currentTimeMillis();
                var timeout = PartitionDdl.DETACH_LOCK_TIMEOUT;
                step("finish pending detach", () -> partitions.finishPendingDetach(c, now, timeout));
                step("partition drop", () -> partitions.dropExpired(c, now, timeout));
                // before the orphan check: a table of the next range whose attach was cut off is attached, not reported
                step("partition premake", () -> partitions.ensurePremade(c, now));
                orphaned = Double.NaN; // unknown unless the step completes
                step("orphan drop", () -> {
                    int kept = 0;
                    for (var orphan : partitions.dropOrphans(c, now, timeout)) {
                        var advice = attachBack(c, orphan);
                        if (advice == null) {
                            continue;
                        }
                        kept++;
                        log.error("outbox inbox table {} is a range of {} that is not attached (detached by hand, or"
                                + " left by an interrupted run) and not yet past outbox.inbox.retention: ids in its"
                                + " range are not deduplicated, and where the inbox has ranges on both sides of it,"
                                + " inserts of those ids fail (SQLState 23514). It is kept (gauge"
                                + " outbox.inbox.partitions.orphaned); {}", orphan.table(), names.base(), advice);
                    }
                    orphaned = kept;
                });
            }
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            try {
                if (locked) {
                    advisory(c, "pg_advisory_unlock", ns);
                }
                execute(c, "reset idle_session_timeout");
            } catch (SQLException e) { // the session died, and its lock and setting with it: keep the cause on top
                if (failure == null) {
                    throw e;
                }
                failure.addSuppressed(e);
            }
        }
    }

    /**
     * Attaches {@code orphan} back when {@code outbox.inbox.attachOrphans} is on and it
     * {@link InboxPartitions#leavesNoGap}; null when attached, else the advice its ERROR ends with. The attach
     * statement is advised only for one that leaves no gap: run on another, it would make inserts fail.
     */
    @Nullable
    private String attachBack(Connection c, InboxPartitions.Orphan orphan) throws SQLException {
        if (!partitions.leavesNoGap(c, orphan)) {
            return "no attached range lies next to it, so do not attach it by hand either: the ranges between would be"
                    + " missing, and inserts of their ids would fail. One above the attached ranges is attached by"
                    + " premake once they reach it, one below them is dropped once past outbox.inbox.retention";
        }
        var attach = "attach it back with alter table " + names.base() + " attach partition " + orphan.table() + " "
                + PartitionDdl.bounds(orphan.from(), orphan.to());
        if (!config.inbox().attachOrphans()) {
            return attach;
        }
        try {
            partitions.attachBack(c, orphan);
        } catch (SQLException e) { // nothing changed, so the step goes on with the next orphan
            return attach + "; maintenance could not (SQLState " + e.getSQLState() + "): " + e.getMessage();
        }
        log.info("outbox inbox table {}, a range of {} detached inside the dedup window, is attached back",
                orphan.table(), names.base());
        return null;
    }

    private boolean sessionStable(Connection c) throws SQLException {
        var pids = PartitionDdl.backendChanges(c);
        transactionPooling = pids == null ? 0 : 1;
        long now = System.nanoTime();
        long last = poolingLogged.get();
        if (pids != null && now - last >= WARN_INTERVAL && poolingLogged.compareAndSet(last, now)) {
            log.error("outbox inbox maintenance of {} {}: pg_backend_pid() was {} in consecutive statements on one"
                    + " connection, so its session lock and settings would leak into other clients' sessions; runs are"
                    + " skipped (gauge outbox.inbox.maintenance.transaction_pooling) until the DataSource reaches"
                    + " Postgres directly or through a session-mode pool; this ERROR repeats at most once a minute",
                    names.base(), PartitionDdl.SESSION_POOLING_REQUIRED, pids);
        }
        return pids == null;
    }

    private void scheduledMaintenance() {
        try {
            runMaintenance();
        } catch (VirtualMachineError e) { // rethrown, the scheduler cancels all later runs without a word
            log.error("outbox inbox maintenance of {} stopped by a fatal error; no further runs until restart",
                    names.base(), e);
            throw e;
        } catch (Throwable t) {
            log.error("outbox inbox maintenance of {} failed; next run in {}", names.base(),
                    config.partitions().maintenanceInterval(), t);
        }
    }

    /** A failed step is logged (the next run retries it), so the remaining steps still run. */
    private void step(String name, SqlRunnable step) {
        try {
            step.run();
        } catch (SQLException e) {
            log.warn("outbox inbox maintenance step '{}' on {} failed (SQLState {}); the next run retries it: {}",
                    name, names.base(), e.getSQLState(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("outbox inbox maintenance step '{}' on {} failed; the next run retries it", name, names.base(),
                    e);
        }
    }

    private Connection currentConnection() {
        var ctx = db.currentConnectionContext();
        if (ctx == null || ctx.connection() == null) {
            throw new IllegalStateException(NOT_IN_TX);
        }
        return ctx.connection();
    }

    private boolean dedups(UUID id) {
        if (!UuidV7.isV7(id)) {
            return false;
        }
        long millis = UuidV7.timestampMillis(id);
        var w = window;
        return w.contains(millis) && millis >= dedupFrom(w);
    }

    /** The window's lower bound, raised to what is safe now: the cached window may predate a drop elsewhere. */
    private long dedupFrom(Window w) {
        return Math.max(w.from(), partitions.safeFrom(System.currentTimeMillis()));
    }

    private void acceptedOutside(UUID id) {
        telemetry.inboxOutsideWindow();
        long now = System.nanoTime();
        long last = lastWarn.get();
        if (now - last >= WARN_INTERVAL && lastWarn.compareAndSet(last, now)) {
            var w = window;
            var from = Instant.ofEpochMilli(dedupFrom(w));
            var reason = UuidV7.isV7(id)
                    ? "its time " + Instant.ofEpochMilli(UuidV7.timestampMillis(id)) + " lies outside the dedup"
                            + " window [" + from + ", " + Instant.ofEpochMilli(w.to()) + ")"
                    : "it is not a UUIDv7";
            log.warn("outbox inbox {}: message id {} accepted without dedup, {}; such ids are counted in"
                    + " outbox.inbox.outside_window, this WARN repeats at most once a minute", names.base(), id,
                    reason);
        }
    }

    /** {@link #scope}: this inbox, deduplicating within {@code consumer}. */
    private final class Scope implements Inbox {

        private final String consumer;

        Scope(String consumer) {
            this.consumer = consumer;
        }

        @Override
        public boolean tryAccept(UUID messageId) {
            return accept(currentConnection(), messageId, consumer);
        }

        @Override
        public boolean tryAccept(@Nullable Connection c, UUID messageId) {
            return accept(c, messageId, consumer);
        }

        @Override
        public Set<UUID> tryAcceptAll(Collection<UUID> messageIds) {
            return acceptAll(currentConnection(), messageIds, consumer);
        }

        @Override
        public Set<UUID> tryAcceptAll(@Nullable Connection c, Collection<UUID> messageIds) {
            return acceptAll(c, messageIds, consumer);
        }

        @Override
        public Inbox scope(String other) {
            return PgInbox.this.scope(other);
        }
    }

    private static boolean advisory(Connection c, String function, int ns) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, ns);
            ps.setInt(2, TableNames.LOCK_INBOX_MAINTENANCE);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return function.equals("pg_advisory_lock") || rs.getBoolean(1); // that one returns void
            }
        }
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }
}
