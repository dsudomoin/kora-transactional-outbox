package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.application.graph.Lifecycle;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * Idempotent startup bootstrap, on a connection that must keep one server session (no transaction-pooling
 * proxy, {@link PartitionDdl#backendChanges}; checked first, startup fails otherwise): tables, the hot-table index,
 * premade xid partitions with the tail, one initial tick and one cursor per shard, all under
 * {@code pg_advisory_lock(internalNs, LOCK_SCHEMA)}; like every lock namespace, {@code internalNs} comes from the table
 * the configured name resolves to ({@link TableNames#resolve}), which {@link #names()} hands to the other components.
 * Fails fast with {@link IllegalStateException} when {@code shards} differs from the shards in the cursor table and
 * when an identity sequence of the outbox, tick or cursor table has {@code CACHE > 1}. An xid regression (the counter
 * below a tick's xmax) never fails it: it is recorded in {@code <base>_halt} and, with
 * {@code outbox.recovery.xidRegression = auto} and at most {@code outbox.recovery.maxRows} rows to re-send, recovered
 * before any relay starts ({@link XidRecovery}, under {@code LOCK_MAINTENANCE}, taken after {@code LOCK_SCHEMA});
 * otherwise, or while another session holds that lock, startup goes on halted (the relay delivers nothing, writes go
 * on) with an ERROR naming the admin call that runs the recovery. The recovery runs under its own timeouts (a
 * 10 min {@code statement_timeout}), and other instances' startups wait for it behind the schema lock, at most a
 * minute. The schema lock and every bootstrap DDL statement wait at
 * most 5 s for a lock ({@code lock_timeout}), and every bootstrap statement runs under a 1 min
 * {@code statement_timeout} (replacing a shorter role default): one that needs longer, such as an index built anew on a
 * populated table, fails startup with SQLState 57014 and is left to run by hand. The schema lock is retried for a
 * minute, then startup fails. On existing tables the bootstrap takes no lock that writers wait behind (index creation
 * is skipped when the index exists, partition extension is optional and left to maintenance when it times out). A tick
 * table made before {@code max_id} or {@code dirty}, a cursor table made before {@code parked_due_at}, a retry table
 * made before {@code requeues} and a dead table made before the dead-letter hook's columns get each column once, and a
 * tick table's {@code shards} (the shard sets {@code dirty} replaces) is dropped once; that ALTER waits for running
 * relay transactions and fails startup on timeout. Ticks without {@code dirty} (null) count as holding rows of every
 * shard; dead letters older than {@code status} are kept and the hook sees new ones only: one ALTER adds the hook's
 * columns, and their check is not validated, so it scans no row of the dead table under that ALTER's lock.
 */
public final class OutboxSchema implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxSchema.class);
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(5);
    /** Bounds every bootstrap statement, and replaces a shorter role default that would cut its lock waits short. */
    private static final Duration STATEMENT_TIMEOUT = Duration.ofMinutes(1);
    /** {@code LOCK_TIMEOUT} each: a minute for other instances' bootstraps, which take milliseconds each. */
    private static final int SCHEMA_LOCK_ATTEMPTS = 12;
    private static final String LOCK_NOT_AVAILABLE = "55P03";

    private final JdbcConnectionFactory db;
    private final OutboxConfig config;
    private final TableNames spelled;
    private final int schemaLockAttempts;
    @Nullable
    private volatile TableNames names;

    /**
     * Validates the whole outbox config: every other outbox component starts after this one, so a bad value fails
     * startup before any DDL or relay thread.
     */
    public OutboxSchema(JdbcConnectionFactory db, OutboxConfig config) {
        this(db, config, SCHEMA_LOCK_ATTEMPTS);
    }

    OutboxSchema(JdbcConnectionFactory db, OutboxConfig config, int schemaLockAttempts) {
        validate(config);
        this.db = db;
        this.config = config;
        this.spelled = TableNames.of(config.table());
        this.schemaLockAttempts = schemaLockAttempts;
    }

    /**
     * The one check of the outbox config ({@link InboxPartitions} checks the inbox keys). The other components that
     * take the config run it too ({@link io.github.dsudomoin.kora.outbox.OutboxRelay}, {@link ShardProcessor}, and
     * {@link PgOutbox} for {@code shards}), so built on their own they reject the same values with the same messages.
     */
    public static void validate(OutboxConfig config) {
        var relay = config.relay();
        var retry = relay.retry();
        var requeue = relay.requeue();
        var partitions = config.partitions();
        var deadLetters = config.deadLetters();
        validateShards(config.shards());
        require(relay.workers() >= 1, "outbox.relay.workers must be at least 1: " + relay.workers());
        require(relay.batchSize() >= 1, "outbox.relay.batchSize must be at least 1: " + relay.batchSize());
        require(relay.maxTicksPerBatch() >= 1,
                "outbox.relay.maxTicksPerBatch must be at least 1: " + relay.maxTicksPerBatch());
        require(!relay.minTickInterval().isNegative(),
                "outbox.relay.minTickInterval must not be negative: " + relay.minTickInterval());
        require(relay.idlePollInterval().compareTo(Duration.ZERO) > 0,
                "outbox.relay.idlePollInterval must be positive: " + relay.idlePollInterval());
        require(relay.batchTimeout().compareTo(Duration.ZERO) > 0,
                "outbox.relay.batchTimeout must be positive: " + relay.batchTimeout());
        require(!relay.shutdownTimeout().isNegative(),
                "outbox.relay.shutdownTimeout must not be negative: " + relay.shutdownTimeout());
        require(relay.headFailuresBeforeDivert() >= 1,
                "outbox.relay.headFailuresBeforeDivert must be at least 1: " + relay.headFailuresBeforeDivert());
        require(retry.initialDelay().compareTo(Duration.ZERO) > 0,
                "outbox.relay.retry.initialDelay must be positive: " + retry.initialDelay());
        require(retry.maxDelay().compareTo(retry.initialDelay()) >= 0,
                "outbox.relay.retry.maxDelay must be at least outbox.relay.retry.initialDelay: " + retry.maxDelay());
        require(retry.maxAttempts() >= 0,
                "outbox.relay.retry.maxAttempts must not be negative: " + retry.maxAttempts());
        require(requeue.delay().compareTo(Duration.ZERO) > 0,
                "outbox.relay.requeue.delay must be positive: " + requeue.delay());
        require(requeue.maxRequeues() >= 0,
                "outbox.relay.requeue.maxRequeues must not be negative: " + requeue.maxRequeues());
        require(partitions.xidsPerPartition() >= 1000,
                "outbox.partitions.xidsPerPartition must be at least 1000: " + partitions.xidsPerPartition());
        require(partitions.premake() >= 2, "outbox.partitions.premake must be at least 2: " + partitions.premake());
        require(partitions.maintenanceInterval().compareTo(Duration.ZERO) > 0,
                "outbox.partitions.maintenanceInterval must be positive: " + partitions.maintenanceInterval());
        // sent as whole milliseconds, and lock_timeout = 0 is no limit at all
        require(partitions.swapLockTimeout().toMillis() > 0,
                "outbox.partitions.swapLockTimeout must be positive: " + partitions.swapLockTimeout());
        require(deadLetters.batchSize() >= 1,
                "outbox.deadLetters.batchSize must be at least 1: " + deadLetters.batchSize());
        require(deadLetters.timeout().compareTo(Duration.ZERO) > 0,
                "outbox.deadLetters.timeout must be positive: " + deadLetters.timeout());
        require(!deadLetters.retention().isNegative(),
                "outbox.deadLetters.retention must not be negative: " + deadLetters.retention());
        require(deadLetters.redriveHistory().compareTo(Duration.ZERO) > 0,
                "outbox.deadLetters.redriveHistory must be positive: " + deadLetters.redriveHistory());
        var recovery = config.recovery();
        require(recovery.xidRegression().equals("auto") || recovery.xidRegression().equals("halt"),
                "outbox.recovery.xidRegression must be auto or halt: " + recovery.xidRegression());
        require(recovery.maxRows() >= 0, "outbox.recovery.maxRows must not be negative: " + recovery.maxRows());
        require(!recovery.clockMargin().isNegative(),
                "outbox.recovery.clockMargin must not be negative: " + recovery.clockMargin());
    }

    static void validateShards(int shards) {
        require(shards >= 1 && shards <= Short.MAX_VALUE,
                "outbox.shards must be within 1.." + Short.MAX_VALUE + ": " + shards);
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * The table names with the lock namespaces of the table {@link #init()} resolved, which every component locks
     * with: an instance configured {@code outbox} and one configured {@code public.outbox} share all locks.
     */
    public TableNames names() {
        var n = names;
        if (n == null) {
            throw new IllegalStateException("OutboxSchema.init() has not run: the lock namespaces of "
                    + spelled.base() + " come from the table it resolves to");
        }
        return n;
    }

    @Override
    public void init() throws SQLException {
        try (var c = db.newConnection()) {
            boolean autoCommit = c.getAutoCommit();
            c.setAutoCommit(true);
            try (var st = c.createStatement()) {
                // first: behind a transaction-pooling proxy even the settings below would leak
                PartitionDdl.requireSessionStable(c, "outbox bootstrap of " + spelled.base());
                // before the schema lock, so that wait is bounded too
                st.execute("set lock_timeout = " + LOCK_TIMEOUT.toMillis());
                st.execute("set statement_timeout = " + STATEMENT_TIMEOUT.toMillis());
                try {
                    // where the table is or will be created, so every spelling of it takes the same schema lock
                    var resolved = spelled.resolve(c);
                    lockSchema(c, resolved);
                    try {
                        bootstrap(c, resolved);
                        names = spelled.resolve(c); // the table exists now: checks that the name finds it there
                    } finally {
                        advisory(c, "pg_advisory_unlock", resolved, TableNames.LOCK_SCHEMA);
                    }
                } finally {
                    st.execute("reset lock_timeout");
                    st.execute("reset statement_timeout");
                }
            } finally {
                c.setAutoCommit(autoCommit);
            }
        }
    }

    @Override
    public void release() {}

    private void bootstrap(Connection c, TableNames n) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("create table if not exists " + n.base() + """
                     (
                      id          bigint generated always as identity,
                      shard       smallint    not null,
                      xid         xid8        not null default pg_current_xact_id(),
                      created_at  timestamptz not null default clock_timestamp(),
                      message_id  uuid        not null,
                      topic       text        not null,
                      key         bytea,
                      payload     bytea,
                      headers     bytea
                    ) partition by range (xid)""");
            createIndex(c, n.base() + "_shard_xid", n.base() + " (shard, xid)");
            st.execute("create table if not exists " + n.tick() + """
                     (
                      id         bigint generated always as identity primary key,
                      snapshot   pg_snapshot not null,
                      created_at timestamptz not null default clock_timestamp(),
                      max_id     bigint,
                      dirty      bit varying
                    )""");
            if (!columnExists(c, n.tick(), "max_id")) { // a tick table made before max_id: a metadata-only change
                st.execute("alter table " + n.tick() + " add column if not exists max_id bigint");
            }
            addColumn(c, n.tick(), "dirty", "bit varying"); // made before dirty: likewise; its ticks stay null (all)
            if (columnExists(c, n.tick(), "shards")) { // the shard sets dirty replaces
                st.execute("alter table " + n.tick() + " drop column if exists shards");
            }
            createIndex(c, n.tick() + "_xmax", n.tick() + " ((pg_snapshot_xmax(snapshot)))"); // Maintenance's xid check
            st.execute("create table if not exists " + n.cursor() + """
                     (
                      shard          smallint not null,
                      seq            bigint generated always as identity,
                      tick_id        bigint not null,
                      target_tick_id bigint,
                      last_id        bigint not null default 0,
                      paused_until   timestamptz,
                      failures       int not null default 0,
                      retry_due_at   timestamptz,
                      parked_due_at  timestamptz,
                      primary key (shard, seq)
                    )""");
            if (!columnExists(c, n.cursor(), "parked_due_at")) { // made before requeue: metadata-only, rows get null
                st.execute("alter table " + n.cursor() + " add column if not exists parked_due_at timestamptz");
            }
            st.execute("create table if not exists " + n.retry() + """
                     (
                      id              bigint primary key,
                      shard           smallint not null,
                      message_id      uuid not null,
                      topic           text not null,
                      key             bytea,
                      payload         bytea,
                      headers         bytea,
                      created_at      timestamptz not null,
                      attempts        int not null default 0,
                      next_attempt_at timestamptz,
                      last_error      text,
                      requeues        int not null default 0
                    )""");
            if (!columnExists(c, n.retry(), "requeues")) { // made before requeue: likewise, a constant default
                st.execute("alter table " + n.retry() + " add column if not exists requeues int not null default 0");
            }
            createIndex(c, n.retry() + "_key", n.retry() + " (shard, topic, key, id)");
            createIndex(c, n.retry() + "_due",
                    n.retry() + " (shard, next_attempt_at) where next_attempt_at is not null");
            // requeued records: unlike retry rows, they do not block their key
            st.execute("create table if not exists " + n.parked() + """
                     (
                      id         bigint primary key,
                      shard      smallint not null,
                      message_id uuid not null,
                      topic      text not null,
                      key        bytea,
                      payload    bytea,
                      headers    bytea,
                      created_at timestamptz not null,
                      attempts   int not null default 0,
                      requeues   int not null default 0,
                      due_at     timestamptz not null
                    )""");
            createIndex(c, n.parked() + "_key", n.parked() + " (shard, topic, key, id)");
            createIndex(c, n.parked() + "_due", n.parked() + " (shard, due_at)");
            st.execute("create table if not exists " + n.dead() + """
                     (
                      id         bigint primary key,
                      shard      smallint not null,
                      message_id uuid not null,
                      topic      text not null,
                      key        bytea,
                      payload    bytea,
                      headers    bytea,
                      created_at timestamptz not null,
                      attempts   int not null,
                      failed_at  timestamptz not null default now(),
                      error      text
                    )""");
            // the dead-letter hook's columns, on a fresh table too, in one ALTER while any is missing (due_at: the next
            // hook call of a pending letter, the redrive of a scheduled one); letters of a table made before them
            // become kept, so the status check, added with its column, skips them (not valid)
            boolean noStatus = !columnExists(c, n.dead(), "status");
            boolean missing = noStatus;
            for (var column : List.of("due_at", "handler_attempts", "last_handler_error", "redrives")) {
                missing |= !columnExists(c, n.dead(), column);
            }
            if (missing) {
                st.execute("alter table " + n.dead() + " add column if not exists status text not null default 'kept',"
                        + " add column if not exists due_at timestamptz,"
                        + " add column if not exists handler_attempts int not null default 0,"
                        + " add column if not exists last_handler_error text,"
                        + " add column if not exists redrives int not null default 0"
                        + (noStatus ? ", add constraint " + n.unqualified(n.dead()) + "_status"
                                + " check (status in ('pending', 'kept', 'scheduled')) not valid" : ""));
            }
            createIndex(c, n.dead() + "_due", n.dead() + " (due_at) where due_at is not null");
            // how many times a message was redriven, read by its next dead row
            st.execute("create table if not exists " + n.redriven() + """
                     (
                      message_id uuid primary key,
                      redrives   int not null,
                      at         timestamptz not null default now()
                    )""");
            // the xid-regression halt, persisted by Maintenance: one row while halted
            st.execute("create table if not exists " + n.halt() + """
                     (
                      id            int primary key default 1 check (id = 1),
                      halted_at     timestamptz not null default now(),
                      max_tick_xmax xid8 not null
                    )""");
            // when a maintenance run last completed every step, on any instance; until the first one, bootstrap time
            st.execute("create table if not exists " + n.maintenance() + """
                     (
                      id           int primary key default 1 check (id = 1),
                      succeeded_at timestamptz not null
                    )""");
            st.execute("insert into " + n.maintenance() + " (succeeded_at) values (now()) on conflict (id) do nothing");
            // XidPartitions: the ranges it detaches as relayed, by unqualified name, until they are dropped
            st.execute("create table if not exists " + n.dropping() + """
                     (
                      table_name text primary key,
                      at         timestamptz not null default now()
                    )""");

            checkSequenceCache(c, n);
            long shardsPresent = queryLong(st, "select count(distinct shard) from " + n.cursor());
            if (shardsPresent != 0 && shardsPresent != config.shards()) {
                throw new IllegalStateException("outbox.shards = " + config.shards() + " but " + n.cursor()
                        + " holds cursors for " + shardsPresent + " shards; resharding requires an empty outbox"
                        + " (see README)");
            }
            checkXidCounter(c, n);

            ensurePartitions(c, n);

            st.execute("insert into " + n.tick() + " (snapshot, max_id) select pg_current_snapshot(), "
                    + Ticks.maxIdQuery(c, n) + " where not exists (select 1 from " + n.tick() + ")");
            // not after a recovery, which makes them
            try (var ps = c.prepareStatement("insert into " + n.cursor() + " (shard, tick_id) select s, (select max(id)"
                    + " from " + n.tick() + ") from generate_series(0, ? - 1) s where not exists (select 1 from "
                    + n.cursor() + ")")) {
                ps.setInt(1, config.shards());
                ps.executeUpdate();
            }
        }
    }

    /**
     * Fresh table: creates {@code premake} ranges and the tail. Existing table: first finishes a detach left pending
     * (an interrupted range drop's, which would fail the next one with 55000, or an operator's tail detach), then the
     * same extension maintenance runs: the tail swap, the one ACCESS EXCLUSIVE on the parent, under
     * {@code outbox.partitions.swapLockTimeout}, after waiting up to {@code LOCK_TIMEOUT} for relay steps in flight. A
     * lock timeout there (the swap waiting for running transactions in a rolling deploy) is logged and left to
     * maintenance while inserts at the current xid still find a partition. A hot tail is left to maintenance too:
     * turning it into a range scans it, which would hold the schema lock and every other instance's startup.
     * Partition DDL is serialised by {@code LOCK_MAINTENANCE}, taken here after {@code LOCK_SCHEMA};
     * {@link Maintenance} takes it with {@code pg_try_advisory_lock} and never takes {@code LOCK_SCHEMA}, so the two
     * can never deadlock.
     */
    private void ensurePartitions(Connection c, TableNames n) throws SQLException {
        var partitions = XidPartitions.forBootstrap(n, config.partitions());
        partitions.warnIfActivityHidden(c); // once per start
        try {
            advisory(c, "pg_advisory_lock", n, TableNames.LOCK_MAINTENANCE); // bounded by the session lock_timeout
            try {
                partitions.finishPendingDetach(c, LOCK_TIMEOUT);
                partitions.ensurePremade(c, LOCK_TIMEOUT, LOCK_TIMEOUT);
            } finally {
                advisory(c, "pg_advisory_unlock", n, TableNames.LOCK_MAINTENANCE);
            }
        } catch (SQLException e) {
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState()) || !partitions.acceptsInserts(c)) {
                throw e;
            }
            log.warn("outbox partition extension of {} hit a lock timeout at startup; maintenance retries it: {}",
                    n.base(), e.getMessage());
        }
    }

    private void createIndex(Connection c, String index, String definition) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?) is null")) {
            ps.setString(1, index);
            try (var rs = ps.executeQuery()) {
                rs.next();
                if (!rs.getBoolean(1)) {
                    return; // CREATE INDEX IF NOT EXISTS would still queue a SHARE lock behind running writers
                }
            }
        }
        try (var st = c.createStatement()) {
            st.execute("create index " + spelled.unqualified(index) + " on " + definition);
        }
    }

    /** Once per table: ACCESS EXCLUSIVE for a metadata-only change (a constant default or none). */
    private static void addColumn(Connection c, String table, String column, String definition) throws SQLException {
        if (!columnExists(c, table, column)) {
            try (var st = c.createStatement()) {
                st.execute("alter table " + table + " add column if not exists " + column + " " + definition);
            }
        }
    }

    /** Checked first, as ALTER TABLE takes its lock even when the column exists. */
    static boolean columnExists(Connection c, String table, String column) throws SQLException {
        try (var ps = c.prepareStatement("select exists (select 1 from pg_attribute"
                + " where attrelid = to_regclass(?) and attname = ? and not attisdropped)")) {
            ps.setString(1, table);
            ps.setString(2, column);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /**
     * Per-key order is id order, and ticks and cursor rows are ordered by id too: with {@code CACHE > 1} a session
     * draws ids from its own cached block, so a transaction that took a lock later can still get a lower id.
     */
    private static void checkSequenceCache(Connection c, TableNames n) throws SQLException {
        try (var ps = c.prepareStatement("select string_agg(format('%s has cache %s', s.seqrelid::regclass,"
                + " s.seqcache), ', ') from pg_sequence s where s.seqcache > 1 and s.seqrelid in ("
                + "pg_get_serial_sequence(?, 'id')::regclass, pg_get_serial_sequence(?, 'id')::regclass,"
                + " pg_get_serial_sequence(?, 'seq')::regclass)")) {
            ps.setString(1, n.base());
            ps.setString(2, n.tick());
            ps.setString(3, n.cursor());
            try (var rs = ps.executeQuery()) {
                rs.next();
                var cached = rs.getString(1);
                if (cached != null) {
                    throw new IllegalStateException("outbox identity sequence " + cached + ": cached ids are not"
                            + " assigned in lock order (each session draws from its own block), which breaks the"
                            + " order of a key's messages, of ticks and of cursor rows; set it back with"
                            + " ALTER SEQUENCE ... CACHE 1");
                }
            }
        }
    }

    /**
     * The xid check {@link Maintenance} runs too: a halt found is recorded, then recovered as
     * {@link XidRecovery#autoRecover} decides, under {@code LOCK_MAINTENANCE} waited for within the session's
     * {@code lock_timeout}; else an ERROR, and startup goes on halted. A halt whose ticks were repaired is left for
     * {@link Maintenance}'s check to lift.
     */
    private void checkXidCounter(Connection c, TableNames n) throws SQLException {
        var recovery = new XidRecovery(n, config);
        var check = recovery.check(c);
        if (!check.halted()) {
            return;
        }
        if (check.recorded() == null) {
            recovery.recordHalt(c, check.tickXmax());
        }
        String why = XidRecovery.HALT_MODE;
        if (config.recovery().xidRegression().equals("auto")) {
            why = XidRecovery.LOCK_BUSY;
            if (lockMaintenance(c, n)) {
                try {
                    why = recovery.autoRecover(c);
                } finally {
                    advisory(c, "pg_advisory_unlock", n, TableNames.LOCK_MAINTENANCE);
                }
            }
        }
        if (why != null) {
            log.error(recovery.halted(check, why));
        }
    }

    /** {@code pg_advisory_lock(internalNs, LOCK_MAINTENANCE)}; false when the session's lock_timeout ran out. */
    private static boolean lockMaintenance(Connection c, TableNames n) throws SQLException {
        try {
            advisory(c, "pg_advisory_lock", n, TableNames.LOCK_MAINTENANCE);
            return true;
        } catch (SQLException e) {
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                throw e;
            }
            return false;
        }
    }

    /**
     * Waits for other instances' bootstraps {@code schemaLockAttempts} times {@code LOCK_TIMEOUT}, then fails startup
     * naming the lock: a holder frozen mid-bootstrap must not hang every later startup.
     */
    private void lockSchema(Connection c, TableNames n) throws SQLException {
        for (int attempt = 1; ; attempt++) {
            try {
                advisory(c, "pg_advisory_lock", n, TableNames.LOCK_SCHEMA);
                return;
            } catch (SQLException e) {
                if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                    throw e;
                }
                var lock = "pg_advisory_lock(" + n.internalNamespace() + ", " + TableNames.LOCK_SCHEMA + ")";
                if (attempt >= schemaLockAttempts) {
                    throw new IllegalStateException("outbox bootstrap of " + n.base() + " gave up after "
                            + LOCK_TIMEOUT.multipliedBy(attempt).toSeconds() + " s waiting for its schema lock " + lock
                            + ", held by another session (another instance's bootstrap, possibly stuck); find it with"
                            + " select pid from pg_locks where locktype = 'advisory' and classid = "
                            + Integer.toUnsignedLong(n.internalNamespace()) + " and objid = " + TableNames.LOCK_SCHEMA
                            + " and objsubid = 2", e);
                }
                log.warn("outbox bootstrap of {} waits for its schema lock {}, held by another session (attempt {} of"
                        + " {})", n.base(), lock, attempt, schemaLockAttempts);
            }
        }
    }

    private static void advisory(Connection c, String function, TableNames n, int lock) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, n.internalNamespace());
            ps.setInt(2, lock);
            ps.execute();
        }
    }

    private static long queryLong(Statement st, String sql) throws SQLException {
        try (var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
