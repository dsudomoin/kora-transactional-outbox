package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
 * The shard count ({@link #config()}): {@code outbox.shards} when set, else the cursors' count when there are cursors
 * (with an INFO), else {@link OutboxConfig#DEFAULT_SHARDS}. A set {@code outbox.shards} other than the cursors' count
 * reshards ({@link #reshard}), which fails startup unless every check passes. Fails fast with
 * {@link IllegalStateException} when an identity sequence of the outbox, tick or cursor table has {@code CACHE > 1}.
 * An xid regression (the counter below a tick's xmax) never fails it: it is recorded in {@code <base>_halt} and, with
 * {@code outbox.recovery.xidRegression = auto} and at most {@code outbox.recovery.maxRows} rows to re-send, recovered
 * before any relay starts ({@link XidRecovery}, under {@code LOCK_MAINTENANCE}, taken after {@code LOCK_SCHEMA});
 * otherwise, or while another session holds that lock, startup goes on halted (the relay delivers nothing, writes go
 * on) with an ERROR naming the admin call that runs the recovery. The recovery runs under its own timeouts (one
 * deadline of {@code outbox.recovery.timeout}, 10 min by default), and other instances' startups wait for it behind
 * the schema lock, at most a minute. The schema lock and every bootstrap DDL statement wait at
 * most 5 s for a lock ({@code lock_timeout}), and every bootstrap statement runs under a 1 min
 * {@code statement_timeout} (replacing a shorter role default): one that needs longer, such as an index built anew on a
 * populated table, fails startup with SQLState 57014 and is left to run by hand; the dead table's {@code <dead>_due},
 * new with the dead-letter hook, is built here only while that table has no row, otherwise (an INFO) by
 * {@link Maintenance}, concurrently. The schema lock is retried for a
 * minute, then startup fails. On existing tables the bootstrap takes no lock that writers wait behind (index creation
 * is skipped when the index exists, partition extension is optional and left to maintenance when it times out). A tick
 * table made before {@code max_id}, {@code dirty} or {@code shards}, a cursor table made before
 * {@code parked_due_at}, a retry table made before {@code requeues} and a dead table made before the dead-letter hook's
 * columns get each column once; such an ALTER waits for running relay transactions and fails startup on timeout. The
 * tick table keeps {@code shards}, the shard sets {@code dirty} replaces, for the previous release, which writes and
 * reads it beside or after this one; this version leaves it null (every shard). Ticks without {@code dirty} (null)
 * count as holding rows of every shard; dead letters older than {@code status} are kept and the hook sees new ones only: one ALTER adds the hook's
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
    private static final String DUPLICATE_TABLE = "42P07";
    /** Bound of {@code deadLetters.retention} and {@code redriveHistory}: 10 years, an age maintenance compares. */
    private static final Duration MAX_DEAD_LETTER_AGE = Duration.ofDays(3650);
    /**
     * Added to twice the longer of {@code idlePollInterval} and {@code minTickInterval}: a reshard is refused while the
     * newest tick is younger than that, as every running relay ticks at least that often, idle or not.
     */
    private static final Duration TICK_MARGIN = Duration.ofSeconds(5);
    /** How many of the latest ticks the reshard's tick guard looks at for the largest gap between two. */
    private static final int TICK_GAPS = 20;
    /** Heartbeat rows (this one's too) stay live for this many of the writer's {@code maintenanceInterval}. */
    static final int LIVE_INTERVALS = 3;

    private final JdbcConnectionFactory db;
    private final OutboxConfig config;
    private final TableNames spelled;
    private final int schemaLockAttempts;
    private final String instanceId = Heartbeat.newId();
    private final ShardGuard guard = new ShardGuard();
    /** The tick guard's lower bound, whatever the intervals say; shorter in tests. */
    Duration tickFloor = Duration.ofMinutes(1);
    /** How long the reshard waits for the tick guard alone to pass before it refuses; shorter in tests. */
    Duration tickWait = Duration.ofMinutes(2);
    @Nullable
    private volatile TableNames names;
    @Nullable
    private volatile OutboxConfig resolved;

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
        if (config.shards() != null) {
            validateShards(config.shards());
        }
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
        require(relay.cursorAnchorInterval().compareTo(Duration.ZERO) > 0,
                "outbox.relay.cursorAnchorInterval must be positive: " + relay.cursorAnchorInterval());
        require(relay.cursorAnchorInterval().compareTo(partitions.maintenanceInterval()) >= 0,
                "outbox.relay.cursorAnchorInterval must be at least outbox.partitions.maintenanceInterval: "
                        + relay.cursorAnchorInterval());
        // 0 is no wait; a wait is sent as whole milliseconds, and lock_timeout = 0 is no limit at all
        var swapWait = partitions.swapLockTimeout();
        require(swapWait.isZero() || swapWait.toMillis() > 0,
                "outbox.partitions.swapLockTimeout must be 0 or at least 1ms: " + swapWait);
        require(partitions.maxTailScan() >= 0,
                "outbox.partitions.maxTailScan must not be negative: " + partitions.maxTailScan());
        XidPartitions.Window.parse(partitions.tailConversion().window()); // its message names the key
        try {
            java.time.ZoneId.of(partitions.tailConversion().zone());
        } catch (java.time.DateTimeException e) {
            throw new IllegalArgumentException("outbox.partitions.tailConversion.zone must be a time zone id such as"
                    + " Europe/Moscow or UTC: " + partitions.tailConversion().zone(), e);
        }
        require(partitions.tailConversion().maxStall().toMillis() >= 1,
                "outbox.partitions.tailConversion.maxStall must be at least 1ms: "
                        + partitions.tailConversion().maxStall());
        require(deadLetters.batchSize() >= 1,
                "outbox.deadLetters.batchSize must be at least 1: " + deadLetters.batchSize());
        // a batch's redrive holds a key lock per letter in one transaction, as an admin chunk does
        require(deadLetters.batchSize() <= PgOutboxAdmin.CHUNK,
                "outbox.deadLetters.batchSize must be at most " + PgOutboxAdmin.CHUNK + ": " + deadLetters.batchSize());
        require(deadLetters.timeout().compareTo(Duration.ZERO) > 0,
                "outbox.deadLetters.timeout must be positive: " + deadLetters.timeout());
        require(!deadLetters.retention().isNegative(),
                "outbox.deadLetters.retention must not be negative: " + deadLetters.retention());
        require(deadLetters.retention().compareTo(MAX_DEAD_LETTER_AGE) <= 0,
                "outbox.deadLetters.retention must be at most 3650d: " + deadLetters.retention());
        require(deadLetters.redriveHistory().compareTo(Duration.ZERO) > 0,
                "outbox.deadLetters.redriveHistory must be positive: " + deadLetters.redriveHistory());
        require(deadLetters.redriveHistory().compareTo(MAX_DEAD_LETTER_AGE) <= 0,
                "outbox.deadLetters.redriveHistory must be at most 3650d: " + deadLetters.redriveHistory());
        var recovery = config.recovery();
        require(recovery.xidRegression().equals("auto") || recovery.xidRegression().equals("halt"),
                "outbox.recovery.xidRegression must be auto or halt: " + recovery.xidRegression());
        require(recovery.maxRows() >= 0, "outbox.recovery.maxRows must not be negative: " + recovery.maxRows());
        require(!recovery.clockMargin().isNegative(),
                "outbox.recovery.clockMargin must not be negative: " + recovery.clockMargin());
        require(recovery.timeout().toMillis() > 0, "outbox.recovery.timeout must be positive: " + recovery.timeout());
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

    /**
     * The config every other component runs with: this one with {@code shards} as {@link #init()} resolved it, never
     * null.
     */
    public OutboxConfig config() {
        var r = resolved;
        if (r == null) {
            throw new IllegalStateException("OutboxSchema.init() has not run: the shard count of " + spelled.base()
                    + " comes from its cursors when outbox.shards is not set");
        }
        return r;
    }

    /**
     * This instance's id in {@code <base>_instance}, {@code <host>/<pid>/<8 hex>}: the bootstrap writes its row, the
     * relay's {@link Heartbeat} keeps it fresh.
     */
    public String instanceId() {
        return instanceId;
    }

    /** The cursors' shard count as this instance last read it, which every write of it checks. */
    ShardGuard shardGuard() {
        return guard;
    }

    /**
     * {@code config} with the shard count resolved at bootstrap, and this instance's {@link ShardGuard}, which the
     * writes of every component built from it check ({@link ShardGuard#of}).
     */
    record Resolved(OutboxConfig config, Integer shards, ShardGuard guard) implements OutboxConfig {

        @Override
        public String table() {
            return config.table();
        }

        @Override
        public RelayConfig relay() {
            return config.relay();
        }

        @Override
        public PartitionsConfig partitions() {
            return config.partitions();
        }

        @Override
        public InboxConfig inbox() {
            return config.inbox();
        }

        @Override
        public DeadLettersConfig deadLetters() {
            return config.deadLetters();
        }

        @Override
        public RecoveryConfig recovery() {
            return config.recovery();
        }
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
                // a role's own value would end the session during the reshard's tick wait
                st.execute("set idle_session_timeout = 0");
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
                    st.execute("reset idle_session_timeout");
                }
            } finally {
                c.setAutoCommit(autoCommit);
            }
        }
    }

    /** Deletes this instance's row; the relay's {@link Heartbeat} deletes it first when it ran. */
    @Override
    public void release() {
        var n = names;
        if (n != null) {
            Heartbeat.delete(db, n, instanceId);
        }
    }

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
                      dirty      bit varying,
                      shards     smallint[]
                    )""");
            if (!columnExists(c, n.tick(), "max_id")) { // a tick table made before max_id: a metadata-only change
                st.execute("alter table " + n.tick() + " add column if not exists max_id bigint");
            }
            addColumn(c, n.tick(), "dirty", "bit varying"); // made before dirty: likewise; its ticks stay null (all)
            // the shard sets dirty replaces, kept for the previous release beside or after this one: it inserts them
            // in every tick and reads them (null: every shard, as this version leaves them). Made again where an
            // earlier build of this version dropped it
            addColumn(c, n.tick(), "shards", "smallint[]");
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
            // maintenance's checkpoint, which moves idle shards on without cursor rows (Cursors); none until its run
            st.execute("create table if not exists " + n.checkpoint() + """
                     (
                      seq          bigint generated always as identity primary key,
                      tick_id      bigint not null,
                      busy         bit varying not null,
                      busy_tick_id bigint not null,
                      created_at   timestamptz not null default clock_timestamp()
                    )""");
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
            // on a dead table with rows (kept forever by default) the build would hold dead inserts and outlast the
            // statement_timeout: Maintenance builds it concurrently, and the pass claims by a scan meanwhile
            String deadDue = n.dead() + "_due";
            if (indexMissing(c, deadDue)) {
                if (PartitionDdl.hasRows(c, n.dead())) {
                    log.info("outbox index {} is missing on {}, which holds rows: maintenance builds it concurrently"
                            + " in its next run", deadDue, n.dead());
                } else {
                    createIndex(c, deadDue, n.dead() + " (due_at) where due_at is not null");
                }
            }
            // how many times a message was redriven, read by its next dead row
            st.execute("create table if not exists " + n.redriven() + """
                     (
                      message_id uuid primary key,
                      redrives   int not null,
                      at         timestamptz not null default now()
                    )""");
            // the xid-regression halt, persisted by Maintenance: one row while halted; recovery_failed: why the
            // automatic recovery stopped trying (XidRecovery#autoRecover)
            st.execute("create table if not exists " + n.halt() + """
                     (
                      id              int primary key default 1 check (id = 1),
                      halted_at       timestamptz not null default now(),
                      max_tick_xmax   xid8 not null,
                      recovery_failed text
                    )""");
            addColumn(c, n.halt(), "recovery_failed", "text"); // the previous release's table
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
            // Heartbeat: the instances running, by the database clock, for the reshard guard and the admin status; a
            // row is live until its expires_at, which its writer sets from its own maintenanceInterval
            st.execute("create table if not exists " + n.instance() + """
                     (
                      instance_id text primary key,
                      shards      int not null,
                      last_seen   timestamptz not null default now(),
                      hook_active boolean not null default false,
                      expires_at  timestamptz
                    )""");
            addColumn(c, n.instance(), "expires_at", "timestamptz"); // made by an earlier build of this version

            checkSequenceCache(c, n);
            int present = (int) queryLong(st, Cursors.shardCount(n));
            int shards;
            if (config.shards() == null) {
                shards = present == 0 ? OutboxConfig.DEFAULT_SHARDS : present;
                if (present != 0) {
                    log.info("outbox.shards is not set: {} holds cursors for {} shards, which this instance uses",
                            n.cursor(), present);
                }
            } else {
                shards = config.shards();
                if (present != 0 && present != shards) {
                    reshard(c, n, present, shards);
                }
            }
            var r = new Resolved(config, shards, guard);
            checkXidCounter(c, n, r);

            ensurePartitions(c, n);

            st.execute("insert into " + n.tick() + " (snapshot, max_id) select pg_current_snapshot(), "
                    + Ticks.maxIdQuery(c, n) + " where not exists (select 1 from " + n.tick() + ")");
            // not after a recovery, which makes them
            try (var ps = c.prepareStatement(insertCursors(n) + " where not exists (select 1 from " + n.cursor()
                    + ")")) {
                ps.setInt(1, shards);
                ps.executeUpdate();
            }
            // still under the schema lock: from here on, another instance's reshard sees this one
            try (var ps = c.prepareStatement(Heartbeat.upsert(n))) {
                Heartbeat.bind(ps, 0, instanceId, shards, false, config.partitions().maintenanceInterval());
                ps.executeUpdate();
            }
            resolved = r;
        }
    }

    /** One cursor per shard {@code 0..?-1} at the latest tick. */
    private static String insertCursors(TableNames n) {
        return "insert into " + n.cursor() + " (shard, tick_id) select s, (select max(id) from " + n.tick()
                + ") from generate_series(0, ? - 1) s";
    }

    /**
     * The emergency reshard from the cursors' {@code from} shards to {@code to}, under the schema lock, so one at a
     * time; the procedure for the team: stop writers, wait for {@code outbox.shard.lag} 0, stop all instances, start
     * with the new value. Refused while a live row of another shard count is in {@code <base>_instance} (its
     * {@code expires_at} ahead, set by each instance's bootstrap and {@link Heartbeat} from its own interval), and
     * while the newest tick is younger than the longest of: 2 × the longer of {@code idlePollInterval} and
     * {@code minTickInterval} plus {@link #TICK_MARGIN}; 3 × the largest gap among the last {@value #TICK_GAPS} ticks
     * (relays with longer intervals than this instance's), at most {@link #tickWait}; {@link #tickFloor}: a relay of a
     * release without the heartbeat still runs. That guard alone is waited out, up to {@link #tickWait}, before it
     * refuses. Then one read-committed transaction: {@code pg_advisory_xact_lock} on {@code LOCK_MAINTENANCE},
     * {@code LOCK_SWAP} (relay steps hold it shared) and {@code LOCK_TICK}, in that order, so no maintenance run, step
     * or tick interleaves; then {@code SHARE} on the outbox, so no write commits between the checks and the switch
     * (writers wait and go on after the commit); each lock wait is bounded by the session's {@code lock_timeout}, and a
     * timeout fails startup with the drain procedure; the drain checks: the retry and parked tables empty, no batch in
     * progress, no committed row that its shard's effective tick ({@link Cursors}) does not see, no standalone range,
     * no xid regression. All pass: the cursors and checkpoints are replaced by one cursor per new shard at a new tick,
     * with an INFO; otherwise startup fails naming every failed check with its count. Dead letters keep their shard: a
     * redrive computes it again.
     */
    private void reshard(Connection c, TableNames n, int from, int to) throws SQLException {
        var keep = " To keep the current count, start with outbox.shards = " + from + " or leave it unset.";
        var live = new ArrayList<String>();
        try (var ps = c.prepareStatement("select instance_id, shards, round(extract(epoch from clock_timestamp()"
                + " - last_seen))::bigint from " + n.instance() + " where shards <> ? and expires_at >"
                + " clock_timestamp() order by instance_id")) {
            ps.setInt(1, to);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    live.add(rs.getString(1) + " (" + rs.getInt(2) + " shards, seen " + rs.getLong(3) + " s ago)");
                }
            }
        }
        if (!live.isEmpty()) {
            throw new IllegalStateException("outbox.shards = " + to + " but " + n.cursor() + " holds cursors for "
                    + from + " shards, and instances with another shard count are running (their heartbeat has not"
                    + " expired): " + String.join(", ", live) + ". A reshard needs every"
                    + " instance stopped: stop writers, wait for outbox.shard.lag 0, stop all instances, start with the"
                    + " new value." + keep);
        }
        awaitNoTicks(c, n, from, to, keep);
        c.setAutoCommit(false);
        try {
            try {
                // read committed whatever the session default: under repeatable read the lock statement would fix the
                // snapshot before SHARE is granted, so the checks and the new tick would miss a write committed while
                // the reshard waited for it, and its row would be read under its old shard
                try (var st = c.createStatement()) {
                    st.execute("set transaction isolation level read committed");
                }
                try (var ps = c.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
                    for (int lock : List.of(TableNames.LOCK_MAINTENANCE, TableNames.LOCK_SWAP, TableNames.LOCK_TICK)) {
                        ps.setInt(1, n.internalNamespace());
                        ps.setInt(2, lock);
                        ps.execute();
                    }
                }
                try (var st = c.createStatement()) {
                    st.execute("lock table " + n.base() + " in share mode"); // held writes wait for the commit
                }
            } catch (SQLException e) {
                if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                    throw e;
                }
                throw new IllegalStateException("outbox.shards = " + to + " but " + n.cursor() + " holds cursors for "
                        + from + " shards, and the reshard waited " + LOCK_TIMEOUT.toSeconds() + " s for a lock that a"
                        + " running relay, its maintenance or a writer of " + n.base() + " holds: " + e.getMessage()
                        + ". A reshard needs every instance stopped: stop writers, wait for outbox.shard.lag 0, stop"
                        + " all instances, start with the new value." + keep, e);
            }
            // a relay that ticked between the wait and the locks: no tick is made while LOCK_TICK is held
            var age = tickAge(c, n);
            if (age != null && age.ago() < age.bound()) {
                throw relayRunning(n, from, to, keep, age);
            }
            var failed = drainChecks(c, n, from);
            if (!failed.isEmpty()) {
                throw new IllegalStateException("outbox.shards = " + to + " but " + n.cursor() + " holds cursors for "
                        + from + " shards, and " + n.base() + " is not drained, so its rows would be skipped or read"
                        + " under their old shard: " + String.join("; ", failed) + ". To reshard: stop writers, wait for"
                        + " outbox.shard.lag 0, stop all instances, start with the new value (README «Решардинг»)."
                        + keep);
            }
            try (var st = c.createStatement()) {
                st.execute("delete from " + n.cursor());
                st.execute("delete from " + n.checkpoint());
                st.execute("insert into " + n.tick() + " (snapshot, max_id) select pg_current_snapshot(), "
                        + Ticks.maxIdQuery(c, n));
            }
            try (var ps = c.prepareStatement(insertCursors(n))) {
                ps.setInt(1, to);
                ps.executeUpdate();
            }
            c.commit();
        } catch (SQLException | RuntimeException e) {
            try {
                c.rollback();
            } catch (SQLException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
        log.info("outbox resharded {} from {} to {} shards: it was drained, and every shard's cursor starts at a new"
                + " tick; dead letters keep their old shard until redriven", n.base(), from, to);
    }

    /**
     * Fails unless the newest tick is older than the tick guard's bound (see {@link #reshard}), after waiting for it
     * up to {@link #tickWait}, rechecking: a relay that stops meanwhile lets the reshard go on, not a restart loop.
     */
    private void awaitNoTicks(Connection c, TableNames n, int from, int to, String keep) throws SQLException {
        long deadline = System.nanoTime() + tickWait.toNanos();
        while (true) {
            var age = tickAge(c, n);
            if (age == null || age.ago() >= age.bound()) {
                return;
            }
            long ago = age.ago();
            long bound = age.bound();
            long left = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (bound - ago > left) {
                throw relayRunning(n, from, to, keep, age);
            }
            log.info("outbox reshard of {} waits {} ms for the latest tick to be {} ms old: a relay may still run",
                    n.base(), bound - ago, bound);
            try {
                Thread.sleep(bound - ago + 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("outbox reshard of " + n.base() + " interrupted", e);
            }
        }
    }

    /** The newest tick's age and the tick guard's bound, in ms; null without ticks. */
    private record TickAge(long ago, long bound) {}

    @Nullable
    private TickAge tickAge(Connection c, TableNames n) throws SQLException {
        var relay = config.relay();
        var own = max(relay.idlePollInterval(), relay.minTickInterval()).multipliedBy(2).plus(TICK_MARGIN);
        try (var st = c.createStatement(); var rs = st.executeQuery("select round(extract(epoch from"
                + " clock_timestamp() - max(created_at)) * 1000)::bigint, coalesce(round(extract(epoch from"
                + " max(gap)) * 1000)::bigint, 0) from (select created_at, created_at - lag(created_at) over"
                + " (order by id) gap from (select id, created_at from " + n.tick() + " order by id desc limit "
                + TICK_GAPS + ") l) g")) {
            rs.next();
            long ago = rs.getLong(1);
            if (rs.wasNull()) {
                return null;
            }
            // the cadence term capped at the wait: one old long pause among the ticks cannot block a reshard
            long cadence = Math.min(3 * rs.getLong(2), tickWait.toMillis());
            return new TickAge(ago, Math.max(Math.max(own.toMillis(), cadence), tickFloor.toMillis()));
        }
    }

    private IllegalStateException relayRunning(TableNames n, int from, int to, String keep, TickAge age) {
        return new IllegalStateException("outbox.shards = " + to + " but " + n.cursor() + " holds cursors for " + from
                + " shards, and a relay is still running (latest tick " + age.ago() + " ms ago): a reshard needs every"
                + " instance stopped, those of releases without the heartbeat too, and no tick for " + age.bound()
                + " ms (waited up to " + tickWait.toSeconds() + " s). Stop writers, wait for outbox.shard.lag 0, stop"
                + " all instances, start with the new value." + keep);
    }

    /**
     * The checks a reshard needs, each failed one with its count; under {@code SHARE} on the outbox. The unrelayed rows
     * are the README query's: a scan of the outbox (writers are stopped by then).
     */
    private List<String> drainChecks(Connection c, TableNames n, int from) throws SQLException {
        var failed = new ArrayList<String>();
        try (var st = c.createStatement()) {
            for (var table : List.of(n.retry(), n.parked())) {
                long rows = queryLong(st, "select count(*) from " + table);
                if (rows > 0) {
                    failed.add(rows + " rows in " + table);
                }
            }
            try (var rs = st.executeQuery("select count(*), array_agg(shard order by shard) from (select distinct on"
                    + " (shard) shard, target_tick_id from " + n.cursor() + " order by shard, seq desc) c"
                    + " where target_tick_id is not null")) {
                rs.next();
                if (rs.getLong(1) > 0) {
                    failed.add(rs.getLong(1) + " shards with a batch in progress (" + shardList(rs.getArray(2))
                            + ")");
                }
            }
            try (var rs = st.executeQuery("select count(*), array_agg(distinct o.shard order by o.shard) from "
                    + n.base() + " o left join (select c.shard, " + Cursors.effective(from) + " tick_id from (select"
                    + " distinct on (shard) shard, tick_id from " + n.cursor() + " order by shard, seq desc) c"
                    + " left join " + Cursors.newestCheckpoint(n) + " k on true) e on e.shard = o.shard left join " + n.tick()
                    + " t on t.id = e.tick_id where t.snapshot is null or o.xid >= pg_snapshot_xmin(t.snapshot)"
                    + " and not pg_visible_in_snapshot(o.xid, t.snapshot)")) {
                rs.next();
                if (rs.getLong(1) > 0) {
                    failed.add(rs.getLong(1) + " committed rows not relayed (shards " + shardList(rs.getArray(2))
                            + ")");
                }
            }
        }
        var standalone = XidPartitions.forBootstrap(n, config.partitions()).orphans(c).stream()
                .map(XidPartitions.Orphan::table).toList();
        if (!standalone.isEmpty()) {
            failed.add(standalone.size() + " standalone ranges (" + standalone + "), whose rows lie outside the outbox:"
                    + " restore or drop them first (OutboxAdmin.status() lists them)");
        }
        if (new XidRecovery(n, new Resolved(config, from, guard)).check(c).halted()) {
            failed.add("1 xid regression (the xid counter is below a tick's xmax): start with the current count, so"
                    + " the relay recovers it first");
        }
        return failed;
    }

    private static String shardList(Array shards) throws SQLException {
        return Arrays.toString((Object[]) shards.getArray());
    }

    private static Duration max(Duration a, Duration b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    /**
     * Fresh table: creates {@code premake} ranges and the tail. Existing table: first finishes a detach left pending
     * (an interrupted range drop's, which would fail the next one with 55000, or an operator's tail detach), then the
     * same extension maintenance runs: the tail swap, the one ACCESS EXCLUSIVE on the parent, taken without waiting
     * (and {@code outbox.partitions.swapLockTimeout}), after waiting up to {@code LOCK_TIMEOUT} for relay steps in
     * flight. A lock failure there (running transactions in a rolling deploy) is logged and left to
     * maintenance while inserts at the current xid still find a partition. A hot tail is left to maintenance too:
     * turning it into a range scans it, which would hold the schema lock and every other instance's startup.
     * Partition DDL is serialised by {@code LOCK_MAINTENANCE}, taken here after {@code LOCK_SCHEMA};
     * {@link Maintenance} takes it with {@code pg_try_advisory_lock} and never takes {@code LOCK_SCHEMA}, so the two
     * can never deadlock.
     */
    private void ensurePartitions(Connection c, TableNames n) throws SQLException {
        var partitions = XidPartitions.forBootstrap(n, config.partitions());
        partitions.warnIfActivityHidden(c); // once per start
        partitions.logTailConversion();
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
        if (!indexMissing(c, index)) {
            return; // CREATE INDEX IF NOT EXISTS would still queue a SHARE lock behind running writers
        }
        try (var st = c.createStatement()) {
            st.execute("create index " + spelled.unqualified(index) + " on " + definition);
        } catch (SQLException e) {
            if (!DUPLICATE_TABLE.equals(e.getSQLState())) { // made meanwhile, by a dead-index build
                throw e;
            }
        }
    }

    private static boolean indexMissing(Connection c, String index) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?) is null")) {
            ps.setString(1, index);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
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
    private void checkXidCounter(Connection c, TableNames n, OutboxConfig resolved) throws SQLException {
        var recovery = new XidRecovery(n, resolved);
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
