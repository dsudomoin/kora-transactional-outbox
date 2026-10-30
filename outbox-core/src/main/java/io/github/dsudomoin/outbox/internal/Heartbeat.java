package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxTransactions;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps this instance's row in {@code <base>_instance} fresh: {@code (instance_id, shards, last_seen, hook_active,
 * expires_at, relay, contracts)}, which the bootstrap first writes ({@link OutboxSchema}), upserted right after
 * {@link #start()} and then every {@code maintenanceInterval} on the daemon thread {@code outbox-heartbeat}, by the
 * database clock, with {@code expires_at} {@value OutboxSchema#LIVE_INTERVALS} intervals ahead, so readers need not
 * know this instance's interval. {@code hook_active}: the relay runs here and its dead-letter hook is active;
 * {@code relay}: the relay runs here; {@code contracts}: the version each typed route reads, by topic ({@code {}}
 * without one). First, in a statement of its own, a beat reads the live relay instances' reports into the instance's
 * {@link ContractGuard}. Then one statement deletes other instances' rows expired for {@link #FORGET_AFTER} (instances
 * that died without a clean shutdown) and upserts the row, committed on their own. Last, in a transaction of its own,
 * it reads the cursors' shard count into the instance's {@link ShardGuard}, so a writer-only instance stops writing
 * within one beat of another instance's reshard. {@link #stop()} deletes the row; a beat that commits after it deletes
 * it again. A failed beat is logged, and the next one retries. A beat waits at most {@code 100 ms} for a lock: a
 * recovery holds the cursor table for its run, so the shard count then stands as last read, with a one-line WARN; the
 * reports were read and the row refreshed before, so the guards of other instances keep counting this one.
 */
public final class Heartbeat {

    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);
    static final Duration FORGET_AFTER = Duration.ofHours(1);

    private final OutboxTransactions db;
    private final TableNames names;
    private final String id;
    private final int shards;
    private final boolean hookActive;
    private final boolean relay;
    private final Duration interval;
    private final ShardGuard guard;
    private final ContractGuard contracts;
    private final String beat;
    private ScheduledExecutorService scheduler;
    private volatile boolean stopped;

    /** {@code config}: the instance's, as {@link OutboxSchema#config()} resolved it. */
    public Heartbeat(OutboxTransactions db, OutboxSchema schema, OutboxConfig config, boolean hookActive) {
        this.db = db;
        this.names = schema.names();
        this.id = schema.instanceId();
        this.shards = config.shards();
        this.hookActive = hookActive;
        this.relay = config.relay().enabled();
        this.interval = config.partitions().maintenanceInterval();
        this.guard = schema.shardGuard();
        this.contracts = schema.contractGuard();
        this.beat = "with gone as (delete from " + names.instance() + " where instance_id <> ? and coalesce(expires_at,"
                + " last_seen) < now() - " + FORGET_AFTER.toSeconds() + " * interval '1 second') " + upsert(names);
    }

    /** {@code <host>/<pid>/<8 hex>}, new on every call. */
    static String newId() {
        return Host.NAME + "/" + ProcessHandle.current().pid() + "/"
                + String.format("%08x", ThreadLocalRandom.current().nextInt());
    }

    /** The upsert of a row; {@link #bind} sets its parameters. */
    static String upsert(TableNames names) {
        return "insert into " + names.instance() + " (instance_id, shards, hook_active, relay, contracts, last_seen,"
                + " expires_at) values (?, ?, ?, ?, (select coalesce(jsonb_object_agg(t, v), '{}') from"
                + " unnest(?::text[], ?::int[]) u(t, v)), now(), now() + make_interval(secs => ?)) on conflict"
                + " (instance_id) do update set shards = excluded.shards, hook_active = excluded.hook_active,"
                + " relay = excluded.relay, contracts = excluded.contracts, last_seen = excluded.last_seen,"
                + " expires_at = excluded.expires_at";
    }

    /** Binds {@link #upsert}'s parameters after the first {@code before} ones of {@code ps}. */
    static void bind(PreparedStatement ps, int before, String id, int shards, boolean hookActive, boolean relay,
            Map<String, Integer> contracts, Duration interval) throws SQLException {
        ps.setString(before + 1, id);
        ps.setInt(before + 2, shards);
        ps.setBoolean(before + 3, hookActive);
        ps.setBoolean(before + 4, relay);
        var topics = new Object[contracts.size()];
        var versions = new Object[contracts.size()];
        int i = 0;
        for (var e : contracts.entrySet()) {
            topics[i] = e.getKey();
            versions[i++] = e.getValue();
        }
        ps.setArray(before + 5, ps.getConnection().createArrayOf("text", topics));
        ps.setArray(before + 6, ps.getConnection().createArrayOf("int4", versions));
        ps.setDouble(before + 7, interval.multipliedBy(OutboxSchema.LIVE_INTERVALS).toNanos() / 1e9);
    }

    /** Deletes the row of {@code id}; a failure is a WARN, as the row expires anyway. */
    static void delete(OutboxTransactions db, TableNames names, String id) {
        try (var c = db.newConnection(); var ps = c.prepareStatement(
                "delete from " + names.instance() + " where instance_id = ?")) {
            c.setAutoCommit(true);
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (Exception e) {
            log.warn("outbox instance {} could not delete its row from {}; it expires by itself: {}", id,
                    names.instance(), e.toString());
        }
    }

    public synchronized void start() {
        if (scheduler != null || stopped) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "outbox-heartbeat");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::beat, 0, interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Stops the beats, waiting up to 5 s for one in flight, then deletes the row. */
    public synchronized void stop() {
        stopped = true;
        var s = scheduler;
        scheduler = null;
        if (s != null) {
            s.shutdownNow();
            try {
                s.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        delete(db, names, id);
    }

    void beat() { // package-private for a test
        boolean refreshed = false;
        try (var c = db.newConnection()) {
            c.setAutoCommit(false);
            lockTimeout(c);
            contracts.refresh(c, names); // before the statement below, which may give up on its lock
            try (var ps = c.prepareStatement(beat)) {
                ps.setString(1, id); // the cleanup's; the upsert's parameters follow
                bind(ps, 1, id, shards, hookActive, relay, contracts.contracts(), interval);
                ps.executeUpdate();
            }
            if (stopped) {
                c.rollback(); // stop() deleted the row, or is about to
                return;
            }
            c.commit(); // before the shard count, whose lock a recovery may hold for its run
            refreshed = true;
            if (stopped) { // stop() deleted it before this commit
                delete(db, names, id);
                return;
            }
            lockTimeout(c);
            try (var st = c.createStatement(); var rs = st.executeQuery(Cursors.shardCount(names))) {
                rs.next();
                guard.observed(rs.getInt(1));
            }
            c.commit();
        } catch (Throwable t) { // logged, never thrown: a throw would cancel every later beat without a word
            if (refreshed && t instanceof SQLException e && "55P03".equals(e.getSQLState())) {
                log.warn("outbox heartbeat of instance {} into {} waited {} ms for a lock (a recovery holds {} for its"
                        + " run?); the shard count stands as last read, retrying in {}", id, names.instance(),
                        Maintenance.SHARDS_LOCK_TIMEOUT.toMillis(), names.cursor(), interval);
                return;
            }
            log.warn("outbox heartbeat of instance {} into {} failed; retrying in {}", id, names.instance(), interval,
                    t);
        }
    }

    /** Bounds the lock waits of the transaction in progress; a transaction's setting, so none leaks. */
    private static void lockTimeout(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("set local lock_timeout = " + Maintenance.SHARDS_LOCK_TIMEOUT.toMillis());
        }
    }

    /** Looked up once per JVM: {@code HOSTNAME} (set in containers), else a lookup that may take seconds. */
    private static final class Host {
        static final String NAME = name();

        private static String name() {
            var env = System.getenv("HOSTNAME");
            if (env != null && !env.isBlank()) {
                return env;
            }
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                return "unknown-host";
            }
        }
    }
}
