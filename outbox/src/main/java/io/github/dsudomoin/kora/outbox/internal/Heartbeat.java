package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import java.net.InetAddress;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * Keeps this instance's row in {@code <base>_instance} fresh: {@code (instance_id, shards, last_seen, hook_active,
 * expires_at)}, which the bootstrap first writes ({@link OutboxSchema}), upserted right after {@link #start()} and then
 * every {@code maintenanceInterval} on the daemon thread {@code outbox-heartbeat}, by the database clock, with
 * {@code expires_at} {@value OutboxSchema#LIVE_INTERVALS} intervals ahead, so readers need not know this instance's
 * interval. {@code hook_active}: the relay runs here and its dead-letter hook is active. The same statement deletes
 * other instances' rows expired for {@link #FORGET_AFTER} (instances that died without a clean shutdown) and reads the
 * cursors' shard count into the instance's {@link ShardGuard}, so a writer-only instance stops writing within one beat
 * of another instance's reshard. {@link #stop()} deletes the row; a beat that commits after it deletes it again. A
 * failed beat is logged, and the next one retries. A beat waits at most {@code 100 ms} for a lock (a recovery holds
 * the cursor table for its run): then the shard count stands as last read, with a one-line WARN.
 */
public final class Heartbeat {

    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);
    static final Duration FORGET_AFTER = Duration.ofHours(1);

    private final JdbcConnectionFactory db;
    private final TableNames names;
    private final String id;
    private final int shards;
    private final boolean hookActive;
    private final Duration interval;
    private final ShardGuard guard;
    private final String beat;
    private ScheduledExecutorService scheduler;
    private volatile boolean stopped;

    /** {@code config}: the instance's, as {@link OutboxSchema#config()} resolved it. */
    public Heartbeat(JdbcConnectionFactory db, OutboxSchema schema, OutboxConfig config, boolean hookActive) {
        this.db = db;
        this.names = schema.names();
        this.id = schema.instanceId();
        this.shards = config.shards();
        this.hookActive = hookActive;
        this.interval = config.partitions().maintenanceInterval();
        this.guard = schema.shardGuard();
        this.beat = "with gone as (delete from " + names.instance() + " where instance_id <> ? and coalesce(expires_at,"
                + " last_seen) < now() - " + FORGET_AFTER.toSeconds() + " * interval '1 second'), beat as ("
                + upsert(names) + ") " + Cursors.shardCount(names);
    }

    /** {@code <host>/<pid>/<8 hex>}, new on every call. */
    static String newId() {
        return Host.NAME + "/" + ProcessHandle.current().pid() + "/"
                + String.format("%08x", ThreadLocalRandom.current().nextInt());
    }

    /** The upsert of a row; {@link #bind} sets its parameters. */
    static String upsert(TableNames names) {
        return "insert into " + names.instance() + " (instance_id, shards, hook_active, last_seen, expires_at)"
                + " values (?, ?, ?, now(), now() + make_interval(secs => ?)) on conflict (instance_id) do update set"
                + " shards = excluded.shards, hook_active = excluded.hook_active, last_seen = excluded.last_seen,"
                + " expires_at = excluded.expires_at";
    }

    /** Binds {@link #upsert}'s parameters after the first {@code before} ones of {@code ps}. */
    static void bind(PreparedStatement ps, int before, String id, int shards, boolean hookActive, Duration interval)
            throws SQLException {
        ps.setString(before + 1, id);
        ps.setInt(before + 2, shards);
        ps.setBoolean(before + 3, hookActive);
        ps.setDouble(before + 4, interval.multipliedBy(OutboxSchema.LIVE_INTERVALS).toNanos() / 1e9);
    }

    /** Deletes the row of {@code id}; a failure is a WARN, as the row expires anyway. */
    static void delete(JdbcConnectionFactory db, TableNames names, String id) {
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
        try (var c = db.newConnection()) {
            c.setAutoCommit(false);
            try (var st = c.createStatement()) { // a transaction's: no leak
                st.execute("set local lock_timeout = " + Maintenance.SHARDS_LOCK_TIMEOUT.toMillis());
            }
            try (var ps = c.prepareStatement(beat)) {
                ps.setString(1, id); // the cleanup's; the upsert's parameters follow
                bind(ps, 1, id, shards, hookActive, interval);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    guard.observed(rs.getInt(1));
                }
            }
            if (stopped) {
                c.rollback(); // stop() deleted the row, or is about to
                return;
            }
            c.commit();
            if (stopped) { // stop() deleted it before this commit
                delete(db, names, id);
            }
        } catch (Throwable t) { // logged, never thrown: a throw would cancel every later beat without a word
            if (t instanceof SQLException e && "55P03".equals(e.getSQLState())) {
                log.warn("outbox heartbeat of instance {} into {} waited {} ms for a lock (a recovery holds {} for its"
                        + " run?); the shard count stands as last read, retrying in {}", id, names.instance(),
                        Maintenance.SHARDS_LOCK_TIMEOUT.toMillis(), names.cursor(), interval);
                return;
            }
            log.warn("outbox heartbeat of instance {} into {} failed; retrying in {}", id, names.instance(), interval,
                    t);
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
