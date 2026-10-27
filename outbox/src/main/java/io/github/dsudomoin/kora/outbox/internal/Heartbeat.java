package io.github.dsudomoin.kora.outbox.internal;

import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * This instance's row in {@code <base>_instance}: {@code (instance_id, shards, last_seen, hook_active)}, upserted
 * right after {@link #start()} and then every {@code maintenanceInterval} on the daemon thread
 * {@code outbox-heartbeat}, by the database clock, and deleted by {@link #stop()}. Its id is
 * {@code <host>/<pid>/<8 hex>}, new on every start. The same statement deletes the rows of other instances unseen for
 * {@link #FORGET_AFTER} or 10 × the interval, whichever is longer (instances that died without a clean shutdown). A
 * failed beat is a WARN; the next one retries. The bootstrap's reshard guard ({@link OutboxSchema}), the hookless
 * dead-letter pass ({@link DeadLetterPass}) and {@code OutboxAdmin.status()} read the rows.
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
    private final String beat;
    private ScheduledExecutorService scheduler;

    public Heartbeat(JdbcConnectionFactory db, TableNames names, int shards, boolean hookActive, Duration interval) {
        this.db = db;
        this.names = names;
        this.id = Host.NAME + "/" + ProcessHandle.current().pid() + "/"
                + String.format("%08x", ThreadLocalRandom.current().nextInt());
        this.shards = shards;
        this.hookActive = hookActive;
        this.interval = interval;
        var forget = interval.multipliedBy(10).compareTo(FORGET_AFTER) > 0 ? interval.multipliedBy(10) : FORGET_AFTER;
        this.beat = "with gone as (delete from " + names.instance() + " where instance_id <> ? and last_seen < now() - "
                + forget.toSeconds() + " * interval '1 second') insert into " + names.instance()
                + " (instance_id, shards, hook_active, last_seen) values (?, ?, ?, now()) on conflict (instance_id)"
                + " do update set shards = excluded.shards, hook_active = excluded.hook_active, last_seen = now()";
    }

    public String id() {
        return id;
    }

    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "outbox-heartbeat");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::beat, 0, interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Stops the beats, waiting up to 5 s for one in flight, then deletes the row; a failure is a WARN. */
    public synchronized void stop() {
        var s = scheduler;
        if (s == null) {
            return;
        }
        scheduler = null;
        s.shutdownNow();
        try {
            s.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try (var c = db.newConnection(); var ps = c.prepareStatement(
                "delete from " + names.instance() + " where instance_id = ?")) {
            c.setAutoCommit(true);
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (Exception e) {
            log.warn("outbox instance {} could not delete its row from {}; it is forgotten once unseen for {}: {}", id,
                    names.instance(), FORGET_AFTER, e.toString());
        }
    }

    private void beat() {
        try (var c = db.newConnection(); var ps = c.prepareStatement(beat)) {
            c.setAutoCommit(true);
            ps.setString(1, id);
            ps.setString(2, id);
            ps.setInt(3, shards);
            ps.setBoolean(4, hookActive);
            ps.executeUpdate();
        } catch (Exception e) { // the next beat retries
            log.warn("outbox heartbeat of instance {} into {} failed; retrying in {}: {}", id, names.instance(),
                    interval, e.toString());
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
