package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inbox range partitions. A range {@code [a, b)} holds the message ids whose UUIDv7 timestamp lies in
 * {@code [a, b)} ms, as bounds {@link UuidV7#lowerBound}{@code (a)} and {@code (b)} (uuids compare bytewise). Ranges
 * are {@code partitionInterval} long, aligned to multiples of it since the epoch, named
 * {@code <inbox>_t<yyyyMMddHH>} after their lower bound in UTC ({@code yyyyMMddHHmm} when the interval is not whole
 * hours). They are made from {@code now - retention} on a fresh table up to past {@code now + premake}, and dropped
 * once their upper bound is older than {@code now - retention}. The DDL methods need an autocommit connection, and
 * callers serialise them under {@code LOCK_INBOX_MAINTENANCE}.
 */
final class InboxPartitions {

    private static final Logger log = LoggerFactory.getLogger(InboxPartitions.class);
    private static final long HOUR = Duration.ofHours(1).toMillis();
    private static final long LOG_INTERVAL = TimeUnit.MINUTES.toNanos(1);

    /** A standalone range {@link #dropOrphans} kept, with the bounds its name gives. */
    record Orphan(String table, String from, String to) {}
    private static final int MAX_RANGES = 1000;

    /**
     * Timestamps (ms) that the live ranges cover with one interval of margin on both sides:
     * {@code [lowest lower + interval, highest upper - interval)}. Ids outside it are not deduplicated.
     */
    record Window(long from, long to) {

        static final Window EMPTY = new Window(Long.MAX_VALUE, Long.MIN_VALUE);

        boolean contains(long millis) {
            return from <= millis && millis < to;
        }
    }

    private final String table;
    private final long interval;
    private final long premake;
    private final long retention;
    private final DateTimeFormatter suffix;
    /** Per table whose name is no valid time, when its WARN was last logged ({@code nanoTime}). */
    private final Map<String, Long> unparsableLogged = new ConcurrentHashMap<>();

    /**
     * Validates the {@code outbox.inbox} keys against each other, so the window spans at least one interval (ids up
     * to {@code retention - interval} old are always deduplicated) and maintenance extends before its top reaches
     * now.
     */
    InboxPartitions(String table, OutboxConfig config) {
        this.table = table;
        var cfg = config.inbox();
        var interval = cfg.partitionInterval();
        var twice = interval.multipliedBy(2);
        var maintenance = config.partitions().maintenanceInterval();
        require(interval.compareTo(Duration.ofMinutes(1)) >= 0,
                "outbox.inbox.partitionInterval must be at least 1m: " + interval);
        require(cfg.retention().compareTo(twice) >= 0, "outbox.inbox.retention must be at least 2 ×"
                + " outbox.inbox.partitionInterval (" + twice + "): " + cfg.retention());
        require(cfg.premake().compareTo(twice) >= 0, "outbox.inbox.premake must be at least 2 ×"
                + " outbox.inbox.partitionInterval (" + twice + "): " + cfg.premake());
        require(cfg.premake().minus(interval).compareTo(maintenance) > 0, "outbox.inbox.premake ("
                + cfg.premake() + ") minus outbox.inbox.partitionInterval (" + interval + ") must exceed"
                + " outbox.partitions.maintenanceInterval: " + maintenance);
        var span = cfg.retention().plus(cfg.premake());
        require(interval.multipliedBy(MAX_RANGES).compareTo(span) >= 0, "outbox.inbox.partitionInterval must be at"
                + " least (outbox.inbox.retention + outbox.inbox.premake) / " + MAX_RANGES + " ("
                + span.dividedBy(MAX_RANGES) + "), at most " + MAX_RANGES + " ranges: " + interval);
        this.interval = interval.toMillis();
        this.premake = cfg.premake().toMillis();
        this.retention = cfg.retention().toMillis();
        // an interval of at least a minute (an hour) puts every lower bound in a minute (an hour) of its own
        this.suffix = DateTimeFormatter.ofPattern(this.interval % HOUR == 0 ? "yyyyMMddHH" : "yyyyMMddHHmm")
                .withZone(ZoneOffset.UTC);
    }

    /**
     * Makes ranges from the highest upper bound, or from {@code now - retention} when there is none or it lies below,
     * until one reaches past {@code now + premake}. Returns the number made.
     */
    int ensurePremade(Connection c, long now) throws SQLException {
        long from = floor(now - retention);
        for (var b : PartitionDdl.listRanges(c, table)) {
            from = Math.max(from, millis(b.to()));
        }
        int made = 0;
        for (; from < now + premake; from += interval, made++) {
            PartitionDdl.createAndAttach(c, table, table + "_t" + suffix.format(Instant.ofEpochMilli(from)),
                    bound(from), bound(from + interval), PartitionDdl.CREATE_LOCK_TIMEOUT);
        }
        return made;
    }

    /**
     * Detaches concurrently and drops, lowest first, every range whose upper bound is older than
     * {@code now - retention}. A lock timeout (SQLState 55P03) surfaces as {@link SQLException} and leaves the rest
     * to the next run. Returns the number dropped.
     */
    int dropExpired(Connection c, long now, Duration lockTimeout) throws SQLException {
        var expired = PartitionDdl.listRanges(c, table).stream()
                .filter(b -> millis(b.to()) < now - retention)
                .sorted(Comparator.comparingLong(b -> millis(b.from())))
                .toList();
        for (var b : expired) {
            PartitionDdl.detachConcurrentlyAndDrop(c, table, b.name(), lockTimeout);
        }
        return expired.size();
    }

    /**
     * Drops the ranges left standalone ({@code <inbox>_t<time>} in no partition tree, which {@link #dropExpired} no
     * longer lists), as a run that died, or whose DROP failed, after its detach leaves them, by the same rule: only
     * when the whole range its name gives lies past {@code now - retention}, and every id in it is older than that,
     * checked on the data. One inside the dedup window is never dropped nor read, and is returned with the bounds its
     * name gives: its ids are not deduplicated, and inserts of them fail where the inbox has ranges on both sides of
     * it. A name that is no valid time is skipped with a WARN, at most once a minute per name.
     */
    List<Orphan> dropOrphans(Connection c, long now, Duration lockTimeout) throws SQLException {
        var bound = bound(now - retention);
        var kept = new ArrayList<Orphan>();
        for (var t : PartitionDdl.standalone(c, table, "_t[0-9]{10}([0-9]{2})?")) {
            long from;
            try {
                from = namedFrom(t);
            } catch (DateTimeException e) { // a user table that only looks like a range
                long nowNanos = System.nanoTime();
                var last = unparsableLogged.get(t);
                if (last == null || nowNanos - last >= LOG_INTERVAL) {
                    unparsableLogged.put(t, nowNanos);
                    log.warn("outbox inbox table {} is named like a range of {} but its name is no valid time ({});"
                            + " skipped, this WARN repeats at most once a minute per table", t, table, e.getMessage());
                }
                continue;
            }
            var orphan = new Orphan(t, bound(from), bound(from + interval));
            // inside the window: kept and reported without a read of it; past it: kept when a read finds newer ids
            if (from + interval >= now - retention || !PartitionDdl.dropUnlessFound(
                    c, t, "select 1 from {table} where message_id >= ?::uuid limit 1", bound, lockTimeout)) {
                kept.add(orphan);
            }
        }
        return kept;
    }

    /**
     * Attaches an orphan {@link #dropOrphans} kept back with the bounds its name gives (ATTACH takes SHARE UPDATE
     * EXCLUSIVE on the inbox, which no writer waits for), but only when its range lies in or next to the span of the
     * attached ranges (the {@link #window} without its margins): the window takes them as contiguous, so one further
     * off would put ranges that are missing into it, and inserts of their ids would fail; premake never makes ranges
     * below the highest. Returns false then. A failed attach (an overlapping range, a row outside the bounds, a lock
     * timeout) surfaces as {@link SQLException} and, in autocommit, changes nothing.
     */
    boolean attachBack(Connection c, Orphan orphan) throws SQLException {
        var w = window(c); // EMPTY gives no span, and its sums do not overflow
        if (millis(orphan.from()) > w.to() + interval || millis(orphan.to()) < w.from() - interval) {
            return false;
        }
        PartitionDdl.attach(c, table, orphan.table(), orphan.from(), orphan.to(), PartitionDdl.CREATE_LOCK_TIMEOUT);
        return true;
    }

    /** The lower bound (ms) a range's name gives: {@code <inbox>_t<yyyyMMddHH[mm]>}, UTC. */
    private static long namedFrom(String partition) {
        var d = partition.substring(partition.lastIndexOf("_t") + 2);
        return LocalDateTime.of(Integer.parseInt(d.substring(0, 4)), Integer.parseInt(d.substring(4, 6)),
                        Integer.parseInt(d.substring(6, 8)), Integer.parseInt(d.substring(8, 10)),
                        d.length() == 12 ? Integer.parseInt(d.substring(10, 12)) : 0)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }

    /**
     * Finalizes a range whose detach was left pending, and drops it when its whole range is past retention, as
     * {@link #dropExpired} would; one inside the dedup window (an operator's detach) is only finalized, and
     * {@link #dropOrphans} reports it ({@link #attachBack} may attach it back). The pending detach must be finished
     * either way: Postgres allows one per parent and fails every other {@code DETACH ... CONCURRENTLY} meanwhile
     * (SQLState 55000).
     */
    void finishPendingDetach(Connection c, long now, Duration lockTimeout) throws SQLException {
        var pending = PartitionDdl.pendingDetach(c, table);
        if (pending == null) {
            return;
        }
        boolean expired = PartitionDdl.listRanges(c, table).stream()
                .anyMatch(b -> b.name().equals(pending) && b.to() != null && millis(b.to()) < now - retention);
        if (expired) {
            PartitionDdl.detachConcurrentlyAndDrop(c, table, pending, lockTimeout);
        } else { // an operator's detach inside the window: its dedup records are kept, dropOrphans reports them
            PartitionDdl.detach(c, table, pending, lockTimeout);
        }
    }

    /** The {@link Window} of the attached ranges; a range whose detach is pending takes no rows and does not count. */
    Window window(Connection c) throws SQLException {
        var pending = PartitionDdl.pendingDetach(c, table);
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        for (var b : PartitionDdl.listRanges(c, table)) {
            if (!b.name().equals(pending)) {
                lowest = Math.min(lowest, millis(b.from()));
                highest = Math.max(highest, millis(b.to()));
            }
        }
        return lowest == Long.MAX_VALUE ? Window.EMPTY : new Window(lowest + interval, highest - interval);
    }

    /**
     * The lowest timestamp still safe to insert at {@code now}: ranges below it may be dropped any moment, by another
     * instance since this one last refreshed its {@link Window} or with a clock up to one interval ahead. A gap that
     * a long-dormant table left between its old ranges and the new ones lies below it too.
     */
    long safeFrom(long now) {
        return floor(now - retention) + interval;
    }

    private long floor(long millis) {
        return Math.floorDiv(millis, interval) * interval;
    }

    private static long millis(String bound) {
        return UuidV7.timestampMillis(UUID.fromString(bound));
    }

    private static String bound(long millis) {
        return UuidV7.lowerBound(millis).toString();
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
