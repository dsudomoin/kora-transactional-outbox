package io.github.dsudomoin.kora.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Per-shard progress as an insert-only log {@code <base>_cursor}: the row with the highest {@code seq} of
 * a shard is its current cursor. Writers append under the shard's advisory lock; maintenance deletes superseded rows.
 */
public final class Cursors {

    /**
     * @param tickId last fully processed tick
     * @param targetTickId tick the batch in progress runs to; null when no batch is in progress
     * @param lastId progress inside the batch {@code (tickId, targetTickId]}
     * @param pausedUntil shard backoff while the target is down
     * @param failures consecutive whole-batch failures at this position
     * @param retryDueAt earliest {@code next_attempt_at} of the shard's retry heads
     * @param parkedDueAt earliest {@code due_at} of the shard's parked rows; null when it has none
     */
    public record Cursor(
            int shard,
            long tickId,
            @Nullable Long targetTickId,
            long lastId,
            @Nullable Instant pausedUntil,
            int failures,
            @Nullable Instant retryDueAt,
            @Nullable Instant parkedDueAt) {

        public Cursor withProgress(long tickId, @Nullable Long target, long lastId) {
            return new Cursor(shard, tickId, target, lastId, pausedUntil, failures, retryDueAt, parkedDueAt);
        }

        public Cursor withPause(Instant until, int failures) {
            return new Cursor(shard, tickId, targetTickId, lastId, until, failures, retryDueAt, parkedDueAt);
        }

        public Cursor withRetryDueAt(@Nullable Instant retryDueAt) {
            return new Cursor(shard, tickId, targetTickId, lastId, pausedUntil, failures, retryDueAt, parkedDueAt);
        }

        public Cursor withParkedDueAt(@Nullable Instant parkedDueAt) {
            return new Cursor(shard, tickId, targetTickId, lastId, pausedUntil, failures, retryDueAt, parkedDueAt);
        }

        /** No failures, not paused. */
        public Cursor cleared() {
            return new Cursor(shard, tickId, targetTickId, lastId, null, 0, retryDueAt, parkedDueAt);
        }
    }

    /**
     * A shard's latest cursor; {@code behind}: its tick is below the latest tick and a step has to move it up, as a
     * tick in between has the shard's bit set in its {@code dirty} or none (null, or drawn from another shard count:
     * every shard), or its tick is {@link ShardProcessor#IDLE_APPEND_AFTER} or more older than the latest (or newer:
     * the database clock stepped back), so an idle step appends, or its tick is gone.
     */
    public record Position(Cursor cursor, boolean behind) {}

    private static final String COLUMNS =
            "shard, tick_id, target_tick_id, last_id, paused_until, failures, retry_due_at, parked_due_at";

    private final TableNames names;
    private final String positions;

    public Cursors(TableNames names) {
        this.names = names;
        // the tick window of a shard is scanned only while its tick is younger than IDLE_APPEND_AFTER: a few seconds
        // of ticks, however far a paused shard holds tick cleanup back
        this.positions = "select c.*, c.tick_id < p.latest and case when l.created_at is null or t.created_at is null"
                + " or l.created_at < t.created_at or l.created_at >= t.created_at + p.idle then true"
                + " else exists (select 1 from " + names.tick() + " n where n.id > c.tick_id and n.id <= p.latest"
                + " and case when n.dirty is null or length(n.dirty) <> p.shards then true"
                + " else get_bit(n.dirty, c.shard) = 1 end) end"
                + " from (select ?::bigint latest, ? * interval '1 microsecond' idle, ?::int shards) p"
                + " cross join generate_series(0, p.shards - 1) s cross join lateral (select " + COLUMNS + " from "
                + names.cursor() + " where shard = s order by seq desc limit 1) c"
                + " left join " + names.tick() + " t on t.id = c.tick_id"
                + " left join " + names.tick() + " l on l.id = p.latest order by s";
    }

    /**
     * Latest cursor of shards {@code 0..shards-1}, in shard order: one index probe per shard. A shard without rows is
     * absent, so callers key results by {@link Cursor#shard()}, never by list position.
     */
    public List<Cursor> latestAll(Connection c, int shards) throws SQLException {
        try (var ps = c.prepareStatement("select c.* from generate_series(0, ? - 1) s cross join lateral (select "
                + COLUMNS + " from " + names.cursor() + " where shard = s order by seq desc limit 1) c order by s")) {
            ps.setInt(1, shards);
            try (var rs = ps.executeQuery()) {
                var result = new ArrayList<Cursor>(shards);
                while (rs.next()) {
                    result.add(cursor(rs));
                }
                return result;
            }
        }
    }

    /**
     * {@link #latestAll} with each shard's {@link Position#behind()} up to the tick {@code latestTickId}: one statement,
     * per shard an index probe for the cursor and its tick and, while that tick is young, a scan of the ticks after it.
     */
    public List<Position> positions(Connection c, int shards, long latestTickId) throws SQLException {
        try (var ps = c.prepareStatement(positions)) {
            ps.setLong(1, latestTickId);
            ps.setLong(2, TimeUnit.MICROSECONDS.convert(ShardProcessor.IDLE_APPEND_AFTER));
            ps.setInt(3, shards);
            try (var rs = ps.executeQuery()) {
                var result = new ArrayList<Position>(shards);
                while (rs.next()) {
                    result.add(new Position(cursor(rs), rs.getBoolean(9)));
                }
                return result;
            }
        }
    }

    /** Throws {@link IllegalStateException} when the shard has no cursor (bootstrap creates one per shard). */
    public Cursor latest(Connection c, int shard) throws SQLException {
        try (var ps = c.prepareStatement("select " + COLUMNS + " from " + names.cursor()
                + " where shard = ? order by seq desc limit 1")) {
            ps.setInt(1, shard);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("no cursor for shard " + shard + " in " + names.cursor());
                }
                return cursor(rs);
            }
        }
    }

    public void append(Connection c, Cursor cursor) throws SQLException {
        try (var ps = c.prepareStatement(
                "insert into " + names.cursor() + " (" + COLUMNS + ") values (?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setInt(1, cursor.shard());
            ps.setLong(2, cursor.tickId());
            ps.setObject(3, cursor.targetTickId(), Types.BIGINT);
            ps.setLong(4, cursor.lastId());
            ps.setObject(5, utc(cursor.pausedUntil()), Types.TIMESTAMP_WITH_TIMEZONE);
            ps.setInt(6, cursor.failures());
            ps.setObject(7, utc(cursor.retryDueAt()), Types.TIMESTAMP_WITH_TIMEZONE);
            ps.setObject(8, utc(cursor.parkedDueAt()), Types.TIMESTAMP_WITH_TIMEZONE);
            ps.executeUpdate();
        }
    }

    /** Deletes every row that is not the newest of its shard; returns the number deleted. */
    public int deleteSuperseded(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            return st.executeUpdate("delete from " + names.cursor() + " c where c.seq < (select max(m.seq) from "
                    + names.cursor() + " m where m.shard = c.shard)");
        }
    }

    private static Cursor cursor(ResultSet rs) throws SQLException {
        return new Cursor(
                rs.getInt(1),
                rs.getLong(2),
                rs.getObject(3, Long.class),
                rs.getLong(4),
                instant(rs.getObject(5, OffsetDateTime.class)),
                rs.getInt(6),
                instant(rs.getObject(7, OffsetDateTime.class)),
                instant(rs.getObject(8, OffsetDateTime.class)));
    }

    @Nullable
    private static Instant instant(@Nullable OffsetDateTime t) {
        return t == null ? null : t.toInstant();
    }

    @Nullable
    private static OffsetDateTime utc(@Nullable Instant t) {
        return t == null ? null : t.atOffset(ZoneOffset.UTC);
    }
}
