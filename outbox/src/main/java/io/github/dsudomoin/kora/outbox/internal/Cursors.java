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
     */
    public record Cursor(
            int shard,
            long tickId,
            @Nullable Long targetTickId,
            long lastId,
            @Nullable Instant pausedUntil,
            int failures,
            @Nullable Instant retryDueAt) {

        public Cursor withProgress(long tickId, @Nullable Long target, long lastId) {
            return new Cursor(shard, tickId, target, lastId, pausedUntil, failures, retryDueAt);
        }

        public Cursor withPause(Instant until, int failures) {
            return new Cursor(shard, tickId, targetTickId, lastId, until, failures, retryDueAt);
        }

        public Cursor withRetryDueAt(@Nullable Instant retryDueAt) {
            return new Cursor(shard, tickId, targetTickId, lastId, pausedUntil, failures, retryDueAt);
        }

        /** No failures, not paused. */
        public Cursor cleared() {
            return new Cursor(shard, tickId, targetTickId, lastId, null, 0, retryDueAt);
        }
    }

    private static final String COLUMNS =
            "shard, tick_id, target_tick_id, last_id, paused_until, failures, retry_due_at";

    private final TableNames names;

    public Cursors(TableNames names) {
        this.names = names;
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
                "insert into " + names.cursor() + " (" + COLUMNS + ") values (?, ?, ?, ?, ?, ?, ?)")) {
            ps.setInt(1, cursor.shard());
            ps.setLong(2, cursor.tickId());
            ps.setObject(3, cursor.targetTickId(), Types.BIGINT);
            ps.setLong(4, cursor.lastId());
            ps.setObject(5, utc(cursor.pausedUntil()), Types.TIMESTAMP_WITH_TIMEZONE);
            ps.setInt(6, cursor.failures());
            ps.setObject(7, utc(cursor.retryDueAt()), Types.TIMESTAMP_WITH_TIMEZONE);
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
                instant(rs.getObject(7, OffsetDateTime.class)));
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
