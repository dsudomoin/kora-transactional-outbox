package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.internal.Ticks.Tick;
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
 *
 * <p>A shard without new rows writes no cursor row: each maintenance run writes a checkpoint {@code <base>_checkpoint}
 * instead (one row, {@link #checkpoint}): the latest tick L of its read, a {@code busy} bitmap (bit s, leftmost 0, set
 * for a shard with a batch in progress or {@link Position#behind() behind} at L) and {@code busy_tick_id}, the lowest
 * effective tick of the busy shards (L when none). A shard's <em>effective tick</em> is
 * {@code greatest(cursor.tick_id, ckpt.tick_id)} when the newest checkpoint has {@code outbox.shards} bits and the
 * shard's bit is 0, {@code greatest(cursor.tick_id, ckpt.busy_tick_id)} when it is 1, else {@code cursor.tick_id}; a
 * checkpoint on a tick above the newest belongs to another history (tick ids went backwards under it, as in a partial
 * restore) and counts for nothing, and the next maintenance run replaces it.
 * Every step starts from it, and tick cleanup, the partition drop horizon and {@code outbox.shard.lag} go by it.
 * Writing E for a shard's effective tick, every committed row of the shard visible in E is visible in its cursor's
 * tick, so relayed (a batch in progress: up to {@code lastId}, rows past it lie above E). By induction over the
 * checkpoints, from the first (E = the cursor's tick):
 * <ol>
 *   <li>A 0 bit: no batch was in progress, and no tick in (E', L] held the shard (by its {@code dirty}, null as all
 *       shards, the tick after a gap in ids as all shards too), E' its effective tick at the read. By the proof in
 *       {@link Ticks}, the ticks after E' chain, so no row of the shard is visible in L and not in E', and by induction
 *       none visible in E' and not in its cursor's tick: so none up to L. (A bitmap of another length, from an
 *       instance with another shard count, reads as all shards, but an empty one reads as none: instances with
 *       different shard counts on one table are not supported, and the bootstrap refuses cursors of another
 *       count.)</li>
 *   <li>A 1 bit: {@code busy_tick_id} is at most E', so a row visible in it is visible in E', and by induction in the
 *       cursor's tick.</li>
 *   <li>Cursors only move forward, so a cursor appended after the read keeps both. A step that read its cursor and
 *       effective tick before the checkpoint started from E' or above, and reads the same rows as from the new
 *       effective tick, because the range between them is empty for the shard: so a batch it left in progress also
 *       goes on from the effective tick, above its cursor's tick, whose row cleanup may have deleted.</li>
 *   <li>Every effective tick exists: the new ones are L, {@code busy_tick_id} (an effective tick at the read) or above,
 *       a cursor's tick at or above them, and cleanup deletes only ticks below the lowest effective tick, which no
 *       checkpoint lowers: each new effective tick is L or at least {@code busy_tick_id}, which is at least the lowest
 *       effective tick at the read.</li>
 * </ol>
 * A run writes its checkpoint from its own read, in one statement that also deletes the one before (runs hold
 * {@code LOCK_MAINTENANCE}, so one at a time), so readers see exactly one. A step reads its effective tick's row in
 * the statement that reads its cursor and the checkpoint ({@link #latest}): with one snapshot, the checkpoint it sees
 * is the one cleanup went by when that snapshot also sees the cleanup, so the row is there.
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
     * A shard's latest cursor, its effective tick (see the class comment) and that tick's row, read in one statement,
     * so with one snapshot of the checkpoint and the tick table: null only when the tick was deleted by hand.
     */
    public record Current(Cursor cursor, long effectiveTickId, @Nullable Tick tick) {}

    /**
     * A shard's latest cursor and effective tick; {@code behind}: no batch in progress, the effective tick is below the
     * latest tick, and a tick in between may hold the shard's rows: its {@code dirty} has the shard's bit set or is
     * null (or of another non-zero length: every shard), or it follows a gap in tick ids (a tick deleted by hand:
     * every shard), or the effective tick is not among the ticks read: gone, or below the newest checkpoint's tick,
     * which only a shard busy at that checkpoint can be (so the read covers at most the ticks of one maintenance
     * interval, however far a paused shard lags).
     */
    public record Position(Cursor cursor, boolean behind, long effectiveTickId) {}

    private static final String COLUMNS =
            "shard, tick_id, target_tick_id, last_id, paused_until, failures, retry_due_at, parked_due_at";

    private final TableNames names;
    private final int shards;
    private final String latest;
    private final String positions;
    private final String checkpoint;

    /** {@code shards}: {@code outbox.shards}, the length of a checkpoint's {@code busy} bitmap. */
    public Cursors(TableNames names, int shards) {
        this.names = names;
        this.shards = shards;
        var effective = effective(shards);
        // every statement opens the cursor table, then the checkpoint, then the tick table, as XidRecovery locks them
        this.latest = "select c.*, e.eff, t.snapshot::text, t.created_at, t.max_id from (select " + COLUMNS + " from "
                + names.cursor() + " where shard = ? order by seq desc limit 1) c left join " + newestCheckpoint(names)
                + " k on true cross join lateral (select " + effective + " eff) e left join " + names.tick()
                + " t on t.id = e.eff";
        var ones = "repeat('1', " + shards + ")::varbit";
        // one pass over the ticks after the lowest effective tick (not below the checkpoint's): for each tick, the
        // shards that any tick after it up to the latest may hold rows of (a window bit_or, newest first); each shard
        // looks up its effective tick's row. A shard below the window finds none: behind
        this.positions = "with c0 as (select c.* from generate_series(0, " + shards + " - 1) s cross join lateral"
                + " (select " + COLUMNS + " from " + names.cursor() + " where shard = s order by seq desc limit 1) c),"
                + " k as " + newestCheckpoint(names) + ","
                + " c as (select c.*, " + effective + " eff from c0 c left join k on true),"
                + " w as (select id, bit_or(d) over (order by id desc rows between unbounded preceding and 1 preceding)"
                + " after from (select id, case when dirty is null or lag(id) over (order by id) <> id - 1 then " + ones
                + " when length(dirty) = 0 then repeat('0', " + shards + ")::varbit"
                + " when length(dirty) <> " + shards + " then " + ones + " else dirty end d from " + names.tick()
                + " where id <= ? and id >= (select greatest(min(eff) filter (where target_tick_id is null),"
                + " (select tick_id from k where length(busy) = " + shards + ")) from c)) n)"
                + " select c.*, c.target_tick_id is null and c.eff < ? and case when w.id is null then true"
                + " when w.after is null then false else get_bit(w.after, c.shard) = 1 end"
                + " from c left join w on w.id = c.eff order by c.shard";
        this.checkpoint = "with gone as (delete from " + names.checkpoint() + ") insert into " + names.checkpoint()
                + " (tick_id, busy, busy_tick_id) values (?, ?::varbit, ?)";
    }

    /**
     * A subquery of the newest checkpoint's {@code tick_id, busy, busy_tick_id}, for {@link #effective}; none when its
     * tick is above the newest tick (an index probe): it then belongs to another history, as tick ids went backwards
     * under it in a partial restore, and maintenance replaces it.
     */
    static String newestCheckpoint(TableNames names) {
        return "(select tick_id, busy, busy_tick_id from (select tick_id, busy, busy_tick_id from " + names.checkpoint()
                + " order by seq desc limit 1) n where tick_id <= (select max(id) from " + names.tick() + "))";
    }

    /**
     * The effective tick (see the class comment) of the cursor row {@code c} (its {@code shard} and {@code tick_id}),
     * with the {@link #newestCheckpoint} left joined as {@code k}.
     */
    static String effective(int shards) {
        return "case when length(k.busy) = " + shards + " then greatest(c.tick_id, case when get_bit(k.busy, c.shard)"
                + " = 0 then k.tick_id else k.busy_tick_id end) else c.tick_id end";
    }

    /**
     * The latest cursor of shards {@code 0..shards-1}, in shard order (a shard without rows is absent, so callers key
     * results by {@link Cursor#shard()}, never by list position), with its effective tick and {@link Position#behind()}
     * up to the tick {@code latestTickId}: one statement, per shard an index probe for the cursor, then one scan of the
     * ticks from the lowest effective tick of the shards without a batch in progress (at least the newest
     * checkpoint's) to the latest.
     */
    public List<Position> positions(Connection c, long latestTickId) throws SQLException {
        try (var ps = c.prepareStatement(positions)) {
            ps.setLong(1, latestTickId);
            ps.setLong(2, latestTickId);
            try (var rs = ps.executeQuery()) {
                var result = new ArrayList<Position>(shards);
                while (rs.next()) {
                    result.add(new Position(cursor(rs), rs.getBoolean(10), rs.getLong(9)));
                }
                return result;
            }
        }
    }

    /** The newest checkpoint's tick, valid or not, null without one. */
    @Nullable
    public Long checkpointTick(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select tick_id from " + names.checkpoint()
                + " order by seq desc limit 1")) {
            return rs.next() ? rs.getLong(1) : null;
        }
    }

    /** Busy at a checkpoint: {@link Position#behind() behind}, or a batch in progress. */
    public static boolean busy(Position p) {
        return p.behind() || p.cursor().targetTickId() != null;
    }

    /**
     * The {@code busy_tick_id} of a checkpoint of {@code positions} read at the tick {@code latestTickId}: the lowest
     * effective tick of the busy shards, {@code latestTickId} when none. Under that checkpoint it is the lowest
     * effective tick of all shards, and the tick {@code outbox.shard.lag} measures from.
     */
    public static long busyTick(List<Position> positions, long latestTickId) {
        long tick = latestTickId;
        for (var p : positions) {
            if (busy(p)) {
                tick = Math.min(tick, p.effectiveTickId());
            }
        }
        return tick;
    }

    /**
     * The checkpoint of a maintenance run that read {@code positions} at the tick {@code latestTickId}, replacing the
     * one before (see the class comment).
     */
    public void checkpoint(Connection c, long latestTickId, List<Position> positions) throws SQLException {
        var busy = new StringBuilder("0".repeat(shards));
        for (var p : positions) {
            if (busy(p)) {
                busy.setCharAt(p.cursor().shard(), '1');
            }
        }
        try (var ps = c.prepareStatement(checkpoint)) {
            ps.setLong(1, latestTickId);
            ps.setString(2, busy.toString());
            ps.setLong(3, busyTick(positions, latestTickId));
            ps.executeUpdate();
        }
    }

    /**
     * The shard's cursor, effective tick and that tick's row, in one statement with the newest checkpoint. Throws
     * {@link IllegalStateException} when the shard has no cursor (bootstrap creates one per shard).
     */
    public Current latest(Connection c, int shard) throws SQLException {
        try (var ps = c.prepareStatement(latest)) {
            ps.setInt(1, shard);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("no cursor for shard " + shard + " in " + names.cursor());
                }
                var snapshot = rs.getString(10);
                return new Current(cursor(rs), rs.getLong(9), snapshot == null ? null : new Tick(rs.getLong(9),
                        PgSnapshot.parse(snapshot), rs.getObject(11, OffsetDateTime.class).toInstant(),
                        rs.getObject(12, Long.class)));
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
