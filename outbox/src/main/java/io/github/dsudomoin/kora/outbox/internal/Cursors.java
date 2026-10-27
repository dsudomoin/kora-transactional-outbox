package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.internal.Ticks.Tick;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
 * checkpoint counts only while the tick at its {@code tick_id} exists and was made no later than the checkpoint (both
 * {@code created_at} on the database clock, the checkpoint's never below its tick's, so a clock stepped back between
 * the two cannot make it look older): one whose tick is gone, or newer than itself, belongs to another history
 * (tick ids went backwards under it, as in a partial restore, and new ticks may since have passed its id) and counts for
 * nothing, and the next maintenance run replaces it (with an ERROR, unless its tick is gone and every latest cursor row
 * is at or above it: then it is only obsolete, as the previous release's cleanup leaves it, see
 * {@link #ignoredCheckpointTick}).
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
 *       different shard counts on one table are not supported: the bootstrap reshards only a drained outbox with no
 *       other instance running, and maintenance halts an instance whose count differs from the cursors'.)</li>
 *   <li>A 1 bit: {@code busy_tick_id} is at most E', so a row visible in it is visible in E', and by induction in the
 *       cursor's tick.</li>
 *   <li>Cursors only move forward, so a cursor appended after the read keeps both. A step that read its cursor and
 *       effective tick before the checkpoint started from E' or above, and reads the same rows as from the new
 *       effective tick, because the range between them is empty for the shard: so a batch it left in progress also
 *       goes on from the effective tick, above its cursor's tick.</li>
 *   <li>Every effective tick exists: the new ones are L, {@code busy_tick_id} (an effective tick at the read) or above,
 *       a cursor's tick at or above them, and cleanup deletes only ticks below the lowest effective tick, which no
 *       checkpoint lowers: each new effective tick is L or at least {@code busy_tick_id}, which is at least the lowest
 *       effective tick at the read.</li>
 * </ol>
 *
 * <p>The previous release ({@code feature/requeue}) knows no checkpoint: its steps start from the latest cursor row's
 * {@code tick_id} (and its {@code target_tick_id} while a batch is in progress), its cleanup deletes ticks below the
 * lowest of those, and its ticks carry the shard sets {@code shards} that this version leaves null (every shard). So
 * that it runs beside this version during a rolling deploy and resumes after a rollback, three things hold:
 * <ul>
 *   <li>Tick cleanup's floor is the lower of the lowest effective tick and the lowest latest cursor row's tick
 *       ({@link #lowestTick}): no latest cursor row (nor its target, above it) is on a deleted tick. Cursors only move
 *       forward, so the floor read before the delete stays below every latest row.</li>
 *   <li><em>Anchors</em> ({@link #anchors}): each maintenance run, after its checkpoint, appends for each shard with
 *       a 0 bit, no batch in progress and its latest row on a tick below its effective tick E made
 *       {@code outbox.relay.cursorAnchorInterval} or more ago, that row at E (no batch in progress,
 *       {@code paused_until} and {@code failures} kept). By the 0 bit no row of the shard is visible in E and not in
 *       the row's tick, so it changes neither the shard's rows to read nor its effective tick, and keeps cursors
 *       moving forward; it is written under the shard's lock, from the row a step left. A shard whose row lies
 *       more than {@link #ANCHOR_TICKS} ticks below the latest is anchored however young they are. So ticks are kept
 *       for about one anchor interval and at most about that many, and an idle relay appends one row per shard per
 *       interval.</li>
 *   <li>Every anchor, and every run's <em>due repair</em> for a shard whose latest row has {@code retry_due_at} or
 *       {@code parked_due_at} null while {@code <retry>} or {@code <parked>} hold rows of it (a row of a pre-requeue
 *       instance, a hand edit), carries both dues as the tables hold them: a step of an idle shard looks at those
 *       tables only by its dues, and no tick marks it.</li>
 * </ul>
 * The partition drop horizon stays the effective one ({@link #busyTick}), above some cursor rows' ticks. An old reader
 * starting from such a row's tick C, at or below the shard's effective tick E, is still safe: no row of the shard is
 * visible in E and not in C (the range between is empty for it), so every row it reads is not visible in E, so at or
 * above E's xmin, above the horizon; the dropped ranges below it held only rows visible in E, relayed, and the xids of
 * C's snapshot it looks up there find nothing.
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
    /**
     * How many ticks an idle shard's latest cursor row may lie below the latest before it is anchored, however young
     * those ticks: under load a tick comes every {@code minTickInterval}, and a shard that gets no row for an anchor
     * interval (skewed keys) would keep tens of thousands of ticks, each with its snapshot.
     */
    static final int ANCHOR_TICKS = 10_000;

    private final TableNames names;
    private final int shards;
    private final String latest;
    private final String latestMany;
    private final String appendAll;
    private final String positions;
    private final String checkpoint;
    private final String anchors;
    private final String lowestTick;

    /** {@code shards}: {@code outbox.shards}, the length of a checkpoint's {@code busy} bitmap. */
    public Cursors(TableNames names, int shards) {
        this.names = names;
        this.shards = shards;
        var effective = effective(shards);
        // every statement opens the cursor table, then the checkpoint, then the tick table, as XidRecovery locks them
        var withTick = " left join " + newestCheckpoint(names) + " k on true cross join lateral (select " + effective
                + " eff) e left join " + names.tick() + " t on t.id = e.eff";
        this.latest = "select c.*, e.eff, t.snapshot::text, t.created_at, t.max_id from (select " + COLUMNS + " from "
                + names.cursor() + " where shard = ? order by seq desc limit 1) c" + withTick;
        // the same per shard: one cursor probe each, the checkpoint read once
        this.latestMany = "select c.*, e.eff, t.snapshot::text, t.created_at, t.max_id from unnest(?::int[]) s(shard)"
                + " cross join lateral (select " + COLUMNS + " from " + names.cursor()
                + " where shard = s.shard order by seq desc limit 1) c" + withTick;
        this.appendAll = "insert into " + names.cursor() + " (" + COLUMNS + ") select * from unnest(?::smallint[],"
                + " ?::int8[], ?::int8[], ?::int8[], ?::timestamptz[], ?::int[], ?::timestamptz[], ?::timestamptz[])";
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
        // created no earlier than its tick, by the clock that dated the tick: a clock stepped back since must not make
        // the checkpoint look older than its tick, which reads as another history; a tick of another history at that
        // id (one made after this run) is still newer
        this.checkpoint = "with gone as (delete from " + names.checkpoint() + ") insert into " + names.checkpoint()
                + " (tick_id, busy, busy_tick_id, created_at) values (?, ?::varbit, ?, greatest(clock_timestamp(),"
                + " (select created_at from " + names.tick() + " where id = ?)))";
        // per shard asked: an index probe for the cursor, the checkpoint read once, a primary key probe for the
        // cursor's tick and an index probe each for the earliest retry head and parked row (their _due indexes)
        this.anchors = "select c.shard, case when a.anchor then c.eff else c.tick_id end,"
                + " case when not a.anchor then c.target_tick_id end, case when not a.anchor then c.last_id else 0 end,"
                + " c.paused_until, c.failures, d.retry_due, d.parked_due from (select c.*, " + effective + " eff,"
                + " case when length(k.busy) = " + shards + " then get_bit(k.busy, c.shard) = 0 else false end idle"
                + " from unnest(?::int[]) s(shard) cross join lateral (select " + COLUMNS + " from " + names.cursor()
                + " where shard = s.shard order by seq desc limit 1) c left join " + newestCheckpoint(names)
                + " k on true) c left join " + names.tick() + " t on t.id = c.tick_id"
                + " cross join lateral (select (select min(next_attempt_at) from " + names.retry()
                + " where shard = c.shard and next_attempt_at is not null) retry_due, (select min(due_at) from "
                + names.parked() + " where shard = c.shard) parked_due) d"
                // a tick dated in the future (the clock stepped back since) counts as old, so a wrong clock cannot
                // keep a shard's tick, and every tick above it, forever
                + " cross join lateral (select c.idle and c.target_tick_id is null and c.tick_id < c.eff"
                + " and (t.created_at is null or t.created_at <= clock_timestamp() - ? * interval '1 microsecond'"
                + " or t.created_at > clock_timestamp()"
                // or too many ticks back, young as they are (ANCHOR_TICKS): computed once, the tick table's last id
                + " or c.tick_id < (select max(id) from " + names.tick() + ") - " + ANCHOR_TICKS + ") anchor) a"
                + " where a.anchor or c.retry_due_at is null and d.retry_due is not null"
                + " or c.parked_due_at is null and d.parked_due is not null order by c.shard";
        this.lowestTick = "select min(c.tick_id) from generate_series(0, " + shards + " - 1) s cross join lateral"
                + " (select tick_id from " + names.cursor() + " where shard = s order by seq desc limit 1) c";
    }

    /**
     * The cursor rows maintenance appends for {@code shards} (see the class comment), in shard order: an
     * <em>anchor</em> for a shard idle at the newest checkpoint (bit 0), with no batch in progress, whose latest
     * cursor row is on a tick below its effective tick made {@code anchorInterval} or more ago (or gone, or dated in
     * the future, or more than {@link #ANCHOR_TICKS} ids below the latest tick): that row at the effective tick, no
     * batch in progress, {@code paused_until} and {@code failures} kept; a <em>due repair</em> for a shard with rows in {@code <retry>} or {@code <parked>} whose latest cursor row
     * has the matching due null, whatever its position: that row as it is. Both carry {@code retry_due_at} and
     * {@code parked_due_at} as the tables hold them now ({@link RetryStore#minDue}, {@link ParkedStore#minDue}), so
     * only under the shard's lock are they exact.
     */
    public List<Cursor> anchors(Connection c, Collection<Integer> shards, Duration anchorInterval)
            throws SQLException {
        var result = new ArrayList<Cursor>();
        if (shards.isEmpty()) {
            return result;
        }
        try (var ps = c.prepareStatement(anchors)) {
            ps.setArray(1, c.createArrayOf("int4", shards.toArray()));
            ps.setLong(2, TimeUnit.MICROSECONDS.convert(anchorInterval));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(cursor(rs));
                }
            }
        }
        return result;
    }

    /**
     * The cursors' shard count, 0 without cursors: {@code max(shard) + 1}, an index probe, as bootstrap and a reshard
     * make the cursors of shards 0..n-1 together. The bootstrap, {@link Maintenance} and {@link Heartbeat} read it.
     */
    static String shardCount(TableNames names) {
        return "select coalesce(max(shard) + 1, 0) from " + names.cursor();
    }

    /** The lowest {@code tick_id} of the shards' latest cursor rows; null without cursors. */
    @Nullable
    public Long lowestTick(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(lowestTick)) {
            rs.next();
            return rs.getObject(1, Long.class);
        }
    }

    /**
     * A subquery of the newest checkpoint's {@code tick_id, busy, busy_tick_id}, for {@link #effective}; none unless
     * the tick at its {@code tick_id} exists and was made no later than the checkpoint (a primary key probe): otherwise
     * it belongs to another history, as tick ids went backwards under it in a partial restore, and maintenance replaces
     * it ({@link #ignoredCheckpointTick}).
     */
    static String newestCheckpoint(TableNames names) {
        return "(select n.tick_id, n.busy, n.busy_tick_id from (select tick_id, busy, busy_tick_id, created_at from "
                + names.checkpoint() + " order by seq desc limit 1) n join " + names.tick()
                + " v on v.id = n.tick_id and v.created_at <= n.created_at)";
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

    /**
     * The newest checkpoint's tick when that checkpoint counts for nothing as from another history (see
     * {@link #newestCheckpoint}): a tick at its id made after it, or its tick gone while a shard's latest cursor row is
     * below it. Null when it counts, when there is none, and when it is merely obsolete: its tick gone with every latest
     * cursor row at or above it, as the previous release's cleanup leaves it in a mixed fleet (it deletes below the
     * lowest cursor tick). An obsolete one counts for nothing too, harmlessly, as the cursors are past it, and the run
     * replaces it.
     */
    @Nullable
    public Long ignoredCheckpointTick(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select n.tick_id from (select tick_id, created_at"
                + " from " + names.checkpoint() + " order by seq desc limit 1) n where not exists (select 1 from "
                + names.tick() + " v where v.id = n.tick_id and v.created_at <= n.created_at) and (exists (select 1"
                + " from " + names.tick() + " v where v.id = n.tick_id) or exists (select 1 from generate_series(0, "
                + shards + " - 1) s cross join lateral (select tick_id from " + names.cursor() + " where shard = s"
                + " order by seq desc limit 1) l where l.tick_id < n.tick_id))")) {
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
            ps.setLong(4, latestTickId);
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
                return current(rs);
            }
        }
    }

    /**
     * {@link #latest(Connection, int)} of each of {@code shards}, in one statement (one snapshot of the cursors, the
     * checkpoint and the ticks), keyed by shard. Throws {@link IllegalStateException} when a shard has no cursor.
     */
    public Map<Integer, Current> latest(Connection c, Collection<Integer> shards) throws SQLException {
        var result = new HashMap<Integer, Current>();
        if (shards.isEmpty()) {
            return result;
        }
        try (var ps = c.prepareStatement(latestMany)) {
            ps.setArray(1, c.createArrayOf("int4", shards.toArray()));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var current = current(rs);
                    result.put(current.cursor().shard(), current);
                }
            }
        }
        for (var shard : shards) {
            if (!result.containsKey(shard)) {
                throw new IllegalStateException("no cursor for shard " + shard + " in " + names.cursor());
            }
        }
        return result;
    }

    private static Current current(ResultSet rs) throws SQLException {
        var snapshot = rs.getString(10);
        return new Current(cursor(rs), rs.getLong(9), snapshot == null ? null : new Tick(rs.getLong(9),
                PgSnapshot.parse(snapshot), rs.getObject(11, OffsetDateTime.class).toInstant(),
                rs.getObject(12, Long.class)));
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

    /** {@link #append} of each of {@code cursors}, in order, in one statement. */
    public void appendAll(Connection c, List<Cursor> cursors) throws SQLException {
        if (cursors.isEmpty()) {
            return;
        }
        int n = cursors.size();
        var shard = new Integer[n];
        var tickId = new Long[n];
        var target = new Long[n];
        var lastId = new Long[n];
        var paused = new String[n];
        var failures = new Integer[n];
        var retryDue = new String[n];
        var parkedDue = new String[n];
        for (int i = 0; i < n; i++) {
            var cursor = cursors.get(i);
            shard[i] = cursor.shard();
            tickId[i] = cursor.tickId();
            target[i] = cursor.targetTickId();
            lastId[i] = cursor.lastId();
            paused[i] = text(cursor.pausedUntil());
            failures[i] = cursor.failures();
            retryDue[i] = text(cursor.retryDueAt());
            parkedDue[i] = text(cursor.parkedDueAt());
        }
        try (var ps = c.prepareStatement(appendAll)) {
            ps.setArray(1, c.createArrayOf("int4", shard));
            ps.setArray(2, c.createArrayOf("int8", tickId));
            ps.setArray(3, c.createArrayOf("int8", target));
            ps.setArray(4, c.createArrayOf("int8", lastId));
            ps.setArray(5, c.createArrayOf("text", paused)); // ISO 8601 in UTC, as timestamptz reads it exactly
            ps.setArray(6, c.createArrayOf("int4", failures));
            ps.setArray(7, c.createArrayOf("text", retryDue));
            ps.setArray(8, c.createArrayOf("text", parkedDue));
            ps.executeUpdate();
        }
    }

    @Nullable
    private static String text(@Nullable Instant t) {
        return t == null ? null : t.toString();
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
