package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxRecord;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The batch query: rows of one shard whose xid is visible in the target tick's snapshot and not in the
 * previous tick's, i.e. exactly the transactions that committed between the two ticks, in id order after
 * {@code lastId}. Candidates are "started at or after prev's xmax" ∪ "in flight at prev and no longer at target", so
 * the scan stays proportional to the batch even while a long transaction holds xmin back or keeps writing. An empty
 * candidate xip binds an empty array, which the planner prunes away with its branch.
 */
public final class BatchReader {

    /** A main-stream row ({@code attempts} = 0); {@code blocked} when its (topic, key) has rows in the retry table. */
    public record BatchRow(OutboxRecord record, boolean blocked) {}

    private static final int MAX_FETCH = 5000;

    private final String select;
    private final String selectUnchecked;

    public BatchReader(TableNames names) {
        this.select = sql(names, "exists (select 1 from " + names.retry() + " r"
                + " where r.shard = o.shard and r.topic = o.topic and r.key = o.key)");
        this.selectUnchecked = sql(names, "false");
    }

    private static String sql(TableNames names, String blocked) {
        return "select o.id, o.message_id, o.topic, o.key, o.payload, o.headers, o.created_at, " + blocked
                + " as blocked from ("
                + " select * from " + names.base() + " where shard = ? and xid >= ?::xid8 and xid < ?::xid8"
                + " union all"
                + " select * from " + names.base() + " where shard = ? and xid = any(?::xid8[])"
                + ") o where pg_visible_in_snapshot(o.xid, ?::pg_snapshot) and o.id > ? order by o.id limit ?";
    }

    /** The statement {@link #read} runs. */
    String sql(boolean checkBlocked) {
        return checkBlocked ? select : selectUnchecked;
    }

    /** Binds the parameters of {@link #sql(boolean)}. */
    void bind(PreparedStatement ps, int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit)
            throws SQLException {
        ps.setShort(1, (short) shard);
        ps.setString(2, Long.toString(prev.xmax()));
        ps.setString(3, Long.toString(target.xmax()));
        ps.setShort(4, (short) shard);
        // in flight at prev but not at target: a transaction in target's xip is invisible in target, so its rows
        // (a long writer's, maybe many) need no fetch. A text[] parameter: text[] -> xid8[] folds at plan time, while
        // a varchar goes through the stable array_in and leaves the xip branch scanning every partition
        var running = Arrays.stream(target.xip()).boxed().collect(Collectors.toSet());
        var xip = Arrays.stream(prev.xip()).filter(x -> !running.contains(x)).mapToObj(Long::toString)
                .toArray(String[]::new);
        ps.setArray(5, ps.getConnection().createArrayOf("text", xip));
        ps.setString(6, target.text());
        ps.setLong(7, lastId);
        ps.setInt(8, limit);
    }

    /**
     * Rows of {@code (prev, target]} with {@code id > lastId}, at most {@code limit}, streamed in chunks of up to 5000
     * rows (the caller's transaction must have autocommit off and {@code plan_cache_mode = force_custom_plan} for
     * plan-time partition pruning). {@code checkBlocked = false} skips the per-row retry-table probe: the caller passes
     * it when the shard has no retry rows, and every row then reads as not blocked.
     */
    public List<BatchRow> read(Connection inTx, int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit,
            boolean checkBlocked) throws SQLException {
        try (var ps = inTx.prepareStatement(sql(checkBlocked))) {
            ps.setFetchSize(Math.min(limit, MAX_FETCH));
            bind(ps, shard, prev, target, lastId, limit);
            try (var rs = ps.executeQuery()) {
                var rows = new ArrayList<BatchRow>();
                while (rs.next()) {
                    var record = new OutboxRecord(
                            rs.getLong(1),
                            rs.getObject(2, UUID.class),
                            rs.getString(3),
                            rs.getBytes(4),
                            rs.getBytes(5),
                            HeaderCodec.decode(rs.getBytes(6)),
                            rs.getObject(7, OffsetDateTime.class).toInstant(),
                            0,
                            0);
                    rows.add(new BatchRow(record, rs.getBoolean(8)));
                }
                return rows;
            }
        }
    }
}
