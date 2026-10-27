package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxRecord;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    /** One shard's part of {@link #readMany}: the arguments of {@link #read}. */
    public record ShardRead(int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit,
            boolean checkBlocked) {}

    /**
     * The rows of {@link #readMany}, per shard in id order, with an entry (maybe empty) for every shard read, in the
     * order of the reads. {@code cut}: the total limit was reached, so a shard's rows may be only a prefix of what
     * {@link #read} returns for it, and the shards after it may have none they would have had.
     */
    public record Batch(Map<Integer, List<BatchRow>> rows, boolean cut) {}

    private static final int MAX_FETCH = 5000;
    private static final String BLOCKED = "exists (select 1 from %s r"
            + " where r.shard = o.shard and r.topic = o.topic and r.key = o.key)";

    private final String select;
    private final String selectUnchecked;
    private final String selectMany;

    public BatchReader(TableNames names) {
        var blocked = BLOCKED.formatted(names.retry());
        this.select = sql(names, blocked);
        this.selectUnchecked = sql(names, "false");
        // the per-shard parameters arrive as rows of unnest, so the partition bounds are known only as each shard's
        // lateral runs: pruned at run time on every rescan (the xip branch through its lo/hi bounds, as xid = any of a
        // non-constant array prunes nothing). Each array is cast once, here, not per row
        this.selectMany = "select u.i, o.id, o.message_id, o.topic, o.key, o.payload, o.headers, o.created_at,"
                + " case when u.chk then " + blocked + " else false end from unnest(?::int[], ?::text[]::xid8[],"
                + " ?::text[]::xid8[], ?::text[]::xid8[], ?::text[]::xid8[], ?::text[], ?::text[]::pg_snapshot[],"
                + " ?::int8[], ?::int[], ?::bool[]) with ordinality u(shard, pmax, tmax, lo, hi, xip, snap, last_id, lim,"
                + " chk, i)"
                + " cross join lateral (select o.* from ("
                + " select * from " + names.base() + " where shard = u.shard and xid >= u.pmax and xid < u.tmax"
                + " union all"
                + " select * from " + names.base() + " where shard = u.shard and xid >= u.lo and xid <= u.hi"
                + " and xid = any(u.xip::xid8[])"
                + ") o where pg_visible_in_snapshot(o.xid, u.snap) and o.id > u.last_id order by o.id limit u.lim) o"
                + " limit ?";
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
        // the candidate xip as a text[] parameter: text[] -> xid8[] folds at plan time, while a varchar goes through
        // the stable array_in and leaves the xip branch scanning every partition
        var xip = Arrays.stream(candidateXip(prev, target)).mapToObj(Long::toString).toArray(String[]::new);
        ps.setArray(5, ps.getConnection().createArrayOf("text", xip));
        ps.setString(6, target.text());
        ps.setLong(7, lastId);
        ps.setInt(8, limit);
    }

    /**
     * The candidate xip of the batch {@code (prev, target]}: in flight at prev but not at target. A transaction in
     * target's xip is invisible in target, so its rows (a long writer's, maybe many) need no fetch.
     */
    private static long[] candidateXip(PgSnapshot prev, PgSnapshot target) {
        var running = Arrays.stream(target.xip()).boxed().collect(Collectors.toSet());
        return Arrays.stream(prev.xip()).filter(x -> !running.contains(x)).toArray();
    }

    /** The statement {@link #readMany} runs. */
    String manySql() {
        return selectMany;
    }

    /** Binds the parameters of {@link #manySql()}. */
    void bindMany(PreparedStatement ps, List<ShardRead> reads, int totalLimit) throws SQLException {
        int n = reads.size();
        var shard = new Integer[n];
        var pmax = new String[n];
        var tmax = new String[n];
        var lo = new String[n];
        var hi = new String[n];
        var xip = new String[n];
        var snap = new String[n];
        var lastId = new Long[n];
        var limit = new Integer[n];
        var check = new Boolean[n];
        for (int i = 0; i < n; i++) {
            var r = reads.get(i);
            var candidates = candidateXip(r.prev(), r.target());
            shard[i] = r.shard();
            pmax[i] = Long.toString(r.prev().xmax());
            tmax[i] = Long.toString(r.target().xmax());
            // null bounds when none: the branch then matches nothing and prunes every partition
            lo[i] = candidates.length == 0 ? null : Long.toString(Arrays.stream(candidates).min().getAsLong());
            hi[i] = candidates.length == 0 ? null : Long.toString(Arrays.stream(candidates).max().getAsLong());
            xip[i] = Arrays.stream(candidates).mapToObj(Long::toString).collect(Collectors.joining(",", "{", "}"));
            snap[i] = r.target().text();
            lastId[i] = r.lastId();
            limit[i] = r.limit();
            check[i] = r.checkBlocked();
        }
        var c = ps.getConnection();
        ps.setArray(1, c.createArrayOf("int4", shard));
        ps.setArray(2, c.createArrayOf("text", pmax));
        ps.setArray(3, c.createArrayOf("text", tmax));
        ps.setArray(4, c.createArrayOf("text", lo));
        ps.setArray(5, c.createArrayOf("text", hi));
        ps.setArray(6, c.createArrayOf("text", xip));
        ps.setArray(7, c.createArrayOf("text", snap));
        ps.setArray(8, c.createArrayOf("int8", lastId));
        ps.setArray(9, c.createArrayOf("int4", limit));
        ps.setArray(10, c.createArrayOf("bool", check));
        ps.setInt(11, totalLimit);
    }

    /**
     * {@link #read} of each of {@code reads} (distinct shards) in one statement, at most {@code totalLimit} rows in
     * all: per shard what {@link #read} returns, unless {@link Batch#cut() cut}, and then a prefix of it for each
     * shard (the statement stops in the middle of one shard's rows, in the order of the reads, with the shards after
     * it unread). The caller's transaction must have autocommit off; partitions are pruned at run time, per shard.
     */
    public Batch readMany(Connection inTx, List<ShardRead> reads, int totalLimit) throws SQLException {
        if (totalLimit <= 0) {
            throw new IllegalArgumentException("totalLimit " + totalLimit);
        }
        var rows = new LinkedHashMap<Integer, List<BatchRow>>();
        for (var r : reads) {
            if (rows.put(r.shard(), new ArrayList<>()) != null) {
                throw new IllegalArgumentException("shard " + r.shard() + " read twice");
            }
        }
        if (reads.isEmpty()) {
            return new Batch(rows, false);
        }
        record Positioned(long position, BatchRow row) {}
        var read = new ArrayList<Positioned>();
        try (var ps = inTx.prepareStatement(selectMany)) {
            ps.setFetchSize(Math.min(totalLimit, MAX_FETCH));
            bindMany(ps, reads, totalLimit);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    read.add(new Positioned(rs.getLong(1), new BatchRow(record(rs, 2), rs.getBoolean(9))));
                }
            }
        }
        // no order by in the statement: the lateral's rows come out per shard in id order, but nothing guarantees it
        read.sort(Comparator.comparingLong(Positioned::position).thenComparingLong(p -> p.row().record().id()));
        for (var p : read) {
            rows.get(reads.get((int) p.position() - 1).shard()).add(p.row());
        }
        return new Batch(rows, read.size() == totalLimit);
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
                    rows.add(new BatchRow(record(rs, 1), rs.getBoolean(8)));
                }
                return rows;
            }
        }
    }

    /** The record whose id is in column {@code from}, followed by message id, topic, key, payload, headers, created. */
    private static OutboxRecord record(ResultSet rs, int from) throws SQLException {
        return new OutboxRecord(
                rs.getLong(from),
                rs.getObject(from + 1, UUID.class),
                rs.getString(from + 2),
                rs.getBytes(from + 3),
                rs.getBytes(from + 4),
                HeaderCodec.decode(rs.getBytes(from + 5)),
                rs.getObject(from + 6, OffsetDateTime.class).toInstant(),
                0,
                0);
    }
}
