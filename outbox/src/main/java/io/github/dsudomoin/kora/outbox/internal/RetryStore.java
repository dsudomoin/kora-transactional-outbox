package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Action;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Follower;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Head;
import jakarta.annotation.Nullable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The rare path: blocked (topic, key) streams in {@code <base>_retry} (a head with
 * {@code next_attempt_at}, followers with null) and permanently failed rows in {@code <base>_dead}. Error texts are
 * cut to 2000 chars, NUL characters (rejected by Postgres text) replaced by {@code ?}. Every dead row, here and in
 * {@link ParkedStore}, is written by {@link #toDead} or {@link #apply}: pending and due at once while a dead-letter
 * hook is active, kept otherwise, with the redrives {@code <base>_redriven} counts for its message.
 */
public final class RetryStore {

    static final int MAX_ERROR_LENGTH = 2000;
    static final String COLUMNS = "id, shard, message_id, topic, key, payload, headers, created_at";
    /** What {@link #record} reads. */
    static final String RECORD_COLUMNS = "id, message_id, topic, key, payload, headers, created_at, attempts, requeues";

    private final String deadTable;
    private final String redriven;
    private final boolean hookActive;
    private final String insertRetry;
    private final String insertDead;
    private final String selectDue;
    private final String selectFollowers;
    private final String delete;
    private final String promote;
    private final String reschedule;
    private final String moveToDead;
    private final String minDue;
    private final String size;
    private final String deadSize;

    public RetryStore(TableNames names, boolean hookActive) {
        var retry = names.retry();
        this.deadTable = names.dead();
        this.redriven = names.redriven();
        this.hookActive = hookActive;
        insertRetry = "insert into " + retry + " (" + COLUMNS + ", attempts, next_attempt_at, last_error, requeues)"
                + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        insertDead = "insert into " + deadColumns() + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + deadState("?") + ")";
        selectDue = "select " + RECORD_COLUMNS + " from " + retry
                + " where shard = ? and next_attempt_at <= ? order by id limit ?";
        selectFollowers = "select h.id, f.id, f.message_id, f.topic, f.key, f.payload, f.headers, f.created_at,"
                + " f.attempts, f.requeues from " + retry + " h cross join lateral (select * from " + retry + " f"
                + " where f.shard = h.shard and f.topic = h.topic and f.key = h.key and f.id > h.id"
                + " order by f.id limit ?) f where h.id = any(?) order by f.id";
        delete = "delete from " + retry + " where id = any(?)";
        promote = "update " + retry + " set next_attempt_at = ? where id = (select id from " + retry
                + " where shard = ? and topic = ? and key = ? order by id limit 1)";
        reschedule = "update " + retry
                + " set attempts = ?, next_attempt_at = ?, last_error = coalesce(?, last_error) where id = ?";
        moveToDead = toDead("delete from " + retry + " where id = ? returning " + COLUMNS + ", last_error",
                "coalesce(?, last_error)");
        minDue = "select min(next_attempt_at) from " + retry + " where shard = ? and next_attempt_at is not null";
        size = "select count(*) from " + retry;
        deadSize = "select count(*) from " + deadTable;
    }

    /**
     * Moves the row {@code delete} removes ({@code delete ... where id = ? returning} {@link #COLUMNS} and more) to the
     * dead table: parameter 2 its attempts, {@code error} its error text, an expression over parameter 3 and the
     * returned columns.
     */
    String toDead(String delete, String error) {
        return "with moved as (" + delete + ") insert into " + deadColumns() + " select " + COLUMNS + ", ?, " + error
                + ", " + deadState("moved.message_id") + " from moved";
    }

    private String deadColumns() {
        return deadTable + " (" + COLUMNS + ", attempts, error, status, due_at, redrives)";
    }

    /** Status, due_at and redrives of a new dead row whose message id is the SQL expression {@code messageId}. */
    private String deadState(String messageId) {
        return (hookActive ? "'pending', now()" : "'kept', null") + ", coalesce((select r.redrives from " + redriven
                + " r where r.message_id = " + messageId + "), 0)";
    }

    /**
     * Heads and followers into the retry table, dead rows into the dead table; one JDBC batch each. Any other action
     * fails, so the shard step rolls back.
     */
    public void apply(Connection inTx, int shard, List<Action> actions) throws SQLException {
        if (actions.isEmpty()) {
            return;
        }
        try (var retry = inTx.prepareStatement(insertRetry); var dead = inTx.prepareStatement(insertDead)) {
            int retries = 0;
            int deads = 0;
            for (var action : actions) {
                if (action instanceof Dead d) {
                    bindRow(dead, shard, d.r());
                    dead.setInt(9, d.attempts());
                    dead.setString(10, errorText(d.error()));
                    dead.setObject(11, d.r().messageId()); // its redrives
                    dead.addBatch();
                    deads++;
                } else if (action instanceof Head || action instanceof Follower) {
                    var head = action instanceof Head h ? h : null;
                    bindRow(retry, shard, action.r());
                    retry.setInt(9, head == null ? 0 : head.attempts());
                    if (head == null) {
                        retry.setNull(10, Types.TIMESTAMP_WITH_TIMEZONE);
                    } else {
                        retry.setObject(10, timestamp(head.nextAttemptAt()));
                    }
                    retry.setString(11, head == null ? null : errorText(head.error()));
                    retry.setInt(12, action.r().requeues());
                    retry.addBatch();
                    retries++;
                } else {
                    throw new IllegalStateException("not a retry or dead table action: " + action);
                }
            }
            if (retries > 0) {
                retry.executeBatch();
            }
            if (deads > 0) {
                dead.executeBatch();
            }
        }
    }

    /** Heads of {@code shard} due at {@code now}, in id order; {@code attempts} as stored. */
    public List<OutboxRecord> dueHeads(Connection c, int shard, Instant now, int limit) throws SQLException {
        try (var ps = c.prepareStatement(selectDue)) {
            ps.setShort(1, (short) shard);
            ps.setObject(2, timestamp(now));
            ps.setInt(3, limit);
            try (var rs = ps.executeQuery()) {
                var heads = new ArrayList<OutboxRecord>();
                while (rs.next()) {
                    heads.add(record(rs, 1));
                }
                return heads;
            }
        }
    }

    /**
     * The followers of each keyed head in {@code heads} (rows of its (topic, key) after it), at most {@code perKey}
     * each, in id order, keyed by the head's id; a head without followers is absent.
     */
    public Map<Long, List<OutboxRecord>> followers(Connection c, List<OutboxRecord> heads, int perKey)
            throws SQLException {
        try (var ps = c.prepareStatement(selectFollowers)) {
            ps.setInt(1, perKey);
            ps.setArray(2, ids(c, heads.stream().filter(h -> h.key() != null).toList()));
            try (var rs = ps.executeQuery()) {
                var followers = new HashMap<Long, List<OutboxRecord>>();
                while (rs.next()) {
                    followers.computeIfAbsent(rs.getLong(1), id -> new ArrayList<>()).add(record(rs, 2));
                }
                return followers;
            }
        }
    }

    /** Deletes delivered rows; a row that is not in the table is a bug (see {@link #requireOneRow}). */
    public void delete(Connection c, List<OutboxRecord> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        try (var ps = c.prepareStatement(delete)) {
            ps.setArray(1, ids(c, rows));
            int deleted = ps.executeUpdate();
            if (deleted != rows.size()) {
                throw new IllegalStateException("retry rows " + rows.stream().map(OutboxRecord::id).toList()
                        + " not all found (" + deleted + " deleted)");
            }
        }
    }

    /** A null {@code error} keeps the head's last error. */
    public void reschedule(Connection c, OutboxRecord head, int attempts, Instant next, @Nullable String error)
            throws SQLException {
        try (var ps = c.prepareStatement(reschedule)) {
            ps.setInt(1, attempts);
            ps.setObject(2, timestamp(next));
            ps.setString(3, errorText(error));
            ps.setLong(4, head.id());
            ps.executeUpdate();
        }
    }

    /** Moves a retry row to the dead table. A null {@code error} keeps its last error. */
    public void deadFromRetry(Connection c, OutboxRecord row, int attempts, @Nullable String error)
            throws SQLException {
        try (var ps = c.prepareStatement(moveToDead)) {
            ps.setLong(1, row.id());
            ps.setInt(2, attempts);
            ps.setString(3, errorText(error));
            requireOneRow(ps.executeUpdate(), "retry", row);
        }
    }

    /**
     * Makes the first row left of {@code row}'s (topic, key) its head, due at {@code now}: the caller's clock, so
     * {@link #dueHeads} with the same {@code now} finds it in the same pass. Returns whether there was one; an unkeyed
     * row has no followers.
     */
    public boolean promote(Connection c, int shard, OutboxRecord row, Instant now) throws SQLException {
        if (row.key() == null) {
            return false;
        }
        try (var ps = c.prepareStatement(promote)) {
            ps.setObject(1, timestamp(now));
            ps.setShort(2, (short) shard);
            ps.setString(3, row.topic());
            ps.setBytes(4, row.key());
            return ps.executeUpdate() > 0;
        }
    }

    /** Earliest {@code next_attempt_at} of the heads of {@code shard}; null without heads. */
    public @Nullable Instant minDue(Connection c, int shard) throws SQLException {
        return minTimestamp(c, minDue, shard);
    }

    public long size(Connection c) throws SQLException {
        return count(c, size);
    }

    public long deadSize(Connection c) throws SQLException {
        return count(c, deadSize);
    }

    /**
     * A row that is not in its {@code table} ({@code retry}, {@code parked}) is a bug: fail so the shard transaction
     * rolls back.
     */
    static void requireOneRow(int rows, String table, OutboxRecord row) {
        if (rows != 1) {
            throw new IllegalStateException(table + " row " + row.id() + " not found (" + rows + " rows)");
        }
    }

    /** Columns {@code first..first+8}: {@link #RECORD_COLUMNS}. */
    static OutboxRecord record(ResultSet rs, int first) throws SQLException {
        return new OutboxRecord(
                rs.getLong(first),
                rs.getObject(first + 1, UUID.class),
                rs.getString(first + 2),
                rs.getBytes(first + 3),
                rs.getBytes(first + 4),
                HeaderCodec.decode(rs.getBytes(first + 5)),
                rs.getObject(first + 6, OffsetDateTime.class).toInstant(),
                rs.getInt(first + 7),
                rs.getInt(first + 8));
    }

    static Array ids(Connection c, List<OutboxRecord> rows) throws SQLException {
        return c.createArrayOf("bigint", rows.stream().map(OutboxRecord::id).toArray());
    }

    /** Parameters 1..8: {@link #COLUMNS}. */
    static void bindRow(PreparedStatement ps, int shard, OutboxRecord r) throws SQLException {
        ps.setLong(1, r.id());
        ps.setShort(2, (short) shard);
        ps.setObject(3, r.messageId());
        ps.setString(4, r.topic());
        ps.setBytes(5, r.key());
        ps.setBytes(6, r.payload());
        ps.setBytes(7, HeaderCodec.encode(r.headers()));
        ps.setObject(8, timestamp(r.createdAt()));
    }

    static long count(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** The one timestamp {@code sql} selects with {@code shard} as its parameter; null for SQL null. */
    @Nullable
    static Instant minTimestamp(Connection c, String sql, int shard) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            ps.setShort(1, (short) shard);
            try (var rs = ps.executeQuery()) {
                rs.next();
                var min = rs.getObject(1, OffsetDateTime.class);
                return min == null ? null : min.toInstant();
            }
        }
    }

    static OffsetDateTime timestamp(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    @Nullable
    static String errorText(@Nullable String error) {
        if (error == null) {
            return null;
        }
        var text = error.replace('\0', '?');
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
