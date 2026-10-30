package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.internal.RetryStore.COLUMNS;
import static io.github.dsudomoin.outbox.internal.RetryStore.RECORD_COLUMNS;
import static io.github.dsudomoin.outbox.internal.RetryStore.errorText;
import static io.github.dsudomoin.outbox.internal.RetryStore.requireOneRow;
import static io.github.dsudomoin.outbox.internal.RetryStore.timestamp;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Action;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Head;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Keep;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Park;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Unpark;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Requeued records in {@code <base>_parked}: unlike retry rows they block nothing; each comes back right before the
 * next record of its (topic, key) or at its {@code due_at}. Every method runs on the caller's connection, so inside the
 * shard step's transaction a rolled-back step leaves each record where it was.
 */
public final class ParkedStore {

    private final String insert;
    private final String repark;
    private final String keep;
    private final String delete;
    private final String toRetry;
    private final String toDead;
    private final String fromRetry;
    private final String selectDue;
    private final String selectOfKeys;
    private final String minDue;
    private final String size;

    public ParkedStore(TableNames names, RetryStore retries) {
        var parked = names.parked();
        var retry = names.retry();
        insert = "insert into " + parked + " (" + COLUMNS + ", attempts, requeues, due_at)"
                + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        repark = "update " + parked + " set requeues = ?, due_at = ? where id = ?";
        keep = "update " + parked + " set due_at = greatest(due_at, ?) where id = ?";
        delete = "delete from " + parked + " where id = any(?)";
        // every part sees the tables as before the statement; key = null matches nothing, so an unkeyed row moves alone
        toRetry = "with moved as (delete from " + parked + " where id = ? or (shard = ? and topic = ? and key = ?"
                + " and id > ?) returning " + COLUMNS + ", attempts, requeues),"
                + " demoted as (update " + retry + " set next_attempt_at = null, waiting_since = null where shard = ?"
                + " and topic = ? and key = ? and id > ? and next_attempt_at is not null),"
                + " head as (insert into " + retry + " (" + COLUMNS + ", attempts, next_attempt_at, last_error,"
                + " requeues, waiting_since) select " + COLUMNS + ", ?, ?, ?, requeues, ?::timestamptz from moved"
                + " where id = ? returning id),"
                + " followers as (insert into " + retry + " (" + COLUMNS + ", attempts, requeues)"
                + " select " + COLUMNS + ", attempts, requeues from moved where id <> ?)"
                + " select count(*) from head";
        toDead = retries.toDead("delete from " + parked + " where id = ? returning " + COLUMNS, "?");
        fromRetry = "with moved as (delete from " + retry + " where id = ? returning " + COLUMNS + ", attempts)"
                + " insert into " + parked + " (" + COLUMNS + ", attempts, requeues, due_at)"
                + " select " + COLUMNS + ", attempts, ?, ? from moved";
        selectDue = "select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and due_at <= ? order by id limit ?";
        // at most limit rows read per key: a key with thousands of parked rows costs no more than any other
        selectOfKeys = "select " + RECORD_COLUMNS + " from (select distinct t, b from unnest(?::text[], ?::bytea[])"
                + " u(t, b)) k cross join lateral (select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and topic = k.t and key = k.b order by id limit ?) p order by id limit ?";
        minDue = "select min(due_at) from " + parked + " where shard = ?";
        size = "select count(*) from " + parked;
    }

    /**
     * Writes the actions of one handler call, routed by origin ({@code parkedIds}: rows handed from this table).
     * {@link Park}: a main-stream record is inserted, a parked one gets its new {@code requeues} and due time.
     * {@link Keep}: the due time moves later, never earlier. {@link Unpark}: {@link #unpark}. A parked {@link Head}:
     * {@link #toRetry}, a waiting one waiting since {@code now}; a parked {@link Dead}: {@link #toDead}. A parked
     * follower: nothing, as a row before it in the call blocks its key: a parked head, whose {@link #toRetry} carries
     * it, or, in the retry pass (retry heads first), a retry head of its key, whose id is higher, so the row stays
     * parked below every retry row of its key. Other actions of main-stream rows belong to {@link RetryStore#apply}.
     *
     * <p>Precondition: {@code actions} are in the call's order, where every parked row precedes the newer main-stream
     * rows of its key (the relay builds its calls that way). A parked head turns the later rows of its key into
     * followers, so no main-stream {@link Park} of its key comes after it: moving the head at once, before the batched
     * inserts, leaves behind no row it should have carried.
     */
    public void apply(Connection inTx, int shard, List<Action> actions, Set<Long> parkedIds, Instant now)
            throws SQLException {
        var inserts = new ArrayList<Park>();
        var reparks = new ArrayList<Park>();
        var keeps = new ArrayList<Keep>();
        var unparked = new ArrayList<OutboxRecord>();
        for (var action : actions) {
            var r = action.r();
            if (!parkedIds.contains(r.id())) {
                if (action instanceof Park p) {
                    inserts.add(p);
                }
            } else if (action instanceof Park p) {
                reparks.add(p);
            } else if (action instanceof Keep k) {
                keeps.add(k);
            } else if (action instanceof Unpark) {
                unparked.add(r);
            } else if (action instanceof Head h) {
                toRetry(inTx, shard, r, h.attempts(), h.nextAttemptAt(), h.error(), h.waiting() == null ? null : now);
            } else if (action instanceof Dead d) {
                toDead(inTx, r, d.attempts(), d.error());
            }
        }
        if (!inserts.isEmpty()) {
            try (var ps = inTx.prepareStatement(insert)) {
                for (var p : inserts) {
                    RetryStore.bindRow(ps, shard, p.r());
                    ps.setInt(9, p.r().attempts());
                    ps.setInt(10, p.requeues());
                    ps.setObject(11, timestamp(p.dueAt()));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        if (!reparks.isEmpty()) {
            try (var ps = inTx.prepareStatement(repark)) {
                for (var p : reparks) {
                    ps.setInt(1, p.requeues());
                    ps.setObject(2, timestamp(p.dueAt()));
                    ps.setLong(3, p.r().id());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        if (!keeps.isEmpty()) {
            try (var ps = inTx.prepareStatement(keep)) {
                for (var k : keeps) {
                    ps.setObject(1, timestamp(k.dueAt()));
                    ps.setLong(2, k.r().id());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        unpark(inTx, unparked);
    }

    /** Deletes delivered rows; a row that is not in the table is a bug, so it fails and the step rolls back. */
    public void unpark(Connection c, List<OutboxRecord> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        try (var ps = c.prepareStatement(delete)) {
            ps.setArray(1, RetryStore.ids(c, rows));
            int deleted = ps.executeUpdate();
            if (deleted != rows.size()) {
                throw new IllegalStateException("parked rows " + rows.stream().map(OutboxRecord::id).toList()
                        + " not all found (" + deleted + " deleted)");
            }
        }
    }

    /**
     * Makes {@code row} the retry head of its (topic, key), due at {@code nextAttemptAt}, keeping its
     * {@code requeues} and waiting since {@code waitingSince} (null: not waiting): the key's retry heads with a higher
     * id become followers, which do not wait, and its parked rows with a higher id follow it, keeping their
     * {@code attempts} and {@code requeues}, so nothing newer of the key overtakes it. An unkeyed row moves alone.
     */
    public void toRetry(Connection c, int shard, OutboxRecord row, int attempts, Instant nextAttemptAt, String error,
            @Nullable Instant waitingSince) throws SQLException {
        try (var ps = c.prepareStatement(toRetry)) {
            ps.setLong(1, row.id());
            bindNewerOfKey(ps, 2, shard, row); // parked rows
            bindNewerOfKey(ps, 6, shard, row); // retry heads
            ps.setInt(10, attempts);
            ps.setObject(11, timestamp(nextAttemptAt));
            ps.setString(12, errorText(error));
            ps.setObject(13, waitingSince == null ? null : timestamp(waitingSince));
            ps.setLong(14, row.id());
            ps.setLong(15, row.id());
            try (var rs = ps.executeQuery()) {
                rs.next();
                requireOneRow(rs.getInt(1), "parked", row);
            }
        }
    }

    /** Moves a parked row to the dead table, as {@link RetryStore#toDead} writes every dead row. */
    public void toDead(Connection c, OutboxRecord row, int attempts, String error) throws SQLException {
        try (var ps = c.prepareStatement(toDead)) {
            ps.setLong(1, row.id());
            ps.setInt(2, attempts);
            ps.setString(3, errorText(error));
            requireOneRow(ps.executeUpdate(), "parked", row);
        }
    }

    /** Parks a retry row, keeping its {@code attempts}; the caller promotes the next row of its key. */
    public void fromRetry(Connection c, OutboxRecord row, int requeues, Instant dueAt) throws SQLException {
        try (var ps = c.prepareStatement(fromRetry)) {
            ps.setLong(1, row.id());
            ps.setInt(2, requeues);
            ps.setObject(3, timestamp(dueAt));
            requireOneRow(ps.executeUpdate(), "retry", row);
        }
    }

    /** Rows of {@code shard} due at {@code now}, in id order. */
    public List<OutboxRecord> due(Connection c, int shard, Instant now, int limit) throws SQLException {
        try (var ps = c.prepareStatement(selectDue)) {
            ps.setShort(1, (short) shard);
            ps.setObject(2, timestamp(now));
            ps.setInt(3, limit);
            return records(ps.executeQuery());
        }
    }

    /**
     * The parked rows of the (topic, key) pairs of {@code keyed}: the oldest {@code limit} of them, in id order. An
     * unkeyed record matches nothing.
     */
    public List<OutboxRecord> ofKeys(Connection c, int shard, Collection<OutboxRecord> keyed, int limit)
            throws SQLException {
        if (keyed.isEmpty()) {
            return List.of();
        }
        try (var ps = c.prepareStatement(selectOfKeys)) {
            ps.setArray(1, c.createArrayOf("text", keyed.stream().map(OutboxRecord::topic).toArray()));
            ps.setArray(2, c.createArrayOf("bytea", keyed.stream().map(OutboxRecord::key).toArray(byte[][]::new)));
            ps.setShort(3, (short) shard);
            ps.setInt(4, limit);
            ps.setInt(5, limit);
            return records(ps.executeQuery());
        }
    }

    /** Earliest {@code due_at} of the parked rows of {@code shard}; null without any. */
    public @Nullable Instant minDue(Connection c, int shard) throws SQLException {
        return RetryStore.minTimestamp(c, minDue, shard);
    }

    public long size(Connection c) throws SQLException {
        return RetryStore.count(c, size);
    }

    /** Parameters {@code first..first+3} of a "newer rows of the key" condition: shard, topic, key, id. */
    private static void bindNewerOfKey(PreparedStatement ps, int first, int shard, OutboxRecord row)
            throws SQLException {
        ps.setShort(first, (short) shard);
        ps.setString(first + 1, row.topic());
        ps.setBytes(first + 2, row.key());
        ps.setLong(first + 3, row.id());
    }

    private static List<OutboxRecord> records(ResultSet resultSet) throws SQLException {
        try (var rs = resultSet) {
            var records = new ArrayList<OutboxRecord>();
            while (rs.next()) {
                records.add(RetryStore.record(rs, 1));
            }
            return records;
        }
    }
}
