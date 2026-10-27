package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The one bulk insert into the outbox: {@code sendAll}, and every path that puts rows back into it (orphan restore,
 * redrive). A row gets a new id and the inserting transaction's xid; it keeps its {@code message_id} and stored
 * headers, and its shard is recomputed for the current {@code outbox.shards}. Keyed rows take
 * {@code pg_advisory_xact_lock(keyNamespace, keyHash)}, the lock {@code send} takes, before the identity assigns their
 * ids, so a row cannot interleave with a concurrent {@code send} of its (topic, key).
 */
public final class Resender {

    /**
     * One row whatever the driver's fetch size: a row-per-lock result read in portals (pgjdbc
     * {@code defaultRowFetchSize} in a transaction) would take only the first portal's locks. The planner keeps the
     * volatile lock call in the unused subquery column and does not pull the sorted subquery up, so every key is
     * locked, in ascending hash order.
     */
    private static final String LOCK_ALL = "select count(*) from (select pg_advisory_xact_lock(?, h)"
            + " from (select distinct unnest(?::int[]) as h order by 1) s) l";
    /** One row whatever the fetch size too, so every hash is tried; never waits. */
    private static final String TRY_LOCKS = "select array_agg(h) filter (where not pg_try_advisory_xact_lock(?, h))"
            + " from unnest(?::int[]) h";

    /** A row to insert; {@code headers} in the stored encoding ({@link HeaderCodec}). */
    public record Row(
            UUID messageId, String topic, @Nullable byte[] key, @Nullable byte[] payload, @Nullable byte[] headers) {}

    private final int keyNamespace;
    private final TableNames names;
    private final int shards;
    private final ShardGuard guard;
    private final String insertAll;

    /**
     * Without the instance's {@link ShardGuard}: for tests. Main code builds it from the resolved config
     * ({@link #Resender(TableNames, OutboxConfig)}) or, in {@link PgOutbox}, with the guard itself.
     */
    public Resender(TableNames names, int shards) {
        this(names, shards, ShardGuard.NONE);
    }

    /** {@code config}: the instance's, as {@link OutboxSchema#config()} resolved it; every insert checks its guard. */
    public Resender(TableNames names, OutboxConfig config) {
        this(names, config.shards(), ShardGuard.of(config));
    }

    Resender(TableNames names, int shards, ShardGuard guard) {
        this.names = names;
        this.guard = guard;
        this.keyNamespace = names.keyNamespace();
        this.shards = shards;
        this.insertAll = "insert into " + names.base() + " (shard, message_id, topic, key, payload, headers)"
                + " select u.shard, u.mid, u.topic, u.key, u.payload, u.headers"
                + " from unnest(?::smallint[], ?::uuid[], ?::text[], ?::bytea[], ?::bytea[], ?::bytea[])"
                + " with ordinality u(shard, mid, topic, key, payload, headers, ord) order by u.ord";
    }

    /**
     * Tries the key lock of each distinct hash in {@code hashes} (nulls skipped) without waiting, held until
     * {@code inTx} ends; returns the hashes another transaction holds, an open {@code send} of the key. A row whose
     * hash is not among them can go through {@link #insertAll} without waiting for its key.
     */
    public Set<Integer> tryKeyLocks(Connection inTx, Collection<Integer> hashes) throws SQLException {
        var distinct = hashes.stream().filter(Objects::nonNull).distinct().toArray(Integer[]::new);
        if (distinct.length == 0) {
            return Set.of();
        }
        try (var ps = inTx.prepareStatement(TRY_LOCKS)) {
            ps.setInt(1, keyNamespace);
            ps.setArray(2, inTx.createArrayOf("int4", distinct));
            try (var rs = ps.executeQuery()) {
                rs.next();
                var held = rs.getArray(1);
                return held == null ? Set.of() : Set.of((Integer[]) held.getArray());
            }
        }
    }

    /**
     * Locks the rows' distinct key hashes in one statement, then inserts them in one, ids in list order; unkeyed rows
     * take no lock and land on a random shard. Every key lock is held until {@code inTx} ends, so callers bound the
     * list. Returns the rows inserted. Throws {@link IllegalStateException} while the instance's {@link ShardGuard}
     * finds the cursors on another shard count.
     */
    public int insertAll(Connection inTx, List<Row> rows) throws SQLException {
        guard.require(names, shards);
        int n = rows.size();
        if (n == 0) {
            return 0;
        }
        var rowShards = new Short[n];
        var ids = new UUID[n];
        var topics = new String[n];
        var keys = new byte[n][];
        var payloads = new byte[n][];
        var headers = new byte[n][];
        var hashes = new Integer[n];
        int keyed = 0;
        for (int i = 0; i < n; i++) {
            var r = rows.get(i);
            if (r.key() == null) {
                rowShards[i] = (short) Hashing.randomShard(shards);
            } else {
                int hash = Hashing.keyHash(r.topic(), r.key());
                hashes[keyed++] = hash;
                rowShards[i] = (short) Hashing.shard(hash, shards);
            }
            ids[i] = r.messageId();
            topics[i] = r.topic();
            keys[i] = r.key();
            payloads[i] = r.payload();
            headers[i] = r.headers();
        }
        if (keyed > 0) {
            try (var ps = inTx.prepareStatement(LOCK_ALL)) { // distinct and sorted: no deadlock between bulk writers
                ps.setInt(1, keyNamespace);
                ps.setArray(2, inTx.createArrayOf("int4", Arrays.copyOf(hashes, keyed)));
                ps.execute();
            }
        }
        try (var ps = inTx.prepareStatement(insertAll)) {
            ps.setArray(1, inTx.createArrayOf("int2", rowShards));
            ps.setArray(2, inTx.createArrayOf("uuid", ids));
            ps.setArray(3, inTx.createArrayOf("text", topics));
            ps.setArray(4, inTx.createArrayOf("bytea", keys));
            ps.setArray(5, inTx.createArrayOf("bytea", payloads));
            ps.setArray(6, inTx.createArrayOf("bytea", headers));
            return ps.executeUpdate();
        }
    }
}
