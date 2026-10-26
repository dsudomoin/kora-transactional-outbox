package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.database.jdbc.ConnectionContext;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * Enqueue. A keyed row takes {@code pg_advisory_xact_lock(keyNamespace, keyHash)} in its INSERT statement,
 * before the identity assigns the row id, so per (topic, key) id order is commit order. Unkeyed rows take no lock and
 * land on a random shard.
 */
public final class PgOutbox implements Outbox {

    private static final Logger log = LoggerFactory.getLogger(PgOutbox.class);
    private static final String NOT_IN_TX = "Outbox.send must be called inside JdbcConnectionFactory.inTx(...)";
    /**
     * One row whatever the driver's fetch size: a row-per-lock result read in portals (pgjdbc
     * {@code defaultRowFetchSize} in a transaction) would take only the first portal's locks. The planner keeps the
     * volatile lock call in the unused subquery column and does not pull the sorted subquery up, so every key is
     * locked, in ascending hash order.
     */
    private static final String LOCK_ALL = "select count(*) from (select pg_advisory_xact_lock(?, h)"
            + " from (select distinct unnest(?::int[]) as h order by 1) s) l";

    private final JdbcConnectionFactory db;
    private final int keyNamespace;
    private final int shards;
    private final OutboxMetrics metrics;
    private final ConnectionContext.PostCommitAction wakeUpHook;
    private final String insertKeyed;
    private final String insertUnkeyed;
    private final String insertAll;

    public PgOutbox(
            JdbcConnectionFactory db, OutboxSchema schema, int shards, Runnable wakeUpRelay, OutboxMetrics metrics) {
        OutboxSchema.validateShards(shards);
        this.db = db;
        this.keyNamespace = schema.names().keyNamespace();
        this.shards = shards;
        this.metrics = metrics;
        this.wakeUpHook = c -> {
            try {
                wakeUpRelay.run();
            } catch (RuntimeException e) {
                // the transaction is committed: an exception here would make callers retry and duplicate messages
                log.warn("outbox relay wake-up failed after commit; the relay's idle poll delivers instead", e);
            }
        };
        var insert = "insert into " + schema.names().base() + " (shard, message_id, topic, key, payload, headers) ";
        this.insertKeyed = insert + "select ?, ?, ?, ?, ?, ? from (select pg_advisory_xact_lock(?, ?)) l";
        this.insertUnkeyed = insert + "values (?, ?, ?, ?, ?, ?)";
        this.insertAll = insert + "select u.shard, u.mid, u.topic, u.key, u.payload, u.headers"
                + " from unnest(?::smallint[], ?::uuid[], ?::text[], ?::bytea[], ?::bytea[], ?::bytea[])"
                + " with ordinality u(shard, mid, topic, key, payload, headers, ord) order by u.ord";
    }

    @Override
    public UUID send(OutboxMessage message) {
        var ctx = currentTransaction();
        var id = insert(ctx.connection(), message);
        wakeUpAfterCommit(ctx);
        return id;
    }

    @Override
    public UUID send(Connection connection, OutboxMessage message) {
        requireTransaction(connection);
        var id = insert(connection, message);
        var ctx = db.currentConnectionContext();
        if (ctx != null && ctx.connection() == connection) {
            wakeUpAfterCommit(ctx); // the caller passed Kora's own transaction connection
        }
        return id;
    }

    @Override
    public List<UUID> sendAll(List<OutboxMessage> messages) {
        var ctx = currentTransaction();
        int n = messages.size();
        if (n == 0) {
            return List.of();
        }
        var trace = OutboxTracing.currentHeaders();
        var ids = new UUID[n];
        var rowShards = new Short[n];
        var topics = new String[n];
        var keys = new byte[n][];
        var payloads = new byte[n][];
        var headers = new byte[n][];
        var hashes = new Integer[n];
        int keyed = 0;
        for (int i = 0; i < n; i++) {
            var m = messages.get(i);
            ids[i] = UuidV7.next();
            if (m.key() == null) {
                rowShards[i] = (short) Hashing.randomShard(shards);
            } else {
                int hash = Hashing.keyHash(m.topic(), m.key());
                hashes[keyed++] = hash;
                rowShards[i] = (short) Hashing.shard(hash, shards);
            }
            topics[i] = m.topic();
            keys[i] = m.key();
            payloads[i] = m.payload();
            headers[i] = HeaderCodec.encode(withTrace(m.headers(), trace));
        }
        var c = ctx.connection();
        try {
            if (keyed > 0) {
                try (var ps = c.prepareStatement(LOCK_ALL)) { // distinct and sorted: no deadlock between bulk writers
                    ps.setInt(1, keyNamespace);
                    ps.setArray(2, c.createArrayOf("int4", Arrays.copyOf(hashes, keyed)));
                    ps.execute();
                }
            }
            try (var ps = c.prepareStatement(insertAll)) {
                ps.setArray(1, c.createArrayOf("int2", rowShards));
                ps.setArray(2, c.createArrayOf("uuid", ids));
                ps.setArray(3, c.createArrayOf("text", topics));
                ps.setArray(4, c.createArrayOf("bytea", keys));
                ps.setArray(5, c.createArrayOf("bytea", payloads));
                ps.setArray(6, c.createArrayOf("bytea", headers));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
        for (var m : messages) {
            metrics.enqueued(m.topic());
        }
        wakeUpAfterCommit(ctx);
        return List.of(ids);
    }

    private UUID insert(Connection c, OutboxMessage m) {
        var id = UuidV7.next();
        boolean keyed = m.key() != null;
        int hash = keyed ? Hashing.keyHash(m.topic(), m.key()) : 0;
        try (var ps = c.prepareStatement(keyed ? insertKeyed : insertUnkeyed)) {
            ps.setShort(1, (short) (keyed ? Hashing.shard(hash, shards) : Hashing.randomShard(shards)));
            ps.setObject(2, id);
            ps.setString(3, m.topic());
            ps.setBytes(4, m.key());
            ps.setBytes(5, m.payload());
            ps.setBytes(6, HeaderCodec.encode(withTrace(m.headers(), OutboxTracing.currentHeaders())));
            if (keyed) {
                ps.setInt(7, keyNamespace);
                ps.setInt(8, hash);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
        metrics.enqueued(m.topic());
        return id;
    }

    /** Message headers plus the current trace, unless the message already carries a traceparent. */
    private static List<OutboxHeader> withTrace(List<OutboxHeader> headers, List<OutboxHeader> trace) {
        if (trace.isEmpty()) {
            return headers;
        }
        for (var h : headers) {
            if (h.name().equals(OutboxTracing.TRACEPARENT)) {
                return headers;
            }
        }
        var all = new ArrayList<OutboxHeader>(headers.size() + trace.size());
        all.addAll(headers);
        all.addAll(trace);
        return all;
    }

    private ConnectionContext currentTransaction() {
        var ctx = db.currentConnectionContext();
        if (ctx == null || ctx.connection() == null) {
            throw new IllegalStateException(NOT_IN_TX);
        }
        requireTransaction(ctx.connection());
        return ctx;
    }

    private static void requireTransaction(@Nullable Connection c) {
        try {
            if (c == null || c.getAutoCommit()) {
                throw new IllegalStateException(NOT_IN_TX);
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e);
        }
    }

    /**
     * One hook per {@link ConnectionContext}: Kora runs a context's post-commit actions after every {@code inTx}
     * commit on it, so the relay wakes once per committed transaction however many messages it sent.
     */
    private void wakeUpAfterCommit(ConnectionContext ctx) {
        if (!ctx.postCommitActions().contains(wakeUpHook)) {
            try {
                ctx.addPostCommitAction(wakeUpHook);
            } catch (SQLException e) {
                throw new RuntimeSqlException(e);
            }
        }
    }
}
