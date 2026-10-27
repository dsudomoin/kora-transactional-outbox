package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
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
 * land on a random shard. {@code sendAll} inserts through {@link Resender}, every key locked in one statement first.
 */
public final class PgOutbox implements Outbox {

    private static final Logger log = LoggerFactory.getLogger(PgOutbox.class);
    private static final String NOT_IN_TX = "Outbox.send must be called inside JdbcConnectionFactory.inTx(...)";

    private final JdbcConnectionFactory db;
    private final int keyNamespace;
    private final int shards;
    private final OutboxTelemetry telemetry;
    private final ConnectionContext.PostCommitAction wakeUpHook;
    private final String insertKeyed;
    private final String insertUnkeyed;
    private final Resender resender;
    private final TableNames names;
    private final ShardGuard guard;

    public PgOutbox(JdbcConnectionFactory db, OutboxSchema schema, int shards, Runnable wakeUpRelay,
            OutboxTelemetry telemetry) {
        OutboxSchema.validateShards(shards);
        this.db = db;
        this.keyNamespace = schema.names().keyNamespace();
        this.shards = shards;
        this.telemetry = telemetry;
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
        this.names = schema.names();
        this.guard = schema.shardGuard(); // the cursors' count as this instance's heartbeat last read it
        this.resender = new Resender(names, shards, guard);
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
        var enqueue = telemetry.enqueueStarted();
        var ids = new UUID[n];
        try {
            var rows = new ArrayList<Resender.Row>(n);
            for (int i = 0; i < n; i++) {
                var m = messages.get(i);
                ids[i] = UuidV7.next();
                rows.add(new Resender.Row(
                        ids[i], m.topic(), m.key(), m.payload(), HeaderCodec.encode(enqueue.headers(m.headers()))));
            }
            try {
                resender.insertAll(ctx.connection(), rows);
            } catch (SQLException e) {
                throw new RuntimeSqlException(e);
            }
        } catch (RuntimeException | Error e) {
            enqueue.failed(e);
            throw e;
        }
        for (var m : messages) {
            telemetry.enqueued(m.topic());
        }
        enqueue.finished();
        wakeUpAfterCommit(ctx);
        return List.of(ids);
    }

    /** One message, one enqueue of the telemetry. */
    private UUID insert(Connection c, OutboxMessage m) {
        guard.require(names, shards);
        var enqueue = telemetry.enqueueStarted();
        var id = UuidV7.next();
        boolean keyed = m.key() != null;
        int hash = keyed ? Hashing.keyHash(m.topic(), m.key()) : 0;
        try (var ps = c.prepareStatement(keyed ? insertKeyed : insertUnkeyed)) {
            ps.setShort(1, (short) (keyed ? Hashing.shard(hash, shards) : Hashing.randomShard(shards)));
            ps.setObject(2, id);
            ps.setString(3, m.topic());
            ps.setBytes(4, m.key());
            ps.setBytes(5, m.payload());
            ps.setBytes(6, HeaderCodec.encode(enqueue.headers(m.headers())));
            if (keyed) {
                ps.setInt(7, keyNamespace);
                ps.setInt(8, hash);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            var error = new RuntimeSqlException(e);
            enqueue.failed(error);
            throw error;
        } catch (RuntimeException | Error e) {
            enqueue.failed(e);
            throw e;
        }
        telemetry.enqueued(m.topic());
        enqueue.finished();
        return id;
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
