package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enqueue. A keyed row takes {@code pg_advisory_xact_lock(keyNamespace, keyHash)} in its INSERT statement,
 * before the identity assigns the row id, so per (topic, key) id order is commit order. Unkeyed rows take no lock and
 * land on a random shard. {@code sendAll} inserts through {@link Resender}, every key locked in one statement first.
 * Without a handler behind the routes, a message of a topic no route claims is refused before anything is written.
 */
public final class PgOutbox implements Outbox {

    private static final Logger log = LoggerFactory.getLogger(PgOutbox.class);
    private final OutboxTransactions db;
    private final String notInTx;
    private final int keyNamespace;
    private final int shards;
    private final OutboxTelemetry telemetry;
    private final Runnable wakeUpHook;
    private final String insertKeyed;
    private final String insertUnkeyed;
    private final Resender resender;
    private final TableNames names;
    private final ShardGuard guard;
    /** The topics the routes claim; null: a handler takes every other topic, so every topic goes in. */
    @Nullable
    private final Set<String> claimedTopics;

    /** Takes every topic. */
    public PgOutbox(OutboxTransactions db, OutboxSchema schema, int shards, Runnable wakeUpRelay,
            OutboxTelemetry telemetry) {
        this(db, schema, shards, wakeUpRelay, telemetry, null);
    }

    /** {@code claimedTopics}: the only topics it takes, or null for every topic. */
    public PgOutbox(OutboxTransactions db, OutboxSchema schema, int shards, Runnable wakeUpRelay,
            OutboxTelemetry telemetry, @Nullable Set<String> claimedTopics) {
        OutboxSchema.validateShards(shards);
        this.db = db;
        this.notInTx = "Outbox.send must be called inside " + db.transactionHint();
        this.keyNamespace = schema.names().keyNamespace();
        this.shards = shards;
        this.telemetry = telemetry;
        this.wakeUpHook = () -> {
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
        this.claimedTopics = claimedTopics == null ? null : Set.copyOf(claimedTopics);
    }

    @Override
    public UUID send(OutboxMessage message) {
        var c = currentTransaction();
        var id = insert(c, message);
        wakeUpAfterCommit();
        return id;
    }

    @Override
    public UUID send(Connection connection, OutboxMessage message) {
        requireTransaction(connection);
        var id = insert(connection, message);
        if (connection == db.currentConnection()) {
            wakeUpAfterCommit(); // the caller passed its transaction's own connection
        }
        return id;
    }

    @Override
    public List<UUID> sendAll(List<OutboxMessage> messages) {
        var c = currentTransaction();
        int n = messages.size();
        if (n == 0) {
            return List.of();
        }
        for (var m : messages) {
            requireClaimed(m.topic());
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
                resender.insertAll(c, rows);
            } catch (SQLException e) {
                throw db.translate(e);
            }
        } catch (RuntimeException | Error e) {
            enqueue.failed(e);
            throw e;
        }
        for (var m : messages) {
            telemetry.enqueued(m.topic());
        }
        enqueue.finished();
        wakeUpAfterCommit();
        return List.of(ids);
    }

    /** One message, one enqueue of the telemetry. */
    private UUID insert(Connection c, OutboxMessage m) {
        guard.require(names, shards);
        requireClaimed(m.topic());
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
            var error = db.translate(e);
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

    /**
     * A topic no route claims, without a handler behind the routes, is a typo or a route this release lacks: refused
     * here, in the business transaction, instead of a record that waits for an instance with the route.
     */
    private void requireClaimed(String topic) {
        if (claimedTopics != null && !claimedTopics.contains(topic)) {
            throw new IllegalArgumentException("outbox topic " + topic + " is claimed by no OutboxTopicHandler and"
                    + " there is no OutboxHandler: claimed topics are " + new TreeSet<>(claimedTopics));
        }
    }

    private Connection currentTransaction() {
        var c = db.currentConnection();
        if (c == null) {
            throw new IllegalStateException(notInTx);
        }
        requireTransaction(c);
        return c;
    }

    private void requireTransaction(@Nullable Connection c) {
        try {
            if (c == null || c.getAutoCommit()) {
                throw new IllegalStateException(notInTx);
            }
        } catch (SQLException e) {
            throw db.translate(e);
        }
    }

    /**
     * One hook per transaction, however many messages it sent: the binding runs the same instance once after the
     * commit.
     */
    private void wakeUpAfterCommit() {
        try {
            db.afterCommit(wakeUpHook);
        } catch (SQLException e) {
            throw db.translate(e);
        }
    }
}
