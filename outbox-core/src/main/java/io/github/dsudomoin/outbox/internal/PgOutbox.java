package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxHeader;
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
 * Without a handler behind the routes, a message of a topic no route claims is refused before anything is written;
 * so is a contract version some live relay instance cannot read ({@link ContractGuard}). A message of a topic a
 * route of this instance claims is written with the {@code outbox-local} header, which marks its record for a route,
 * after the telemetry's headers and in place of any such header given; any other message is written without one.
 */
public final class PgOutbox implements Outbox {

    /** The header, null-valued, that marks a record for a route: one of any other value marks nothing. */
    public static final String LOCAL_HEADER = "outbox-local";
    private static final Logger log = LoggerFactory.getLogger(PgOutbox.class);
    /** The written type of a message sent as is, which {@link ContractGuard} names. */
    private static final String UNKNOWN_TYPE = "?";
    private static final OutboxHeader LOCAL = new OutboxHeader(LOCAL_HEADER, null);
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
    private final ContractGuard contracts;
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
        this.contracts = schema.contractGuard(); // the live relay instances' reports as its heartbeat last read them
        this.claimedTopics = claimedTopics == null ? null : Set.copyOf(claimedTopics);
    }

    @Override
    public UUID send(OutboxMessage message) {
        return send(message, UNKNOWN_TYPE);
    }

    /** {@link #send(OutboxMessage)} of a message written as {@code type}, the class name a refusal names. */
    UUID send(OutboxMessage message, String type) {
        var c = currentTransaction();
        var id = insert(c, message, type);
        wakeUpAfterCommit();
        return id;
    }

    @Override
    public UUID send(Connection connection, OutboxMessage message) {
        requireTransaction(connection);
        var id = insert(connection, message, UNKNOWN_TYPE);
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
            contracts.require(names, m, UNKNOWN_TYPE, telemetry);
        }
        var enqueue = telemetry.enqueueStarted();
        var ids = new UUID[n];
        try {
            var rows = new ArrayList<Resender.Row>(n);
            for (int i = 0; i < n; i++) {
                var m = messages.get(i);
                ids[i] = UuidV7.next();
                var headers = marked(m.topic(), enqueue.headers(m.headers()));
                rows.add(new Resender.Row(ids[i], m.topic(), m.key(), m.payload(), HeaderCodec.encode(headers)));
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
    private UUID insert(Connection c, OutboxMessage m, String type) {
        guard.require(names, shards);
        requireClaimed(m.topic());
        contracts.require(names, m, type, telemetry);
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
            ps.setBytes(6, HeaderCodec.encode(marked(m.topic(), enqueue.headers(m.headers()))));
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

    /**
     * {@code headers} without their {@code outbox-local} headers, plus the marker if a route of this instance claims
     * {@code topic}; the very list when there is nothing to change, so an unmarked write copies nothing.
     */
    private List<OutboxHeader> marked(String topic, List<OutboxHeader> headers) {
        boolean local = contracts.routes().contains(topic);
        if (!local && !hasLocal(headers)) {
            return headers;
        }
        var stamped = new ArrayList<>(headers);
        stamped.removeIf(h -> h.name().equals(LOCAL_HEADER));
        if (local) {
            stamped.add(LOCAL);
        }
        return stamped;
    }

    /** A loop, not a stream: every unmarked write runs it. */
    private static boolean hasLocal(List<OutboxHeader> headers) {
        for (var h : headers) {
            if (h.name().equals(LOCAL_HEADER)) {
                return true;
            }
        }
        return false;
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
