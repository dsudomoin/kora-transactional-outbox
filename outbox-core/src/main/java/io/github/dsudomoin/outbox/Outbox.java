package io.github.dsudomoin.outbox;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;

/**
 * Writes messages in the caller's transaction; the relay hands them to the handler after commit. Messages with equal
 * topic and key are delivered in commit order, and in call order within one transaction. Each method returns the
 * message id (UUIDv7, Kafka header {@code outbox-message-id}).
 */
public interface Outbox {

    /** Must run inside {@code JdbcConnectionFactory.inTx(...)}; wakes the relay after commit. */
    UUID send(OutboxMessage message);

    /**
     * For code that holds the connection itself; it must be in a transaction (autocommit off). Only Kora's current
     * {@code inTx} connection wakes the relay after commit; with any other connection delivery waits for the relay's
     * idle poll.
     */
    UUID send(Connection connection, OutboxMessage message);

    /**
     * Bulk {@link #send(OutboxMessage)}: two statements whatever the size, ids in input order. Locks all its keys at
     * once in a fixed order, so transactions that each take their outbox key locks in one {@code sendAll} cannot
     * deadlock on them. A transaction that already holds key locks, from an earlier {@code send} or {@code sendAll},
     * can deadlock with another that takes the same keys in a different order; Postgres then aborts one of them. The
     * safe rule: send all keyed messages of a transaction in one {@code sendAll}, with no other {@code send} in that
     * transaction. Sending keys in one consistent order instead only helps when no transaction uses a multi-key
     * {@code sendAll}: it locks in key-hash order, which callers cannot reproduce.
     */
    List<UUID> sendAll(List<OutboxMessage> messages);
}
