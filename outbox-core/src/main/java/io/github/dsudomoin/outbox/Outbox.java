package io.github.dsudomoin.outbox;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;

/**
 * Writes messages in the caller's transaction; the relay hands them to the handler after commit. Messages with equal
 * topic and key are delivered in commit order, and in call order within one transaction. Each method returns the
 * message id (UUIDv7, Kafka header {@code outbox-message-id}).
 *
 * <p>A message of a topic that a route of this runtime claims ({@link OutboxRuntime.Builder#route}) is written with
 * the header {@code outbox-local}, null-valued, which marks its record for the route: a relay instance without that
 * route makes it wait instead of handing it to its handler, and Kafka never receives the header. The name is
 * reserved: a header of that name given with a message is dropped, whatever its value.
 *
 * <p>Each method refuses a message before anything is written: with {@link OutboxMissingRouteException} when a route
 * of this runtime claims its topic and a live relay instance does not route it; with
 * {@link OutboxIncompatibleContractException} when its {@code outbox-contract} header gives a version above 1 of its
 * topic that a live relay instance cannot read; with {@link IllegalArgumentException} when that header gives no
 * version (not a positive decimal int), and, in a runtime whose routes have no handler behind them, when no route
 * claims its topic. {@code sendAll} writes none of its messages then. The live relay instances are as this
 * instance's heartbeat last read their reports; when that read is older than three {@code maintenanceInterval}s, a
 * message these checks cover is let through only if that read found a live relay instance and each one reported and
 * allowed it, else it is refused.
 */
public interface Outbox {

    /**
     * Must run inside the application's transaction (see {@link OutboxTransactions}), e.g.
     * {@code JdbcTransactions.inTx(...)}; wakes the relay after commit.
     */
    UUID send(OutboxMessage message);

    /**
     * For code that holds the connection itself; it must be in a transaction (autocommit off). Only the connection of
     * the caller's transaction ({@link OutboxTransactions#currentConnection()}) wakes the relay after commit; with any
     * other connection delivery waits for the relay's idle poll.
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
