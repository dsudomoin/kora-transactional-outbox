package io.github.dsudomoin.outbox;

import java.sql.Connection;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * Consumer-side dedup: records a message id in the consumer's transaction, so a consumer processes a message
 * once per id within the dedup window. With the outbox this gives effectively-once processing:
 * {@code if (inbox.tryAccept(id)) process(message);} inside one transaction.
 *
 * <p>Dedup is per consumer. An id accepted in one {@link #scope(String) scope} is still new to every other scope, and
 * every independent consumer of the same messages needs a scope of its own: two Kafka listeners in different consumer
 * groups on one topic, or two services that share one inbox table, e.g. {@code inbox.scope("orders-projection")} and
 * {@code inbox.scope("mailer")}. Consumers that share a scope share its ids: the second one skips every message the
 * first one accepted. The methods of the unscoped inbox are the scope {@code ""}.
 *
 * <p>The window goes back {@code outbox.inbox.retention} from now, by the UUIDv7 timestamp of the id (the outbox
 * message id, Kafka header {@code outbox-message-id}), less up to one {@code outbox.inbox.partitionInterval} of margin:
 * ids up to {@code retention - partitionInterval} old are always deduplicated. An id outside the window, or an id that
 * is not UUIDv7, is accepted without dedup ({@code true}, nothing recorded, a rate-limited WARN and
 * {@code outbox.inbox.outside_window}); it never fails the transaction.
 */
public interface Inbox {

    /**
     * Must run inside the application's transaction (see {@link OutboxTransactions}), e.g.
     * {@code JdbcTransactions.inTx(...)}. True the first time this scope sees the id (process the message), false for
     * a duplicate (skip it). A rolled-back transaction records nothing.
     */
    boolean tryAccept(UUID messageId);

    /** For code that holds the connection itself; it must be in a transaction (autocommit off). */
    boolean tryAccept(Connection connection, UUID messageId);

    /**
     * Batch form of {@link #tryAccept(UUID)}: one statement for all ids. Returns the ids this scope sees for the first
     * time; process exactly those, each once. An id that occurs several times in the input is returned at most once.
     *
     * <p>The ids are recorded in one fixed order, so consumers accepting overlapping polls concurrently wait for each
     * other instead of deadlocking, as long as each transaction accepts its poll with one call. Several accept calls in
     * one transaction ({@code tryAccept} or {@code tryAcceptAll}) record their ids in call order and can deadlock with a
     * concurrent consumer of the same ids: Postgres then aborts one of the transactions (SQLState 40P01) and that
     * consumer retries its poll, as after any failed transaction.
     */
    Set<UUID> tryAcceptAll(Collection<UUID> messageIds);

    /** For code that holds the connection itself; it must be in a transaction (autocommit off). */
    Set<UUID> tryAcceptAll(Connection connection, Collection<UUID> messageIds);

    /**
     * This inbox for one of several independent consumers of the same messages: its methods deduplicate within
     * {@code consumer} only, with the same table, window and transaction rules. {@code consumer} is 1 to 64 characters
     * of {@code A-Z a-z 0-9 _ . : -}. Keep it stable across deploys: under a new name every message is new.
     *
     * @throws IllegalArgumentException for any other name
     */
    Inbox scope(String consumer);
}
