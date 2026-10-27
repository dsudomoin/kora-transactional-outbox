package io.github.dsudomoin.kora.outbox;

import jakarta.annotation.Nullable;
import java.util.Collection;

/**
 * The morning tool, for any code of the service (an endpoint, a job, a test): the outbox's status, dead letters and
 * kept orphans. Every call manages its own transactions, so it fails with {@link IllegalStateException} inside the
 * caller's {@code inTx}; any instance gives the same answers, as all act on the shared database.
 *
 * <p>Bulk calls act on the letters dead when they began, none that die meanwhile (a redriven letter dying again
 * included), in chunks of 1 000 letters in id order, one transaction each, so a large redrive never holds more than
 * 1 000 key locks. A failing chunk rolls back alone: the chunks before it stay done, and calling again goes on.
 * They skip, without waiting and without counting them, the letters a running dead-letter hook call holds and, for a
 * redrive, those whose (topic, key) an open transaction is sending: call again for those. Each mutating call logs its
 * arguments and result in one INFO line, or in one WARN when it fails after it began.
 */
public interface OutboxAdmin {

    /** Read from the database now. */
    OutboxStatus status();

    /**
     * Up to {@code limit} (1..1000) letters matching {@code filter} with an id above {@code afterId} (null: from the
     * first), in id order.
     */
    DeadLetterPage findDeadLetters(DeadLetterFilter filter, @Nullable Long afterId, int limit);

    long countDeadLetters(DeadLetterFilter filter);

    /**
     * Sends these letters back into the outbox, whatever their status, as {@link DeadLetterDecision#redrive()} does: a
     * new message after the later messages of its (topic, key), with the same message id, topic, key, payload and
     * headers, attempts reset, the dead row deleted in the same transaction; should it die again within
     * {@code outbox.deadLetters.redriveHistory}, its {@link DeadLetter#redrives()} is one higher. Wakes the relay.
     * Returns how many were redriven: ids of no letter count for nothing.
     */
    long redriveDeadLetters(Collection<Long> ids);

    /**
     * {@link #redriveDeadLetters(Collection)} for the letters matching {@code filter}.
     *
     * @throws IllegalArgumentException for an empty filter: name what to redrive
     */
    long redriveDeadLetters(DeadLetterFilter filter);

    /** Deletes these letters; returns how many. */
    long discardDeadLetters(Collection<Long> ids);

    /**
     * Deletes the letters matching {@code filter}; returns how many.
     *
     * @throws IllegalArgumentException for an empty filter: name what to discard
     */
    long discardDeadLetters(DeadLetterFilter filter);

    /**
     * Moves the rows of {@code table}, an orphan of {@link OutboxStatus#orphans()}, with an xid at or above
     * {@code fromXid} back into the outbox, then drops it: <b>its rows below {@code fromXid} go with the table</b>.
     * Moved rows are new messages, with new ids, so they are delivered after later messages of their keys, and the
     * same message ids. Runs as no maintenance run does at the same time: waits up to 60 s for one in progress, then
     * fails with {@link IllegalStateException} "maintenance is running, retry". A restore that fails midway loses and
     * repeats nothing; calling again goes on. Wakes the relay; returns the rows moved.
     *
     * @throws IllegalArgumentException before any SQL that names it, unless {@code table} is {@code <base>_x<digits>}
     *     or {@code <base>_o<digits>}, unqualified or qualified as the outbox is named, a table in the outbox's schema
     *     in no partition tree, and not one the partition extension made ahead of the tail for its next swap
     */
    long restoreOrphan(String table, long fromXid);
}
