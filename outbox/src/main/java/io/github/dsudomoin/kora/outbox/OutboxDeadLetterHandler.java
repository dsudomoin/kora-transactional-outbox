package io.github.dsudomoin.kora.outbox;

import java.util.List;

/**
 * Decides the fate of dead letters (records that failed permanently or ran out of retry attempts). Optional: without
 * one, or with {@link #NONE}, every dead letter is kept for humans.
 */
public interface OutboxDeadLetterHandler {

    /**
     * Returns one {@link DeadLetterDecision} per letter, in the same order, for at most
     * {@code outbox.deadLetters.batchSize} letters per call.
     *
     * <p>At least once: a letter waits for the hook ({@link DeadLetter.Status#PENDING}) until a decision about it
     * commits. A call that throws (an {@link Error} included), returns an invalid list (null, wrong size, a null
     * element) or runs past {@code outbox.deadLetters.timeout} (it is then interrupted and abandoned) leaves all its
     * letters pending: each is called again after a backoff ({@code outbox.relay.retry.initialDelay} doubling to
     * {@code maxDelay}) with {@link DeadLetter#handlerAttempts()} one higher. A crash between the call and the commit
     * calls it again too. So an alert sent from here is never lost but may be sent twice; key it by
     * {@link DeadLetter#messageId()}.
     *
     * <p>Runs on the daemon thread {@code outbox-dead-letter-hook}, on no relay thread and in no relay transaction, so
     * a slow or failing hook never delays delivery; meanwhile its letters stay locked in a transaction of their own
     * connection, so no instance hands them to another call. Ordering between letters is not guaranteed.
     */
    List<DeadLetterDecision> handle(List<DeadLetter> letters) throws Exception;

    /** No hook: every dead letter is kept for humans, without a call (the Kafka module's default, its DLQ off). */
    OutboxDeadLetterHandler NONE = letters -> {
        throw new UnsupportedOperationException("NONE is never called");
    };
}
