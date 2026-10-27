package io.github.dsudomoin.kora.outbox;

import java.time.Instant;
import java.util.Objects;

/** What becomes of one dead letter, returned by {@link OutboxDeadLetterHandler#handle}. */
public sealed interface DeadLetterDecision {

    /** The letter stays in the dead table for humans; the hook is not called for it again. */
    static DeadLetterDecision keep() {
        return new Keep();
    }

    /** The letter is deleted. */
    static DeadLetterDecision discard() {
        return new Discard();
    }

    /**
     * The letter goes back into the outbox now, as a new message after the later messages of its (topic, key): same
     * message id, topic, key, payload and headers, attempts reset. Should it die again within
     * {@code outbox.deadLetters.redriveHistory}, its {@link DeadLetter#redrives()} is one higher, so a hook can stop a
     * redrive loop. The dead row is deleted in the same transaction.
     */
    static DeadLetterDecision redrive() {
        return new Redrive();
    }

    /**
     * The letter is kept ({@link DeadLetter.Status#SCHEDULED}) and redriven as by {@link #redrive()} once {@code at}
     * comes, without a further call; {@code at} is clamped to [now, now + 7 days].
     */
    static DeadLetterDecision redriveAt(Instant at) {
        return new RedriveAt(at);
    }

    record Keep() implements DeadLetterDecision {}

    record Discard() implements DeadLetterDecision {}

    record Redrive() implements DeadLetterDecision {}

    record RedriveAt(Instant at) implements DeadLetterDecision {
        public RedriveAt {
            Objects.requireNonNull(at, "at");
        }
    }
}
