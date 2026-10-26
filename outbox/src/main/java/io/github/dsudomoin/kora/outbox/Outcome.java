package io.github.dsudomoin.kora.outbox;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.Objects;

/** Result of delivering one record, returned by {@link OutboxHandler#handleBatch}. */
public sealed interface Outcome {

    static Outcome delivered() {
        return Delivered.INSTANCE;
    }

    /** Transient failure: the (topic, key) is blocked and the record is retried with backoff. */
    static Outcome retry(Throwable error) {
        return new Retry(error, null);
    }

    /** Transient failure, retried no earlier than {@code notBefore}. */
    static Outcome retry(Throwable error, Instant notBefore) {
        return new Retry(error, Objects.requireNonNull(notBefore, "notBefore"));
    }

    /** The record can never succeed: it moves to the dead table and its (topic, key) continues. */
    static Outcome permanent(Throwable error) {
        return new Permanent(error);
    }

    /** The handler did not try the record (its key already failed, or the target looks down). */
    static Outcome notAttempted() {
        return new NotAttempted(null);
    }

    static Outcome notAttempted(Instant notBefore) {
        return new NotAttempted(Objects.requireNonNull(notBefore, "notBefore"));
    }

    enum Delivered implements Outcome {
        INSTANCE;

        @Override
        public String toString() {
            return "Delivered";
        }
    }

    record Retry(Throwable error, @Nullable Instant notBefore) implements Outcome {
        public Retry {
            Objects.requireNonNull(error, "error");
        }
    }

    record Permanent(Throwable error) implements Outcome {
        public Permanent {
            Objects.requireNonNull(error, "error");
        }
    }

    record NotAttempted(@Nullable Instant notBefore) implements Outcome {}
}
