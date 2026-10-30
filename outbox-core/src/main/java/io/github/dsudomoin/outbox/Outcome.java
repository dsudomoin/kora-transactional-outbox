package io.github.dsudomoin.outbox;

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

    /**
     * Not now: the record steps aside (it is parked) so the later records of its (topic, key) go on, and is handed
     * again ahead of the next record of its key, or at {@code now + outbox.relay.requeue.delay} if none comes first.
     * Not a failure: it counts no attempt and does not pause the shard. Each requeue adds one to
     * {@link OutboxRecord#requeues()}; past {@code outbox.relay.requeue.maxRequeues} the record goes to the dead table.
     */
    static Outcome requeue() {
        return new Requeue(null);
    }

    /** {@link #requeue()} due at {@code max(now + delay, notBefore)}, {@code notBefore} capped at 24 h from now. */
    static Outcome requeue(Instant notBefore) {
        return new Requeue(Objects.requireNonNull(notBefore, "notBefore"));
    }

    /**
     * This instance cannot handle the record now: it spends no attempt, never goes to the dead table by itself, blocks
     * its (topic, key), and a capable instance takes it later. Kind {@link Waiting.Kind#HANDLER};
     * {@link OutboxWaitException} is the same, thrown from {@link OutboxHandler#handle}.
     */
    static Outcome waitFor(String reason) {
        return new Waiting(Waiting.Kind.HANDLER, reason, null);
    }

    /** {@link #waitFor(String)}, handed again no earlier than {@code notBefore}, capped at 24 h from now. */
    static Outcome waitFor(String reason, Instant notBefore) {
        return new Waiting(Waiting.Kind.HANDLER, reason, Objects.requireNonNull(notBefore, "notBefore"));
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

    record Requeue(@Nullable Instant notBefore) implements Outcome {}

    /** See {@link #waitFor(String)}; {@code reason} is free text, {@code kind} a closed set fit for a metric label. */
    record Waiting(Kind kind, String reason, @Nullable Instant notBefore) implements Outcome {
        public Waiting {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(reason, "reason");
        }

        /** Why the record waits. */
        public enum Kind {
            /** The record's contract version is unreadable or above the one this instance reads. */
            CONTRACT("contract"),
            /** No route on this instance claims the record's topic. */
            NO_ROUTE("no_route"),
            /** The handler said so ({@link Outcome#waitFor}, {@link OutboxWaitException}). */
            HANDLER("handler");

            private final String label;

            Kind(String label) {
                this.label = label;
            }

            /** The metric label: {@code contract}, {@code no_route} or {@code handler}. */
            public String label() {
                return label;
            }
        }
    }
}
