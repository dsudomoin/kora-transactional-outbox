package io.github.dsudomoin.kora.outbox;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A row of the dead table: a record that failed permanently or ran out of {@code outbox.relay.retry.maxAttempts}.
 * {@code id} is the dead row's (its id in the outbox); {@code attempts} counts its failed deliveries, {@code error} the
 * last one's text; {@code redrives} counts the times this message was redriven before it died this time;
 * {@code handlerAttempts} counts failed calls of the dead-letter hook for it; {@code dueAt} is the next hook call of a
 * pending letter or the redrive time of a scheduled one, null for a kept one.
 */
public record DeadLetter(
        long id,
        UUID messageId,
        String topic,
        @Nullable byte[] key,
        @Nullable byte[] payload,
        List<OutboxHeader> headers,
        Instant createdAt,
        int attempts,
        Instant failedAt,
        @Nullable String error,
        Status status,
        int redrives,
        int handlerAttempts,
        @Nullable Instant dueAt) {

    public DeadLetter {
        headers = List.copyOf(headers);
    }

    public enum Status {
        /** Waiting for the dead-letter hook's decision. */
        PENDING,
        /** Kept for humans: decided {@code keep}, or dead while no hook was set. */
        KEPT,
        /** Decided {@code redriveAt}: redriven automatically at {@code dueAt}. */
        SCHEDULED
    }
}
