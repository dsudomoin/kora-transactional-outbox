package io.github.dsudomoin.kora.outbox;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** A stored message as handed to the handler; {@code attempts} counts earlier failed deliveries. */
public record OutboxRecord(
        long id,
        UUID messageId,
        String topic,
        @Nullable byte[] key,
        @Nullable byte[] payload,
        List<OutboxHeader> headers,
        Instant createdAt,
        int attempts) {

    public OutboxRecord {
        headers = List.copyOf(headers);
    }

    public OutboxRecord withHeaders(List<OutboxHeader> headers) {
        return new OutboxRecord(id, messageId, topic, key, payload, headers, createdAt, attempts);
    }
}
