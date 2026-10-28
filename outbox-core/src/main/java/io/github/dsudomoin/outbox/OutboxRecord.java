package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A stored message as handed to the handler; {@code attempts} counts earlier failed deliveries, {@code requeues} the
 * times it was requeued ({@link Outcome#requeue()}) before this attempt. {@code shard}: the shard the relay read it
 * from, {@code hash(topic, key) mod outbox.shards} (random for a keyless message), {@code -1} when unknown (built by
 * hand, rebuilt from a dead letter).
 */
public record OutboxRecord(
        long id,
        UUID messageId,
        String topic,
        @Nullable byte[] key,
        @Nullable byte[] payload,
        List<OutboxHeader> headers,
        Instant createdAt,
        int attempts,
        int requeues,
        int shard) {

    public OutboxRecord {
        headers = List.copyOf(headers);
    }

    /** A record of unknown shard ({@code -1}). */
    public OutboxRecord(long id, UUID messageId, String topic, @Nullable byte[] key, @Nullable byte[] payload,
            List<OutboxHeader> headers, Instant createdAt, int attempts, int requeues) {
        this(id, messageId, topic, key, payload, headers, createdAt, attempts, requeues, -1);
    }

    public OutboxRecord withHeaders(List<OutboxHeader> headers) {
        return new OutboxRecord(id, messageId, topic, key, payload, headers, createdAt, attempts, requeues, shard);
    }
}
