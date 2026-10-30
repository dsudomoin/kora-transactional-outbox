package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

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

    static final String CONTRACT_HEADER = "outbox-contract";

    public OutboxRecord {
        headers = List.copyOf(headers);
    }

    /** A record of unknown shard ({@code -1}). */
    public OutboxRecord(long id, UUID messageId, String topic, @Nullable byte[] key, @Nullable byte[] payload,
            List<OutboxHeader> headers, Instant createdAt, int attempts, int requeues) {
        this(id, messageId, topic, key, payload, headers, createdAt, attempts, requeues, -1);
    }

    /** The key as UTF-8, as {@link TypedOutbox} and {@link OutboxMessage} write a {@code String} key; null if none. */
    @Nullable
    public String keyAsString() {
        return key == null ? null : new String(key, UTF_8);
    }

    /**
     * The version of the payload's contract: the last {@code outbox-contract} header, a decimal int, or 1 without one.
     * A null, unparseable or non-positive value throws {@link OutboxWaitException} of kind
     * {@link Outcome.Waiting.Kind#CONTRACT}: this instance cannot tell what the payload is, so the record waits.
     */
    public int contractVersion() {
        for (int i = headers.size() - 1; i >= 0; i--) {
            var header = headers.get(i);
            if (header.name().equals(CONTRACT_HEADER)) {
                var value = header.value() == null ? null : new String(header.value(), US_ASCII);
                int version;
                try {
                    version = Integer.parseInt(value);
                } catch (NumberFormatException e) { // a null value too
                    version = 0;
                }
                if (version <= 0) {
                    throw new OutboxWaitException(Outcome.Waiting.Kind.CONTRACT,
                            "invalid " + CONTRACT_HEADER + " header: " + value);
                }
                return version;
            }
        }
        return 1;
    }

    public OutboxRecord withHeaders(List<OutboxHeader> headers) {
        return new OutboxRecord(id, messageId, topic, key, payload, headers, createdAt, attempts, requeues, shard);
    }
}
