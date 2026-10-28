package io.github.dsudomoin.outbox.kafka;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxSerializationException;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Typed {@link Outbox#send(OutboxMessage)} to one Kafka topic with any Kafka {@link Serializer}s (the Kora adapter's
 * {@code @Json} ones included). The ordering key is the serialized Kafka key, the bytes Kafka partitions by; a
 * {@code null} key is unordered and a {@code null} value a tombstone. A serializer returning {@code null} for a
 * non-null key or value throws {@link OutboxSerializationException}. Like {@link Outbox#send(OutboxMessage)} it must
 * run inside the application's transaction (see {@link io.github.dsudomoin.outbox.OutboxTransactions}).
 */
public final class KafkaOutboxPublisher<K, V> {

    private static final Pattern VALID_TOPIC = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    private final Outbox outbox;
    private final String topic;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public KafkaOutboxPublisher(
            Outbox outbox, String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.outbox = Objects.requireNonNull(outbox, "outbox");
        // validated now, not on the first send
        if (!validTopic(topic)) {
            throw new IllegalArgumentException("invalid Kafka topic name: '" + topic + "'");
        }
        this.topic = topic;
        this.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer");
        this.valueSerializer = Objects.requireNonNull(valueSerializer, "valueSerializer");
    }

    /** Kafka's rule for topic names (Topic.validate, not public API), which refuses the rest. */
    static boolean validTopic(@Nullable String topic) {
        return topic != null && VALID_TOPIC.matcher(topic).matches() && !topic.equals(".") && !topic.equals("..");
    }

    /** Returns the message id. */
    public UUID send(@Nullable K key, @Nullable V value) {
        return send(key, value, new RecordHeaders());
    }

    /**
     * Returns the message id. The serializers get the topic and a copy of {@code headers}; that copy, with whatever
     * they added, becomes the message's headers.
     */
    public UUID send(@Nullable K key, @Nullable V value, Headers headers) {
        var copy = new RecordHeaders(headers.toArray());
        var keyBytes = serialized(keySerializer.serialize(topic, copy, key), key, keySerializer);
        var valueBytes = serialized(valueSerializer.serialize(topic, copy, value), value, valueSerializer);
        var outboxHeaders = new ArrayList<OutboxHeader>();
        for (var h : copy) {
            outboxHeaders.add(new OutboxHeader(h.key(), h.value()));
        }
        return outbox.send(new OutboxMessage(topic, keyBytes, valueBytes, outboxHeaders));
    }

    /** M2: null for a non-null key would drop its ordering, for a non-null value write a tombstone. */
    @Nullable
    private byte[] serialized(@Nullable byte[] bytes, @Nullable Object data, Serializer<?> serializer) {
        if (bytes == null && data != null) {
            throw new OutboxSerializationException(topic, data,
                    new IllegalStateException(serializer.getClass().getName() + " returned null"));
        }
        return bytes;
    }
}
