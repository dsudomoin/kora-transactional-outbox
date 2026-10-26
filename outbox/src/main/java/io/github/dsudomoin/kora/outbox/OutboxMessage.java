package io.github.dsudomoin.kora.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A message to relay. {@code key} orders messages of one topic ({@code null} = unordered), {@code payload} may be
 * {@code null} (Kafka tombstone).
 */
public record OutboxMessage(String topic, @Nullable byte[] key, @Nullable byte[] payload, List<OutboxHeader> headers) {

    private static final int MAX_TOPIC_LENGTH = 249; // Kafka's limit
    private static final int MAX_KEY_BYTES = 1024;

    public OutboxMessage {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (topic.length() > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("topic longer than " + MAX_TOPIC_LENGTH + " chars: " + topic.length());
        }
        if (key != null && key.length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("key longer than " + MAX_KEY_BYTES + " bytes: " + key.length);
        }
        headers = List.copyOf(headers);
    }

    /** Unkeyed message: no ordering relative to other messages. */
    public static OutboxMessage of(String topic, @Nullable byte[] payload) {
        return new OutboxMessage(topic, null, payload, List.of());
    }

    public static OutboxMessage of(String topic, @Nullable byte[] key, @Nullable byte[] payload) {
        return new OutboxMessage(topic, key, payload, List.of());
    }

    public static OutboxMessage of(String topic, @Nullable String key, @Nullable byte[] payload) {
        return of(topic, key == null ? null : key.getBytes(UTF_8), payload);
    }

    public OutboxMessage withHeader(String name, @Nullable byte[] value) {
        var copy = new ArrayList<>(headers);
        copy.add(new OutboxHeader(name, value));
        return new OutboxMessage(topic, key, payload, copy);
    }
}
