package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * An {@link OutboxTopicHandler} that gets each payload decoded by an {@link OutboxDeserializer}: the value goes to
 * {@link #handle(Object, OutboxRecord)}, a record with a {@code null} payload (a tombstone) to
 * {@link #handleTombstone}. The mirror of {@link TypedOutbox}.
 * <pre>{@code
 * @Component
 * public final class AuditHandler extends TypedOutboxTopicHandler<AuditEvent> {
 *
 *     private final AuditClient client;
 *
 *     public AuditHandler(OutboxDeserializer<AuditEvent> deserializer, AuditClient client) {
 *         super(Set.of("audit"), deserializer);
 *         this.client = client;
 *     }
 *
 *     @Override
 *     protected void handle(AuditEvent event, OutboxRecord record) {
 *         client.post(record.messageId().toString(), event);
 *     }
 * }
 * }</pre>
 * In a Kora app with {@code OutboxJsonModule}, the deserializer of a {@code @Json} class comes from its
 * {@code JsonReader}. Kotlin: {@code override fun handle(value: AuditEvent, record: OutboxRecord)}; the value is never
 * null, tombstones never reach it.
 */
public abstract class TypedOutboxTopicHandler<T> implements OutboxTopicHandler {

    private final Set<String> topics;
    private final OutboxDeserializer<T> deserializer;

    protected TypedOutboxTopicHandler(Set<String> topics, OutboxDeserializer<T> deserializer) {
        this.topics = Set.copyOf(topics);
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    }

    @Override
    public final Set<String> topics() {
        return topics;
    }

    /** Decodes the payload for {@link #handle(Object, OutboxRecord)}; a tombstone goes to {@link #handleTombstone}. */
    @Override
    public final void handle(OutboxRecord record) throws Exception {
        if (record.payload() == null) {
            handleTombstone(record);
        } else {
            handle(decode(record), record);
        }
    }

    /** Delivers one record, as {@link OutboxHandler#handle}; {@code value} is its decoded payload, never null. */
    protected abstract void handle(T value, OutboxRecord record) throws Exception;

    /** Delivers a record with a {@code null} payload, as {@link OutboxHandler#handle}. Override to take them. */
    protected void handleTombstone(OutboxRecord record) throws Exception {
        throw CannotHandle.failure(record, getClass().getName() + " takes no tombstones", null);
    }

    /**
     * The record's decoded payload, {@code null} for a tombstone; for {@code handleBatch} overrides. Throws what
     * {@link #handle(OutboxRecord)} throws for a payload that does not decode.
     */
    @Nullable
    protected final T decode(OutboxRecord record) throws Exception {
        var payload = record.payload();
        if (payload == null) {
            return null;
        }
        T value;
        try {
            value = deserializer.deserialize(record.topic(), payload);
        } catch (Exception e) {
            throw CannotHandle.failure(record, getClass().getName() + " cannot decode its payload", e);
        }
        if (value == null) {
            throw CannotHandle.failure(record, getClass().getName() + " cannot decode its payload: "
                    + deserializer.getClass().getName() + " returned null", null);
        }
        return value;
    }
}
