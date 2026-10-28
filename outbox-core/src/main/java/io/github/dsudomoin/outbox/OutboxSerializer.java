package io.github.dsudomoin.outbox;

/**
 * Turns a value into a message payload for {@link TypedOutbox}. Never called with {@code null} (that is a tombstone),
 * and must not return {@code null}: {@link TypedOutbox} rejects that rather than send an accidental tombstone. Provide
 * one as a component for each type you send; {@code OutboxJsonModule} derives one from Kora's {@code JsonWriter<T>},
 * and an explicit {@code OutboxSerializer<T>} component takes precedence over it for that {@code T}. Other formats
 * (Avro, ...) plug in the same way.
 */
@FunctionalInterface
public interface OutboxSerializer<T> {

    byte[] serialize(String topic, T value) throws Exception;
}
