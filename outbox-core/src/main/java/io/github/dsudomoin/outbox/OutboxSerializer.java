package io.github.dsudomoin.outbox;

/**
 * Turns a value into a message payload for {@link TypedOutbox}. Never called with {@code null} (that is a tombstone),
 * and must not return {@code null}: {@link TypedOutbox} rejects that rather than send an accidental tombstone. Any
 * encoder fits, e.g. a JSON mapper's {@code (topic, value) -> mapper.writeValueAsBytes(value)}; Avro and other
 * formats plug in the same way.
 */
@FunctionalInterface
public interface OutboxSerializer<T> {

    byte[] serialize(String topic, T value) throws Exception;
}
