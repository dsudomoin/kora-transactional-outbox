package io.github.dsudomoin.outbox;

/**
 * Turns a message payload back into a value for {@link TypedOutboxTopicHandler}, the mirror of
 * {@link OutboxSerializer}. Never called with a {@code null} payload (a tombstone), and must not return {@code null}.
 * Any decoder fits, e.g. a JSON mapper's {@code (topic, payload) -> mapper.readValue(payload, OrderCreated.class)} or a
 * protobuf parser's {@code (topic, payload) -> OrderCreated.parseFrom(payload)}.
 */
@FunctionalInterface
public interface OutboxDeserializer<T> {

    T deserialize(String topic, byte[] payload) throws Exception;
}
