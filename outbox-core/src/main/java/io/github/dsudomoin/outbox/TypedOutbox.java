package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * {@link Outbox} for values of one type: serializes with an {@link OutboxSerializer OutboxSerializer&lt;T&gt;}, then
 * sends like {@link Outbox#send(OutboxMessage)}, so it too must run inside the application's transaction (see
 * {@link OutboxTransactions}). A {@code null} value is sent as a {@code null} payload (Kafka tombstone). A failing
 * serializer, or one that returns {@code null} for a value, throws {@link OutboxSerializationException} before anything
 * is written: only a {@code null} value makes a tombstone, which deletes the key on a compacted topic.
 * <pre>{@code
 * var events = new TypedOutbox<OrderCreated>(runtime.outbox(), (topic, event) -> json.writeValueAsBytes(event));
 * transactions.inTx(c -> {
 *     orders.insert(c, order);
 *     return events.send("orders", order.userId(), new OrderCreated(order.id(), order.userId()));
 * });
 * }</pre>
 */
public final class TypedOutbox<T> {

    private final Outbox outbox;
    private final OutboxSerializer<T> serializer;

    public TypedOutbox(Outbox outbox, OutboxSerializer<T> serializer) {
        this.outbox = outbox;
        this.serializer = serializer;
    }

    public UUID send(String topic, @Nullable String key, @Nullable T value) {
        return send(topic, key, value, List.of());
    }

    public UUID send(String topic, @Nullable String key, @Nullable T value, List<OutboxHeader> headers) {
        return send(topic, key == null ? null : key.getBytes(UTF_8), value, headers);
    }

    public UUID send(String topic, @Nullable byte[] key, @Nullable T value, List<OutboxHeader> headers) {
        byte[] payload = null;
        if (value != null) {
            try {
                payload = serializer.serialize(topic, value);
            } catch (Exception e) {
                throw new OutboxSerializationException(topic, value, e);
            }
            if (payload == null) {
                throw new OutboxSerializationException(topic, value, new NullPointerException(
                        serializer.getClass().getName() + " returned null; send a null value for a tombstone"));
            }
        }
        return outbox.send(new OutboxMessage(topic, key, payload, headers));
    }
}
