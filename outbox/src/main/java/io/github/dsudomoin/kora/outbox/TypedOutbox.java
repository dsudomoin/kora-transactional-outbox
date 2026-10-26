package io.github.dsudomoin.kora.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * {@link Outbox} for values of one type: serializes with the {@link OutboxSerializer OutboxSerializer&lt;T&gt;}
 * component, then sends like {@link Outbox#send(OutboxMessage)}, so it too must run inside
 * {@code JdbcConnectionFactory.inTx(...)}. A {@code null} value is sent as a {@code null} payload (Kafka tombstone).
 * A failing serializer, or one that returns {@code null} for a value, throws {@link OutboxSerializationException}
 * before anything is written: only a {@code null} value makes a tombstone, which deletes the key on a compacted topic.
 * <p>
 * {@link OutboxModule} provides {@code TypedOutbox<T>} for every {@code T} with a serializer; for {@code @Json}
 * classes add {@code OutboxJsonModule}.
 * <pre>{@code
 * @Component
 * public final class Orders {
 *     private final JdbcConnectionFactory db;
 *     private final TypedOutbox<OrderCreated> events;
 *
 *     public Orders(JdbcConnectionFactory db, TypedOutbox<OrderCreated> events) {
 *         this.db = db;
 *         this.events = events;
 *     }
 *
 *     public void create(Order order) {
 *         db.inTx(() -> {
 *             // insert the order ...
 *             events.send("orders", order.userId(), new OrderCreated(order.id(), order.userId()));
 *         });
 *     }
 * }
 * }</pre>
 * Kotlin:
 * <pre>{@code
 * @Component
 * class Orders(private val db: JdbcConnectionFactory, private val events: TypedOutbox<OrderCreated>) {
 *     fun create(order: Order) = db.inTx<Unit> { _: Connection ->
 *         // insert the order ...
 *         events.send("orders", order.userId, OrderCreated(order.id, order.userId))
 *     }
 * }
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
