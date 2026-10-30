package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.internal.Contracts;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * {@link Outbox} for values of one type: serializes with an {@link OutboxSerializer OutboxSerializer&lt;T&gt;}, then
 * sends like {@link Outbox#send(OutboxMessage)}, so it too must run inside the application's transaction (see
 * {@link OutboxTransactions}). A {@code null} value is sent as a {@code null} payload (Kafka tombstone). A failing
 * serializer, or one that returns {@code null} for a value, throws {@link OutboxSerializationException} before anything
 * is written: only a {@code null} value makes a tombstone, which deletes the key on a compacted topic.
 * <p>
 * Every message, tombstones included, carries the {@link OutboxContract} version of the declared type {@code T} (never
 * of the value's runtime class) in its {@code outbox-contract} header, in place of any such header given; version 1
 * writes none. {@link #message} builds the same message for {@link Outbox#sendAll}.
 * <pre>{@code
 * var events = new TypedOutbox<>(runtime.outbox(), (topic, event) -> json.writeValueAsBytes(event),
 *         OrderCreated.class);
 * transactions.inTx(c -> {
 *     orders.insert(c, order);
 *     return events.send("orders", order.userId(), new OrderCreated(order.id(), order.userId()));
 * });
 * }</pre>
 */
public final class TypedOutbox<T> {

    private final Outbox outbox;
    private final OutboxSerializer<T> serializer;
    private final Class<T> type;
    private final int version;

    /** {@code type} is {@code T}; its {@link OutboxContract} is read once, here. */
    public TypedOutbox(Outbox outbox, OutboxSerializer<T> serializer, Class<T> type) {
        this.outbox = outbox;
        this.serializer = serializer;
        this.type = Objects.requireNonNull(type, "type");
        this.version = Contracts.version(type);
    }

    public UUID send(String topic, @Nullable String key, @Nullable T value) {
        return outbox.send(message(topic, key, value));
    }

    public UUID send(String topic, @Nullable String key, @Nullable T value, List<OutboxHeader> headers) {
        return outbox.send(message(topic, key, value, headers));
    }

    public UUID send(String topic, @Nullable byte[] key, @Nullable T value, List<OutboxHeader> headers) {
        return outbox.send(message(topic, key, value, headers));
    }

    /** The message {@code send} would write, for {@link Outbox#sendAll}. */
    public OutboxMessage message(String topic, @Nullable String key, @Nullable T value) {
        return message(topic, key, value, List.of());
    }

    /** The message {@code send} would write, for {@link Outbox#sendAll}. */
    public OutboxMessage message(String topic, @Nullable String key, @Nullable T value, List<OutboxHeader> headers) {
        return message(topic, key == null ? null : key.getBytes(UTF_8), value, headers);
    }

    /** The message {@code send} would write, for {@link Outbox#sendAll}. */
    public OutboxMessage message(String topic, @Nullable byte[] key, @Nullable T value, List<OutboxHeader> headers) {
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
        return new OutboxMessage(topic, key, payload, Contracts.stamp(headers, version));
    }

    /** The declared type's name, for the writer guard. */
    String typeName() {
        return type.getName();
    }

    /** The declared type's {@link OutboxContract} version, 1 without it. */
    int contractVersion() {
        return version;
    }
}
