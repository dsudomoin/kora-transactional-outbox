package io.github.dsudomoin.outbox.protobuf;

import com.google.protobuf.MessageLite;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.TypedOutbox;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * Sends any protobuf message (protobuf-java 3.x or newer) as its binary encoding; one instance for all message types.
 * Like {@link Outbox#send(OutboxMessage)} it must run inside the application's transaction (see
 * {@link io.github.dsudomoin.outbox.OutboxTransactions}). As with {@link TypedOutbox}, a {@code null} message is sent
 * as a {@code null} payload (Kafka tombstone).
 * <pre>{@code
 * var events = new ProtobufOutbox(runtime.outbox());
 * transactions.inTx(c -> events.send("orders", userId, OrderCreated.newBuilder().setId(id).setUserId(userId).build()));
 * }</pre>
 */
public final class ProtobufOutbox {

    private final TypedOutbox<MessageLite> outbox;

    public ProtobufOutbox(Outbox outbox) {
        this.outbox = new TypedOutbox<>(outbox, (topic, message) -> message.toByteArray());
    }

    public UUID send(String topic, @Nullable String key, @Nullable MessageLite message) {
        return outbox.send(topic, key, message);
    }

    public UUID send(String topic, @Nullable String key, @Nullable MessageLite message, List<OutboxHeader> headers) {
        return outbox.send(topic, key, message, headers);
    }
}
