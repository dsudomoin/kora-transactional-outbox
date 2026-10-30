package io.github.dsudomoin.outbox.protobuf;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.protobuf.MessageLite;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxSerializationException;
import io.github.dsudomoin.outbox.TypedOutbox;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * Sends any protobuf message (protobuf-java 3.x or newer) as its binary encoding; one instance for all message types.
 * Like {@link Outbox#send(OutboxMessage)} it must run inside the application's transaction (see
 * {@link io.github.dsudomoin.outbox.OutboxTransactions}). As with {@link TypedOutbox}, a {@code null} message is sent
 * as a {@code null} payload (Kafka tombstone). Generated classes cannot carry
 * {@link io.github.dsudomoin.outbox.OutboxContract}, so the headers are sent as given: a hand-set
 * {@code outbox-contract} header is how a protobuf message is versioned.
 * <pre>{@code
 * var events = new ProtobufOutbox(runtime.outbox());
 * transactions.inTx(c -> events.send("orders", userId, OrderCreated.newBuilder().setId(id).setUserId(userId).build()));
 * }</pre>
 */
public final class ProtobufOutbox {

    private final Outbox outbox;

    public ProtobufOutbox(Outbox outbox) {
        this.outbox = outbox;
    }

    public UUID send(String topic, @Nullable String key, @Nullable MessageLite message) {
        return send(topic, key, message, List.of());
    }

    public UUID send(String topic, @Nullable String key, @Nullable MessageLite message, List<OutboxHeader> headers) {
        byte[] payload = null;
        if (message != null) {
            try {
                payload = message.toByteArray();
            } catch (RuntimeException e) {
                throw new OutboxSerializationException(topic, message, e);
            }
        }
        return outbox.send(new OutboxMessage(topic, key == null ? null : key.getBytes(UTF_8), payload, headers));
    }
}
