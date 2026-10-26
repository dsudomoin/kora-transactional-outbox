package io.github.dsudomoin.kora.outbox.protobuf;

import com.google.protobuf.MessageLite;
import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.TypedOutbox;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * Sends any protobuf message (protobuf-java 3.x or newer) as its binary encoding; one component for all message
 * types. Like {@link Outbox#send(OutboxMessage)} it must run inside {@code JdbcConnectionFactory.inTx(...)}. As with
 * {@link TypedOutbox}, a {@code null} message is sent as a {@code null} payload (Kafka tombstone).
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, OutboxProtobufModule {}
 *
 * // in a component taking ProtobufOutbox events:
 * db.inTx(() -> {
 *     events.send("orders", userId, OrderCreated.newBuilder().setId(id).setUserId(userId).build());
 * });
 * }</pre>
 * Kotlin:
 * <pre>{@code
 * db.inTx<Unit> { _: Connection ->
 *     events.send("orders", userId, OrderCreated.newBuilder().setId(id).setUserId(userId).build())
 * }
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
