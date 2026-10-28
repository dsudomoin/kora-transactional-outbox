package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.protobuf.ProtobufOutbox;

/**
 * Adds {@link ProtobufOutbox}; use with {@code OutboxModule}. Needs {@code com.google.protobuf:protobuf-java} (3.x or
 * newer) on the classpath, which protobuf-generated classes bring anyway.
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
public interface OutboxProtobufModule {

    default ProtobufOutbox protobufOutbox(Outbox outbox) {
        return new ProtobufOutbox(outbox);
    }
}
