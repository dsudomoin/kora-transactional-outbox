package io.github.dsudomoin.outbox.protobuf;

import io.github.dsudomoin.outbox.Outbox;

/**
 * Adds {@link ProtobufOutbox}; use with {@code OutboxModule}. Needs {@code com.google.protobuf:protobuf-java} (3.x or
 * newer) on the classpath, which protobuf-generated classes bring anyway.
 */
public interface OutboxProtobufModule {

    default ProtobufOutbox protobufOutbox(Outbox outbox) {
        return new ProtobufOutbox(outbox);
    }
}
