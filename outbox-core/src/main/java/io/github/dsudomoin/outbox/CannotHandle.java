package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;

/**
 * How a record fails that this instance cannot handle at all: no route claims its topic and there is no handler
 * behind the routes, its payload does not decode, or it is a tombstone its typed handler does not take. Every such
 * case comes here, so their outcome is decided in one place; today a transient failure (Retry).
 */
final class CannotHandle {

    private CannotHandle() {}

    static RuntimeException failure(OutboxRecord record, String reason, @Nullable Throwable cause) {
        return new IllegalStateException("cannot handle outbox record " + record.messageId() + " of topic "
                + record.topic() + ": " + reason, cause);
    }
}
