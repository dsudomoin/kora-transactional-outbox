package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;

/**
 * How a record fails that this instance cannot handle: no route claims its topic and there is no handler behind the
 * routes, its contract version is newer than its typed handler reads, its payload does not decode, or it is a tombstone
 * its typed handler does not take. Every such case comes here, so their outcome is decided in one place: the first two
 * wait for an instance that can handle the record, the others can never succeed.
 */
final class CannotHandle {

    private CannotHandle() {}

    /** {@code Waiting} of {@code kind}: another instance, of another release, can handle {@code record}. */
    static OutboxWaitException waiting(OutboxRecord record, Outcome.Waiting.Kind kind, String reason) {
        return new OutboxWaitException(kind, reason);
    }

    /** {@code Permanent}: no instance reading {@code record}'s contract version can ever handle it. */
    static OutboxPermanentException permanent(OutboxRecord record, String reason, @Nullable Throwable cause) {
        return new OutboxPermanentException("cannot handle outbox record " + record.messageId() + " of topic "
                + record.topic() + ": " + reason, cause);
    }
}
