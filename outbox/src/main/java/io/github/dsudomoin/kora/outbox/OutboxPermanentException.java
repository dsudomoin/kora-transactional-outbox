package io.github.dsudomoin.kora.outbox;

/** Thrown from {@link OutboxHandler#handle} for a message that can never succeed; it moves to the dead table. */
public class OutboxPermanentException extends RuntimeException {

    public OutboxPermanentException(String message) {
        super(message);
    }

    public OutboxPermanentException(String message, Throwable cause) {
        super(message, cause);
    }
}
