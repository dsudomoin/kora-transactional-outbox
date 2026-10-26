package io.github.dsudomoin.kora.outbox;

/** An {@link OutboxSerializer} failed; nothing was written, the caller's transaction decides what happens next. */
public class OutboxSerializationException extends RuntimeException {

    public OutboxSerializationException(String topic, Object value, Throwable cause) {
        super("cannot serialize " + value.getClass().getName() + " for topic " + topic, cause);
    }
}
