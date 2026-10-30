package io.github.dsudomoin.outbox;

import java.util.Objects;

/**
 * Thrown from {@link OutboxHandler#handle} for a record this instance cannot handle now; the default
 * {@link OutboxHandler#handleBatch} turns it into {@link Outcome.Waiting} (see {@link Outcome#waitFor}), with the
 * message as the reason.
 */
public final class OutboxWaitException extends RuntimeException {

    private final Outcome.Waiting.Kind kind;

    /** Kind {@link Outcome.Waiting.Kind#HANDLER}. */
    public OutboxWaitException(String reason) {
        this(Outcome.Waiting.Kind.HANDLER, reason);
    }

    OutboxWaitException(Outcome.Waiting.Kind kind, String reason) {
        super(Objects.requireNonNull(reason, "reason"));
        this.kind = Objects.requireNonNull(kind, "kind");
    }

    public Outcome.Waiting.Kind kind() {
        return kind;
    }
}
