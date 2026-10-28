package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.ReceiverContext;
import jakarta.annotation.Nullable;
import java.time.Instant;

/**
 * The observation of one delivered record. Its carrier is the record as stored, so the application's propagating
 * receiver handler parents the observation by the trace the sender stored (the last header of a name wins).
 */
public final class OutboxDeliveryContext extends ReceiverContext<OutboxRecord> {

    private final OutboxRecord record;
    private final Instant now;
    @Nullable
    private volatile Outcome outcome;
    private volatile boolean failed;
    /** The default convention's high cardinality values, once computed; the relay thread starts and stops it. */
    @Nullable
    KeyValues highCardinality;

    OutboxDeliveryContext(OutboxRecord record, Instant now) {
        super((carrier, key) -> {
            var headers = carrier.headers();
            for (int i = headers.size() - 1; i >= 0; i--) {
                var h = headers.get(i);
                if (h.name().equals(key)) {
                    return h.value() == null ? null : new String(h.value(), UTF_8);
                }
            }
            return null;
        }, Kind.CONSUMER);
        this.record = record;
        this.now = now;
        setCarrier(record);
    }

    /** The record as stored. */
    public OutboxRecord record() {
        return record;
    }

    /** The relay step's clock, the end of the record's delay. */
    public Instant now() {
        return now;
    }

    /** The handler's outcome, set before the observation stops; null before that, or when {@link #failed()}. */
    @Nullable
    public Outcome outcome() {
        return outcome;
    }

    /** The handler call ended without outcomes: interrupted, or the relay closing. */
    public boolean failed() {
        return failed;
    }

    void outcome(Outcome outcome) {
        this.outcome = outcome;
    }

    void markFailed() {
        this.failed = true;
    }
}
