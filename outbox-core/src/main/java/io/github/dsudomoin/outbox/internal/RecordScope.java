package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import java.util.concurrent.Callable;

/** The delivery of the handler call running on this thread, behind {@link OutboxHandler#inRecordScope}. */
public final class RecordScope {

    private static final ThreadLocal<OutboxTelemetry.Delivery> CURRENT = new ThreadLocal<>();

    private RecordScope() {}

    /** Runs {@code body} with {@code delivery} bound to this thread; the previous binding is restored after. */
    public static <T> T bound(OutboxTelemetry.Delivery delivery, Callable<T> body) throws Exception {
        var previous = CURRENT.get();
        CURRENT.set(delivery);
        try {
            return body.call();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    /** {@code call} in the record scope of the delivery bound to this thread; outside one, {@code call} alone. */
    public static <T> T run(OutboxRecord record, Callable<T> call) throws Exception {
        var delivery = CURRENT.get();
        return delivery == null ? call.call() : delivery.inRecordScope(record, call);
    }
}
