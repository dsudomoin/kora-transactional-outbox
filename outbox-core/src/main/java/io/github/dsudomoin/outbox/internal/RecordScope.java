package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import jakarta.annotation.Nullable;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;

/**
 * The delivery of the handler call running on this thread, behind {@link OutboxHandler#inRecordScope}; outside one,
 * the {@link Fallback} a framework adapter on the classpath provides.
 */
public final class RecordScope {

    private static final ThreadLocal<OutboxTelemetry.Delivery> CURRENT = new ThreadLocal<>();
    @Nullable
    private static final Fallback FALLBACK = fallback();

    private RecordScope() {}

    /**
     * A record's scope for a call outside a relay call (a handler called directly, or a delegate's default
     * {@code handleBatch} on another thread), found once with {@link ServiceLoader}: the Kora adapter's gives the
     * record's trace as a Kora context, as the default {@code handleBatch} always did.
     */
    public interface Fallback {
        <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception;
    }

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

    /** {@code call} in the record scope of the delivery bound to this thread; outside one, the fallback's. */
    public static <T> T run(OutboxRecord record, Callable<T> call) throws Exception {
        var delivery = CURRENT.get();
        if (delivery != null) {
            return delivery.inRecordScope(record, call);
        }
        return FALLBACK == null ? call.call() : FALLBACK.inRecordScope(record, call);
    }

    @Nullable
    private static Fallback fallback() {
        try {
            return ServiceLoader.load(Fallback.class, RecordScope.class.getClassLoader()).findFirst().orElse(null);
        } catch (ServiceConfigurationError e) { // a broken provider: the plain call, as without one
            return null;
        }
    }
}
