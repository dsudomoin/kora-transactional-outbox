package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutboxTelemetry} that never throws: a hook's exception is logged at WARN, at most once per 10 s per hook,
 * and the outbox goes on as without it (headers and records unchanged, the record's call run once).
 */
public final class GuardedTelemetry implements OutboxTelemetry {

    private static final Logger log = LoggerFactory.getLogger(GuardedTelemetry.class);
    private static final long WARN_INTERVAL = TimeUnit.SECONDS.toNanos(10);

    private final OutboxTelemetry delegate;
    /** Per hook, when its failure was last logged ({@code nanoTime}). */
    private final Map<String, AtomicLong> warned = new ConcurrentHashMap<>();

    private GuardedTelemetry(OutboxTelemetry delegate) {
        this.delegate = delegate;
    }

    /** {@code telemetry} itself for {@link OutboxTelemetry#NONE} or one already guarded, else a guard over it. */
    public static OutboxTelemetry of(OutboxTelemetry telemetry) {
        return telemetry == NONE || telemetry instanceof GuardedTelemetry ? telemetry : new GuardedTelemetry(telemetry);
    }

    @Override
    public Enqueue enqueueStarted() {
        Enqueue enqueue;
        try {
            enqueue = delegate.enqueueStarted();
        } catch (RuntimeException | LinkageError e) {
            warn("enqueueStarted", e);
            return Enqueue.NONE;
        }
        return enqueue == Enqueue.NONE ? enqueue : new GuardedEnqueue(enqueue);
    }

    @Override
    public void enqueued(String topic) {
        try {
            delegate.enqueued(topic);
        } catch (RuntimeException | LinkageError e) {
            warn("enqueued", e);
        }
    }

    @Override
    public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
        Delivery delivery;
        try {
            delivery = delegate.deliveryStarted(records, now);
        } catch (RuntimeException | LinkageError e) {
            warn("deliveryStarted", e);
            return Delivery.NONE;
        }
        return delivery == Delivery.NONE ? delivery : new GuardedDelivery(delivery);
    }

    @Override
    public void delivered(String topic, Duration delay) {
        try {
            delegate.delivered(topic, delay);
        } catch (RuntimeException | LinkageError e) {
            warn("delivered", e);
        }
    }

    @Override
    public void retried(String topic) {
        try {
            delegate.retried(topic);
        } catch (RuntimeException | LinkageError e) {
            warn("retried", e);
        }
    }

    @Override
    public void deadLettered(String topic) {
        try {
            delegate.deadLettered(topic);
        } catch (RuntimeException | LinkageError e) {
            warn("deadLettered", e);
        }
    }

    @Override
    public void requeued(String topic) {
        try {
            delegate.requeued(topic);
        } catch (RuntimeException | LinkageError e) {
            warn("requeued", e);
        }
    }

    @Override
    public void waited(String topic, Outcome.Waiting.Kind kind) {
        try {
            delegate.waited(topic, kind);
        } catch (RuntimeException | LinkageError e) {
            warn("waited", e);
        }
    }

    @Override
    public void contractRejected(String topic, String type, int version) {
        try {
            delegate.contractRejected(topic, type, version);
        } catch (RuntimeException | LinkageError e) {
            warn("contractRejected", e);
        }
    }

    @Override
    public RelayStep stepStarted(List<Integer> shards) {
        RelayStep step;
        try {
            step = delegate.stepStarted(shards);
        } catch (RuntimeException | LinkageError e) {
            warn("stepStarted", e);
            return RelayStep.NONE;
        }
        return step == RelayStep.NONE ? step : new GuardedRelayStep(step);
    }

    @Override
    public void stepFinished(Duration duration) {
        try {
            delegate.stepFinished(duration);
        } catch (RuntimeException | LinkageError e) {
            warn("stepFinished", e);
        }
    }

    @Override
    public void relayFailed(Throwable error) {
        try {
            delegate.relayFailed(error);
        } catch (RuntimeException | LinkageError e) {
            warn("relayFailed", e);
        }
    }

    @Override
    public void shardPaused() {
        try {
            delegate.shardPaused();
        } catch (RuntimeException | LinkageError e) {
            warn("shardPaused", e);
        }
    }

    @Override
    public void deadLetterDecision(String decision) {
        try {
            delegate.deadLetterDecision(decision);
        } catch (RuntimeException | LinkageError e) {
            warn("deadLetterDecision", e);
        }
    }

    @Override
    public void deadLetterHookFailed() {
        try {
            delegate.deadLetterHookFailed();
        } catch (RuntimeException | LinkageError e) {
            warn("deadLetterHookFailed", e);
        }
    }

    @Override
    public void inboxOutsideWindow() {
        try {
            delegate.inboxOutsideWindow();
        } catch (RuntimeException | LinkageError e) {
            warn("inboxOutsideWindow", e);
        }
    }

    @Override
    public void releaseThread() {
        try {
            delegate.releaseThread();
        } catch (RuntimeException | LinkageError e) {
            warn("releaseThread", e);
        }
    }

    @Override
    public void gauge(OutboxGauge gauge, DoubleSupplier value) {
        try {
            delegate.gauge(gauge, value);
        } catch (RuntimeException | LinkageError e) {
            warn("gauge", e);
        }
    }

    private void warn(String hook, Throwable error) {
        long now = System.nanoTime();
        var last = warned.computeIfAbsent(hook, h -> new AtomicLong(now - WARN_INTERVAL));
        long at = last.get();
        if (now - at >= WARN_INTERVAL && last.compareAndSet(at, now)) {
            log.warn("outbox telemetry hook {} failed and is ignored; failures of it in the next 10 s are not logged",
                    hook, error);
        }
    }

    private final class GuardedEnqueue implements Enqueue {

        private final Enqueue delegate;

        GuardedEnqueue(Enqueue delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
            try {
                var result = delegate.headers(headers);
                return result == null ? headers : result;
            } catch (RuntimeException | LinkageError e) {
                warn("Enqueue.headers", e);
                return headers;
            }
        }

        @Override
        public void finished() {
            try {
                delegate.finished();
            } catch (RuntimeException | LinkageError e) {
                warn("Enqueue.finished", e);
            }
        }

        @Override
        public void failed(Throwable error) {
            try {
                delegate.failed(error);
            } catch (RuntimeException | LinkageError e) {
                warn("Enqueue.failed", e);
            }
        }
    }

    private final class GuardedRelayStep implements RelayStep {

        private final RelayStep delegate;

        GuardedRelayStep(RelayStep delegate) {
            this.delegate = delegate;
        }

        @Override
        public void finished() {
            try {
                delegate.finished();
            } catch (RuntimeException | LinkageError e) {
                warn("RelayStep.finished", e);
            }
        }

        @Override
        public void failed(Throwable error) {
            try {
                delegate.failed(error);
            } catch (RuntimeException | LinkageError e) {
                warn("RelayStep.failed", e);
            }
        }
    }

    private final class GuardedDelivery implements Delivery {

        private final Delivery delegate;

        GuardedDelivery(Delivery delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<OutboxRecord> handed(List<OutboxRecord> records) {
            try {
                var result = delegate.handed(records);
                if (result == null || result.size() != records.size()) {
                    throw new IllegalStateException("handed " + (result == null ? null : result.size())
                            + " records for " + records.size());
                }
                return result;
            } catch (RuntimeException | LinkageError e) {
                warn("Delivery.handed", e);
                return records;
            }
        }

        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            var tracked = new TrackedCall<>(call);
            T result;
            try {
                result = delegate.inRecordScope(record, tracked);
            } catch (Throwable e) {
                if (tracked.state == TrackedCall.THREW) {
                    if (e != tracked.error) {
                        warn("Delivery.inRecordScope", e);
                    }
                    throw rethrow(tracked.error); // the handler's own failure, whatever the scope made of it
                }
                if (!(e instanceof Exception || e instanceof LinkageError)) {
                    throw e;
                }
                warn("Delivery.inRecordScope", e);
                return tracked.state == TrackedCall.RETURNED ? tracked.result : call.call();
            }
            if (tracked.state == TrackedCall.THREW) { // a scope that swallowed the handler's failure: not a delivery
                warn("Delivery.inRecordScope", new IllegalStateException("the record scope swallowed its call's "
                        + tracked.error));
                throw rethrow(tracked.error);
            }
            if (tracked.state == TrackedCall.NOT_RUN) { // a scope that skipped its call: run it, as without telemetry
                warn("Delivery.inRecordScope", new IllegalStateException("the record scope did not run its call"));
                return call.call();
            }
            return result;
        }

        @Override
        public void finished(List<Outcome> outcomes) {
            try {
                delegate.finished(outcomes);
            } catch (RuntimeException | LinkageError e) {
                warn("Delivery.finished", e);
            }
        }

        @Override
        public void failed(Throwable error) {
            try {
                delegate.failed(error);
            } catch (RuntimeException | LinkageError e) {
                warn("Delivery.failed", e);
            }
        }
    }

    private static Exception rethrow(Throwable error) {
        if (error instanceof Error e) {
            throw e;
        }
        return (Exception) error;
    }

    /** A record's call that remembers whether it ran and how it ended. */
    private static final class TrackedCall<T> implements Callable<T> {

        static final int NOT_RUN = 0;
        static final int RETURNED = 1;
        static final int THREW = 2;

        private final Callable<T> call;
        int state = NOT_RUN;
        T result;
        Throwable error;

        TrackedCall(Callable<T> call) {
            this.call = call;
        }

        @Override
        public T call() throws Exception {
            try {
                result = call.call();
                state = RETURNED;
                return result;
            } catch (Throwable e) {
                error = e;
                state = THREW;
                throw e;
            }
        }
    }
}
