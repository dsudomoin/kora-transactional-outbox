package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.DoubleSupplier;
import ru.tinkoff.kora.common.Context;

/**
 * The Kora app's telemetry: the {@code outbox.*} meters of {@link OutboxMetrics}; the trace of the Kora context
 * stored with each sent message unless it carries one; with a {@link Tracer}, an {@code outbox deliver <topic>} span
 * per delivered record, parented by the stored trace, whose trace headers the handled record carries; and around each
 * handled record a Kora context carrying its trace, so Kora clients called from the handler continue it.
 */
public final class KoraOutboxTelemetry implements OutboxTelemetry {

    /** Without a tracer: the records as stored, each handled in a Kora context carrying its stored trace. */
    private static final Delivery SCOPE_ONLY = new Delivery() {
        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            return inKoraContext(record, call);
        }
    };

    private final OutboxMetrics metrics;
    @Nullable
    private final Tracer tracer;

    public KoraOutboxTelemetry(OutboxMetrics metrics, @Nullable Tracer tracer) {
        this.metrics = metrics;
        this.tracer = tracer;
    }

    /** The current Kora context's trace, read once per call. */
    @Override
    public Enqueue enqueueStarted() {
        var trace = OutboxTracing.currentHeaders();
        return trace.isEmpty() ? Enqueue.NONE : new TracedEnqueue(trace);
    }

    @Override
    public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
        return tracer == null ? SCOPE_ONLY : new SpanDelivery(tracer, records, now);
    }

    /** Nothing a call set in the Kora context leaks into the next call on the thread. */
    @Override
    public void releaseThread() {
        Context.clear();
    }

    @Override
    public void enqueued(String topic) {
        metrics.enqueued(topic);
    }

    @Override
    public void delivered(String topic, Duration delay) {
        metrics.delivered(topic, delay);
    }

    @Override
    public void retried(String topic) {
        metrics.retried(topic);
    }

    @Override
    public void deadLettered(String topic) {
        metrics.deadLettered(topic);
    }

    @Override
    public void requeued(String topic) {
        metrics.requeued(topic);
    }

    @Override
    public void waited(String topic, Outcome.Waiting.Kind kind) {
        metrics.waited(topic, kind);
    }

    @Override
    public void contractRejected(String topic, String type, int version) {
        metrics.contractRejected(topic, type, version);
    }

    @Override
    public void routeRejected(String topic) {
        metrics.routeRejected(topic);
    }

    @Override
    public void routeUnchecked(String topic) {
        metrics.routeUnchecked(topic);
    }

    @Override
    public void stepFinished(Duration duration) {
        metrics.stepFinished(duration);
    }

    @Override
    public void relayFailed(Throwable error) {
        metrics.relayFailed(error);
    }

    @Override
    public void shardPaused() {
        metrics.shardPaused();
    }

    @Override
    public void deadLetterDecision(String decision) {
        metrics.deadLetterDecision(decision);
    }

    @Override
    public void deadLetterHookFailed() {
        metrics.deadLetterHookFailed();
    }

    @Override
    public void inboxOutsideWindow() {
        metrics.inboxOutsideWindow();
    }

    @Override
    public void gauge(OutboxGauge gauge, DoubleSupplier value) {
        metrics.gauge(gauge, value);
    }

    static <T> T inKoraContext(OutboxRecord record, Callable<T> call) throws Exception {
        return OutboxTracing.withKoraContext(OutboxTracing.extract(record.headers()), call);
    }

    /** Adds the sender's trace to a message without a {@code traceparent}. */
    private record TracedEnqueue(List<OutboxHeader> trace) implements Enqueue {
        @Override
        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
            for (var h : headers) {
                if (h.name().equals(OutboxTracing.TRACEPARENT)) {
                    return headers;
                }
            }
            var all = new ArrayList<OutboxHeader>(headers.size() + trace.size());
            all.addAll(headers);
            all.addAll(trace);
            return all;
        }
    }

    /** A delivery span per record, started before the call and ended with the record's outcome. */
    private static final class SpanDelivery implements Delivery {

        private final Span[] spans;

        SpanDelivery(Tracer tracer, List<OutboxRecord> records, Instant now) {
            spans = new Span[records.size()];
            for (int i = 0; i < spans.length; i++) {
                var r = records.get(i);
                spans[i] = tracer.spanBuilder("outbox deliver " + r.topic())
                        .setSpanKind(SpanKind.PRODUCER)
                        .setParent(OutboxTracing.extract(r.headers()))
                        .setAttribute("messaging.destination.name", r.topic())
                        .setAttribute("outbox.message_id", r.messageId().toString())
                        .setAttribute("outbox.attempt", r.attempts() + 1L)
                        .setAttribute("outbox.delay_ms", delay(r, now).toMillis())
                        .startSpan();
            }
        }

        /** Each record with its span's trace headers in place of the stored ones. */
        @Override
        public List<OutboxRecord> handed(List<OutboxRecord> records) {
            var handed = new ArrayList<OutboxRecord>(records.size());
            for (int i = 0; i < records.size(); i++) {
                var r = records.get(i);
                var ctx = io.opentelemetry.context.Context.root().with(spans[i]);
                handed.add(r.withHeaders(OutboxTracing.replaceTraceHeaders(r.headers(), ctx)));
            }
            return handed;
        }

        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            return inKoraContext(record, call);
        }

        @Override
        public void finished(List<Outcome> outcomes) {
            for (int i = 0; i < spans.length; i++) {
                end(spans[i], outcomes.get(i));
            }
        }

        @Override
        public void failed(Throwable error) {
            for (var span : spans) {
                span.recordException(error);
                span.setStatus(StatusCode.ERROR);
                span.end();
            }
        }

        private static void end(Span span, Outcome outcome) {
            Throwable error = null;
            String name;
            if (outcome instanceof Outcome.Retry r) {
                name = "retry";
                error = r.error();
            } else if (outcome instanceof Outcome.Permanent p) {
                name = "permanent";
                error = p.error();
            } else if (outcome instanceof Outcome.NotAttempted) {
                name = "not_attempted";
            } else if (outcome instanceof Outcome.Requeue) {
                name = "requeued";
            } else if (outcome instanceof Outcome.Waiting) {
                name = "waiting";
            } else {
                name = "delivered";
            }
            span.setAttribute("outbox.outcome", name);
            if (error != null) {
                span.recordException(error);
                span.setStatus(StatusCode.ERROR);
            }
            span.end();
        }

        /** {@code now - createdAt}, zero when the clocks disagree. */
        private static Duration delay(OutboxRecord r, Instant now) {
            var delay = Duration.between(r.createdAt(), now);
            return delay.isNegative() ? Duration.ZERO : delay;
        }
    }
}
