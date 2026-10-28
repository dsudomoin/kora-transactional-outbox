package io.github.dsudomoin.outbox.kora;

import static io.github.dsudomoin.outbox.testing.TestTracing.SPAN_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACEPARENT;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACE_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.header;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.TestTracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

class KoraOutboxTelemetryTest {

    private static final Instant NOW = Instant.parse("2026-10-27T12:00:00Z");

    @AfterEach
    void clear() {
        Context.clear();
    }

    @Test
    void recordScopeCarriesTraceWithoutTracer() throws Exception {
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), null);
        var record = record(List.of(header("traceparent", TRACEPARENT)));
        var delivery = telemetry.deliveryStarted(List.of(record), NOW);

        assertThat(delivery.handed(List.of(record))).containsExactly(record);
        var traceId = delivery.inRecordScope(record,
                () -> OpentelemetryContext.getSpan().getSpanContext().getTraceId());
        assertThat(traceId).isEqualTo(TRACE_ID);
        assertThat(OpentelemetryContext.getSpan()).as("the caller's context is restored").isNull();
    }

    @Test
    void metricsWithoutTracer() {
        var registry = new SimpleMeterRegistry();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(registry), null);

        telemetry.enqueued("orders");
        telemetry.delivered("orders", Duration.ofMillis(10));
        var delivery = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(registry.get("outbox.messages.enqueued").tag("topic", "orders").counter().count()).isEqualTo(1);
        assertThat(registry.get("outbox.messages.delivered").tag("topic", "orders").counter().count()).isEqualTo(1);
    }

    @Test
    void spansWithoutRegistry() {
        var spans = InMemorySpanExporter.create();
        var tracer = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build()
                .get("test");
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer);
        var record = record(List.of(header("traceparent", TRACEPARENT)));

        var delivery = telemetry.deliveryStarted(List.of(record), NOW);
        var handed = delivery.handed(List.of(record));
        delivery.finished(List.of(Outcome.delivered()));

        var span = spans.getFinishedSpanItems().get(0);
        assertThat(span.getName()).isEqualTo("outbox deliver orders");
        assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
        assertThat(span.getParentSpanId()).isEqualTo(SPAN_ID);
        assertThat(span.getAttributes().get(AttributeKey.stringKey("messaging.destination.name"))).isEqualTo("orders");
        assertThat(span.getAttributes().get(AttributeKey.stringKey("outbox.outcome"))).isEqualTo("delivered");
        assertThat(span.getAttributes().get(AttributeKey.longKey("outbox.attempt"))).isEqualTo(1L);
        assertThat(traceparent(handed.get(0))).as("the handed record carries the delivery span")
                .isEqualTo("00-" + TRACE_ID + "-" + span.getSpanId() + "-01");
        assertThat(traceparent(record)).as("the stored record keeps its own").isEqualTo(TRACEPARENT);
    }

    @Test
    void relayFailuresReachTheCounterAndStepsMakeNoSpan() {
        var registry = new SimpleMeterRegistry();
        var spans = InMemorySpanExporter.create();
        var tracer = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build()
                .get("test");
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(registry), tracer);

        telemetry.relayFailed(new SQLException("x"));
        var step = telemetry.stepStarted(List.of(1, 3));
        step.finished();

        assertThat(registry.get("outbox.relay.failures").counter().count()).isEqualTo(1);
        assertThat(step).isSameAs(OutboxTelemetry.RelayStep.NONE);
        assertThat(spans.getFinishedSpanItems()).isEmpty();
    }

    @Test
    void enqueueAddsCurrentTraceUnlessTheMessageHasOne() {
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), null);
        TestTracing.activateSpan();
        var enqueue = telemetry.enqueueStarted();

        assertThat(enqueue.headers(List.of())).containsExactly(header("traceparent", TRACEPARENT));
        var own = List.of(header("traceparent", "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01"));
        assertThat(enqueue.headers(own)).isSameAs(own);
    }

    @Test
    void releaseThreadClearsTheKoraContext() {
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), null);
        TestTracing.activateSpan();

        telemetry.releaseThread();

        assertThat(OpentelemetryContext.getSpan()).isNull();
    }

    private static OutboxRecord record(List<OutboxHeader> headers) {
        return new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, headers, NOW.minusSeconds(1), 0, 0);
    }

    private static String traceparent(OutboxRecord record) {
        return record.headers().stream().filter(h -> h.name().equals("traceparent"))
                .map(h -> new String(h.value(), UTF_8)).findFirst().orElseThrow();
    }
}
