package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelBaggageManager;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** One message's trace from the writer's span through enqueue and delivery to the handler, on Postgres. */
class ObservationEndToEndIT {

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final SdkTracerProvider provider = SdkTracerProvider.builder().setSampler(Sampler.alwaysOn())
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build();
    private final io.opentelemetry.api.trace.Tracer otel = provider.get("test");
    private final OtelCurrentTraceContext current = new OtelCurrentTraceContext();
    private final Tracer tracer = new OtelTracer(otel, current, event -> {},
            new OtelBaggageManager(current, List.of(), List.of()));
    private final Propagator propagator =
            new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel);
    private final ObservationRegistry observations = ObservationRegistry.create();

    @AfterEach
    void close() {
        provider.close();
    }

    @Test
    void traceRunsFromTheWriterToTheHandler() throws Exception {
        var meters = new SimpleMeterRegistry();
        observations.observationConfig()
                .observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                        new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                        new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
                        new DefaultTracingObservationHandler(tracer)))
                .observationHandler(new DefaultMeterObservationHandler(meters));
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator).build();
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var inScope = new CopyOnWriteArrayList<String>(); // the handler's current span, in the record's scope
        var handed = new CopyOnWriteArrayList<String>(); // the traceparent of the record the handler got
        OutboxHandler handler = record -> {
            inScope.add(tracer.currentSpan().context().spanId());
            handed.add(record.headers().stream().filter(h -> h.name().equals("traceparent"))
                    .map(h -> new String(h.value(), UTF_8)).findFirst().orElseThrow());
        };
        var runtime = OutboxRuntime.builder(new TestOutboxConfig(), transactions, handler).telemetry(telemetry).build();
        runtime.start();
        var parent = tracer.nextSpan().name("writer").start();
        try {
            try (var scope = tracer.withSpan(parent)) {
                transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("orders", "k", "p".getBytes(UTF_8))));
            }
            parent.end();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (finished("outbox deliver orders") == null && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
        } finally {
            runtime.close();
        }

        var enqueue = finished("outbox enqueue");
        var delivery = finished("outbox deliver orders");
        assertThat(enqueue.getParentSpanId()).isEqualTo(parent.context().spanId());
        assertThat(delivery.getParentSpanId()).isEqualTo(enqueue.getSpanId());
        assertThat(delivery.getTraceId()).isEqualTo(parent.context().traceId());
        assertThat(inScope).containsExactly(delivery.getSpanId());
        assertThat(handed).containsExactly("00-" + delivery.getTraceId() + "-" + delivery.getSpanId() + "-"
                + delivery.getSpanContext().getTraceFlags().asHex()); // newer OpenTelemetry adds the random flag
    }

    private SpanData finished(String name) {
        return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).findFirst().orElse(null);
    }
}
