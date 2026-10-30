package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.common.KeyValue;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ObservationOutboxTelemetryTest {

    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String SPAN_ID = "b7ad6b7169203331";
    private static final String TRACEPARENT = "00-" + TRACE_ID + "-" + SPAN_ID + "-01";
    private static final String UNSAMPLED = "00-" + TRACE_ID + "-" + SPAN_ID + "-00";
    private static final Instant NOW = Instant.parse("2026-10-27T12:00:00Z");

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final SdkTracerProvider provider = SdkTracerProvider.builder().setSampler(Sampler.alwaysOn())
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build();
    private final io.opentelemetry.api.trace.Tracer otel = provider.get("test");
    private final OtelCurrentTraceContext current = new OtelCurrentTraceContext();
    private final Tracer tracer = new OtelTracer(otel, current, event -> {},
            new OtelBaggageManager(current, List.of(), List.of()));
    private final Propagator propagator =
            new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final ObservationRegistry observations = ObservationRegistry.create();

    @BeforeEach
    void handlers() {
        observations.observationConfig()
                .observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                        new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                        new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
                        new DefaultTracingObservationHandler(tracer)))
                .observationHandler(new DefaultMeterObservationHandler(meters));
    }

    @AfterEach
    void close() {
        provider.close();
    }

    @Test
    void enqueueContextLandsInHeaders() {
        var parent = tracer.nextSpan().name("parent").start();
        OutboxTelemetry.Enqueue enqueue;
        List<OutboxHeader> headers;
        try (var scope = tracer.withSpan(parent)) {
            enqueue = telemetry().enqueueStarted();
            headers = enqueue.headers(List.of());
        }
        enqueue.finished();
        parent.end();

        var span = finished("outbox enqueue");
        assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(span.getParentSpanId()).isEqualTo(parent.context().spanId());
        // its own flags: newer OpenTelemetry sets the random trace id flag besides the sampled one
        assertThat(traceparent(headers)).isEqualTo("00-" + span.getTraceId() + "-" + span.getSpanId() + "-"
                + span.getSpanContext().getTraceFlags().asHex());
    }

    @Test
    void messageWithTraceContextKeepsIt() {
        var enqueue = telemetry().enqueueStarted();
        var own = List.of(header("traceparent", "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01"));

        assertThat(enqueue.headers(own)).isSameAs(own);
        enqueue.finished();
    }

    @Test
    void deliveryParentedByStoredContext() {
        var stored = record(List.of(header("traceparent", TRACEPARENT)));
        var delivery = telemetry().deliveryStarted(List.of(stored), NOW);
        delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        var span = finished("outbox deliver orders");
        assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
        assertThat(span.getParentSpanId()).isEqualTo(SPAN_ID);
        assertThat(attribute(span, "messaging.destination.name")).isEqualTo("orders");
        assertThat(attribute(span, "outbox.outcome")).isEqualTo("delivered");
        assertThat(attribute(span, "outbox.message_id")).isEqualTo(stored.messageId().toString());
        assertThat(attribute(span, "outbox.attempt")).isEqualTo("1");
        assertThat(attribute(span, "outbox.delay_ms")).isEqualTo("1000");
    }

    @Test
    void deliveryCarriesTheShard() {
        var stopped = new CopyOnWriteArrayList<OutboxDeliveryContext>();
        observations.observationConfig().observationHandler(new ObservationHandler<OutboxDeliveryContext>() {
            @Override
            public void onStop(OutboxDeliveryContext context) {
                stopped.add(context);
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return context instanceof OutboxDeliveryContext;
            }
        });
        var known = record(List.of(), 3);
        var unknown = record(List.of(), -1);
        var delivery = telemetry().deliveryStarted(List.of(known, unknown), NOW);
        delivery.handed(List.of(known, unknown));
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));

        assertThat(stopped).hasSize(2);
        assertThat(stopped.get(0).getHighCardinalityKeyValues()).contains(KeyValue.of("outbox.shard", "3"));
        assertThat(stopped.get(1).getHighCardinalityKeyValue("outbox.shard")).isNull();
    }

    @Test
    void handedRecordCarriesDeliveryContext() {
        var stored = record(List.of(header("a", "1"), header("traceparent", TRACEPARENT)));
        var delivery = telemetry().deliveryStarted(List.of(stored), NOW);
        var handed = delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        var span = finished("outbox deliver orders");
        assertThat(traceparent(handed.get(0).headers()))
                .isEqualTo("00-" + span.getTraceId() + "-" + span.getSpanId() + "-01");
        assertThat(handed.get(0).headers()).extracting(OutboxHeader::name).containsOnlyOnce("a");
        assertThat(traceparent(stored.headers())).as("the stored record keeps its own").isEqualTo(TRACEPARENT);
    }

    @Test
    void storedBaggageSurvivesHanding() {
        var withBaggage = new OtelPropagator(ContextPropagators.create(TextMapPropagator.composite(
                W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())), otel);
        var registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                new PropagatingReceiverTracingObservationHandler<>(tracer, withBaggage),
                new DefaultTracingObservationHandler(tracer)));
        var telemetry = ObservationOutboxTelemetry.builder(registry, new OutboxMetrics(meters))
                .tracing(tracer, withBaggage).build();
        var stored = record(List.of(header("traceparent", TRACEPARENT), header("baggage", "tenant=acme")));
        var delivery = telemetry.deliveryStarted(List.of(stored), NOW);
        var handed = delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        var span = finished("outbox deliver orders");
        assertThat(traceparent(handed.get(0).headers()))
                .isEqualTo("00-" + span.getTraceId() + "-" + span.getSpanId() + "-01");
        assertThat(handed.get(0).headers()).filteredOn(h -> h.name().equals("baggage"))
                .extracting(h -> new String(h.value(), UTF_8)).containsExactly("tenant=acme");
    }

    @Test
    void deliveryWithoutItsOwnSpanKeepsTheStoredTraceInsideAStep() {
        observations.observationConfig().observationPredicate((name, context) -> !name.equals("outbox.deliver"));
        var telemetry = telemetry();
        var stored = record(List.of(header("traceparent", TRACEPARENT)));
        var step = telemetry.stepStarted(List.of(1));
        List<OutboxRecord> handed;
        try {
            assertThat(tracer.currentSpan()).as("the step span is current").isNotNull();
            var delivery = telemetry.deliveryStarted(List.of(stored), NOW);
            handed = delivery.handed(List.of(stored));
            delivery.finished(List.of(Outcome.delivered()));
        } finally {
            step.finished();
        }

        assertThat(traceparent(handed.get(0).headers())).isEqualTo(TRACEPARENT);
    }

    @Test
    void anUnsampledRecordIsObservedWhenAsked() {
        var stored = record(List.of(header("traceparent", UNSAMPLED)));
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator)
                .observeUnsampled(true)
                .build();
        var delivery = telemetry.deliveryStarted(List.of(stored), NOW);
        delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(meters.get("outbox.deliver").timer().count()).isEqualTo(1);
        assertThat(finished("outbox deliver orders").getParentSpanId()).isEqualTo(SPAN_ID);
    }

    @Test
    void anUnsampledRecordIsNotObservedByDefaultAndKeepsItsHeaders() throws Exception {
        var headers = List.of(header("a", "1"), header("traceparent", UNSAMPLED), header("baggage", "tenant=acme"));
        var stored = record(headers);
        var delivery = telemetry().deliveryStarted(List.of(stored), NOW);
        var handed = delivery.handed(List.of(stored));
        var inside = delivery.inRecordScope(handed.get(0), () -> tracer.currentSpan());
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(handed.get(0)).isSameAs(stored);
        assertThat(handed.get(0).headers()).isSameAs(headers);
        assertThat(inside).as("no scope of its own").isNull();
        assertThat(meters.find("outbox.deliver").timer()).isNull();
        assertThat(spans.getFinishedSpanItems()).isEmpty();
    }

    @Test
    void sampledMissingAndMalformedTraceparentsAreObserved() {
        var records = List.of(
                record(List.of(header("traceparent", TRACEPARENT))),
                record(List.of(header("b3", TRACE_ID + "-" + SPAN_ID + "-0"))),
                record(List.of(header("traceparent", "00-" + TRACE_ID + "-" + SPAN_ID + "-0"))),
                record(List.of(header("traceparent", "00-" + TRACE_ID.toUpperCase() + "-" + SPAN_ID + "-00"))),
                record(List.of(header("traceparent", "00-" + "0".repeat(32) + "-" + SPAN_ID + "-00"))),
                record(List.of(header("traceparent", "00-" + TRACE_ID + "-" + "0".repeat(16) + "-00"))),
                record(List.of(header("traceparent", "ff-" + TRACE_ID + "-" + SPAN_ID + "-00"))),
                record(List.of(header("traceparent", UNSAMPLED + "-extra"))),
                record(List.of(header("traceparent", UNSAMPLED), header("traceparent", TRACEPARENT))));
        var delivery = telemetry().deliveryStarted(records, NOW);
        delivery.handed(records);
        delivery.finished(records.stream().map(r -> Outcome.delivered()).toList());

        assertThat(meters.get("outbox.deliver").timer().count()).isEqualTo(records.size());
    }

    @Test
    void aMixedCallKeepsOrderAndTheOutcomesOfObservedRecords() {
        var a = record(List.of(header("traceparent", UNSAMPLED)));
        var b = record(List.of(header("traceparent", TRACEPARENT)));
        var c = record(List.of(header("traceparent", UNSAMPLED)));
        var d = record(List.of());
        var delivery = telemetry().deliveryStarted(List.of(a, b, c, d), NOW);
        var handed = delivery.handed(List.of(a, b, c, d));
        delivery.finished(List.of(Outcome.delivered(), Outcome.retry(new IOException("x")),
                Outcome.permanent(new IOException("y")), Outcome.requeue()));

        assertThat(handed).hasSize(4);
        assertThat(handed.get(0)).isSameAs(a);
        assertThat(handed.get(1).messageId()).isEqualTo(b.messageId());
        assertThat(handed.get(2)).isSameAs(c);
        assertThat(handed.get(3).messageId()).isEqualTo(d.messageId());
        assertThat(traceparent(handed.get(1).headers())).isNotEqualTo(TRACEPARENT);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "retry").timer().count()).isEqualTo(1);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "requeued").timer().count()).isEqualTo(1);
        assertThat(meters.find("outbox.deliver").tag("outbox.outcome", "delivered").timer()).isNull();
        assertThat(meters.find("outbox.deliver").tag("outbox.outcome", "permanent").timer()).isNull();
        assertThat(spans.getFinishedSpanItems()).extracting(s -> attribute(s, "outbox.message_id"))
                .containsExactlyInAnyOrder(b.messageId().toString(), d.messageId().toString());
    }

    @Test
    void withoutTracingHandedRecordsAreTheInput() {
        var plain = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters)).build();
        var records = List.of(record(List.of(header("traceparent", TRACEPARENT))));
        var delivery = plain.deliveryStarted(records, NOW);

        var handed = delivery.handed(records);
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(handed).isSameAs(records);
    }

    @Test
    void recordScopeMakesDeliveryCurrent() throws Exception {
        var records = List.of(record(List.of(header("traceparent", TRACEPARENT))));
        var delivery = telemetry().deliveryStarted(records, NOW);
        var handed = delivery.handed(records);

        var inside = delivery.inRecordScope(handed.get(0), () -> tracer.currentSpan().context().spanId());
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(inside).isEqualTo(finished("outbox deliver orders").getSpanId());
        assertThat(tracer.currentSpan()).as("the scope is closed after the call").isNull();
    }

    @Test
    void outcomeAndErrorRecorded() {
        var telemetry = telemetry();
        var retried = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        retried.handed(List.of(record(List.of())));
        retried.finished(List.of(Outcome.retry(new IOException("x"))));

        var span = finished("outbox deliver orders");
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(span.getEvents()).anyMatch(e -> e.getName().equals("exception"));
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "retry").timer().count()).isEqualTo(1);

        var failed = telemetry.deliveryStarted(List.of(record(List.of()), record(List.of())), NOW);
        failed.failed(new IllegalStateException("closed"));
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "failed").timer().count()).isEqualTo(2);
    }

    @Test
    void aRejectedContractIsCounted() {
        telemetry().contractRejected("orders", "Created", 2);

        assertThat(meters.get("outbox.messages.contract_rejected").tags("topic", "orders", "type", "Created",
                "version", "2").counter().count()).isEqualTo(1);
    }

    @Test
    void aWaitingRecordIsLabelledWaitingWithoutAnErrorAndCounted() {
        var telemetry = telemetry();
        var delivery = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        delivery.handed(List.of(record(List.of())));
        delivery.finished(List.of(Outcome.waitFor("v3 is not readable here")));
        telemetry.waited("orders", Outcome.Waiting.Kind.CONTRACT);

        var span = finished("outbox deliver orders");
        assertThat(attribute(span, "outbox.outcome")).isEqualTo("waiting");
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "waiting").timer().count()).isEqualTo(1);
        assertThat(meters.get("outbox.messages.waiting").tags("topic", "orders", "kind", "contract").counter().count())
                .isEqualTo(1);
    }

    @Test
    void stepObservationIsCurrentUntilItEnds() {
        var stopped = stoppedSteps();
        var step = telemetry().stepStarted(List.of(1, 3));

        var current = observations.getCurrentObservation();
        assertThat(current).isNotNull();
        assertThat(current.getContext()).isInstanceOfSatisfying(OutboxStepContext.class,
                c -> assertThat(c.shards()).containsExactly(1, 3));
        step.finished();

        assertThat(observations.getCurrentObservation()).isNull();
        assertThat(stopped).hasSize(1);
        var context = stopped.get(0);
        assertThat(context.getName()).isEqualTo("outbox.relay.step");
        assertThat(context.getContextualName()).isEqualTo("outbox relay step");
        assertThat(context.getLowCardinalityKeyValues()).isEmpty();
        assertThat(context.getHighCardinalityKeyValues()).containsExactly(KeyValue.of("outbox.shards", "1,3"));
        assertThat(context.getError()).isNull();
        assertThat(finished("outbox relay step").getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
    }

    @Test
    void failedStepRecordsTheErrorAndLeavesNoScope() {
        var stopped = stoppedSteps();
        var error = new SQLException("x");

        telemetry().stepStarted(List.of(2)).failed(error);

        assertThat(observations.getCurrentObservation()).isNull();
        assertThat(tracer.currentSpan()).isNull();
        assertThat(stopped).hasSize(1);
        assertThat(stopped.get(0).getError()).isSameAs(error);
        assertThat(finished("outbox relay step").getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    }

    @Test
    void relayFailuresReachTheCounter() {
        telemetry().relayFailed(new SQLException("x"));

        assertThat(meters.counter("outbox.relay.failures").count()).isEqualTo(1);
    }

    @Test
    void customConventionRenames() {
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator)
                .deliveryConvention(new OutboxDeliveryObservationConvention() {
                    @Override
                    public String getName() {
                        return "custom.process";
                    }
                })
                .build();
        var delivery = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(meters.get("custom.process").timer().count()).isEqualTo(1);
        assertThat(spans.getFinishedSpanItems()).extracting(SpanData::getName).containsExactly("custom.process");
    }

    @Test
    void meterFilterRenamesGauge() {
        meters.config().meterFilter(new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                return id.getName().equals("outbox.shard.lag") ? id.withName("custom.lag") : id;
            }
        });

        telemetry().gauge(OutboxGauge.SHARD_LAG, () -> 7);

        assertThat(meters.get("custom.lag").gauge().value()).isEqualTo(7);
    }

    private ObservationOutboxTelemetry telemetry() {
        return ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator)
                .build();
    }

    /** The step contexts as they stop. */
    private List<OutboxStepContext> stoppedSteps() {
        var stopped = new CopyOnWriteArrayList<OutboxStepContext>();
        observations.observationConfig().observationHandler(new ObservationHandler<OutboxStepContext>() {
            @Override
            public void onStop(OutboxStepContext context) {
                stopped.add(context);
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return context instanceof OutboxStepContext;
            }
        });
        return stopped;
    }

    private SpanData finished(String name) {
        return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).findFirst().orElseThrow();
    }

    private static String attribute(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.stringKey(key));
    }

    private static OutboxRecord record(List<OutboxHeader> headers) {
        return new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, headers, NOW.minusSeconds(1), 0, 0);
    }

    private static OutboxRecord record(List<OutboxHeader> headers, int shard) {
        return new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, headers, NOW.minusSeconds(1), 0, 0,
                shard);
    }

    private static OutboxHeader header(String name, String value) {
        return new OutboxHeader(name, value.getBytes(UTF_8));
    }

    private static String traceparent(List<OutboxHeader> headers) {
        return headers.stream().filter(h -> h.name().equals("traceparent")).map(h -> new String(h.value(), UTF_8))
                .reduce((first, last) -> last).orElseThrow();
    }
}
