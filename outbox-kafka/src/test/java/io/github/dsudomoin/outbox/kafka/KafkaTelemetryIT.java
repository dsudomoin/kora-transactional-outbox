package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.micrometer.ObservationOutboxTelemetry;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
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
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The telemetry stage by stage on the framework-free path: {@link OutboxRuntime} over {@link JdbcTransactions}, with
 * {@link KafkaOutboxSender}'s handler and {@link ObservationOutboxTelemetry} over {@link OutboxMetrics}, traced through
 * the OpenTelemetry bridge of micrometer-tracing, against a real Postgres and a real broker.
 */
class KafkaTelemetryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final String STEP = "outbox relay step";
    private static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("outbox.message_id");
    private static final AttributeKey<String> SHARD = AttributeKey.stringKey("outbox.shard");
    private static final AttributeKey<String> SHARDS = AttributeKey.stringKey("outbox.shards");

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
    private final ObservationOutboxTelemetry telemetry = observed();
    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> closing = new ArrayList<>(); // senders and runtimes, closed last first

    @AfterEach
    void close() throws Exception {
        try {
            for (int i = closing.size() - 1; i >= 0; i--) {
                closing.get(i).close();
            }
        } finally {
            provider.close();
        }
    }

    /**
     * Three writers, each in a span of its own, send one message each; the relay hands the three records to the
     * handler in one call, and each keeps its own writer's trace through enqueue, delivery and the Kafka record.
     */
    @Test
    void oneCallWithRecordsFromThreeTracesKeepsEachTrace() throws Exception {
        var topic = TestKafka.createTopic(3);
        var handler = new Handler();
        var runtime = start(config().relayEnabled(false), handler);
        var writers = new ArrayList<Span>();
        var ids = new ArrayList<UUID>();
        for (int i = 0; i < 3; i++) {
            var writer = tracer.nextSpan().name("writer " + i).start();
            var message = OutboxMessage.of(topic, "k" + i, bytes("m" + i));
            try (var scope = tracer.withSpan(writer)) {
                ids.add(transactions.inTx(c -> runtime.outbox().send(message)));
            } finally {
                writer.end();
            }
            writers.add(writer);
        }

        runtime.relay().drain();

        assertThat(handler.calls).as("handler calls").singleElement()
                .satisfies(call -> assertThat(call).containsExactlyInAnyOrderElementsOf(ids));
        var received = byMessageId(
                TestKafka.consumeUntil(topic, TIMEOUT, r -> byMessageId(r).keySet().containsAll(ids)));
        for (int i = 0; i < ids.size(); i++) {
            var writer = writers.get(i).context();
            var enqueue = span(s -> s.getName().equals("outbox enqueue")
                    && s.getParentSpanId().equals(writer.spanId()));
            var delivery = delivery(ids.get(i));
            assertThat(delivery.getParentSpanId()).as("delivery %d", i).isEqualTo(enqueue.getSpanId());
            assertThat(delivery.getTraceId()).as("delivery %d", i).isEqualTo(writer.traceId());
            assertThat(traceparent(received.get(ids.get(i)))).as("Kafka record %d", i)
                    .isEqualTo("00-" + writer.traceId() + "-" + delivery.getSpanId() + "-"
                            + delivery.getSpanContext().getTraceFlags().asHex());
        }
    }

    /**
     * The meters of real sends as README «Метрики» defines them: three messages delivered at once, one retried once
     * then delivered, one permanent failure, one requeued once then delivered. {@code failed{outcome=retry}} counts
     * rows diverted to the retry table; with {@code headFailuresBeforeDivert} 1 the first failed attempt diverts.
     */
    @Test
    void countersThroughRealSends() throws Exception {
        var topic = TestKafka.createTopic(3);
        var handler = new Handler();
        handler.fail = r -> switch (key(r)) {
            case "retried" -> r.attempts() == 0 ? Outcome.retry(new IOException("down once")) : null;
            case "dead" -> Outcome.permanent(new IllegalArgumentException("refused for good"));
            case "requeued" -> r.requeues() == 0 ? Outcome.requeue() : null;
            default -> null;
        };
        var runtime = start(config().headFailuresBeforeDivert(1), handler);
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(Stream.of("a", "b", "c", "retried", "dead",
                "requeued").map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));
        var dead = ids.get(4);

        await(TIMEOUT, () -> counter("outbox.messages.delivered", "topic", topic) == 5
                && counter("outbox.messages.failed", "topic", topic, "outcome", "dead") == 1);
        assertThat(counter("outbox.messages.enqueued", "topic", topic)).isEqualTo(6);
        assertThat(counter("outbox.messages.delivered", "topic", topic)).isEqualTo(5);
        assertThat(counter("outbox.messages.failed", "topic", topic, "outcome", "retry")).isEqualTo(1);
        assertThat(counter("outbox.messages.failed", "topic", topic, "outcome", "dead")).isEqualTo(1);
        assertThat(counter("outbox.messages.requeued", "topic", topic)).isEqualTo(1);
        var sent = ids.stream().filter(id -> !id.equals(dead)).toList();
        var received = TestKafka.consumeUntil(topic, TIMEOUT, r -> byMessageId(r).keySet().containsAll(sent));
        assertThat(byMessageId(received)).doesNotContainKey(dead);

        await(Duration.ofSeconds(30), () -> runtime.maintenance().runOnce());
        assertThat(meters.get("outbox.shard.lag").gauge().value()).isNotNaN().isGreaterThanOrEqualTo(0.0);
    }

    /**
     * A drained step is observed without error. A step whose backend dies after the sender returned fails (a handler
     * failure would not: it is an outcome): its observation records the error, and the worker counts the failed pass.
     */
    @Test
    void relayStepsAreObservedAndFailuresCounted() throws Exception {
        var topic = TestKafka.createTopic(3);
        var config = config().relayEnabled(false);
        var drained = start(config, new Handler());
        transactions.inTx(c -> drained.outbox().send(OutboxMessage.of(topic, "k", bytes("drained"))));

        drained.relay().drain();

        var shard = Integer.toString(Hashing.shard(Hashing.keyHash(topic, bytes("k")), config.shards()));
        assertThat(spans(STEP)).singleElement().satisfies(step -> {
            assertThat(step.getAttributes().get(SHARDS).split(",")).contains(shard);
            assertThat(step.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        });
        assertThat(meters.get("outbox.relay.step").tag("error", "none").timer().count()).isEqualTo(1);
        assertThat(counter("outbox.relay.failures")).isZero();

        var handler = new Handler();
        var running = start(config(), handler);
        var crashed = new AtomicBoolean();
        var terminated = new AtomicInteger();
        handler.afterSend = records -> {
            if (crashed.compareAndSet(false, true)) {
                terminated.set(terminateStep(running, records.get(0).shard()));
            }
        };
        transactions.inTx(c -> running.outbox().send(OutboxMessage.of(topic, "k", bytes("crashed"))));

        await(TIMEOUT, () -> counter("outbox.relay.failures") >= 1
                && spans(STEP).stream().anyMatch(s -> s.getStatus().getStatusCode() == StatusCode.ERROR));
        assertThat(terminated).as("the step's backend").hasValue(1);
    }

    /**
     * A message sent outside any span still stores trace context: the root enqueue span its send started, which
     * parents its delivery span. A row written without telemetry stores none: its delivery span is a child of the
     * relay step that delivers it, the current span on the stepping thread.
     */
    @Test
    void aRecordWithoutTraceContextInsideAStep() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = config().relayEnabled(false);
        var runtime = start(config, new Handler());
        var untraced = start(OutboxRuntime.builder(sameTables(config).relayEnabled(false), transactions, r -> {}));
        var outsideAnySpan = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(topic, "a", bytes("a"))));
        var withoutContext = transactions.inTx(c -> untraced.outbox().send(OutboxMessage.of(topic, "b", bytes("b"))));

        runtime.relay().drain();

        var enqueue = span(s -> s.getName().equals("outbox enqueue"));
        assertThat(enqueue.getParentSpanContext().isValid()).as("the enqueue span has a parent").isFalse();
        assertThat(delivery(outsideAnySpan).getParentSpanId()).isEqualTo(enqueue.getSpanId());
        var delivery = delivery(withoutContext);
        var parent = span(s -> s.getSpanId().equals(delivery.getParentSpanId()));
        assertThat(parent.getName()).isEqualTo(STEP);
        assertThat(delivery.getTraceId()).isEqualTo(parent.getTraceId());
    }

    @Test
    void shardOnDeliverySpans() throws Exception {
        var topic = TestKafka.createTopic(3);
        var config = config().relayEnabled(false);
        var runtime = start(config, new Handler());
        var keys = IntStream.range(0, 8).mapToObj(i -> "k" + i).toList();
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(
                keys.stream().map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));

        runtime.relay().drain();

        for (int i = 0; i < keys.size(); i++) {
            int shard = Hashing.shard(Hashing.keyHash(topic, bytes(keys.get(i))), config.shards());
            assertThat(delivery(ids.get(i)).getAttributes().get(SHARD)).as(keys.get(i))
                    .isEqualTo(Integer.toString(shard));
        }
    }

    /**
     * {@link KafkaOutboxSender#handler()} that keeps the message ids of each call: a record {@link #fail} answers for
     * gets that outcome instead of being sent (every test has one message per key, so no later record of its key is
     * in the call); {@link #afterSend} runs once the sender returned, before the relay sees the outcomes.
     */
    private final class Handler implements OutboxHandler {

        private final KafkaOutboxHandler kafka = sender().handler();
        final List<List<UUID>> calls = new CopyOnWriteArrayList<>();
        volatile Function<OutboxRecord, Outcome> fail = r -> null; // null: send it
        volatile Consumer<List<OutboxRecord>> afterSend = records -> {};

        @Override
        public void handle(OutboxRecord record) {
            kafka.handle(record);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            calls.add(records.stream().map(OutboxRecord::messageId).toList());
            var outcomes = new Outcome[records.size()];
            var sending = new ArrayList<OutboxRecord>();
            for (int i = 0; i < outcomes.length; i++) {
                outcomes[i] = fail.apply(records.get(i));
                if (outcomes[i] == null) {
                    sending.add(records.get(i));
                }
            }
            var sent = (sending.isEmpty() ? List.<Outcome>of() : kafka.handleBatch(sending)).iterator();
            for (int i = 0; i < outcomes.length; i++) {
                if (outcomes[i] == null) {
                    outcomes[i] = sent.next();
                }
            }
            afterSend.accept(records);
            return Arrays.asList(outcomes);
        }
    }

    /** As an application wires it: propagating sender and receiver handlers, default tracing, meters. */
    private ObservationOutboxTelemetry observed() {
        var observations = ObservationRegistry.create();
        observations.observationConfig()
                .observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                        new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                        new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
                        new DefaultTracingObservationHandler(tracer)))
                .observationHandler(new DefaultMeterObservationHandler(meters));
        return ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator).build();
    }

    /** A fresh table; the sender bounds a call by delivery.timeout.ms (20 s) + 5 s, so the relay gives it longer. */
    private static TestOutboxConfig config() {
        return new TestOutboxConfig().batchTimeout(Duration.ofSeconds(30)).deadLettersTimeout(Duration.ofSeconds(30));
    }

    /** {@link #config()} on {@code config}'s tables, for a second runtime on them. */
    private static TestOutboxConfig sameTables(TestOutboxConfig config) {
        return config().table(config.table()).inboxTable(config.inbox().table());
    }

    private OutboxRuntime start(TestOutboxConfig config, Handler handler) {
        return start(OutboxRuntime.builder(config, transactions, handler).telemetry(telemetry));
    }

    private OutboxRuntime start(OutboxRuntime.Builder builder) {
        var runtime = builder.build();
        closing.add(runtime);
        runtime.start();
        return runtime;
    }

    private KafkaOutboxSender sender() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        var sender = new KafkaOutboxSender(() -> properties);
        closing.add(sender);
        return sender;
    }

    /**
     * Terminates the backend holding {@code shard}'s advisory lock: the connection of the step working on it, which
     * holds the lock until its commit. Returns how many it terminated.
     */
    private int terminateStep(OutboxRuntime runtime, int shard) {
        try (var c = transactions.newConnection(); var ps = c.prepareStatement("select pg_terminate_backend(pid, 5000)"
                + " from pg_locks where locktype = 'advisory' and classid = ?::int4::oid"
                + " and objid = ?::int4::oid and objsubid = 2 and granted")) {
            ps.setInt(1, runtime.schema().names().internalNamespace());
            ps.setInt(2, TableNames.LOCK_SHARD_BASE + shard);
            int terminated = 0;
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    terminated += rs.getBoolean(1) ? 1 : 0;
                }
            }
            return terminated;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<SpanData> spans(String name) {
        return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).toList();
    }

    /** The one finished span matching {@code filter}. */
    private SpanData span(Predicate<SpanData> filter) {
        var found = spans.getFinishedSpanItems().stream().filter(filter).toList();
        assertThat(found).hasSize(1);
        return found.get(0);
    }

    private SpanData delivery(UUID messageId) {
        return span(s -> s.getName().startsWith("outbox deliver ")
                && messageId.toString().equals(s.getAttributes().get(MESSAGE_ID)));
    }

    /** The count of the counter, 0 while it is not registered. */
    private double counter(String name, String... tags) {
        var counter = meters.find(name).tags(tags).counter();
        return counter == null ? 0 : counter.count();
    }

    /** The first occurrence of each message id. */
    private static Map<UUID, ConsumerRecord<byte[], byte[]>> byMessageId(List<ConsumerRecord<byte[], byte[]>> records) {
        var result = new HashMap<UUID, ConsumerRecord<byte[], byte[]>>();
        records.forEach(r -> result.putIfAbsent(KafkaOutboxHeaders.messageId(r), r));
        return result;
    }

    private static String traceparent(ConsumerRecord<byte[], byte[]> record) {
        var header = record.headers().lastHeader("traceparent");
        return header == null ? null : new String(header.value(), UTF_8);
    }

    private static String key(OutboxRecord record) {
        return new String(record.key(), UTF_8);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(10);
        }
    }
}
