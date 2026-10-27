package io.github.dsudomoin.outbox.kafka;

import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.kora.KoraOutboxTelemetry;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTracing;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/** The relay with {@link KafkaOutboxSender#handler()} against a real broker. */
class KafkaEndToEndIT {

    private static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(30);

    private final TestDatabase db = TestDb.newDatabase(20);
    // the sender bounds a call by delivery.timeout.ms (20 s) + 5 s, so the relay must give it longer
    private final TestOutboxConfig config = new TestOutboxConfig().batchTimeout(Duration.ofSeconds(30));
    private final OutboxMetrics metrics = new OutboxMetrics(null);
    private final List<KafkaOutboxSender> senders = new ArrayList<>();
    private final SdkTracerProvider tracing = SdkTracerProvider.builder().build();
    private OutboxSchema schema;
    private OutboxRelay relay;
    private PgOutbox outbox;

    @AfterEach
    void release() {
        if (relay != null) {
            relay.close();
        }
        senders.forEach(KafkaOutboxSender::release);
        tracing.close();
        db.release();
        Context.clear();
    }

    @Test
    void deliversToKafkaWithKeyHeadersAndMessageId() throws Exception {
        var topic = TestKafka.createTopic(3);
        start(sender(Map.of()).handler(), null);

        var keyed = send(OutboxMessage.of(topic, "k1", bytes("v1"))
                .withHeader("h", bytes("a"))
                .withHeader("no-value", null)
                .withHeader("h", bytes("b")));
        var tombstone = send(OutboxMessage.of(topic, (byte[]) null));

        var received = byMessageId(TestKafka.consumeUntil(topic, CONSUME_TIMEOUT, r -> r.size() >= 2));
        assertThat(received.keySet()).containsExactlyInAnyOrder(keyed, tombstone);
        var record = received.get(keyed);
        assertThat(record.key()).isEqualTo(bytes("k1"));
        assertThat(record.value()).isEqualTo(bytes("v1"));
        assertThat(headers(record)).containsExactly(
                Map.entry("h", "a"), Map.entry("no-value", "<null>"), Map.entry("h", "b"),
                Map.entry(KafkaOutboxHeaders.MESSAGE_ID, keyed.toString()));
        assertThat(received.get(tombstone).key()).isNull();
        assertThat(received.get(tombstone).value()).isNull();
    }

    /**
     * The outage is a 15 s {@code docker pause} rather than a stop/start, which can change the mapped port: the broker
     * freezes with its connections open. The producer timeouts are cut to a few seconds, so the outage outlasts them
     * (the 20 s default delivery timeout would ride it out): in-flight requests time out, producers are closed and
     * records retried, while requests the frozen broker already holds may still be appended after it resumes.
     * Writers keep committing throughout. Duplicates are allowed; per key, the first occurrence of each message must
     * follow send order and none may be missing.
     */
    @Test
    void perKeyOrderSurvivesBrokerRestart() throws Exception {
        int writers = 4;
        int keysPerWriter = 5;
        int messagesPerKey = 300;
        config.workers(2).shards(4);
        var topic = TestKafka.createTopic(6);
        var failures = new AtomicInteger();
        var kafka = sender(Map.of(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "4000",
                ProducerConfig.MAX_BLOCK_MS_CONFIG, "2000")).handler();
        start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                kafka.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                var outcomes = kafka.handleBatch(records);
                failures.addAndGet((int) outcomes.stream().filter(o -> !(o instanceof Outcome.Delivered)).count());
                return outcomes;
            }
        }, null);

        var sent = new ConcurrentHashMap<String, List<UUID>>(); // per key, in send order
        var pool = Executors.newFixedThreadPool(writers);
        try {
            var writing = IntStream.range(0, writers).mapToObj(w -> pool.submit(() -> {
                var keys = IntStream.range(0, keysPerWriter).mapToObj(k -> "key-" + w + "-" + k).toList();
                keys.forEach(k -> sent.put(k, new ArrayList<>()));
                for (int seq = 0; seq < messagesPerKey; seq++) {
                    int s = seq;
                    var messages = keys.stream().map(k -> OutboxMessage.of(topic, k, bytes(k + ":" + s))).toList();
                    var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));
                    for (int i = 0; i < keys.size(); i++) {
                        sent.get(keys.get(i)).add(ids.get(i));
                    }
                    Thread.sleep(80); // spreads ~24 s of writes over the outage
                }
                return null;
            })).toList();
            Thread.sleep(3_000);
            TestKafka.pause(Duration.ofSeconds(15));
            for (var w : writing) {
                w.get(2, TimeUnit.MINUTES);
            }
        } finally {
            pool.shutdownNow();
        }

        var all = new HashMap<UUID, String>();
        sent.forEach((key, ids) -> ids.forEach(id -> all.put(id, key)));
        assertThat(all).hasSize(writers * keysPerWriter * messagesPerKey);
        var records = TestKafka.consumeUntil(topic, Duration.ofMinutes(3),
                r -> r.stream().map(KafkaOutboxHeaders::messageId).distinct().filter(all::containsKey).count()
                        == all.size());

        var firstOccurrences = new HashMap<String, Set<UUID>>();
        for (var record : records) { // per partition in offset order, and a key lives in one partition
            var key = new String(record.key(), UTF_8);
            firstOccurrences.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(KafkaOutboxHeaders.messageId(record));
        }
        assertThat(firstOccurrences.keySet()).isEqualTo(sent.keySet());
        sent.forEach((key, ids) -> assertThat(List.copyOf(firstOccurrences.get(key))).as(key).isEqualTo(ids));
        assertThat(failures.get()).as("the outage reached the sender").isPositive();
    }

    @Test
    void tooLargeGoesToDead() throws Exception {
        var topic = TestKafka.createTopic(3);
        start(sender(Map.of()).handler(), null);

        var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(
                OutboxMessage.of(topic, "k", bytes("before")),
                OutboxMessage.of(topic, "k", new byte[2 * 1024 * 1024]), // default max.request.size is 1 MiB
                OutboxMessage.of(topic, "k", bytes("after")),
                OutboxMessage.of(topic, "j", bytes("other")))));
        var tooLarge = ids.get(1);
        var others = List.of(ids.get(0), ids.get(2), ids.get(3));

        var received = TestKafka.consumeUntil(topic, CONSUME_TIMEOUT,
                r -> r.stream().map(KafkaOutboxHeaders::messageId).toList().containsAll(others));
        assertThat(received).extracting(KafkaOutboxHeaders::messageId).doesNotContain(tooLarge);
        assertThat(received).filteredOn(r -> new String(r.key(), UTF_8).equals("k"))
                .extracting(KafkaOutboxHeaders::messageId)
                .containsExactly(ids.get(0), ids.get(2));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        long deadRows = tooLargeDeadRows(tooLarge);
        while (deadRows == 0 && System.nanoTime() < deadline) { // may commit after the others were consumed
            Thread.sleep(20);
            deadRows = tooLargeDeadRows(tooLarge);
        }
        assertThat(deadRows).isEqualTo(1);
    }

    /**
     * C1: the broker does not auto-create topics (as in production), so sends to {@code missing} wait for metadata
     * until {@code max.block.ms} (cut to 2 s here). Those rows are retried; the existing topic's rows interleaved with
     * them keep flowing.
     */
    @Test
    void missingTopicDoesNotStallOtherTopics() throws Exception {
        var topic = TestKafka.createTopic(3);
        var missing = "missing-" + UUID.randomUUID();
        start(sender(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, "2000")).handler(), null);

        var expected = new HashSet<UUID>();
        for (int chunk = 0; chunk < 20; chunk++) {
            var messages = new ArrayList<OutboxMessage>();
            for (int i = 0; i < 100; i++) {
                messages.add(OutboxMessage.of(topic, "k" + (i % 37), bytes("v")));
                if (i == 50) {
                    messages.add(OutboxMessage.of(missing, "k" + chunk, bytes("nowhere")));
                }
            }
            var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));
            for (int i = 0; i < ids.size(); i++) {
                if (messages.get(i).topic().equals(topic)) {
                    expected.add(ids.get(i));
                }
            }
        }

        TestKafka.consumeUntil(topic, Duration.ofSeconds(10),
                r -> r.stream().map(KafkaOutboxHeaders::messageId).collect(Collectors.toSet()).containsAll(expected));
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(distinct topic, ',') from " + schema.names().retry()))
                    .isEqualTo(missing);
        }
    }

    private long tooLargeDeadRows(UUID messageId) throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + schema.names().dead() + " where message_id = '" + messageId
                    + "' and error like '%RecordTooLargeException%'");
        }
    }

    /** With a relay tracer the record carries the delivery span, a child of the writer's; without, the writer's. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void traceparentReachesConsumer(boolean relayTraces) throws Exception {
        var topic = TestKafka.createTopic(1);
        start(sender(Map.of()).handler(), relayTraces ? tracing.get("test") : null);

        TestTracing.activateSpan();
        var id = send(OutboxMessage.of(topic, "k", bytes("traced")));
        Context.clear();

        var record = byMessageId(TestKafka.consumeUntil(topic, CONSUME_TIMEOUT, r -> !r.isEmpty())).get(id);
        var traceparent = new String(record.headers().lastHeader("traceparent").value(), UTF_8).split("-");
        assertThat(traceparent[1]).isEqualTo(TestTracing.TRACE_ID);
        if (relayTraces) {
            assertThat(traceparent[2]).isNotEqualTo(TestTracing.SPAN_ID);
        } else {
            assertThat(String.join("-", traceparent)).isEqualTo(TestTracing.TRACEPARENT);
        }
    }

    private void start(OutboxHandler handler, @Nullable Tracer tracer) throws Exception {
        schema = new OutboxSchema(db, config);
        schema.start();
        relay = new OutboxRelay(db, config, schema, handler, new KoraOutboxTelemetry(metrics, tracer),
                new Maintenance(db, schema.names(), config, metrics),
                new DeadLetterPass(db, schema.names(), config, null, metrics));
        relay.start();
        outbox = new PgOutbox(db, schema, config.shards(), relay::wake, new KoraOutboxTelemetry(metrics, null));
    }

    private UUID send(OutboxMessage message) {
        return db.inTx((SqlFunction0<UUID>) () -> outbox.send(message));
    }

    private KafkaOutboxSender sender(Map<String, String> overrides) {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        properties.putAll(overrides);
        var sender = new KafkaOutboxSender(() -> properties);
        senders.add(sender);
        return sender;
    }

    private static Map<UUID, ConsumerRecord<byte[], byte[]>> byMessageId(List<ConsumerRecord<byte[], byte[]>> records) {
        var result = new HashMap<UUID, ConsumerRecord<byte[], byte[]>>();
        records.forEach(r -> result.putIfAbsent(KafkaOutboxHeaders.messageId(r), r));
        return result;
    }

    private static List<Map.Entry<String, String>> headers(ConsumerRecord<byte[], byte[]> record) {
        var result = new ArrayList<Map.Entry<String, String>>();
        for (Header h : record.headers()) {
            result.add(Map.entry(h.key(), h.value() == null ? "<null>" : new String(h.value(), UTF_8)));
        }
        return result;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
