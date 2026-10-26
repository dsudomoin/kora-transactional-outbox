package io.github.dsudomoin.kora.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.OutboxPermanentException;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaOutboxSenderTest {

    private static final String TOPIC = "t";
    private static final String OTHER = "u";
    private static final int PARTITIONS = 4;
    private static final Node NODE = new Node(0, "localhost", 9092);
    private static final Cluster CLUSTER = new Cluster("c", List.of(NODE),
            Stream.of(TOPIC, OTHER).flatMap(topic -> IntStream.range(0, PARTITIONS)
                    .mapToObj(i -> new PartitionInfo(topic, i, NODE, new Node[] {NODE}, new Node[] {NODE})))
                    .toList(),
            Set.of(), Set.of());
    private static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(20);

    private final List<MockProducer<byte[], byte[]>> producers = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void allAckedAreDelivered() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a"), record(2, "b"), record(3, null)));
        var producer = awaitSent(0, 3);
        producer.completeNext();
        producer.completeNext();
        producer.completeNext();

        assertThat(result.get(5, SECONDS)).containsExactly(Outcome.delivered(), Outcome.delivered(), Outcome.delivered());
        assertThat(producer.closed()).isFalse();

        var second = sendAsync(sender, List.of(record(4, "a")));
        awaitSent(0, 4).completeNext();
        assertThat(second.get(5, SECONDS)).containsExactly(Outcome.delivered());
        assertThat(producers).as("the healthy producer is pooled and reused").hasSize(1);
    }

    @Test
    void firstFailureClosesProducerAndRestRetry() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a"), record(2, "a"), record(3, "b")));
        var producer = awaitSent(0, 3);
        producer.errorNext(new NotLeaderOrFollowerException("gone"));

        var outcomes = result.get(5, SECONDS);
        assertThat(outcomes.get(0)).isInstanceOfSatisfying(Outcome.Retry.class,
                r -> assertThat(r.error()).isInstanceOf(NotLeaderOrFollowerException.class));
        assertThat(outcomes.subList(1, 3)).allSatisfy(o -> assertThat(o).isInstanceOf(Outcome.Retry.class));
        assertThat(producer.closed()).isTrue();

        var next = sendAsync(sender, List.of(record(4, "c")));
        awaitSent(1, 1).completeNext();
        assertThat(next.get(5, SECONDS)).containsExactly(Outcome.delivered());
        assertThat(producers).as("the closed producer is discarded").hasSize(2);
    }

    @Test
    void producerFailedOnTheIoThreadIsNeverPooled() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a")));
        var producer = awaitSent(0, 1);
        // widens the window between recording the outcome and closing, where the caller used to pool the producer
        producer.errorNext(new TimeoutException("expired") {
            @Override
            public String toString() {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.toString();
            }
        });
        assertThat(result.get(5, SECONDS).get(0)).isInstanceOf(Outcome.Retry.class);

        var next = sendAsync(sender, List.of(record(2, null))); // unkeyed: a's partition cools down
        awaitSent(1, 1).completeNext();
        assertThat(next.get(5, SECONDS)).containsExactly(Outcome.delivered());
        assertThat(producer.history()).hasSize(1);
    }

    /** C1: a metadata failure for one topic skips that topic for the rest of the call; the rest flows. */
    @ParameterizedTest
    @ValueSource(strings = {"timeout", "authorization"})
    void badTopicDoesNotStallOtherTopics(String failure) {
        var lookups = new AtomicInteger();
        var sender = sender(() -> new TestProducer(true, r -> null) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(String topic) {
                if (!topic.equals("missing")) {
                    return super.partitionsFor(topic);
                }
                lookups.incrementAndGet();
                throw failure.equals("timeout")
                        ? new TimeoutException("Topic missing not present in metadata after 5000 ms.")
                        : new TopicAuthorizationException(Set.of(topic));
            }
        }, DELIVERY_TIMEOUT);
        var error = failure.equals("timeout") ? TimeoutException.class : TopicAuthorizationException.class;

        var outcomes = sender.send(List.of(
                recordTo(TOPIC, 1, "a"), recordTo("missing", 2, "k"), recordTo(TOPIC, 3, "b"), recordTo("missing", 4, "j"),
                recordTo(TOPIC, 5, "c")));

        assertThat(outcomes.get(0)).isEqualTo(Outcome.delivered());
        assertThat(outcomes.get(1)).isInstanceOfSatisfying(Outcome.Retry.class,
                r -> assertThat(r.error()).isInstanceOf(error));
        assertThat(outcomes.get(2)).isEqualTo(Outcome.delivered());
        assertThat(outcomes.get(3)).as("the rest of the topic").isInstanceOf(Outcome.NotAttempted.class);
        assertThat(outcomes.get(4)).isEqualTo(Outcome.delivered());
        assertThat(producers.get(0).closed()).as("nothing was appended for the failed record").isFalse();

        var before = Instant.now();
        var next = sender.send(List.of(recordTo("missing", 6, "k"), recordTo(TOPIC, 7, "a")));
        assertThat(next.get(0)).as("the topic cools down").isInstanceOfSatisfying(Outcome.NotAttempted.class,
                n -> assertThat(n.notBefore()).isAfter(before).isBeforeOrEqualTo(Instant.now().plusSeconds(1)));
        assertThat(next.get(1)).isEqualTo(Outcome.delivered());
        assertThat(lookups).as("a cooling topic does not touch the producer").hasValue(1);
        assertThat(producers).as("the producer was pooled and reused").hasSize(1);
    }

    /**
     * One metadata wait per call: after a metadata timeout, topics this producer has not resolved yet are not looked
     * up (each could wait {@code max.block.ms}); topics it resolved before keep flowing.
     */
    @Test
    void oneMetadataWaitPerCall() {
        var lookups = new CopyOnWriteArrayList<String>();
        var sender = sender(() -> new TestProducer(true, r -> null) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(String topic) {
                lookups.add(topic);
                if (topic.startsWith("missing")) {
                    sleep(300);
                    throw new TimeoutException("Topic " + topic + " not present in metadata after 300 ms.");
                }
                return super.partitionsFor(topic);
            }
        }, DELIVERY_TIMEOUT);
        assertThat(sender.send(List.of(recordTo(TOPIC, 1, "a")))).containsExactly(Outcome.delivered());
        lookups.clear();

        var outcomes = sender.send(List.of(recordTo("missing-1", 2, "k"), recordTo("missing-2", 3, "k"),
                recordTo(TOPIC, 4, "b"), recordTo(OTHER, 5, "c")));

        assertThat(outcomes.get(0)).isInstanceOfSatisfying(Outcome.Retry.class,
                r -> assertThat(r.error()).isInstanceOf(TimeoutException.class));
        assertThat(outcomes.get(1)).isEqualTo(Outcome.notAttempted());
        assertThat(outcomes.get(2)).as("resolved by this producer before").isEqualTo(Outcome.delivered());
        assertThat(outcomes.get(3)).as("never resolved by this producer").isEqualTo(Outcome.notAttempted());
        assertThat(lookups).containsExactly("missing-1", TOPIC);
        assertThat(producers).hasSize(1);
    }

    /** What a producer resolved is forgotten after 5 min unused, as Kafka drops the topic from its metadata. */
    @Test
    void resolvedTopicsExpireLikeKafkaMetadata() {
        var clock = new TestClock();
        var lookups = new CopyOnWriteArrayList<String>();
        var sender = sender(() -> new TestProducer(true, r -> null) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(String topic) {
                lookups.add(topic);
                if (topic.startsWith("missing")) {
                    throw new TimeoutException("Topic " + topic + " not present in metadata after 5000 ms.");
                }
                return super.partitionsFor(topic);
            }
        }, DELIVERY_TIMEOUT, clock);
        assertThat(sender.send(List.of(recordTo(TOPIC, 1, "a")))).containsExactly(Outcome.delivered());

        clock.advance(Duration.ofMinutes(5).minusMillis(1));
        assertThat(sender.send(List.of(recordTo("missing-1", 2, "k"), recordTo(TOPIC, 3, "a")))).satisfiesExactly(
                o -> assertThat(o).isInstanceOf(Outcome.Retry.class),
                o -> assertThat(o).as("used 1 ms short of 5 min ago").isEqualTo(Outcome.delivered()));

        clock.advance(Duration.ofMinutes(5));
        assertThat(sender.send(List.of(recordTo("missing-2", 4, "k"), recordTo(TOPIC, 5, "a")))).satisfiesExactly(
                o -> assertThat(o).isInstanceOf(Outcome.Retry.class),
                o -> assertThat(o).as("unused for 5 min: Kafka may have dropped it").isEqualTo(Outcome.notAttempted()));
        assertThat(lookups).containsExactly(TOPIC, "missing-1", TOPIC, "missing-2");
    }

    /** A call that already waited for metadata once does not lease a topic it will not look up. */
    @Test
    void noTopicLeaseAfterTheMetadataWait() {
        var clock = new TestClock();
        var lookups = new ConcurrentHashMap<String, AtomicInteger>();
        var sender = sender(() -> new TestProducer(true, r -> null) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(String topic) {
                lookups.computeIfAbsent(topic, t -> new AtomicInteger()).incrementAndGet();
                if (topic.startsWith("missing")) {
                    throw new TimeoutException("Topic " + topic + " not present in metadata after 5000 ms.");
                }
                return super.partitionsFor(topic);
            }
        }, DELIVERY_TIMEOUT, clock);
        sender.send(List.of(recordTo("missing-1", 1, "k"))); // each topic cools for 1 s
        sender.send(List.of(recordTo("missing-2", 2, "k")));
        clock.advance(Duration.ofSeconds(1));

        var outcomes = sender.send(List.of(recordTo("missing-1", 3, "k"), recordTo("missing-2", 4, "k")));
        assertThat(outcomes.get(0)).isInstanceOf(Outcome.Retry.class); // the probe of missing-1
        assertThat(outcomes.get(1)).isEqualTo(Outcome.notAttempted()); // one metadata wait per call

        sender.send(List.of(recordTo("missing-2", 5, "k")));
        assertThat(lookups.get("missing-2")).as("missing-2 was free to probe, not leased by the call that skipped it")
                .hasValue(2);
    }

    /**
     * A call can take delivery.timeout.ms + 5 s + max.block.ms; the relay gives it batchTimeout. The defaults fit:
     * 20 s + 5 s + 5 s against 30 s.
     */
    @Test
    void warnsOnlyWhenACallCanOutlastBatchTimeout() {
        KafkaOutboxConfig defaults = () -> props("bootstrap.servers", "localhost:9092");
        KafkaOutboxConfig slowMetadata = () -> props("bootstrap.servers", "localhost:9092", "max.block.ms", 10000);
        var module = new KafkaOutboxModule() {};
        var relayDefault = outboxConfig(new OutboxConfig.RelayConfig() {}.batchTimeout());
        try (var err = new CapturedStderr()) {
            module.kafkaOutboxSender(defaults, relayDefault).release();
            module.kafkaOutboxSender(slowMetadata, outboxConfig(Duration.ofSeconds(35))).release();
            module.kafkaOutboxSender(slowMetadata, null).release(); // no relay in the app
            assertThat(err.text()).doesNotContain("batchTimeout");
        }
        try (var err = new CapturedStderr()) {
            module.kafkaOutboxSender(slowMetadata, relayDefault).release();
            assertThat(err.text()).contains("WARN").contains("batchTimeout");
        }
    }

    @Test
    void invalidTopicIsPermanentAndSkipsItsTopic() {
        var sender = sender(() -> new TestProducer(true, r -> null) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(String topic) {
                if (topic.equals("bad/topic")) {
                    throw new InvalidTopicException(Set.of(topic));
                }
                return super.partitionsFor(topic);
            }
        }, DELIVERY_TIMEOUT);

        var outcomes = sender.send(List.of(recordTo("bad/topic", 1, "k"), recordTo("bad/topic", 2, null), recordTo(TOPIC, 3, "k")));

        assertThat(outcomes.get(0)).isInstanceOfSatisfying(Outcome.Permanent.class,
                p -> assertThat(p.error()).isInstanceOf(InvalidTopicException.class));
        assertThat(outcomes.get(1)).isEqualTo(Outcome.notAttempted());
        assertThat(outcomes.get(2)).isEqualTo(Outcome.delivered());
        assertThat(producers.get(0).closed()).isFalse();
    }

    /** A metadata timeout reported through the callback inside {@code send()}: the record never joined a batch. */
    @Test
    void metadataTimeoutInsideSendSkipsItsTopicWithoutClosing() throws Exception {
        var sender = sender(() -> new TestProducer(true,
                r -> new String(r.value(), UTF_8).equals("down") ? new TimeoutException("no metadata") : null),
                DELIVERY_TIMEOUT);

        var outcomes = sender.send(List.of(recordTo(OTHER, 1, "a"), record(2, "a", "down"), record(3, "a"),
                record(4, "b")));

        assertThat(outcomes.get(0)).isEqualTo(Outcome.delivered());
        assertThat(outcomes.get(1)).isInstanceOfSatisfying(Outcome.Retry.class,
                r -> assertThat(r.error()).isInstanceOf(TimeoutException.class));
        assertThat(outcomes.subList(2, 4)).as("never handed to Kafka")
                .allSatisfy(o -> assertThat(o).isInstanceOf(Outcome.NotAttempted.class));
        assertThat(producers.get(0).history()).singleElement().satisfies(r -> assertThat(r.topic()).isEqualTo(OTHER));
        assertThat(producers.get(0).closed()).isFalse();
    }

    /** Buffer exhaustion is not a topic fault: only the record's own key is skipped for the rest of the call. */
    @Test
    void bufferExhaustedSkipsOnlyItsKey() {
        var sender = sender(() -> new TestProducer(true,
                r -> new String(r.value(), UTF_8).equals("full") ? new BufferExhaustedException("no memory") : null),
                DELIVERY_TIMEOUT);

        var outcomes = sender.send(List.of(record(1, "a", "full"), record(2, "a"), record(3, "b"), record(4, null)));

        assertThat(outcomes.get(0)).isInstanceOfSatisfying(Outcome.Retry.class,
                r -> assertThat(r.error()).isInstanceOf(BufferExhaustedException.class));
        assertThat(outcomes.get(1)).isEqualTo(Outcome.notAttempted());
        assertThat(outcomes.subList(2, 4)).containsExactly(Outcome.delivered(), Outcome.delivered());
        assertThat(producers.get(0).closed()).isFalse();
    }

    @Test
    void recordTooLargeIsPermanentWithoutClosing() throws Exception {
        var sender = sender(() -> new TestProducer(false,
                r -> r.value().length > 100 ? new RecordTooLargeException("too large") : null), DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a"), record(2, "a", "x".repeat(101)), record(3, "a")));
        var producer = awaitSent(0, 2);
        producer.completeNext();
        producer.completeNext();

        var outcomes = result.get(5, SECONDS);
        assertThat(outcomes.get(0)).isEqualTo(Outcome.delivered());
        assertThat(outcomes.get(1)).isInstanceOfSatisfying(Outcome.Permanent.class,
                p -> assertThat(p.error()).isInstanceOf(RecordTooLargeException.class));
        assertThat(outcomes.get(2)).as("a dead record does not block its key").isEqualTo(Outcome.delivered());
        assertThat(producer.closed()).isFalse();
    }

    @Test
    void brokerSideRecordTooLargeIsPermanentAndCloses() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a"), record(2, "b")));
        var producer = awaitSent(0, 2);
        producer.errorNext(new RecordTooLargeException("broker says too large")); // batch already left the client

        var outcomes = result.get(5, SECONDS);
        assertThat(outcomes.get(0)).isInstanceOf(Outcome.Permanent.class);
        assertThat(outcomes.get(1)).isInstanceOf(Outcome.Retry.class);
        assertThat(producer.closed()).isTrue();
    }

    @Test
    void invalidRecordIsPermanent() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a")));
        awaitSent(0, 1).errorNext(new InvalidRecordException("bad record"));
        assertThat(result.get(5, SECONDS).get(0)).isInstanceOfSatisfying(Outcome.Permanent.class,
                p -> assertThat(p.error()).isInstanceOf(InvalidRecordException.class));

        var second = sendAsync(sender, List.of(record(2, "a")));
        awaitSent(1, 1).errorNext(new InvalidTopicException("bad topic"));
        assertThat(second.get(5, SECONDS).get(0)).isInstanceOfSatisfying(Outcome.Permanent.class,
                p -> assertThat(p.error()).isInstanceOf(InvalidTopicException.class));
    }

    @Test
    void authorizationErrorIsRetryAndCoolsTheTopic() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, keyOnPartition(0))));
        awaitSent(0, 1).errorNext(new TopicAuthorizationException(Set.of(TOPIC)));

        assertThat(result.get(5, SECONDS).get(0)).isInstanceOfSatisfying(Outcome.Retry.class,
                r -> assertThat(r.error()).isInstanceOf(TopicAuthorizationException.class));

        var next = sendAsync(sender, List.of(record(2, keyOnPartition(1)), recordTo(OTHER, 3, "a")));
        awaitSent(1, 1).completeNext();
        assertThat(next.get(5, SECONDS)).satisfiesExactly(
                o -> assertThat(o).as("another partition of the topic")
                        .isInstanceOfSatisfying(Outcome.NotAttempted.class, n -> assertThat(n.notBefore()).isNotNull()),
                o -> assertThat(o).isEqualTo(Outcome.delivered()));
    }

    @Test
    void coolingPartitionRecordsAreNotAttempted() throws Exception {
        var onP = keyOnPartition(0);
        var onQ = keyOnPartition(1);
        var sender = sender(DELIVERY_TIMEOUT);
        var before = Instant.now();

        var first = sendAsync(sender, List.of(record(1, onP), record(2, onQ)));
        awaitSent(0, 2).errorNext(new TimeoutException("partition p expired"));
        assertThat(first.get(5, SECONDS)).allSatisfy(o -> assertThat(o).isInstanceOf(Outcome.Retry.class));

        var second = sendAsync(sender, List.of(record(3, onP), record(4, onQ)));
        var producer = awaitSent(1, 1);
        producer.completeNext();
        var outcomes = second.get(5, SECONDS);

        assertThat(outcomes.get(0)).isInstanceOfSatisfying(Outcome.NotAttempted.class,
                n -> assertThat(n.notBefore()).isAfter(before).isBeforeOrEqualTo(Instant.now().plusSeconds(1)));
        assertThat(outcomes.get(1)).as("aborted by our close, so q did not cool down").isEqualTo(Outcome.delivered());
        assertThat(producer.history()).singleElement()
                .satisfies(r -> assertThat(new String(r.key(), UTF_8)).isEqualTo(onQ));
    }

    /**
     * I1: an unkeyed record's own failure cools the partition Kafka chose for it (the callback's metadata names it);
     * while it cools, the sender picks the partition of unkeyed records itself, one per call.
     */
    @Test
    void unkeyedRecordsAvoidACoolingPartition() throws Exception {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var requested = new CopyOnWriteArrayList<Optional<Integer>>(); // ProducerRecord.partition() as the sender set it
        var sender = sender(() -> new MockProducer<>(CLUSTER, true, new ByteArraySerializer(), new ByteArraySerializer()) {
            private int next; // stands in for Kafka's partitioner

            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> r, Callback callback) {
                requested.add(Optional.ofNullable(r.partition()));
                int partition = r.partition() != null ? r.partition() : next++ % PARTITIONS;
                if (partition != 0) {
                    return super.send(new ProducerRecord<>(r.topic(), partition, r.key(), r.value(), r.headers()),
                            callback);
                }
                var tp = new TopicPartition(r.topic(), 0);
                scheduler.schedule(() -> callback.onCompletion(new RecordMetadata(tp, -1, -1, -1, -1, -1),
                        new NotEnoughReplicasException("partition 0 rejects writes")), 100, MILLISECONDS);
                return new CompletableFuture<>();
            }
        }, DELIVERY_TIMEOUT);
        var unkeyed = IntStream.range(0, 8).mapToObj(i -> record(i, null)).toList();
        try {
            var first = sender.send(unkeyed);
            assertThat(requested).as("nothing cools: Kafka's partitioner").containsOnly(Optional.empty());
            assertThat(first).filteredOn(o -> o instanceof Outcome.Retry).isNotEmpty();

            requested.clear();
            assertThat(sender.send(unkeyed)).containsOnly(Outcome.delivered());
            assertThat(requested).hasSize(8).doesNotContain(Optional.of(0), Optional.empty());
            assertThat(Set.copyOf(requested)).as("one partition per call").hasSize(1);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void unkeyedRecordsWaitWhileEveryPartitionCools() throws Exception {
        var clock = new TestClock();
        var sender = sender(() -> new TestProducer(false, r -> null), DELIVERY_TIMEOUT, clock);
        for (int p = 0; p < PARTITIONS; p++) {
            var call = sendAsync(sender, List.of(record(p, keyOnPartition(p))));
            awaitSent(p, 1).errorNext(new TimeoutException("partition " + p + " expired"));
            call.get(5, SECONDS);
        }

        assertThat(sender.send(List.of(record(9, null)))).singleElement()
                .isInstanceOfSatisfying(Outcome.NotAttempted.class,
                        n -> assertThat(n.notBefore()).isEqualTo(clock.instant().plusSeconds(1)));
        assertThat(producers.get(PARTITIONS).history()).isEmpty();
    }

    @Test
    void messageIdHeaderAdded() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var messageId = UUID.randomUUID();
        var headers = List.of(new OutboxHeader("traceparent", "00-trace".getBytes(UTF_8)), new OutboxHeader("x", null));
        var record = new OutboxRecord(1, messageId, TOPIC, "k".getBytes(UTF_8), "v".getBytes(UTF_8), headers,
                Instant.now(), 0);

        var result = sendAsync(sender, List.of(record));
        var producer = awaitSent(0, 1);
        producer.completeNext();
        assertThat(result.get(5, SECONDS)).containsExactly(Outcome.delivered());

        var sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo(TOPIC);
        assertThat(sent.partition()).isNull();
        assertThat(sent.timestamp()).isNull();
        assertThat(sent.key()).isEqualTo("k".getBytes(UTF_8));
        assertThat(sent.value()).isEqualTo("v".getBytes(UTF_8));
        assertThat(sent.headers().toArray()).extracting(Header::key)
                .containsExactly("traceparent", "x", "outbox-message-id");
        assertThat(sent.headers().headers("x").iterator().next().value()).isNull();
        assertThat(sent.headers().lastHeader("outbox-message-id").value())
                .isEqualTo(messageId.toString().getBytes(UTF_8));
        assertThat(record.headers()).isEqualTo(headers);
    }

    @Test
    void inFlightAboveFiveRejected() {
        assertThatThrownBy(() -> new KafkaOutboxSender(() -> props(
                "bootstrap.servers", "localhost:9092", "max.in.flight.requests.per.connection", 6)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max.in.flight.requests.per.connection");
    }

    /** With push on, kafka-clients 3.9 logs an ERROR from the I/O thread after every {@code close(Duration.ZERO)}. */
    @Test
    void metricsPushOffUnlessSet() {
        assertThat(KafkaOutboxSender.producerConfig(props("bootstrap.servers", "localhost:9092"))
                .getBoolean(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG)).isFalse();
        assertThat(KafkaOutboxSender.producerConfig(props("bootstrap.servers", "localhost:9092",
                ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, "true"))
                .getBoolean(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG)).isTrue();
    }

    @Test
    void producerConfigIsForcedAndValidated() {
        var user = props("bootstrap.servers", "localhost:9092", "key.serializer", StringSerializer.class.getName(),
                "acks", "-1", "enable.idempotence", "true", "max.in.flight.requests.per.connection", 5);
        var config = KafkaOutboxSender.producerConfig(user);
        assertThat(config.originals())
                .containsEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName())
                .containsEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName())
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000")
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000")
                .containsEntry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000")
                .containsEntry("bootstrap.servers", "localhost:9092");
        assertThat(user).containsEntry("key.serializer", StringSerializer.class.getName()); // not mutated
        assertThatCode(() -> new KafkaProducer<byte[], byte[]>(config.originals()).close(Duration.ZERO))
                .as("Kafka accepts the defaults (delivery.timeout.ms >= linger.ms + request.timeout.ms)")
                .doesNotThrowAnyException();

        var explicit = KafkaOutboxSender.producerConfig(props("bootstrap.servers", "localhost:9092",
                "delivery.timeout.ms", 60000, "max.block.ms", 3000));
        assertThat(explicit.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).isEqualTo(60000);
        assertThat(explicit.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo(30000); // Kafka's default
        assertThat(explicit.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG)).isEqualTo(3000);

        for (var bad : List.of(List.of("enable.idempotence", "false"), List.of("enable.idempotence", false),
                List.of("acks", "1"), List.of("acks", 0), List.of("transactional.id", "tx"),
                List.of("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner"),
                List.of("partitioner.ignore.keys", "true"),
                List.of("request.timeout.ms", 30000))) { // the 20 s default delivery timeout cannot cover it
            assertThatThrownBy(() -> KafkaOutboxSender.producerConfig(
                    props("bootstrap.servers", "localhost:9092", bad.get(0), bad.get(1))))
                    .as("%s", bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining((String) bad.get(0));
        }
        assertThatThrownBy(() -> KafkaOutboxSender.producerConfig(props("acks", "all")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bootstrap.servers");
        assertThatThrownBy(() -> new KafkaOutboxSender(() -> props("bootstrap.servers", "localhost:9092", "retries", 0)))
                .as("Kafka's own validation runs at construction")
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("retries");
    }

    @Test
    void overallTimeoutClosesAndRetries() throws Exception {
        var sender = sender(() -> new TestProducer(false, r -> {
            if (new String(r.key(), UTF_8).equals("a")) {
                sleep(1500); // blocking in send (metadata) counts against the same deadline
            }
            return null;
        }), Duration.ofMillis(200));
        var started = System.nanoTime();
        var result = sendAsync(sender, List.of(record(1, "a"), record(2, "b")));
        var producer = awaitSent(0, 2);

        var outcomes = result.get(7, SECONDS);
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .as("delivery.timeout.ms + 5 s from entry").isBetween(Duration.ofMillis(5200), Duration.ofMillis(6400));
        assertThat(outcomes).allSatisfy(o -> assertThat(o).isInstanceOf(Outcome.Retry.class));
        assertThat(producer.closed()).isTrue();
    }

    /** M1: handing records over honours the deadline too: what is left then is not attempted. */
    @Test
    void handOverStopsAtTheDeadline() {
        var sender = sender(() -> new TestProducer(true, r -> {
            sleep(300); // e.g. waiting for buffer space
            return null;
        }), Duration.ofMillis(200));
        var started = System.nanoTime();

        var outcomes = sender.send(IntStream.range(0, 30).mapToObj(i -> record(i, "k" + i)).toList());

        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .as("delivery.timeout.ms + 5 s, plus the send in progress").isLessThan(Duration.ofMillis(6500));
        assertThat(outcomes.subList(0, 15)).containsOnly(Outcome.delivered());
        assertThat(outcomes.get(29)).isEqualTo(Outcome.notAttempted());
        assertThat(outcomes).containsOnly(Outcome.delivered(), Outcome.notAttempted());
    }

    /** A metadata lookup that ends past the deadline hands nothing over: send() could block max.block.ms again. */
    @Test
    void noSendAfterALookupThatEndsPastTheDeadline() {
        var sender = sender(() -> new TestProducer(true, r -> null) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(String topic) {
                if (topic.equals("slow")) {
                    sleep(5300); // resolves, but after the 5.2 s deadline
                    return super.partitionsFor(TOPIC);
                }
                return super.partitionsFor(topic);
            }
        }, Duration.ofMillis(200));

        var outcomes = sender.send(List.of(recordTo("slow", 1, "k"), record(2, "a")));

        assertThat(outcomes).containsExactly(Outcome.notAttempted(), Outcome.notAttempted());
        assertThat(producers.get(0).history()).isEmpty();
        assertThat(producers.get(0).closed()).isFalse();
    }

    /**
     * M3: only partition-health errors from the I/O thread cool a partition; a healthy batch-mate of an invalid record
     * gets a generic {@link KafkaException}.
     */
    @Test
    void batchMateOfAnInvalidRecordDoesNotCoolItsPartition() throws Exception {
        var onP = keyOnPartition(0);
        var sender = sender(DELIVERY_TIMEOUT);
        var first = sendAsync(sender, List.of(record(1, onP)));
        awaitSent(0, 1).errorNext(new KafkaException(
                "Failed to append record because it was part of a batch which had one more more invalid records"));
        assertThat(first.get(5, SECONDS).get(0)).isInstanceOf(Outcome.Retry.class);

        var next = sendAsync(sender, List.of(record(2, onP)));
        awaitSent(1, 1).completeNext();
        assertThat(next.get(5, SECONDS)).containsExactly(Outcome.delivered());
    }

    @Test
    void interruptWhileSendingLeavesTheRestUnsent() {
        var sender = sender(() -> new TestProducer(false, r -> {
            if (new String(r.key(), UTF_8).equals("b")) {
                throw new InterruptException("waiting for metadata"); // sets the flag, like KafkaProducer
            }
            return null;
        }), DELIVERY_TIMEOUT);

        var outcomes = sender.send(List.of(record(1, "a"), record(2, "b"), record(3, "c")));
        assertThat(Thread.interrupted()).as("flag kept").isTrue();

        assertThat(outcomes.get(0)).as("in flight, aborted by our close").isInstanceOf(Outcome.Retry.class);
        assertThat(outcomes.subList(1, 3)).containsExactly(Outcome.notAttempted(), Outcome.notAttempted());
        assertThat(producers.get(0).closed()).isTrue();
    }

    /**
     * The relay interrupts a slow call and waits for its outcomes: acked records stay Delivered, records in flight
     * are Retry (producer closed), the record blocked in {@code send()} and the rest are NotAttempted.
     */
    @Test
    void interruptReturnsRealOutcomesPromptly() throws Exception {
        var sender = sender(() -> new TestProducer(false, r -> null) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                if (new String(record.key(), UTF_8).equals("c")) {
                    try { // like KafkaProducer waiting for buffer space
                        Thread.sleep(10_000);
                    } catch (InterruptedException e) {
                        throw new InterruptException(e);
                    }
                }
                return super.send(record, callback);
            }
        }, DELIVERY_TIMEOUT);
        var result = new CompletableFuture<List<Outcome>>();
        var flagKept = new AtomicBoolean();
        var thread = new Thread(() -> {
            var outcomes = sender.send(List.of(record(1, "a"), record(2, "b"), record(3, "c"), record(4, "d")));
            flagKept.set(Thread.currentThread().isInterrupted());
            result.complete(outcomes);
        });
        thread.start();
        var producer = awaitSent(0, 2);
        producer.completeNext();
        thread.interrupt();

        assertThat(result.get(2, SECONDS)).satisfiesExactly(
                o -> assertThat(o).isEqualTo(Outcome.delivered()),
                o -> assertThat(o).isInstanceOf(Outcome.Retry.class),
                o -> assertThat(o).isEqualTo(Outcome.notAttempted()),
                o -> assertThat(o).isEqualTo(Outcome.notAttempted()));
        assertThat(flagKept).isTrue();
        assertThat(producer.closed()).isTrue();
    }

    @Test
    void interruptOnEntryKeepsTheHealthyProducer() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var first = sendAsync(sender, List.of(record(1, "a")));
        awaitSent(0, 1).completeNext();
        first.get(5, SECONDS);

        Thread.currentThread().interrupt();
        var outcomes = sender.send(List.of(record(2, "a"), record(3, null)));
        assertThat(Thread.interrupted()).as("flag kept").isTrue();

        assertThat(outcomes).containsExactly(Outcome.notAttempted(), Outcome.notAttempted());
        assertThat(producers.get(0).closed()).isFalse();
        var next = sendAsync(sender, List.of(record(4, "a")));
        awaitSent(0, 2).completeNext();
        assertThat(next.get(5, SECONDS)).containsExactly(Outcome.delivered());
        assertThat(producers).as("the producer went back to the pool").hasSize(1);
    }

    @Test
    void interruptClosesRetriesAndKeepsTheFlag() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = new CompletableFuture<List<Outcome>>();
        var flagKept = new AtomicBoolean();
        var thread = new Thread(() -> {
            var outcomes = sender.send(List.of(record(1, "a"), record(2, "b")));
            flagKept.set(Thread.currentThread().isInterrupted());
            result.complete(outcomes);
        });
        thread.start();
        var producer = awaitSent(0, 2);
        thread.interrupt();

        assertThat(result.get(5, SECONDS)).allSatisfy(o -> assertThat(o).isInstanceOf(Outcome.Retry.class));
        assertThat(flagKept).isTrue();
        assertThat(producer.closed()).isTrue();

        var next = sendAsync(sender, List.of(record(3, "a"), record(4, "b")));
        var fresh = awaitSent(1, 2);
        fresh.completeNext();
        fresh.completeNext();
        assertThat(next.get(5, SECONDS)).as("records aborted by our close do not cool their partitions")
                .containsExactly(Outcome.delivered(), Outcome.delivered());
    }

    @Test
    void cooldownDecisionIsStableWithinACall() throws Exception {
        var onP = keyOnPartition(0);
        var onQ = keyOnPartition(1);
        var otherWorker = new CompletableFuture<List<Outcome>>();
        var holder = new KafkaOutboxSender[1];
        // producer 0 cools p, producer 1 runs the call under test, producer 2 (auto-completing) the other worker
        holder[0] = sender(() -> new TestProducer(producers.size() == 2, r -> {
            if (producers.size() == 2 && onQ.equals(new String(r.key(), UTF_8))) {
                sleep(1100); // p's cooldown ends while the call under test is still sending
                otherWorker.complete(holder[0].send(List.of(record(9, onP)))); // success on p resets its cooldown
            }
            return null;
        }), DELIVERY_TIMEOUT);
        var sender = holder[0];
        var first = sendAsync(sender, List.of(record(1, onP)));
        awaitSent(0, 1).errorNext(new TimeoutException("expired"));
        first.get(5, SECONDS);

        var result = sendAsync(sender, List.of(record(2, onP), record(3, onQ), record(4, onP)));
        awaitSent(1, 1).completeNext();
        var outcomes = result.get(5, SECONDS);

        assertThat(otherWorker.get(0, SECONDS)).containsExactly(Outcome.delivered());
        assertThat(outcomes.get(0)).isInstanceOf(Outcome.NotAttempted.class);
        assertThat(outcomes.get(1)).isEqualTo(Outcome.delivered());
        assertThat(outcomes.get(2)).as("same key as the skipped record 2").isInstanceOf(Outcome.NotAttempted.class);
    }

    /**
     * I2: when a partition's cooldown ends, one call probes it; the others still skip it, but retry within a second,
     * so a successful probe frees the partition quickly.
     */
    @Test
    void expiredCooldownAdmitsOneProbe() throws Exception {
        var clock = new TestClock();
        var onP = keyOnPartition(0);
        var sender = sender(() -> new TestProducer(false, r -> null), DELIVERY_TIMEOUT, clock);
        var first = sendAsync(sender, List.of(record(1, onP)));
        awaitSent(0, 1).errorNext(new TimeoutException("partition p expired"));
        first.get(5, SECONDS);
        clock.advance(Duration.ofSeconds(1)); // the 1 s cooldown ends

        var probe = sendAsync(sender, List.of(record(2, onP)));
        var prober = awaitSent(1, 1); // the probe holds the lease and waits for its ack
        var skipped = sender.send(List.of(record(3, onP)));

        assertThat(skipped).singleElement().isInstanceOfSatisfying(Outcome.NotAttempted.class,
                n -> assertThat(n.notBefore()).as("soon, not at the lease end").isEqualTo(clock.instant().plusSeconds(1)));
        assertThat(producers.get(2).history()).as("the other call hands nothing over").isEmpty();

        prober.completeNext();
        assertThat(probe.get(5, SECONDS)).containsExactly(Outcome.delivered());
        var next = sendAsync(sender, List.of(record(4, onP)));
        awaitSent(1, 2).completeNext(); // the prober's producer went back to the pool last, so it is first
        assertThat(next.get(5, SECONDS)).as("the probe's success ended the cooldown").containsExactly(Outcome.delivered());
    }

    /**
     * Only the four types KafkaProducer reports before the append count as refused. Anything else on the caller
     * thread may have been appended and closes the producer as before: through the callback (e.g.
     * ProducerFencedException, which {@code maybeAddPartition} raises after the append) or thrown out of
     * {@code send()}.
     */
    @ParameterizedTest
    @ValueSource(strings = {"callback", "thrown"})
    void otherCallerThreadErrorsStillClose(String how) {
        var sender = sender(() -> new TestProducer(false, r -> null) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                if (!new String(record.value(), UTF_8).equals("after-append")) {
                    return super.send(record, callback);
                }
                if (how.equals("thrown")) {
                    throw new KafkaException("Cannot execute transactional method because we are in an error state");
                }
                var e = new ProducerFencedException("fenced");
                callback.onCompletion(null, e);
                return CompletableFuture.failedFuture(e);
            }
        }, DELIVERY_TIMEOUT);

        var outcomes = sender.send(List.of(record(1, "a"), record(2, "b", "after-append"), recordTo(OTHER, 3, "c")));

        assertThat(outcomes.get(0)).as("in flight, aborted by the close").isInstanceOf(Outcome.Retry.class);
        assertThat(outcomes.get(1)).isInstanceOfSatisfying(Outcome.Retry.class, r -> assertThat(r.error())
                .isInstanceOf(how.equals("thrown") ? KafkaException.class : ProducerFencedException.class));
        assertThat(outcomes.get(2)).isEqualTo(Outcome.notAttempted());
        assertThat(producers.get(0).closed()).isTrue();
    }

    @Test
    void interruptWhileWaitingForMetadata() throws Exception {
        var waiting = new CountDownLatch(1);
        var sender = sender(() -> new TestProducer(false, r -> null) {
            @Override
            public List<PartitionInfo> partitionsFor(String topic) {
                if (!topic.equals("slow")) {
                    return super.partitionsFor(topic);
                }
                waiting.countDown();
                try { // like KafkaProducer waiting for metadata
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    throw new InterruptException(e);
                }
                throw new AssertionError("not interrupted");
            }
        }, DELIVERY_TIMEOUT);
        var result = new CompletableFuture<List<Outcome>>();
        var flagKept = new AtomicBoolean();
        var thread = new Thread(() -> {
            var outcomes = sender.send(List.of(record(1, "a"), recordTo("slow", 2, "k"), record(3, "b")));
            flagKept.set(Thread.currentThread().isInterrupted());
            result.complete(outcomes);
        });
        thread.start();
        assertThat(waiting.await(5, SECONDS)).isTrue();
        thread.interrupt();

        assertThat(result.get(2, SECONDS)).satisfiesExactly(
                o -> assertThat(o).as("in flight, aborted by the close").isInstanceOf(Outcome.Retry.class),
                o -> assertThat(o).isEqualTo(Outcome.notAttempted()),
                o -> assertThat(o).isEqualTo(Outcome.notAttempted()));
        assertThat(flagKept).isTrue();
        assertThat(producers.get(0).closed()).isTrue();
    }

    @Test
    void expiredTopicCooldownAdmitsOneProbe() throws Exception {
        var lookups = new AtomicInteger();
        var sender = sender(() -> new TestProducer(true, r -> null) {
            @Override
            public synchronized List<PartitionInfo> partitionsFor(String topic) {
                if (!topic.equals("missing")) {
                    return super.partitionsFor(topic);
                }
                lookups.incrementAndGet();
                sleep(300);
                throw new TimeoutException("Topic missing not present in metadata after 300 ms.");
            }
        }, DELIVERY_TIMEOUT);
        sender.send(List.of(recordTo("missing", 1, "k")));
        Thread.sleep(1100); // the 1 s cooldown ends

        var a = sendAsync(sender, List.of(recordTo("missing", 2, "k")));
        var b = sendAsync(sender, List.of(recordTo("missing", 3, "k")));

        assertThat(List.of(a.get(5, SECONDS).get(0), b.get(5, SECONDS).get(0))).satisfiesExactlyInAnyOrder(
                o -> assertThat(o).isInstanceOf(Outcome.Retry.class),
                o -> assertThat(o).isInstanceOf(Outcome.NotAttempted.class));
        assertThat(lookups).as("the first failure and one probe").hasValue(2);
    }

    @Test
    void successOnPartitionResetsItsCooldown() throws Exception {
        var onP = keyOnPartition(0);
        var sender = sender(DELIVERY_TIMEOUT);
        var first = sendAsync(sender, List.of(record(1, onP)));
        awaitSent(0, 1).errorNext(new TimeoutException("expired"));
        first.get(5, SECONDS);
        Thread.sleep(1100); // first cooldown (1 s) ends

        var second = sendAsync(sender, List.of(record(2, onP), record(3, onP)));
        var producer = awaitSent(1, 2);
        producer.completeNext();
        producer.errorNext(new TimeoutException("expired again"));
        assertThat(second.get(5, SECONDS).get(0)).isEqualTo(Outcome.delivered());

        var third = sender.send(List.of(record(4, onP)));
        assertThat(third.get(0)).as("the delivery reset the doubling: 1 s, not 2 s")
                .isInstanceOfSatisfying(Outcome.NotAttempted.class,
                        n -> assertThat(n.notBefore()).isBeforeOrEqualTo(Instant.now().plusSeconds(1)));
    }

    @Test
    void handlerAdapterThrowsOnFailure() throws Exception {
        var sender = sender(() -> new TestProducer(true, r -> switch (new String(r.value(), UTF_8)) {
            case "big" -> new RecordTooLargeException("too large");
            case "down" -> new TimeoutException("no metadata");
            default -> null;
        }), DELIVERY_TIMEOUT);
        var handler = sender.handler();

        assertThatCode(() -> handler.handle(record(1, "a"))).doesNotThrowAnyException();
        assertThatThrownBy(() -> handler.handle(record(2, "a", "big")))
                .isInstanceOf(OutboxPermanentException.class)
                .hasCauseInstanceOf(RecordTooLargeException.class);
        assertThatThrownBy(() -> handler.handle(record(3, "a", "down")))
                .isNotInstanceOf(OutboxPermanentException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        assertThatThrownBy(() -> handler.handle(record(4, "a")))
                .as("the topic cools down after a metadata timeout")
                .isInstanceOf(KafkaException.class)
                .isNotInstanceOf(OutboxPermanentException.class)
                .hasMessageContaining("cools down until");
        assertThat(handler.handleBatch(List.of(record(5, null), recordTo(OTHER, 6, "a")))).satisfiesExactly(
                o -> assertThat(o).isInstanceOf(Outcome.NotAttempted.class),
                o -> assertThat(o).isEqualTo(Outcome.delivered()));
    }

    @Test
    void releaseClosesPooledProducers() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a")));
        var producer = awaitSent(0, 1);
        producer.completeNext();
        result.get(5, SECONDS);

        sender.release();
        assertThat(producer.closed()).isTrue();
    }

    @Test
    void producerReturnedAfterReleaseIsClosed() throws Exception {
        var sender = sender(DELIVERY_TIMEOUT);
        var result = sendAsync(sender, List.of(record(1, "a")));
        var producer = awaitSent(0, 1);
        sender.release(); // the call in flight still holds the producer
        producer.completeNext();

        assertThat(result.get(5, SECONDS)).containsExactly(Outcome.delivered());
        assertThat(producer.closed()).isTrue();
    }

    /** A clock that moves only when told. */
    private static final class TestClock extends Clock {
        private volatile Instant now = Instant.parse("2026-10-26T00:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * MockProducer closer to KafkaProducer: {@code failure} fails matching records the way client-side errors are
     * reported (callback inside {@code send}), and {@code close} aborts every incomplete send through its callback.
     */
    private static class TestProducer extends MockProducer<byte[], byte[]> {
        private final Function<ProducerRecord<byte[], byte[]>, RuntimeException> failure;

        TestProducer(boolean autoComplete, Function<ProducerRecord<byte[], byte[]>, RuntimeException> failure) {
            super(CLUSTER, autoComplete, new ByteArraySerializer(), new ByteArraySerializer());
            this.failure = failure;
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            var e = failure.apply(record);
            if (e == null) {
                return super.send(record, callback);
            }
            callback.onCompletion(null, e);
            return CompletableFuture.failedFuture(e);
        }

        @Override
        public synchronized void close(Duration timeout) {
            super.close(timeout);
            while (errorNext(new KafkaException("Producer is closed forcefully."))) {
                // abort the next incomplete send
            }
        }
    }

    private KafkaOutboxSender sender(Duration deliveryTimeout) {
        return sender(() -> new TestProducer(false, r -> null), deliveryTimeout);
    }

    private KafkaOutboxSender sender(Supplier<MockProducer<byte[], byte[]>> make, Duration deliveryTimeout) {
        return sender(make, deliveryTimeout, Clock.systemUTC());
    }

    private KafkaOutboxSender sender(Supplier<MockProducer<byte[], byte[]>> make, Duration deliveryTimeout,
            Clock clock) {
        return new KafkaOutboxSender(() -> {
            var producer = make.get();
            producers.add(producer);
            return producer;
        }, deliveryTimeout, clock);
    }

    private Future<List<Outcome>> sendAsync(KafkaOutboxSender sender, List<OutboxRecord> records) {
        return executor.submit(() -> sender.send(records));
    }

    private MockProducer<byte[], byte[]> awaitSent(int producer, int records) throws InterruptedException {
        var deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (producers.size() <= producer || producers.get(producer).history().size() < records) {
            assertThat(System.nanoTime()).as("producer %d sent %d records", producer, records).isLessThan(deadline);
            Thread.sleep(5);
        }
        return producers.get(producer);
    }

    private static String keyOnPartition(int partition) {
        for (int i = 0; ; i++) {
            var key = "k" + i;
            if (BuiltInPartitioner.partitionForKey(key.getBytes(UTF_8), PARTITIONS) == partition) {
                return key;
            }
        }
    }

    private static OutboxRecord record(long id, @Nullable String key) {
        return record(id, key, "payload");
    }

    private static OutboxRecord record(long id, @Nullable String key, String payload) {
        return record(id, TOPIC, key, payload);
    }

    private static OutboxRecord recordTo(String topic, long id, @Nullable String key) {
        return record(id, topic, key, "payload");
    }

    private static OutboxRecord record(long id, String topic, @Nullable String key, String payload) {
        return new OutboxRecord(id, UUID.randomUUID(), topic, key == null ? null : key.getBytes(UTF_8),
                payload.getBytes(UTF_8), List.of(), Instant.now(), 0);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static OutboxConfig outboxConfig(Duration batchTimeout) {
        return new OutboxConfig() {
            @Override
            public RelayConfig relay() {
                return new RelayConfig() {
                    @Override
                    public Duration batchTimeout() {
                        return batchTimeout;
                    }
                };
            }
        };
    }

    private static Properties props(Object... keyValues) {
        var p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            p.put(keyValues[i], keyValues[i + 1]);
        }
        return p;
    }
}
