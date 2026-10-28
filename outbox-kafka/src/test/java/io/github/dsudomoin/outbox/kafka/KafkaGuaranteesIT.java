package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The delivery guarantees end to end on the framework-free path: {@link OutboxRuntime} over {@link JdbcTransactions}
 * and a plain DataSource, with {@link KafkaOutboxSender}'s handler, against a real Postgres and a real broker. Every
 * test reads the topic and checks that no committed message is missing and that per key the first occurrences follow
 * send order.
 */
class KafkaGuaranteesIT {

    private static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(60);

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> closing = new ArrayList<>(); // senders and runtimes, closed last first
    /** Per key, the ids of the committed messages in send order. */
    private final Map<String, List<UUID>> committed = new ConcurrentHashMap<>();

    @AfterEach
    void close() throws Exception {
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
    }

    @Test
    void rolledBackMessagesNeverReachKafka() throws Exception {
        var topic = TestKafka.createTopic(3);
        var runtime = start(config(), new Handler());
        var rolledBack = new AtomicReference<UUID>();

        assertThatThrownBy(() -> transactions.inTx(c -> {
            rolledBack.set(runtime.outbox().send(OutboxMessage.of(topic, "k", bytes("m1"))));
            throw new IllegalStateException("the business transaction fails");
        })).hasMessage("the business transaction fails");
        sendAll(runtime, List.of(OutboxMessage.of(topic, "k", bytes("m2")))); // the marker

        var records = consumeCommitted(topic, r -> true);
        assertThat(records).extracting(KafkaOutboxHeaders::messageId).doesNotContain(rolledBack.get());
    }

    /**
     * The step's backend dies after the sender returned, as in a crash between the Kafka send and the cursor commit:
     * the step rolls back, and its records are read and sent again.
     */
    @Test
    void crashAfterTheKafkaSendBeforeTheCursorCommitRedelivers() throws Exception {
        var topic = TestKafka.createTopic(3);
        var handler = new Handler();
        var runtime = start(config(), handler);
        var crashed = new AtomicBoolean();
        var terminated = new AtomicInteger();
        handler.afterSend = records -> records.stream()
                .filter(r -> Arrays.equals(r.payload(), bytes("x")))
                .findFirst()
                .filter(r -> crashed.compareAndSet(false, true))
                .ifPresent(r -> terminated.set(terminateStep(runtime, r.shard())));

        var ids = sendAll(runtime, List.of(
                OutboxMessage.of(topic, "k", bytes("k#1")),
                OutboxMessage.of(topic, "k", bytes("k#2")),
                OutboxMessage.of(topic, "k", bytes("x")),
                OutboxMessage.of(topic, "k", bytes("k#4"))));
        var x = ids.get(2);

        var records = consumeCommitted(topic, r -> r.stream().filter(e -> x.equals(KafkaOutboxHeaders.messageId(e)))
                .count() >= 2);
        assertThat(terminated).as("the step's backend").hasValue(1);
        assertThat(records).filteredOn(r -> x.equals(KafkaOutboxHeaders.messageId(r)))
                .extracting(r -> new String(r.value(), UTF_8))
                .hasSizeGreaterThanOrEqualTo(2)
                .containsOnly("x");
    }

    @Test
    void aFailingKeyIsRetriedWhileOtherKeysFlow() throws Exception {
        var topic = TestKafka.createTopic(1); // one partition: offsets order every arrival
        var handler = new Handler();
        var badAttempts = new AtomicInteger();
        handler.fail = r -> key(r).equals("bad") && badAttempts.getAndIncrement() < 2
                ? Outcome.retry(new IOException("bad is down"))
                : null;
        // one shard: the other keys come after bad on its cursor, so they get past it, not merely beside it
        var runtime = start(config().shards(1), handler);

        var ids = sendAll(runtime, List.of(
                OutboxMessage.of(topic, "bad", bytes("bad#1")),
                OutboxMessage.of(topic, "bad", bytes("bad#2")),
                OutboxMessage.of(topic, "bad", bytes("bad#3")),
                OutboxMessage.of(topic, "a", bytes("a#1")),
                OutboxMessage.of(topic, "b", bytes("b#1")),
                OutboxMessage.of(topic, "a", bytes("a#2"))));

        var received = byMessageId(consumeCommitted(topic, r -> true));
        var bad = ids.subList(0, 3).stream().map(id -> received.get(id).offset()).toList();
        var others = ids.subList(3, ids.size()).stream().map(id -> received.get(id).offset()).toList();
        assertThat(Collections.min(bad)).as("bad arrives after the others").isGreaterThan(Collections.max(others));
    }

    @Test
    void twoRuntimesShareOneDatabase() throws Exception {
        int writers = 4;
        int keysPerWriter = 10;
        int messagesPerKey = 250;
        var topic = TestKafka.createTopic(6);
        var config = config();
        var handlers = List.of(new Handler(), new Handler());
        var runtimes = List.of(start(config, handlers.get(0)), start(sameTables(config), handlers.get(1)));

        var pool = Executors.newFixedThreadPool(writers);
        try {
            var writing = IntStream.range(0, writers).mapToObj(w -> pool.submit(() -> {
                var keys = IntStream.range(0, keysPerWriter).mapToObj(k -> "key-" + w + "-" + k).toList();
                for (int seq = 0; seq < messagesPerKey; seq++) {
                    int s = seq;
                    sendAll(runtimes.get((w + seq) % 2),
                            keys.stream().map(k -> OutboxMessage.of(topic, k, bytes(k + ":" + s))).toList());
                }
                return null;
            })).toList();
            for (var w : writing) {
                w.get(2, TimeUnit.MINUTES);
            }
        } finally {
            pool.shutdownNow();
        }

        var records = consumeCommitted(topic, r -> true);
        assertThat(records).as("each message once").hasSize(writers * keysPerWriter * messagesPerKey);
        assertThat(handlers).allSatisfy(h -> assertThat(h.deliveringCalls).hasPositiveValue());
    }

    /**
     * The message is larger than its topic's {@code max.message.bytes}: the broker refuses it for good, it dies and
     * the dead-letter hook publishes it to its dead-letter topic, which has Kafka's default limit. It is larger than
     * the producer's {@code batch.size} (16 KiB) too, so it travels in a producer batch of its own.
     */
    @Test
    void aPoisonMessageGoesToTheDeadLetterTopicAndItsKeyContinues() throws Exception {
        var topic = TestKafka.createTopic("t-" + UUID.randomUUID(), 1, Map.of("max.message.bytes", "1024"));
        var dlq = TestKafka.createTopic(topic + ".dlq", 1, Map.of());
        var sender = sender();
        var runtime = start(OutboxRuntime.builder(config(), transactions, new Handler(sender))
                .deadLetterHandler(new KafkaDeadLetterHandler(sender, "{topic}.dlq")));

        var ids = sendAll(runtime, List.of(
                OutboxMessage.of(topic, "k", bytes("before")),
                OutboxMessage.of(topic, "k", new byte[20 * 1024]),
                OutboxMessage.of(topic, "k", bytes("after"))));
        var poison = ids.get(1);
        committed.get("k").remove(poison); // its place is the dead-letter topic

        var main = consumeCommitted(topic, r -> true);
        assertThat(main).extracting(KafkaOutboxHeaders::messageId).doesNotContain(poison);
        var dead = TestKafka.consumeUntil(dlq, CONSUME_TIMEOUT, r -> !r.isEmpty());
        assertThat(dead).extracting(KafkaOutboxHeaders::messageId).containsOnly(poison);
        // the discard commits after the publish
        await(Duration.ofSeconds(10), () -> runtime.admin().countDeadLetters(DeadLetterFilter.any()) == 0);
    }

    @Test
    void aStoppedRuntimeLeavesItsBacklogToTheNext() throws Exception {
        var topic = TestKafka.createTopic(3);
        // calls of at most 10 records, 20 ms each: 500 messages keep A busy for half a second at least
        var config = config().batchSize(10);
        var slow = new Handler();
        slow.afterSend = records -> {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        var a = start(config, slow);
        sendAll(a, IntStream.range(0, 500)
                .mapToObj(i -> OutboxMessage.of(topic, "k" + i % 10, bytes("m" + i))).toList());

        await(Duration.ofSeconds(30), () -> slow.delivered.get() >= 50); // acknowledged with acks=all: in the topic
        a.close();
        var next = new Handler();
        start(sameTables(config), next);

        var records = consumeCommitted(topic, r -> true);
        assertThat(next.delivered).as("the backlog A left").hasPositiveValue();
        assertThat(records.size() - 500).as("duplicates")
                .isLessThanOrEqualTo(config.batchSize() * config.workers());
    }

    /**
     * {@link KafkaOutboxSender#handler()} for the relay: a record {@link #fail} answers for gets that outcome instead
     * of being sent, and the records of its key after it in the call are not attempted, as the handler contract asks;
     * {@link #afterSend} runs once the sender returned, before the relay sees the outcomes.
     */
    private final class Handler implements OutboxHandler {

        private final KafkaOutboxHandler kafka;
        volatile Function<OutboxRecord, Outcome> fail = r -> null; // null: send it
        volatile Consumer<List<OutboxRecord>> afterSend = records -> {};
        /** Records Kafka acknowledged, and the calls in which it acknowledged any. */
        final AtomicInteger delivered = new AtomicInteger();
        final AtomicInteger deliveringCalls = new AtomicInteger();

        Handler() {
            this(sender());
        }

        Handler(KafkaOutboxSender sender) {
            this.kafka = sender.handler();
        }

        @Override
        public void handle(OutboxRecord record) {
            kafka.handle(record);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            var outcomes = new Outcome[records.size()];
            var failedKeys = new HashSet<String>();
            var toSend = new ArrayList<Integer>();
            for (int i = 0; i < records.size(); i++) {
                var record = records.get(i);
                var outcome = failedKeys.contains(key(record)) ? Outcome.notAttempted() : fail.apply(record);
                if (outcome == null) {
                    toSend.add(i);
                } else {
                    outcomes[i] = outcome;
                    failedKeys.add(key(record));
                }
            }
            var sending = toSend.stream().map(records::get).toList();
            var sent = sending.isEmpty() ? List.<Outcome>of() : kafka.handleBatch(sending);
            for (int j = 0; j < sent.size(); j++) {
                outcomes[toSend.get(j)] = sent.get(j);
            }
            int acknowledged = (int) sent.stream().filter(o -> o == Outcome.delivered()).count();
            delivered.addAndGet(acknowledged);
            if (acknowledged > 0) {
                deliveringCalls.incrementAndGet();
            }
            afterSend.accept(records);
            return Arrays.asList(outcomes);
        }
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
        return start(OutboxRuntime.builder(config, transactions, handler));
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

    /** Sends {@code messages} in one transaction; once it committed, they are expected in the topic. */
    private List<UUID> sendAll(OutboxRuntime runtime, List<OutboxMessage> messages) {
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(messages));
        for (int i = 0; i < ids.size(); i++) {
            committed.computeIfAbsent(new String(messages.get(i).key(), UTF_8), k -> new ArrayList<>()).add(ids.get(i));
        }
        return ids;
    }

    /**
     * Reads {@code topic} until it holds every committed message and {@code until} holds too, then checks that per key
     * the first occurrences are exactly the committed messages in send order; returns what it read.
     */
    private List<ConsumerRecord<byte[], byte[]>> consumeCommitted(
            String topic, Predicate<List<ConsumerRecord<byte[], byte[]>>> until) {
        var expected = committed.values().stream().flatMap(List::stream).collect(Collectors.toSet());
        var records = TestKafka.consumeUntil(topic, CONSUME_TIMEOUT,
                r -> byMessageId(r).keySet().containsAll(expected) && until.test(r));
        var firstOccurrences = firstOccurrenceOrderPerKey(records);
        assertThat(firstOccurrences.keySet()).isEqualTo(committed.keySet());
        committed.forEach((key, ids) -> assertThat(firstOccurrences.get(key)).as(key).isEqualTo(ids));
        return records;
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

    /** The first occurrence of each message id. */
    private static Map<UUID, ConsumerRecord<byte[], byte[]>> byMessageId(List<ConsumerRecord<byte[], byte[]>> records) {
        var result = new HashMap<UUID, ConsumerRecord<byte[], byte[]>>();
        records.forEach(r -> result.putIfAbsent(KafkaOutboxHeaders.messageId(r), r));
        return result;
    }

    /** Per key, the message ids in the order of their first occurrence; a key lives in one partition, read in order. */
    private static Map<String, List<UUID>> firstOccurrenceOrderPerKey(List<ConsumerRecord<byte[], byte[]>> records) {
        var result = new HashMap<String, LinkedHashSet<UUID>>();
        for (var r : records) {
            result.computeIfAbsent(new String(r.key(), UTF_8), k -> new LinkedHashSet<>())
                    .add(KafkaOutboxHeaders.messageId(r));
        }
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> List.copyOf(e.getValue())));
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
