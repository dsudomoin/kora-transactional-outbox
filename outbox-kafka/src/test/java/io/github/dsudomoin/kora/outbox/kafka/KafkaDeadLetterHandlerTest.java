package io.github.dsudomoin.kora.outbox.kafka;

import static io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxSenderTest.DLQ;
import static io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxSenderTest.TOPIC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.DeadLetterDecision;
import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxSenderTest.TestProducer;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaDeadLetterHandlerTest {

    private static final String TEMPLATE = "{topic}.dlq";
    private static final Instant FAILED_AT = Instant.parse("2026-10-27T10:15:30.123456Z");

    private final List<TestProducer> producers = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{topic}", "{topic} dlq", "{topic}.dl/q", "{topics}.dlq", "{Topic}.dlq", "{topic", ".",
            "..", "dlq.{topic}}"})
    void invalidTemplateFailsAtStartup(String template) {
        assertThatThrownBy(() -> handler(template, r -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outbox.kafka.deadLetterTopic");
    }

    @Test
    void templateWhoseShortestTopicIsTooLongFailsAtStartup() {
        assertThatThrownBy(() -> handler("x".repeat(250), r -> null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> handler("x".repeat(249) + "{topic}", r -> null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> handler("x".repeat(248) + "{topic}", r -> null)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{topic}.dlq", "dlq.{topic}", "dead-letters", "{topic}_{topic}", "A.b-c_9.{topic}"})
    void validTemplate(String template) {
        assertThatCode(() -> handler(template, r -> null)).doesNotThrowAnyException();
    }

    @Test
    void publishesEachLetterWithItsHeadersAndDiscardsThem() throws Exception {
        var handler = handler(TEMPLATE, r -> null);
        var keyed = letter(1, "k", "v", "java.lang.IllegalStateException: boom", new OutboxHeader("h", bytes("a")),
                new OutboxHeader("no-value", null), new OutboxHeader("h", bytes("b")));
        var unkeyed = new DeadLetter(2, UUID.randomUUID(), TOPIC, null, null, List.of(),
                Instant.now(), 7, FAILED_AT, null, DeadLetter.Status.PENDING, 0, 0, Instant.now());

        var decisions = handler.handle(List.of(keyed, unkeyed));

        assertThat(decisions).containsExactly(DeadLetterDecision.discard(), DeadLetterDecision.discard());
        var sent = producers.get(0).history();
        assertThat(sent).hasSize(2);
        assertThat(sent.get(0).topic()).isEqualTo(DLQ);
        assertThat(sent.get(0).key()).isEqualTo(bytes("k"));
        assertThat(sent.get(0).value()).isEqualTo(bytes("v"));
        assertThat(headers(sent.get(0))).containsExactly(
                Map.entry("h", "a"), Map.entry("no-value", "<null>"), Map.entry("h", "b"),
                Map.entry(KafkaDeadLetterHandler.DEAD_TOPIC, TOPIC),
                Map.entry(KafkaDeadLetterHandler.DEAD_ERROR, "java.lang.IllegalStateException: boom"),
                Map.entry(KafkaDeadLetterHandler.DEAD_ATTEMPTS, "3"),
                Map.entry(KafkaDeadLetterHandler.DEAD_FAILED_AT, "2026-10-27T10:15:30.123456Z"),
                Map.entry(KafkaOutboxHeaders.MESSAGE_ID, keyed.messageId().toString()));
        assertThat(sent.get(1).key()).isNull();
        assertThat(sent.get(1).value()).isNull();
        assertThat(headers(sent.get(1))).containsExactly(
                Map.entry(KafkaDeadLetterHandler.DEAD_TOPIC, TOPIC),
                Map.entry(KafkaDeadLetterHandler.DEAD_ERROR, "<null>"),
                Map.entry(KafkaDeadLetterHandler.DEAD_ATTEMPTS, "7"),
                Map.entry(KafkaDeadLetterHandler.DEAD_FAILED_AT, "2026-10-27T10:15:30.123456Z"),
                Map.entry(KafkaOutboxHeaders.MESSAGE_ID, unkeyed.messageId().toString()));
    }

    @Test
    void anUnacknowledgedLetterFailsTheCallNamingHowMany() {
        var handler = handler(TEMPLATE, r -> new String(r.value(), UTF_8).equals("down")
                ? new NotEnoughReplicasException("not enough replicas") : null);

        assertThatThrownBy(() -> handler.handle(List.of(letter(1, "a", "v", null), letter(2, "b", "v", null),
                letter(3, "c", "down", null))))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("1 of 3 dead letters")
                .hasCauseInstanceOf(NotEnoughReplicasException.class);
        assertThat(producers).as("no refusal, no second round").hasSize(1);
    }

    /**
     * The broker refuses letter 1 for good after the append (on the I/O thread): that closes the producer and fails
     * letter 2 with it. Letter 2 goes again in the same call, so the call does not throw and loop on every retry.
     */
    @Test
    void aBrokerRefusalDoesNotFailItsBatchMates() throws Exception {
        var handler = handler(TEMPLATE, false, r -> null);

        var letters = List.of(letter(1, "a", "big", null), letter(2, "b", "v", null));
        var call = executor.submit(() -> handler.handle(letters));
        awaitSent(0, 2).errorNext(new RecordTooLargeException("too large for the dead-letter topic"));
        awaitSent(1, 1).completeNext();

        assertThat(call.get(5, SECONDS)).containsExactly(DeadLetterDecision.keep(), DeadLetterDecision.discard());
        assertThat(producers.get(1).history()).extracting(r -> new String(r.value(), UTF_8)).containsExactly("v");
    }

    /** Each refusal sets its letter aside and sends the rest again: two refusals, three rounds. */
    @Test
    void everyBrokerRefusalSetsOnlyItsLetterAside() throws Exception {
        var handler = handler(TEMPLATE, false, r -> null);

        var call = executor.submit(() -> handler.handle(List.of(letter(1, "a", "big", null),
                letter(2, "b", "big", null), letter(3, "c", "v", null))));
        awaitSent(0, 3).errorNext(new RecordTooLargeException("too large"));
        awaitSent(1, 2).errorNext(new RecordTooLargeException("too large"));
        awaitSent(2, 1).completeNext();

        assertThat(call.get(5, SECONDS))
                .containsExactly(DeadLetterDecision.keep(), DeadLetterDecision.keep(), DeadLetterDecision.discard());
    }

    /** A batch-mate that fails again after the refusal's round fails the call. */
    @Test
    void aBatchMateFailingAgainAfterTheRefusalFailsTheCall() throws Exception {
        var handler = handler(TEMPLATE, false, r -> null);

        var letters = List.of(letter(1, "a", "big", null), letter(2, "b", "v", null));
        var call = executor.submit(() -> handler.handle(letters));
        awaitSent(0, 2).errorNext(new RecordTooLargeException("too large"));
        awaitSent(1, 1).errorNext(new NotEnoughReplicasException("not enough replicas"));

        assertThatThrownBy(() -> call.get(5, SECONDS)).cause()
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("1 of 2 dead letters")
                .hasCauseInstanceOf(NotEnoughReplicasException.class);
        assertThat(producers).hasSize(2);
    }

    /** Kafka refuses it for good (here over {@code max.request.size}): kept for humans, not handed again forever. */
    @Test
    void aLetterKafkaRefusesForGoodIsKept() throws Exception {
        var handler = handler(TEMPLATE, r -> new String(r.value(), UTF_8).equals("big")
                ? new RecordTooLargeException("too large") : null);

        try (var err = new CapturedStderr()) {
            var decisions = handler.handle(List.of(letter(1, "a", "v", null), letter(2, "b", "big", null)));

            assertThat(decisions).containsExactly(DeadLetterDecision.discard(), DeadLetterDecision.keep());
            assertThat(err.text()).contains("WARN").contains("RecordTooLargeException");
        }
    }

    @Test
    void aTopicTooLongForKafkaIsKeptWithoutASend() throws Exception {
        var handler = handler(TEMPLATE, r -> null);
        var tooLong = new DeadLetter(1, UUID.randomUUID(), "x".repeat(246), bytes("k"), bytes("v"), List.of(),
                Instant.now(), 1, FAILED_AT, null, DeadLetter.Status.PENDING, 0, 0, Instant.now());

        try (var err = new CapturedStderr()) {
            var decisions = handler.handle(List.of(tooLong, letter(2, "a", "v", null)));

            assertThat(decisions).containsExactly(DeadLetterDecision.keep(), DeadLetterDecision.discard());
            assertThat(producers.get(0).history()).extracting(ProducerRecord::topic).containsExactly(DLQ);
            assertThat(err.text()).contains("WARN").contains("x".repeat(246) + ".dlq");
        }
    }

    /**
     * A refusal on the caller thread leaves the producer open: the batch-mate failed for a reason of its own, so it is
     * not sent again, and the call fails at once.
     */
    @Test
    void aClientSideRefusalSendsNothingAgain() {
        var handler = handler(TEMPLATE, r -> switch (new String(r.value(), UTF_8)) {
            case "big" -> new RecordTooLargeException("over max.request.size");
            case "down" -> new NotEnoughReplicasException("not enough replicas");
            default -> null;
        });

        assertThatThrownBy(() -> handler.handle(List.of(letter(1, "a", "big", null), letter(2, "b", "down", null))))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("1 of 2 dead letters");
        assertThat(producers).as("one round").hasSize(1);
        assertThat(producers.get(0).history()).isEmpty(); // neither record was appended
    }

    /**
     * Many refusals in one batch take a round each; should the pass cut the call meanwhile, the refusals seen so far
     * are remembered: the next call keeps those letters with no send and publishes the rest.
     */
    @Test
    void refusalsOfACallCutShortAreRememberedByTheNext() throws Exception {
        var handler = handler(TEMPLATE, false, r -> null);
        var letters = List.of(letter(1, "a", "big", null), letter(2, "b", "v", null), letter(3, "c", "v", null));

        var cut = executor.submit(() -> handler.handle(letters));
        awaitSent(0, 3).errorNext(new RecordTooLargeException("too large"));
        awaitSent(1, 2); // its batch-mates' second round, in flight
        cut.cancel(true); // as the pass does at outbox.deadLetters.timeout

        try (var err = new CapturedStderr()) {
            var next = executor.submit(() -> handler.handle(letters)); // runs once the cut call has returned
            awaitSent(2, 2).completeNext();
            producers.get(2).completeNext();

            assertThat(next.get(5, SECONDS)).containsExactly(DeadLetterDecision.keep(), DeadLetterDecision.discard(),
                    DeadLetterDecision.discard());
            assertThat(err.text()).contains("WARN")
                    .contains("refused it for good on its dead-letter topic " + DLQ + " before");
        }
        assertThat(producers.get(2).history()).extracting(r -> new String(r.key(), UTF_8)).containsExactly("b", "c");
    }

    /** A fixed template naming the letter's own topic would publish it back where it died. */
    @Test
    void aDeadLetterTopicThatIsTheLettersOwnIsKept() throws Exception {
        var handler = handler(TOPIC, r -> null);

        try (var err = new CapturedStderr()) {
            assertThat(handler.handle(List.of(letter(1, "a", "v", null)))).containsExactly(DeadLetterDecision.keep());
            assertThat(err.text()).contains("WARN").contains("names its own topic");
        }
        assertThat(producers).isEmpty();
    }

    /** {@code outbox.kafka.deadLetterTopic} absent: inactive, and should it be called all the same, it keeps all. */
    @Test
    void withoutATopicEveryLetterIsKept() throws Exception {
        var handler = handler(null, r -> null);

        assertThat(handler.active()).isFalse();
        assertThat(handler(TEMPLATE, r -> null).active()).isTrue();
        assertThat(handler.handle(List.of(letter(1, "a", "v", null), letter(2, "b", "v", null))))
                .containsExactly(DeadLetterDecision.keep(), DeadLetterDecision.keep());
        assertThat(producers).isEmpty();
    }

    /**
     * A sender's call can take delivery.timeout.ms + 5 s + max.block.ms; the dead-letter pass gives a hook call
     * {@code outbox.deadLetters.timeout}. The defaults fit: 20 s + 5 s + 5 s against 30 s.
     */
    @Test
    void warnsOnlyWhenACallCanOutlastTheHookTimeout() {
        var module = new KafkaOutboxModule() {};
        var outbox = new OutboxConfig() {};
        var defaults = kafkaConfig(TEMPLATE, props("bootstrap.servers", "dlq-test:9092"));
        var slow = kafkaConfig(TEMPLATE, props("bootstrap.servers", "dlq-test:9092", "max.block.ms", "10000"));
        var off = kafkaConfig(null, props("bootstrap.servers", "dlq-test:9092", "max.block.ms", "10000"));
        try (var err = new CapturedStderr()) {
            module.kafkaDeadLetterHandler(defaults, new KafkaOutboxSender(defaults), outbox);
            module.kafkaDeadLetterHandler(slow, new KafkaOutboxSender(slow), null); // no relay in the app
            module.kafkaDeadLetterHandler(off, new KafkaOutboxSender(off), outbox); // no calls
            assertThat(err.text()).doesNotContain("deadLetters.timeout");
        }
        try (var err = new CapturedStderr()) {
            module.kafkaDeadLetterHandler(slow, new KafkaOutboxSender(slow), outbox);
            assertThat(err.text()).contains("WARN").contains("outbox.deadLetters.timeout");
        }
    }

    private KafkaDeadLetterHandler handler(@Nullable String template,
            Function<ProducerRecord<byte[], byte[]>, RuntimeException> failure) {
        return handler(template, true, failure);
    }

    /** {@code autoComplete} false: the test completes or fails each send, as Kafka's I/O thread would. */
    private KafkaDeadLetterHandler handler(@Nullable String template, boolean autoComplete,
            Function<ProducerRecord<byte[], byte[]>, RuntimeException> failure) {
        var sender = new KafkaOutboxSender(() -> {
            var producer = new TestProducer(autoComplete, failure);
            producers.add(producer);
            return producer;
        }, Duration.ofSeconds(20), Clock.systemUTC());
        return new KafkaDeadLetterHandler(sender, template);
    }

    private TestProducer awaitSent(int producer, int records) throws InterruptedException {
        var deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (producers.size() <= producer || producers.get(producer).history().size() < records) {
            assertThat(System.nanoTime()).as("producer %d sent %d records", producer, records).isLessThan(deadline);
            Thread.sleep(5);
        }
        return producers.get(producer);
    }

    private static KafkaOutboxConfig kafkaConfig(@Nullable String template, Properties driverProperties) {
        return new KafkaOutboxConfig() {
            @Override
            public Properties driverProperties() {
                return driverProperties;
            }

            @Override
            public String deadLetterTopic() {
                return template;
            }
        };
    }

    private static Properties props(String... keyValues) {
        var p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            p.put(keyValues[i], keyValues[i + 1]);
        }
        return p;
    }

    private static DeadLetter letter(long id, String key, String payload, @Nullable String error,
            OutboxHeader... headers) {
        return new DeadLetter(id, UUID.randomUUID(), TOPIC, bytes(key), bytes(payload),
                List.of(headers), Instant.now(), 3, FAILED_AT, error, DeadLetter.Status.PENDING, 0, 0, Instant.now());
    }

    private static List<Map.Entry<String, String>> headers(ProducerRecord<byte[], byte[]> record) {
        var result = new ArrayList<Map.Entry<String, String>>();
        record.headers().forEach(h -> result.add(Map.entry(h.key(), h.value() == null ? "<null>"
                : new String(h.value(), UTF_8))));
        return result;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
