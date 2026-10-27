package io.github.dsudomoin.kora.outbox.kafka;

import static io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxSenderTest.DLQ;
import static io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxSenderTest.TOPIC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.DeadLetterDecision;
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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaDeadLetterHandlerTest {

    private static final String TEMPLATE = "{topic}.dlq";
    private static final Instant FAILED_AT = Instant.parse("2026-10-27T10:15:30.123456Z");

    private final List<TestProducer> producers = new CopyOnWriteArrayList<>();

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

    /** {@code outbox.kafka.deadLetterTopic} absent: every letter is kept, nothing is sent. */
    @Test
    void withoutATopicEveryLetterIsKept() throws Exception {
        var handler = handler(null, r -> null);

        assertThat(handler.handle(List.of(letter(1, "a", "v", null), letter(2, "b", "v", null))))
                .containsExactly(DeadLetterDecision.keep(), DeadLetterDecision.keep());
        assertThat(producers).isEmpty();
    }

    private KafkaDeadLetterHandler handler(@Nullable String template,
            Function<ProducerRecord<byte[], byte[]>, RuntimeException> failure) {
        var sender = new KafkaOutboxSender(() -> {
            var producer = new TestProducer(true, failure);
            producers.add(producer);
            return producer;
        }, Duration.ofSeconds(20), Clock.systemUTC());
        return new KafkaDeadLetterHandler(sender, template);
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
