package io.github.dsudomoin.kora.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.DeadLetterDecision;
import io.github.dsudomoin.kora.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KafkaOutboxModule}'s dead-letter hook. With {@code outbox.kafka.deadLetterTopic} set, it publishes each
 * letter through the relay's {@link KafkaOutboxSender} to the topic that template names, {@code {topic}} replaced by
 * the letter's topic: the letter's key, payload and headers, then {@link #DEAD_TOPIC}, {@link #DEAD_ERROR},
 * {@link #DEAD_ATTEMPTS}, {@link #DEAD_FAILED_AT} and the sender's {@link KafkaOutboxHeaders#MESSAGE_ID}. Letters Kafka
 * acknowledged are discarded. Kept for humans, with a WARN: a letter whose dead-letter topic is no valid topic name
 * (longer than 249 characters) or is the letter's own topic, and one Kafka refuses for good (too large). A refusal
 * after the append closes the producer and fails the letter's batch-mates too: those are sent again in the same call,
 * once per refusal. Any other failure (not acknowledged in time, a cooling or missing topic) throws a
 * {@link KafkaException} naming how many failed, and the dead-letter pass hands every letter of the call again after a
 * backoff: letters acknowledged in a call that throws repeat on their dead-letter topic, once per failed call for as
 * long as a batch-mate keeps failing. Where brokers do not create topics on first use, create the dead-letter topics
 * beforehand; until then their letters wait.
 *
 * <p>Without the key it is not {@link #active()}: dead letters are written kept, with no call. A type of its own, so
 * that an app's hook component replaces it: Kora 1.2.23 takes a default declared as exactly
 * {@code OutboxDeadLetterHandler} over an app component of a subtype ({@code @Component class Hook implements
 * OutboxDeadLetterHandler}).
 */
public final class KafkaDeadLetterHandler implements OutboxDeadLetterHandler {

    /** The topic the letter died on. */
    public static final String DEAD_TOPIC = "outbox-dead-topic";
    /** The text of its last failure; no value when there is none. */
    public static final String DEAD_ERROR = "outbox-dead-error";
    /** How many deliveries of it failed, in decimal. */
    public static final String DEAD_ATTEMPTS = "outbox-dead-attempts";
    /** When it died, as {@link java.time.Instant#toString()} (ISO-8601, UTC). */
    public static final String DEAD_FAILED_AT = "outbox-dead-failed-at";

    private static final String TOPIC = "{topic}";
    private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterHandler.class);

    private final KafkaOutboxSender sender;
    @Nullable
    private final String template;

    /** A null {@code template} keeps every letter; an invalid one throws {@link IllegalArgumentException}. */
    KafkaDeadLetterHandler(KafkaOutboxSender sender, @Nullable String template) {
        // the shortest topic it names, for a one-character topic, must be valid; {topic} alone names the letter's own
        if (template != null
                && (template.equals(TOPIC) || !KafkaOutboxPublisher.validTopic(template.replace(TOPIC, "x")))) {
            throw new IllegalArgumentException("outbox.kafka.deadLetterTopic: '" + template + "' is not a Kafka topic"
                    + " name (at most 249 of [a-zA-Z0-9._-]) where {topic} stands for the letter's topic, nor may it be"
                    + " {topic} alone");
        }
        this.sender = sender;
        this.template = template;
    }

    /** Whether {@code outbox.kafka.deadLetterTopic} is set. */
    @Override
    public boolean active() {
        return template != null;
    }

    @Override
    public List<DeadLetterDecision> handle(List<DeadLetter> letters) {
        var decisions = new DeadLetterDecision[letters.size()];
        Arrays.fill(decisions, DeadLetterDecision.keep());
        if (template == null) {
            return List.of(decisions);
        }
        var records = new OutboxRecord[letters.size()];
        var unsent = new ArrayList<Integer>(letters.size()); // index in letters
        for (int i = 0; i < letters.size(); i++) {
            var letter = letters.get(i);
            var topic = template.replace(TOPIC, letter.topic());
            if (!KafkaOutboxPublisher.validTopic(topic)) {
                log.warn("Outbox dead letter {} (message {}) is kept: its dead-letter topic '{}' is not a Kafka topic"
                        + " name (at most 249 of [a-zA-Z0-9._-])", letter.id(), letter.messageId(), topic);
            } else if (topic.equals(letter.topic())) {
                log.warn("Outbox dead letter {} (message {}) is kept: outbox.kafka.deadLetterTopic names its own topic"
                        + " {}", letter.id(), letter.messageId(), topic);
            } else {
                records[i] = record(letter, topic);
                unsent.add(i);
            }
        }
        int published = unsent.size();
        Throwable cause = null;
        boolean refused = true;
        // a refusal after the append closed the producer and failed the records still in flight, so those go again;
        // each round sets at least one letter aside, so there are at most as many rounds as letters
        while (refused && !unsent.isEmpty()) {
            var outcomes = sender.send(unsent.stream().map(i -> records[i]).toList());
            var failed = new ArrayList<Integer>();
            refused = false;
            cause = null;
            for (int j = 0; j < unsent.size(); j++) {
                int i = unsent.get(j);
                var outcome = outcomes.get(j);
                if (outcome == Outcome.delivered()) {
                    decisions[i] = DeadLetterDecision.discard();
                } else if (outcome instanceof Outcome.Permanent p) { // as the relay does: retrying cannot help
                    refused = true;
                    log.warn("Outbox dead letter {} (message {}) is kept: Kafka refused it for good on its dead-letter"
                            + " topic {}", letters.get(i).id(), letters.get(i).messageId(), records[i].topic(),
                            p.error());
                } else {
                    failed.add(i);
                    if (cause == null && outcome instanceof Outcome.Retry r) {
                        cause = r.error();
                    }
                }
            }
            unsent = failed;
        }
        if (!unsent.isEmpty()) {
            throw new KafkaException(unsent.size() + " of " + published + " dead letters were not published to their"
                    + " dead-letter topics; all " + letters.size() + " are handed again after a backoff", cause);
        }
        return List.of(decisions);
    }

    /** As the relay reads a record: no earlier attempts or requeues. */
    private static OutboxRecord record(DeadLetter letter, String topic) {
        var headers = new ArrayList<OutboxHeader>(letter.headers().size() + 4);
        headers.addAll(letter.headers());
        headers.add(header(DEAD_TOPIC, letter.topic()));
        headers.add(header(DEAD_ERROR, letter.error()));
        headers.add(header(DEAD_ATTEMPTS, Integer.toString(letter.attempts())));
        headers.add(header(DEAD_FAILED_AT, letter.failedAt().toString()));
        return new OutboxRecord(letter.id(), letter.messageId(), topic, letter.key(), letter.payload(), headers,
                letter.createdAt(), 0, 0);
    }

    private static OutboxHeader header(String name, @Nullable String value) {
        return new OutboxHeader(name, value == null ? null : value.getBytes(UTF_8));
    }
}
