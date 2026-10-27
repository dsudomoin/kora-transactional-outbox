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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KafkaOutboxModule}'s dead-letter hook. With {@code outbox.kafka.deadLetterTopic} set, it publishes each
 * letter through the relay's {@link KafkaOutboxSender} to the topic that template names, {@code {topic}} replaced by
 * the letter's topic: the letter's key, payload and headers, then {@link #DEAD_TOPIC}, {@link #DEAD_ERROR},
 * {@link #DEAD_ATTEMPTS}, {@link #DEAD_FAILED_AT} and the sender's {@link KafkaOutboxHeaders#MESSAGE_ID}. Letters Kafka
 * acknowledged are discarded. Kept for humans: a letter whose dead-letter topic is no valid topic name (longer than
 * 249 characters) or is the letter's own topic, and one Kafka refuses for good (too large). Kafka's refusals are
 * remembered by message id (the last 10 000, until a restart), so such a letter handed again, after a call that
 * failed or was cut, is kept at once with no send. A refusal by the broker, after the append, closes the producer and
 * fails the letter's batch-mates too: those are sent again in the same call, once per such refusal. Batch-mates
 * already in flight may have been written all the same, and the new producer's idempotence cannot tell, so they can
 * land twice even in a call that discards them all. Any other failure (not acknowledged in time, a cooling or missing
 * topic) throws a {@link KafkaException} naming how many failed, and the dead-letter pass hands every letter of the
 * call again after a backoff: letters acknowledged in a call that throws repeat on their dead-letter topic, once per
 * failed call for as long as a batch-mate keeps failing. Each kept letter has a WARN, at most one a minute of each
 * kind with a count of those not logged. Where brokers do not create topics on first use, create the dead-letter
 * topics beforehand; until then their letters wait.
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
    private static final int REFUSALS_REMEMBERED = 10_000;
    private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterHandler.class);

    private final KafkaOutboxSender sender;
    @Nullable
    private final String template;
    /** Message ids Kafka refused for good, least recently handed first. */
    private final Map<UUID, Boolean> refusals = Collections.synchronizedMap(
            new LinkedHashMap<UUID, Boolean>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<UUID, Boolean> eldest) {
                    return size() > REFUSALS_REMEMBERED;
                }
            });
    private final Warning invalidTopic = new Warning();
    private final Warning ownTopic = new Warning();
    private final Warning refused = new Warning();
    private final Warning refusedBefore = new Warning();

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
            if (refusals.get(letter.messageId()) != null) {
                refusedBefore.warn(name(letter) + " is kept: Kafka refused it for good on its dead-letter topic "
                        + topic + " before", null);
            } else if (!KafkaOutboxPublisher.validTopic(topic)) {
                invalidTopic.warn(name(letter) + " is kept: its dead-letter topic '" + topic + "' is not a Kafka topic"
                        + " name (at most 249 of [a-zA-Z0-9._-])", null);
            } else if (topic.equals(letter.topic())) {
                ownTopic.warn(name(letter) + " is kept: outbox.kafka.deadLetterTopic names its own topic " + topic,
                        null);
            } else {
                records[i] = record(letter, topic);
                unsent.add(i);
            }
        }
        int published = unsent.size();
        Throwable cause = null;
        // a refusal after the append closed the producer and failed the records still in flight, so those go again;
        // each such round sets one letter aside at least, so there are at most as many rounds as letters
        while (!unsent.isEmpty()) {
            var sent = sender.sendAndReport(unsent.stream().map(i -> records[i]).toList());
            var failed = new ArrayList<Integer>();
            cause = null;
            for (int j = 0; j < unsent.size(); j++) {
                int i = unsent.get(j);
                var outcome = sent.outcomes().get(j);
                if (outcome == Outcome.delivered()) {
                    decisions[i] = DeadLetterDecision.discard();
                } else if (outcome instanceof Outcome.Permanent p) { // as the relay does: retrying cannot help
                    refusals.put(letters.get(i).messageId(), Boolean.TRUE); // before a cut of this call loses it
                    refused.warn(name(letters.get(i)) + " is kept: Kafka refused it for good on its dead-letter topic "
                            + records[i].topic(), p.error());
                } else {
                    failed.add(i);
                    if (cause == null && outcome instanceof Outcome.Retry r) {
                        cause = r.error();
                    }
                }
            }
            unsent = failed;
            if (!sent.closedByRefusal()) { // the others failed for reasons of their own
                break;
            }
        }
        if (!unsent.isEmpty()) {
            throw new KafkaException(unsent.size() + " of " + published + " dead letters were not published to their"
                    + " dead-letter topics; all " + letters.size() + " are handed again after a backoff", cause);
        }
        return List.of(decisions);
    }

    private static String name(DeadLetter letter) {
        return "Outbox dead letter " + letter.id() + " (message " + letter.messageId() + ")";
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

    /** One kind of WARN, at most once a minute, as the dead-letter pass logs its own. */
    private static final class Warning {

        private static final long INTERVAL = TimeUnit.MINUTES.toNanos(1);

        /** When it was last logged ({@code nanoTime}), and how many went unlogged since. */
        private final AtomicLong logged = new AtomicLong(System.nanoTime() - INTERVAL);
        private final AtomicInteger suppressed = new AtomicInteger();

        void warn(String what, @Nullable Throwable error) {
            long now = System.nanoTime();
            long last = logged.get();
            if (now - last < INTERVAL || !logged.compareAndSet(last, now)) {
                suppressed.incrementAndGet();
                return;
            }
            if (error == null) {
                log.warn("{} ({} more since the previous report not logged)", what, suppressed.getAndSet(0));
            } else {
                log.warn("{} ({} more since the previous report not logged)", what, suppressed.getAndSet(0), error);
            }
        }
    }
}
