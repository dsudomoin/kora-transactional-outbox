package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.common.KeyValues;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code outbox.deliver}, span {@code outbox deliver <topic>}. Low cardinality: {@code messaging.destination.name}
 * and, once known, {@code outbox.outcome} ({@code delivered}, {@code retry}, {@code permanent}, {@code not_attempted},
 * {@code requeued}, or {@code failed} when the handler call ended without outcomes). High cardinality:
 * {@code outbox.message_id}, {@code outbox.attempt} (this attempt, from 1), {@code outbox.delay_ms} and, when the
 * record's shard is known, {@code outbox.shard}.
 */
public class DefaultOutboxDeliveryObservationConvention implements OutboxDeliveryObservationConvention {

    public static final DefaultOutboxDeliveryObservationConvention INSTANCE =
            new DefaultOutboxDeliveryObservationConvention();

    /** Per topic: names and values that do not change per record (as many as the meters' {@code topic} tags). */
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    @Override
    public String getName() {
        return "outbox.deliver";
    }

    @Override
    public String getContextualName(OutboxDeliveryContext context) {
        return topic(context).name();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(OutboxDeliveryContext context) {
        var topic = topic(context);
        var outcome = context.outcome();
        if (outcome != null) {
            return topic.withOutcome(name(outcome));
        }
        return context.failed() ? topic.withOutcome("failed") : topic.values();
    }

    /** Computed once per record: the observation asks at start and at stop, and the record and the clock are fixed. */
    @Override
    public KeyValues getHighCardinalityKeyValues(OutboxDeliveryContext context) {
        var cached = context.highCardinality;
        if (cached != null) {
            return cached;
        }
        var r = context.record();
        var delay = Duration.between(r.createdAt(), context.now());
        var values = KeyValues.of(
                "outbox.message_id", r.messageId().toString(),
                "outbox.attempt", Long.toString(r.attempts() + 1L),
                "outbox.delay_ms", Long.toString(delay.isNegative() ? 0 : delay.toMillis()));
        values = r.shard() < 0 ? values : values.and("outbox.shard", Integer.toString(r.shard()));
        context.highCardinality = values;
        return values;
    }

    private Topic topic(OutboxDeliveryContext context) {
        return topics.computeIfAbsent(context.record().topic(), Topic::of);
    }

    private static String name(Outcome outcome) {
        if (outcome instanceof Outcome.Retry) {
            return "retry";
        } else if (outcome instanceof Outcome.Permanent) {
            return "permanent";
        } else if (outcome instanceof Outcome.NotAttempted) {
            return "not_attempted";
        } else if (outcome instanceof Outcome.Requeue) {
            return "requeued";
        }
        return "delivered";
    }

    /** The contextual name and the low cardinality values, without an outcome and with each outcome seen. */
    private record Topic(String name, KeyValues values, Map<String, KeyValues> byOutcome) {
        static Topic of(String topic) {
            return new Topic("outbox deliver " + topic, KeyValues.of("messaging.destination.name", topic),
                    new ConcurrentHashMap<>());
        }

        /** Made on the first use of a name, so every outcome name has its values. */
        KeyValues withOutcome(String outcome) {
            var cached = byOutcome.get(outcome);
            return cached != null ? cached
                    : byOutcome.computeIfAbsent(outcome, o -> values.and("outbox.outcome", o));
        }
    }
}
