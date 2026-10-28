package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.common.KeyValues;
import java.time.Duration;

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

    @Override
    public String getName() {
        return "outbox.deliver";
    }

    @Override
    public String getContextualName(OutboxDeliveryContext context) {
        return "outbox deliver " + context.record().topic();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(OutboxDeliveryContext context) {
        var topic = KeyValues.of("messaging.destination.name", context.record().topic());
        var outcome = context.outcome();
        if (outcome != null) {
            return topic.and("outbox.outcome", name(outcome));
        }
        return context.failed() ? topic.and("outbox.outcome", "failed") : topic;
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(OutboxDeliveryContext context) {
        var r = context.record();
        var delay = Duration.between(r.createdAt(), context.now());
        var values = KeyValues.of(
                "outbox.message_id", r.messageId().toString(),
                "outbox.attempt", Long.toString(r.attempts() + 1L),
                "outbox.delay_ms", Long.toString(delay.isNegative() ? 0 : delay.toMillis()));
        return r.shard() < 0 ? values : values.and("outbox.shard", Integer.toString(r.shard()));
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
}
