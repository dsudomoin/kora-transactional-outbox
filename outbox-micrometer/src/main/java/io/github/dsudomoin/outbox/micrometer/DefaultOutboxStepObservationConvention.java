package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.common.KeyValues;
import java.util.stream.Collectors;

/**
 * {@code outbox.relay.step}, span {@code outbox relay step}. No low-cardinality key values. High cardinality:
 * {@code outbox.shards}, the step's shards joined with {@code ,}.
 */
public class DefaultOutboxStepObservationConvention implements OutboxStepObservationConvention {

    public static final DefaultOutboxStepObservationConvention INSTANCE = new DefaultOutboxStepObservationConvention();

    @Override
    public String getName() {
        return "outbox.relay.step";
    }

    @Override
    public String getContextualName(OutboxStepContext context) {
        return "outbox relay step";
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(OutboxStepContext context) {
        return KeyValues.of("outbox.shards",
                context.shards().stream().map(String::valueOf).collect(Collectors.joining(",")));
    }
}
