package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/** Names and key values of the enqueue observation; {@link DefaultOutboxEnqueueObservationConvention} by default. */
public interface OutboxEnqueueObservationConvention extends ObservationConvention<OutboxEnqueueContext> {

    @Override
    default boolean supportsContext(Observation.Context context) {
        return context instanceof OutboxEnqueueContext;
    }
}
