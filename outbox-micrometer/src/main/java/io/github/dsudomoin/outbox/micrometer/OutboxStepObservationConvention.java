package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/** Names and key values of the relay step observation; {@link DefaultOutboxStepObservationConvention} by default. */
public interface OutboxStepObservationConvention extends ObservationConvention<OutboxStepContext> {

    @Override
    default boolean supportsContext(Observation.Context context) {
        return context instanceof OutboxStepContext;
    }
}
