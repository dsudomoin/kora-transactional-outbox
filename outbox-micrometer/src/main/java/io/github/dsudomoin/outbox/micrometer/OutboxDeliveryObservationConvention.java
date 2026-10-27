package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/** Names and key values of the delivery observation; {@link DefaultOutboxDeliveryObservationConvention} by default. */
public interface OutboxDeliveryObservationConvention extends ObservationConvention<OutboxDeliveryContext> {

    @Override
    default boolean supportsContext(Observation.Context context) {
        return context instanceof OutboxDeliveryContext;
    }
}
