package io.github.dsudomoin.outbox.micrometer;

/** {@code outbox.enqueue}, span {@code outbox enqueue}, no key values. */
public class DefaultOutboxEnqueueObservationConvention implements OutboxEnqueueObservationConvention {

    public static final DefaultOutboxEnqueueObservationConvention INSTANCE =
            new DefaultOutboxEnqueueObservationConvention();

    @Override
    public String getName() {
        return "outbox.enqueue";
    }

    @Override
    public String getContextualName(OutboxEnqueueContext context) {
        return "outbox enqueue";
    }
}
