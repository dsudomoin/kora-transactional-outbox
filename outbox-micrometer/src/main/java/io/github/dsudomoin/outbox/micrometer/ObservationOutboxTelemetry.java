package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.DoubleSupplier;

/**
 * The outbox on Micrometer Observation: an {@code outbox.enqueue} observation per {@code send}/{@code sendAll} call,
 * whose trace context the call's messages store; an {@code outbox.relay.step} observation per relay step, current on
 * the thread that steps (a relay worker, or the caller of {@code OutboxRelay.drain()}) while the step runs; and an
 * {@code outbox.deliver} observation per delivered record, parented by that stored context, whose own context the
 * record handed to the handler carries. The other events and the gauges are {@link OutboxMetrics}' meters. Names and
 * key values come from the conventions; meter names and tags can be changed with a {@code MeterFilter}.
 *
 * <p>The enqueue observation is a sender one and the delivery observation a receiver one, so the application's
 * propagating tracing handlers inject and extract the context; the delivery span is therefore of kind CONSUMER. A
 * record stored without trace context (written without this telemetry) gets the step's span, current then, as its
 * parent. {@link Builder#tracing} lets the handed records carry the delivery span; without it they keep their headers.
 */
public final class ObservationOutboxTelemetry implements OutboxTelemetry {

    private final ObservationRegistry observations;
    private final OutboxMetrics metrics;
    @Nullable
    private final TraceInjector injector;
    @Nullable
    private final OutboxEnqueueObservationConvention enqueueConvention;
    @Nullable
    private final OutboxDeliveryObservationConvention deliveryConvention;
    @Nullable
    private final OutboxStepObservationConvention stepConvention;

    private ObservationOutboxTelemetry(Builder builder) {
        this.observations = builder.observations;
        this.metrics = builder.metrics;
        this.injector = builder.injector;
        this.enqueueConvention = builder.enqueueConvention;
        this.deliveryConvention = builder.deliveryConvention;
        this.stepConvention = builder.stepConvention;
    }

    public static Builder builder(ObservationRegistry observations, OutboxMetrics metrics) {
        return new Builder(observations, metrics);
    }

    @Override
    public Enqueue enqueueStarted() {
        var context = new OutboxEnqueueContext();
        var observation = Observation.createNotStarted(enqueueConvention,
                DefaultOutboxEnqueueObservationConvention.INSTANCE, () -> context, observations).start();
        return new ObservedEnqueue(observation, List.copyOf(context.getCarrier()));
    }

    @Override
    public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
        var started = new Observation[records.size()];
        var contexts = new OutboxDeliveryContext[records.size()];
        for (int i = 0; i < started.length; i++) {
            var context = new OutboxDeliveryContext(records.get(i), now);
            contexts[i] = context;
            started[i] = Observation.createNotStarted(deliveryConvention,
                    DefaultOutboxDeliveryObservationConvention.INSTANCE, () -> context, observations).start();
        }
        return new ObservedDelivery(records, started, contexts);
    }

    /** Opens the step observation's scope on the thread that steps; the step closes it there before it stops. */
    @Override
    public RelayStep stepStarted(List<Integer> shards) {
        var context = new OutboxStepContext(shards);
        var observation = Observation.createNotStarted(stepConvention, DefaultOutboxStepObservationConvention.INSTANCE,
                () -> context, observations).start();
        return new ObservedStep(observation, observation.openScope());
    }

    @Override
    public void enqueued(String topic) {
        metrics.enqueued(topic);
    }

    @Override
    public void delivered(String topic, Duration delay) {
        metrics.delivered(topic, delay);
    }

    @Override
    public void retried(String topic) {
        metrics.retried(topic);
    }

    @Override
    public void deadLettered(String topic) {
        metrics.deadLettered(topic);
    }

    @Override
    public void requeued(String topic) {
        metrics.requeued(topic);
    }

    @Override
    public void stepFinished(Duration duration) {
        metrics.stepFinished(duration);
    }

    @Override
    public void relayFailed(Throwable error) {
        metrics.relayFailed(error);
    }

    @Override
    public void shardPaused() {
        metrics.shardPaused();
    }

    @Override
    public void deadLetterDecision(String decision) {
        metrics.deadLetterDecision(decision);
    }

    @Override
    public void deadLetterHookFailed() {
        metrics.deadLetterHookFailed();
    }

    @Override
    public void inboxOutsideWindow() {
        metrics.inboxOutsideWindow();
    }

    @Override
    public void gauge(OutboxGauge gauge, DoubleSupplier value) {
        metrics.gauge(gauge, value);
    }

    /** Stores the injected trace headers unless a message has a header of one of their names. */
    private record ObservedEnqueue(Observation observation, List<OutboxHeader> injected) implements Enqueue {
        @Override
        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
            if (injected.isEmpty()) {
                return headers;
            }
            for (var h : headers) {
                for (var i : injected) {
                    if (h.name().equals(i.name())) {
                        return headers;
                    }
                }
            }
            var all = new ArrayList<OutboxHeader>(headers.size() + injected.size());
            all.addAll(headers);
            all.addAll(injected);
            return all;
        }

        @Override
        public void finished() {
            observation.stop();
        }

        @Override
        public void failed(Throwable error) {
            observation.error(error);
            observation.stop();
        }
    }

    /** The scope is closed even when recording the error fails: no step stays current on the relay thread. */
    private record ObservedStep(Observation observation, Observation.Scope scope) implements RelayStep {
        @Override
        public void finished() {
            scope.close();
            observation.stop();
        }

        @Override
        public void failed(Throwable error) {
            try {
                observation.error(error);
            } finally {
                scope.close();
            }
            observation.stop();
        }
    }

    /** The observations of one handler call, in record order; started on the relay thread, stopped there too. */
    private final class ObservedDelivery implements Delivery {

        private final Observation[] started;
        private final OutboxDeliveryContext[] contexts;
        /** The observation of each record the handler may get: the stored ones and the handed ones. */
        private final Map<OutboxRecord, Observation> byRecord = new IdentityHashMap<>();

        ObservedDelivery(List<OutboxRecord> records, Observation[] started, OutboxDeliveryContext[] contexts) {
            this.started = started;
            this.contexts = contexts;
            for (int i = 0; i < started.length; i++) {
                byRecord.put(records.get(i), started[i]);
            }
        }

        @Override
        public List<OutboxRecord> handed(List<OutboxRecord> records) {
            if (injector == null) {
                return records;
            }
            var handed = new ArrayList<OutboxRecord>(records.size());
            for (int i = 0; i < records.size(); i++) {
                var record = injector.inject(started[i], records.get(i));
                byRecord.put(record, started[i]);
                handed.add(record);
            }
            return handed;
        }

        /** On the handler thread: the record's observation is the current one, its span the current span. */
        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            var observation = byRecord.get(record);
            if (observation == null) {
                return call.call();
            }
            try (var scope = observation.openScope()) {
                return call.call();
            }
        }

        @Override
        public void finished(List<Outcome> outcomes) {
            for (int i = 0; i < started.length; i++) {
                var outcome = outcomes.get(i);
                contexts[i].outcome(outcome);
                if (outcome instanceof Outcome.Retry r) {
                    started[i].error(r.error());
                } else if (outcome instanceof Outcome.Permanent p) {
                    started[i].error(p.error());
                }
                started[i].stop();
            }
        }

        @Override
        public void failed(Throwable error) {
            for (int i = 0; i < started.length; i++) {
                contexts[i].markFailed();
                started[i].error(error);
                started[i].stop();
            }
        }
    }

    public static final class Builder {

        private final ObservationRegistry observations;
        private final OutboxMetrics metrics;
        @Nullable
        private TraceInjector injector;
        @Nullable
        private OutboxEnqueueObservationConvention enqueueConvention;
        @Nullable
        private OutboxDeliveryObservationConvention deliveryConvention;
        @Nullable
        private OutboxStepObservationConvention stepConvention;

        private Builder(ObservationRegistry observations, OutboxMetrics metrics) {
            this.observations = Objects.requireNonNull(observations, "observations");
            this.metrics = Objects.requireNonNull(metrics, "metrics");
        }

        /**
         * The records handed to the handler carry their delivery span, as {@code propagator} writes it (its fields
         * replace the stored trace headers). Needs micrometer-tracing on the classpath; without this call it is not
         * needed, and handed records keep their headers.
         */
        public Builder tracing(Tracer tracer, Propagator propagator) {
            this.injector = new TraceInjector(Objects.requireNonNull(tracer, "tracer"),
                    Objects.requireNonNull(propagator, "propagator"));
            return this;
        }

        public Builder enqueueConvention(OutboxEnqueueObservationConvention convention) {
            this.enqueueConvention = Objects.requireNonNull(convention, "convention");
            return this;
        }

        public Builder deliveryConvention(OutboxDeliveryObservationConvention convention) {
            this.deliveryConvention = Objects.requireNonNull(convention, "convention");
            return this;
        }

        public Builder stepConvention(OutboxStepObservationConvention convention) {
            this.stepConvention = Objects.requireNonNull(convention, "convention");
            return this;
        }

        public ObservationOutboxTelemetry build() {
            return new ObservationOutboxTelemetry(this);
        }
    }
}
