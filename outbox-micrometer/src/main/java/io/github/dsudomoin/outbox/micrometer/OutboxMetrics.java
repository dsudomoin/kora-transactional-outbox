package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleSupplier;

/**
 * The outbox's meters on a Micrometer {@link MeterRegistry} ({@code outbox.*}); every method is a no-op without a
 * registry. Events and gauges only: traces need an implementation of the enqueue and delivery hooks on top.
 */
public final class OutboxMetrics implements OutboxTelemetry {

    @Nullable
    private final MeterRegistry registry;

    private final Map<String, TopicMeters> topics = new ConcurrentHashMap<>();

    public OutboxMetrics(@Nullable MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void enqueued(String topic) {
        if (registry != null) {
            meters(topic).enqueued.increment();
        }
    }

    @Override
    public void delivered(String topic, Duration delay) {
        if (registry != null) {
            var m = meters(topic);
            m.delay.record(delay);
            m.delivered.increment();
        }
    }

    @Override
    public void retried(String topic) {
        if (registry != null) {
            meters(topic).failedRetry.increment();
        }
    }

    @Override
    public void deadLettered(String topic) {
        if (registry != null) {
            meters(topic).failedDead.increment();
        }
    }

    @Override
    public void requeued(String topic) {
        if (registry != null) {
            meters(topic).requeued.increment();
        }
    }

    /** {@code outbox.messages.waiting{topic, kind}}; the rare path, so the registry looks the counter up. */
    @Override
    public void waited(String topic, Outcome.Waiting.Kind kind) {
        if (registry != null) {
            registry.counter("outbox.messages.waiting", "topic", topic, "kind", kind.label()).increment();
        }
    }

    @Override
    public void stepFinished(Duration duration) {
        if (registry != null) {
            Timer.builder("outbox.batch.duration").publishPercentileHistogram().register(registry).record(duration);
        }
    }

    @Override
    public void relayFailed(Throwable error) {
        if (registry != null) {
            registry.counter("outbox.relay.failures").increment();
        }
    }

    @Override
    public void shardPaused() {
        if (registry != null) {
            registry.counter("outbox.shard.paused").increment();
        }
    }

    @Override
    public void inboxOutsideWindow() {
        if (registry != null) {
            registry.counter("outbox.inbox.outside_window").increment();
        }
    }

    @Override
    public void deadLetterDecision(String decision) {
        if (registry != null) {
            registry.counter("outbox.dead.decisions", "decision", decision).increment();
        }
    }

    @Override
    public void deadLetterHookFailed() {
        if (registry != null) {
            registry.counter("outbox.dead.hook.failures").increment();
        }
    }

    /** Registers the gauge under {@link OutboxGauge#metricName()} once; later calls keep the first supplier. */
    @Override
    public void gauge(OutboxGauge gauge, DoubleSupplier value) {
        if (registry != null) {
            Gauge.builder(gauge.metricName(), value::getAsDouble).register(registry);
        }
    }

    private TopicMeters meters(String topic) {
        var m = topics.get(topic); // plain get first: no lambda allocation on the hot path
        return m != null ? m : topics.computeIfAbsent(topic, t -> new TopicMeters(registry, t));
    }

    private static final class TopicMeters {
        final Counter enqueued;
        final Counter delivered;
        final Timer delay;
        final Counter failedRetry;
        final Counter failedDead;
        final Counter requeued;

        TopicMeters(MeterRegistry registry, String topic) {
            enqueued = registry.counter("outbox.messages.enqueued", "topic", topic);
            delivered = registry.counter("outbox.messages.delivered", "topic", topic);
            // outages and retries push delays far past Micrometer's default 30s ceiling
            delay = Timer.builder("outbox.delivery.delay")
                    .tag("topic", topic)
                    .publishPercentileHistogram()
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofHours(6))
                    .register(registry);
            failedRetry = registry.counter("outbox.messages.failed", "topic", topic, "outcome", "retry");
            failedDead = registry.counter("outbox.messages.failed", "topic", topic, "outcome", "dead");
            requeued = registry.counter("outbox.messages.requeued", "topic", topic);
        }
    }
}
