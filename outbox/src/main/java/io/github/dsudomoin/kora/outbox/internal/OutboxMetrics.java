package io.github.dsudomoin.kora.outbox.internal;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Micrometer facade; every method is a no-op without a registry. */
public final class OutboxMetrics {

    @Nullable
    private final MeterRegistry registry;

    private final Map<String, TopicMeters> topics = new ConcurrentHashMap<>();

    public OutboxMetrics(@Nullable MeterRegistry registry) {
        this.registry = registry;
    }

    public void enqueued(String topic) {
        if (registry != null) {
            meters(topic).enqueued.increment();
        }
    }

    public void delivered(String topic, Duration delay) {
        if (registry != null) {
            var m = meters(topic);
            m.delay.record(delay);
            m.delivered.increment();
        }
    }

    /** {@code outcome} is {@code retry} or {@code dead}. */
    public void failed(String topic, String outcome) {
        if (registry != null) {
            switch (outcome) {
                case "retry" -> meters(topic).failedRetry.increment();
                case "dead" -> meters(topic).failedDead.increment();
                default -> registry.counter("outbox.messages.failed", "topic", topic, "outcome", outcome).increment();
            }
        }
    }

    public void batch(Duration duration) {
        if (registry != null) {
            Timer.builder("outbox.batch.duration").publishPercentileHistogram().register(registry).record(duration);
        }
    }

    public void shardPaused() {
        if (registry != null) {
            registry.counter("outbox.shard.paused").increment();
        }
    }

    public void inboxOutsideWindow() {
        if (registry != null) {
            registry.counter("outbox.inbox.outside_window").increment();
        }
    }

    /**
     * Registers the gauge once; later calls with the same name keep the first supplier. Names: {@code
     * outbox.shard.lag}, {@code outbox.retry.size}, {@code outbox.dead.size}, {@code outbox.parked.size},
     * {@code outbox.partitions.premade}, {@code outbox.partitions.overflow}.
     */
    public void gauge(String name, Supplier<Number> value) {
        if (registry != null) {
            Gauge.builder(name, value).register(registry);
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
        }
    }
}
