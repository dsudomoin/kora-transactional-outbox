package io.github.dsudomoin.outbox.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class OutboxMetricsTest {

    private final SimpleMeterRegistry registry = new PrometheusLikeRegistry();
    private final OutboxMetrics metrics = new OutboxMetrics(registry);

    @Test
    void nullRegistryIsNoop() {
        var noop = new OutboxMetrics(null);

        assertThatCode(() -> {
            noop.enqueued("orders");
            noop.delivered("orders", Duration.ofMillis(5));
            noop.retried("orders");
            noop.requeued("orders");
            noop.waited("orders", Outcome.Waiting.Kind.HANDLER);
            noop.contractRejected("orders", "Created", 2);
            noop.routeRejected("orders");
            noop.routeUnchecked("orders");
            noop.stepFinished(Duration.ofMillis(5));
            noop.shardPaused();
            noop.inboxOutsideWindow();
            noop.deadLetterDecision("keep");
            noop.deadLetterHookFailed();
            noop.relayFailed(new SQLException("x"));
            noop.gauge(OutboxGauge.RETRY_SIZE, () -> 1);
        }).doesNotThrowAnyException();
    }

    @Test
    void deliveredRecordsTimerAndCounterWithTopicTag() {
        metrics.delivered("orders", Duration.ofMillis(10));
        metrics.delivered("orders", Duration.ofMillis(30));
        metrics.delivered("payments", Duration.ofMillis(5));

        var timer = registry.get("outbox.delivery.delay").tag("topic", "orders").timer();
        assertThat(timer.count()).isEqualTo(2);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(40);
        assertThat(registry.get("outbox.messages.delivered").tag("topic", "orders").counter().count()).isEqualTo(2);
        assertThat(registry.get("outbox.messages.delivered").tag("topic", "payments").counter().count()).isEqualTo(1);
    }

    @Test
    void delayTimerPublishesHistogram() {
        metrics.delivered("orders", Duration.ofMillis(10));

        var timer = registry.get("outbox.delivery.delay").tag("topic", "orders").timer();
        assertThat(timer.takeSnapshot().histogramCounts()).isNotEmpty();
    }

    @Test
    void delayHistogramCoversOutages() {
        metrics.delivered("orders", Duration.ofMinutes(10));

        var counts = registry.get("outbox.delivery.delay").timer().takeSnapshot().histogramCounts();
        assertThat(counts[counts.length - 1].bucket(TimeUnit.SECONDS)).isGreaterThanOrEqualTo(6 * 3600);
        assertThat(Arrays.stream(counts).filter(b -> b.bucket(TimeUnit.SECONDS) >= 600).findFirst())
                .hasValueSatisfying(b -> assertThat(b.count()).isEqualTo(1));
    }

    @Test
    void metersAreCachedPerTopic() {
        metrics.delivered("orders", Duration.ofMillis(1));
        metrics.delivered("orders", Duration.ofMillis(2));

        assertThat(registry.find("outbox.delivery.delay").timers()).hasSize(1);
        assertThat(registry.find("outbox.messages.delivered").counters()).hasSize(1);

        metrics.delivered("payments", Duration.ofMillis(1));

        assertThat(registry.find("outbox.delivery.delay").timers()).hasSize(2);
        assertThat(registry.find("outbox.messages.delivered").counters()).hasSize(2);
        assertThat(registry.get("outbox.delivery.delay").tag("topic", "payments").timer().count()).isEqualTo(1);
    }

    @Test
    void countersBatchAndGaugesUseSpecNames() {
        metrics.enqueued("orders");
        metrics.retried("orders");
        metrics.deadLettered("orders");
        metrics.deadLettered("orders");
        metrics.stepFinished(Duration.ofMillis(7));
        metrics.shardPaused();
        metrics.inboxOutsideWindow();
        metrics.gauge(OutboxGauge.RETRY_SIZE, () -> 42);
        metrics.gauge(OutboxGauge.RETRY_SIZE, () -> 99);

        assertThat(registry.get("outbox.messages.enqueued").tag("topic", "orders").counter().count()).isEqualTo(1);
        assertThat(registry.get("outbox.messages.failed").tags("topic", "orders", "outcome", "retry").counter().count())
                .isEqualTo(1);
        assertThat(registry.get("outbox.messages.failed").tags("topic", "orders", "outcome", "dead").counter().count())
                .isEqualTo(2);
        var batch = registry.get("outbox.batch.duration").timer();
        assertThat(batch.count()).isEqualTo(1);
        assertThat(batch.takeSnapshot().histogramCounts()).isNotEmpty();
        assertThat(registry.get("outbox.shard.paused").counter().count()).isEqualTo(1);
        assertThat(registry.get("outbox.inbox.outside_window").counter().count()).isEqualTo(1);
        assertThat(registry.get("outbox.retry.size").gauges()).hasSize(1);
        assertThat(registry.get("outbox.retry.size").gauge().value()).isEqualTo(42);
    }

    @Test
    void requeuedCountsPerTopic() {
        metrics.requeued("orders");
        metrics.requeued("orders");
        metrics.requeued("payments");

        assertThat(registry.get("outbox.messages.requeued").tag("topic", "orders").counter().count()).isEqualTo(2);
        assertThat(registry.get("outbox.messages.requeued").tag("topic", "payments").counter().count()).isEqualTo(1);
    }

    @Test
    void waitedCountsPerTopicAndKind() {
        metrics.waited("orders", Outcome.Waiting.Kind.CONTRACT);
        metrics.waited("orders", Outcome.Waiting.Kind.CONTRACT);
        metrics.waited("orders", Outcome.Waiting.Kind.NO_ROUTE);
        metrics.waited("payments", Outcome.Waiting.Kind.HANDLER);

        assertThat(waits("orders", "contract")).isEqualTo(2);
        assertThat(waits("orders", "no_route")).isEqualTo(1);
        assertThat(waits("payments", "handler")).isEqualTo(1);
        assertThat(registry.find("outbox.messages.waiting").counters()).as("no series for kinds never seen").hasSize(3);
        assertThat(registry.find("outbox.messages.failed").counters()).as("not a failure").isEmpty();
    }

    @Test
    void contractRejectedCountsPerTopicTypeAndVersion() {
        metrics.contractRejected("orders", "com.acme.OrderCreated", 2);
        metrics.contractRejected("orders", "com.acme.OrderCreated", 2);
        metrics.contractRejected("orders", "?", 3);

        assertThat(registry.get("outbox.messages.contract_rejected")
                .tags("topic", "orders", "type", "com.acme.OrderCreated", "version", "2").counter().count())
                .isEqualTo(2);
        assertThat(registry.get("outbox.messages.contract_rejected")
                .tags("topic", "orders", "type", "?", "version", "3").counter().count()).isEqualTo(1);
    }

    @Test
    void routeRejectedAndRouteUncheckedCountPerTopic() {
        metrics.routeRejected("orders");
        metrics.routeRejected("orders");
        metrics.routeRejected("audit");
        metrics.routeUnchecked("orders");

        assertThat(registry.get("outbox.messages.route_rejected").tags("topic", "orders").counter().count())
                .isEqualTo(2);
        assertThat(registry.get("outbox.messages.route_rejected").tags("topic", "audit").counter().count())
                .isEqualTo(1);
        assertThat(registry.get("outbox.messages.route_unchecked").tags("topic", "orders").counter().count())
                .isEqualTo(1);
    }

    @Test
    void relayFailuresCount() {
        metrics.relayFailed(new SQLException("x"));
        metrics.relayFailed(new SQLException("y"));

        assertThat(registry.counter("outbox.relay.failures").count()).isEqualTo(2.0);
    }

    private double waits(String topic, String kind) {
        return registry.get("outbox.messages.waiting").tags("topic", topic, "kind", kind).counter().count();
    }

    /** SimpleMeterRegistry never materializes percentile-histogram buckets; Prometheus does, like this one. */
    private static final class PrometheusLikeRegistry extends SimpleMeterRegistry {
        @Override
        protected Timer newTimer(Meter.Id id, DistributionStatisticConfig config, PauseDetector pauseDetector) {
            return new CumulativeTimer(id, clock, config, pauseDetector, getBaseTimeUnit(), true);
        }
    }
}
