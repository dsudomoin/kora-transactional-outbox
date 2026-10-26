package io.github.dsudomoin.kora.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.typesafe.config.ConfigFactory;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.config.common.extractor.DurationConfigValueExtractor;
import ru.tinkoff.kora.config.common.origin.SimpleConfigOrigin;
import ru.tinkoff.kora.config.hocon.HoconConfigFactory;

class OutboxConfigTest {

    private static final DurationConfigValueExtractor DURATION = new DurationConfigValueExtractor();
    private static final $OutboxConfig_ConfigValueExtractor EXTRACTOR = new $OutboxConfig_ConfigValueExtractor(
            new $OutboxConfig_RelayConfig_ConfigValueExtractor(DURATION,
                    new $OutboxConfig_RetryConfig_ConfigValueExtractor(DURATION),
                    new $OutboxConfig_RequeueConfig_ConfigValueExtractor(DURATION)),
            new $OutboxConfig_PartitionsConfig_ConfigValueExtractor(DURATION),
            new $OutboxConfig_InboxConfig_ConfigValueExtractor(DURATION));

    private static OutboxConfig parse(String hocon) {
        var config = HoconConfigFactory.fromHocon(
                new SimpleConfigOrigin("test"), ConfigFactory.parseString(hocon).resolve());
        return EXTRACTOR.extract(config.get("outbox"));
    }

    @Test
    void defaultsWhenBlockEmpty() {
        assertDefaults(parse("outbox {}"));
        assertDefaults(parse(""));
    }

    @Test
    void defaultsWhenNestedBlocksMissing() {
        var config = parse("outbox { table = orders_outbox, relay { workers = 2 } }");

        assertThat(config.table()).isEqualTo("orders_outbox");
        assertThat(config.relay().workers()).isEqualTo(2);
        assertThat(config.relay().batchSize()).isEqualTo(1000);
        assertThat(config.relay().retry().initialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.partitions().xidsPerPartition()).isEqualTo(16_777_216L);
        assertThat(config.inbox().table()).isEqualTo("inbox");
    }

    @Test
    void overridesApply() {
        var config = parse("""
                outbox {
                  shards = 8
                  relay.workers = 8
                  relay.minTickInterval = 20ms
                  relay.retry.maxAttempts = 5
                  partitions.xidsPerPartition = 1000
                  inbox.retention = 2h
                }
                """);

        assertThat(config.shards()).isEqualTo(8);
        assertThat(config.relay().workers()).isEqualTo(8);
        assertThat(config.relay().minTickInterval()).isEqualTo(Duration.ofMillis(20));
        assertThat(config.relay().retry().maxAttempts()).isEqualTo(5);
        assertThat(config.partitions().xidsPerPartition()).isEqualTo(1000L);
        assertThat(config.inbox().retention()).isEqualTo(Duration.ofHours(2));
    }

    @Test
    void requeueKeysAreReadAndValidated() {
        var requeue = parse("outbox.relay.requeue { delay = 250ms, maxRequeues = 5 }").relay().requeue();

        assertThat(requeue.delay()).isEqualTo(Duration.ofMillis(250));
        assertThat(requeue.maxRequeues()).isEqualTo(5);
        assertThatThrownBy(() -> OutboxSchema.validate(parse("outbox.relay.requeue.delay = 0s")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("outbox.relay.requeue.delay must be positive: PT0S");
        assertThatThrownBy(() -> OutboxSchema.validate(parse("outbox.relay.requeue.maxRequeues = -1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("outbox.relay.requeue.maxRequeues must not be negative: -1");
    }

    private static void assertDefaults(OutboxConfig config) {
        assertThat(config.table()).isEqualTo("outbox");
        assertThat(config.shards()).isEqualTo(32);

        var relay = config.relay();
        assertThat(relay.enabled()).isTrue();
        assertThat(relay.workers()).isEqualTo(4);
        assertThat(relay.batchSize()).isEqualTo(1000);
        assertThat(relay.maxTicksPerBatch()).isEqualTo(100);
        assertThat(relay.minTickInterval()).isEqualTo(Duration.ofMillis(50));
        assertThat(relay.idlePollInterval()).isEqualTo(Duration.ofMillis(200));
        assertThat(relay.batchTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(relay.shutdownTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(relay.headFailuresBeforeDivert()).isEqualTo(3);
        assertThat(relay.retry().initialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(relay.retry().maxDelay()).isEqualTo(Duration.ofMinutes(5));
        assertThat(relay.retry().maxAttempts()).isZero();
        assertThat(relay.requeue().delay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(relay.requeue().maxRequeues()).isZero();

        var partitions = config.partitions();
        assertThat(partitions.xidsPerPartition()).isEqualTo(16_777_216L);
        assertThat(partitions.premake()).isEqualTo(64);
        assertThat(partitions.maintenanceInterval()).isEqualTo(Duration.ofSeconds(30));

        var inbox = config.inbox();
        assertThat(inbox.table()).isEqualTo("inbox");
        assertThat(inbox.partitionInterval()).isEqualTo(Duration.ofHours(1));
        assertThat(inbox.premake()).isEqualTo(Duration.ofHours(24));
        assertThat(inbox.retention()).isEqualTo(Duration.ofHours(24));
    }
}
