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
            new $OutboxConfig_PartitionsConfig_ConfigValueExtractor(DURATION,
                    new $OutboxConfig_TailConversionConfig_ConfigValueExtractor(DURATION)),
            new $OutboxConfig_InboxConfig_ConfigValueExtractor(DURATION),
            new $OutboxConfig_DeadLettersConfig_ConfigValueExtractor(DURATION),
            new $OutboxConfig_RecoveryConfig_ConfigValueExtractor(DURATION));

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
                  partitions.restoreOrphans = false
                  inbox.retention = 2h
                  inbox.attachOrphans = false
                  deadLetters.retention = 90d
                  recovery.xidRegression = halt
                  recovery.maxRows = 5000
                  recovery.clockMargin = 1h
                  recovery.timeout = 30m
                }
                """);

        assertThat(config.shards()).isEqualTo(8);
        assertThat(config.relay().workers()).isEqualTo(8);
        assertThat(config.relay().minTickInterval()).isEqualTo(Duration.ofMillis(20));
        assertThat(config.relay().retry().maxAttempts()).isEqualTo(5);
        assertThat(config.partitions().xidsPerPartition()).isEqualTo(1000L);
        assertThat(config.partitions().restoreOrphans()).isFalse();
        assertThat(config.inbox().retention()).isEqualTo(Duration.ofHours(2));
        assertThat(config.inbox().attachOrphans()).isFalse();
        assertThat(config.deadLetters().retention()).isEqualTo(Duration.ofDays(90));
        assertThat(config.recovery().xidRegression()).isEqualTo("halt");
        assertThat(config.recovery().maxRows()).isEqualTo(5000);
        assertThat(config.recovery().clockMargin()).isEqualTo(Duration.ofHours(1));
        assertThat(config.recovery().timeout()).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void zeroSwapLockTimeoutIsNoWait() {
        var config = parse("outbox.partitions.swapLockTimeout = 0ms");

        OutboxSchema.validate(config);
        assertThat(config.partitions().swapLockTimeout()).isZero();
    }

    @Test
    void negativeSwapLockTimeoutIsRejected() {
        var config = parse("outbox.partitions.swapLockTimeout = -1ms");

        assertThatThrownBy(() -> OutboxSchema.validate(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("outbox.partitions.swapLockTimeout must be 0 or at least 1ms: PT-0.001S");
    }

    @Test
    void subMillisecondSwapLockTimeoutIsRejected() { // sent as whole milliseconds, and lock_timeout = 0 is no limit
        var config = parse("outbox.partitions.swapLockTimeout = 500us");

        assertThatThrownBy(() -> OutboxSchema.validate(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("outbox.partitions.swapLockTimeout must be 0 or at least 1ms: PT0.0005S");
    }

    @Test
    void negativeMaxTailScanIsRejected() {
        var config = parse("outbox.partitions.maxTailScan = -1");

        assertThatThrownBy(() -> OutboxSchema.validate(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("outbox.partitions.maxTailScan must not be negative: -1");
    }

    @Test
    void tailConversionIsReadAndValidated() {
        var tail = parse("outbox.partitions.tailConversion { window = \"23:00-01:00\", maxStall = 300ms }")
                .partitions().tailConversion();
        assertThat(tail.window()).isEqualTo("23:00-01:00");
        assertThat(tail.maxStall()).isEqualTo(Duration.ofMillis(300));
        OutboxSchema.validate(parse("outbox.partitions.tailConversion.window = \"\""));

        assertThatThrownBy(() -> OutboxSchema.validate(parse("outbox.partitions.tailConversion.window = 3-5")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outbox.partitions.tailConversion.window").hasMessageContaining("HH:mm-HH:mm");
        assertThatThrownBy(() -> OutboxSchema.validate(parse("outbox.partitions.tailConversion.maxStall = 0s")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("outbox.partitions.tailConversion.maxStall must be at least 1ms: PT0S");
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

    @Test
    void badDeadLettersValuesAreRejected() {
        assertRejected("outbox.deadLetters.batchSize = 0", "outbox.deadLetters.batchSize must be at least 1: 0");
        assertRejected("outbox.deadLetters.batchSize = 1001",
                "outbox.deadLetters.batchSize must be at most 1000: 1001");
        assertRejected("outbox.deadLetters.timeout = 0ms", "outbox.deadLetters.timeout must be positive: PT0S");
        assertRejected("outbox.deadLetters.retention = -1ms",
                "outbox.deadLetters.retention must not be negative: PT-0.001S");
        assertRejected("outbox.deadLetters.redriveHistory = 0ms",
                "outbox.deadLetters.redriveHistory must be positive: PT0S");
        assertRejected("outbox.deadLetters.retention = 3651d",
                "outbox.deadLetters.retention must be at most 3650d: PT87624H");
        assertRejected("outbox.deadLetters.redriveHistory = 3651d",
                "outbox.deadLetters.redriveHistory must be at most 3650d: PT87624H");
    }

    @Test
    void badRecoveryValuesAreRejected() {
        assertRejected("outbox.recovery.xidRegression = manual",
                "outbox.recovery.xidRegression must be auto or halt: manual");
        assertRejected("outbox.recovery.maxRows = -1", "outbox.recovery.maxRows must not be negative: -1");
        assertRejected("outbox.recovery.clockMargin = -1ms",
                "outbox.recovery.clockMargin must not be negative: PT-0.001S");
        assertRejected("outbox.recovery.timeout = 0ms", "outbox.recovery.timeout must be positive: PT0S");
    }

    private static void assertRejected(String hocon, String message) {
        var config = parse(hocon);

        assertThatThrownBy(() -> OutboxSchema.validate(config))
                .as(hocon)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    private static void assertDefaults(OutboxConfig config) {
        assertThat(config.table()).isEqualTo("outbox");
        assertThat(config.shards()).as("not set: an existing outbox keeps its count").isNull();

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
        assertThat(partitions.swapLockTimeout()).isZero();
        assertThat(partitions.maxTailScan()).isEqualTo(16L * 1024 * 1024);
        assertThat(partitions.restoreOrphans()).isTrue();
        assertThat(partitions.tailConversion().window()).isEqualTo("03:00-05:00");
        assertThat(partitions.tailConversion().maxStall()).isEqualTo(Duration.ofSeconds(1));

        var inbox = config.inbox();
        assertThat(inbox.table()).isEqualTo("inbox");
        assertThat(inbox.partitionInterval()).isEqualTo(Duration.ofHours(1));
        assertThat(inbox.premake()).isEqualTo(Duration.ofHours(24));
        assertThat(inbox.retention()).isEqualTo(Duration.ofHours(24));
        assertThat(inbox.attachOrphans()).isTrue();

        var deadLetters = config.deadLetters();
        assertThat(deadLetters.batchSize()).isEqualTo(100);
        assertThat(deadLetters.timeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(deadLetters.retention()).isZero();
        assertThat(deadLetters.redriveHistory()).isEqualTo(Duration.ofDays(30));

        var recovery = config.recovery();
        assertThat(recovery.xidRegression()).isEqualTo("auto");
        assertThat(recovery.maxRows()).isEqualTo(1_000_000L);
        assertThat(recovery.clockMargin()).isEqualTo(Duration.ofMinutes(10));
        assertThat(recovery.timeout()).isEqualTo(Duration.ofMinutes(10));
    }
}
