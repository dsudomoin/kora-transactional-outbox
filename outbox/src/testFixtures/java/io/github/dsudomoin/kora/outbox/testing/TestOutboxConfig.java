package io.github.dsudomoin.kora.outbox.testing;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import java.time.Duration;

/**
 * Mutable config with fast test defaults and fresh {@link TestDb#uniqueTable()} names for both tables. Relay, retry,
 * requeue and partition settings live on this object itself ({@link #relay()}, {@link #retry()}, {@link #requeue()}
 * and {@link #partitions()} return {@code this}); the inbox block is separate because its {@code table()} and
 * {@code premake()} clash with the outbox ones.
 */
public final class TestOutboxConfig implements OutboxConfig, OutboxConfig.RelayConfig, OutboxConfig.RetryConfig,
        OutboxConfig.RequeueConfig, OutboxConfig.PartitionsConfig {

    private String table = TestDb.uniqueTable();
    private int shards = 4;
    private boolean relayEnabled = true;
    private int workers = 2;
    private int batchSize = 100;
    private int maxTicksPerBatch = 100;
    private Duration minTickInterval = Duration.ofMillis(5);
    private Duration idlePollInterval = Duration.ofMillis(20);
    private Duration batchTimeout = Duration.ofSeconds(5);
    private Duration shutdownTimeout = Duration.ofSeconds(5);
    private int headFailuresBeforeDivert = 3;
    private Duration retryInitial = Duration.ofMillis(50);
    private Duration retryMax = Duration.ofMillis(500);
    private int retryMaxAttempts = 0;
    private Duration requeueDelay = Duration.ofMillis(100);
    private int maxRequeues = 0;
    private long xidsPerPartition = 1_000_000L;
    private int premake = 4;
    private Duration maintenanceInterval = Duration.ofHours(1); // tests call maintenance directly
    private String inboxTable = TestDb.uniqueTable();
    private Duration inboxPartitionInterval = Duration.ofHours(1);
    private Duration inboxPremake = Duration.ofHours(24);
    private Duration inboxRetention = Duration.ofHours(24);

    // OutboxConfig

    @Override
    public String table() {
        return table;
    }

    public TestOutboxConfig table(String table) {
        this.table = table;
        return this;
    }

    @Override
    public int shards() {
        return shards;
    }

    public TestOutboxConfig shards(int shards) {
        this.shards = shards;
        return this;
    }

    @Override
    public RelayConfig relay() {
        return this;
    }

    @Override
    public PartitionsConfig partitions() {
        return this;
    }

    @Override
    public InboxConfig inbox() {
        return new InboxConfig() {
            @Override
            public String table() {
                return inboxTable;
            }

            @Override
            public Duration partitionInterval() {
                return inboxPartitionInterval;
            }

            @Override
            public Duration premake() {
                return inboxPremake;
            }

            @Override
            public Duration retention() {
                return inboxRetention;
            }
        };
    }

    // RelayConfig

    @Override
    public boolean enabled() {
        return relayEnabled;
    }

    public TestOutboxConfig relayEnabled(boolean relayEnabled) {
        this.relayEnabled = relayEnabled;
        return this;
    }

    @Override
    public int workers() {
        return workers;
    }

    public TestOutboxConfig workers(int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public int batchSize() {
        return batchSize;
    }

    public TestOutboxConfig batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public int maxTicksPerBatch() {
        return maxTicksPerBatch;
    }

    public TestOutboxConfig maxTicksPerBatch(int maxTicksPerBatch) {
        this.maxTicksPerBatch = maxTicksPerBatch;
        return this;
    }

    @Override
    public Duration minTickInterval() {
        return minTickInterval;
    }

    public TestOutboxConfig minTickInterval(Duration minTickInterval) {
        this.minTickInterval = minTickInterval;
        return this;
    }

    @Override
    public Duration idlePollInterval() {
        return idlePollInterval;
    }

    public TestOutboxConfig idlePollInterval(Duration idlePollInterval) {
        this.idlePollInterval = idlePollInterval;
        return this;
    }

    @Override
    public Duration batchTimeout() {
        return batchTimeout;
    }

    public TestOutboxConfig batchTimeout(Duration batchTimeout) {
        this.batchTimeout = batchTimeout;
        return this;
    }

    @Override
    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    public TestOutboxConfig shutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
        return this;
    }

    @Override
    public int headFailuresBeforeDivert() {
        return headFailuresBeforeDivert;
    }

    public TestOutboxConfig headFailuresBeforeDivert(int headFailuresBeforeDivert) {
        this.headFailuresBeforeDivert = headFailuresBeforeDivert;
        return this;
    }

    @Override
    public RetryConfig retry() {
        return this;
    }

    public TestOutboxConfig retry(Duration initialDelay, Duration maxDelay, int maxAttempts) {
        this.retryInitial = initialDelay;
        this.retryMax = maxDelay;
        this.retryMaxAttempts = maxAttempts;
        return this;
    }

    @Override
    public RequeueConfig requeue() {
        return this;
    }

    // RetryConfig

    @Override
    public Duration initialDelay() {
        return retryInitial;
    }

    @Override
    public Duration maxDelay() {
        return retryMax;
    }

    @Override
    public int maxAttempts() {
        return retryMaxAttempts;
    }

    // RequeueConfig

    @Override
    public Duration delay() {
        return requeueDelay;
    }

    public TestOutboxConfig requeueDelay(Duration requeueDelay) {
        this.requeueDelay = requeueDelay;
        return this;
    }

    @Override
    public int maxRequeues() {
        return maxRequeues;
    }

    public TestOutboxConfig maxRequeues(int maxRequeues) {
        this.maxRequeues = maxRequeues;
        return this;
    }

    // PartitionsConfig

    @Override
    public long xidsPerPartition() {
        return xidsPerPartition;
    }

    public TestOutboxConfig xidsPerPartition(long xidsPerPartition) {
        this.xidsPerPartition = xidsPerPartition;
        return this;
    }

    @Override
    public int premake() {
        return premake;
    }

    public TestOutboxConfig premake(int premake) {
        this.premake = premake;
        return this;
    }

    @Override
    public Duration maintenanceInterval() {
        return maintenanceInterval;
    }

    public TestOutboxConfig maintenanceInterval(Duration maintenanceInterval) {
        this.maintenanceInterval = maintenanceInterval;
        return this;
    }

    // InboxConfig

    public TestOutboxConfig inboxTable(String inboxTable) {
        this.inboxTable = inboxTable;
        return this;
    }

    public TestOutboxConfig inboxPartitionInterval(Duration inboxPartitionInterval) {
        this.inboxPartitionInterval = inboxPartitionInterval;
        return this;
    }

    public TestOutboxConfig inboxPremake(Duration inboxPremake) {
        this.inboxPremake = inboxPremake;
        return this;
    }

    public TestOutboxConfig inboxRetention(Duration inboxRetention) {
        this.inboxRetention = inboxRetention;
        return this;
    }
}
