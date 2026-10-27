package io.github.dsudomoin.outbox;

/** The values the outbox reports as gauges; {@link #metricName()} is the name its own meters use. */
public enum OutboxGauge {
    SHARD_LAG("outbox.shard.lag"),
    RETRY_SIZE("outbox.retry.size"),
    DEAD_SIZE("outbox.dead.size"),
    DEAD_PENDING("outbox.dead.pending"),
    DEAD_PENDING_OLDEST("outbox.dead.pending.oldest"),
    PARKED_SIZE("outbox.parked.size"),
    PARTITIONS_PREMADE("outbox.partitions.premade"),
    PARTITIONS_OVERFLOW("outbox.partitions.overflow"),
    PARTITIONS_XIDS_LEFT("outbox.partitions.xids_left"),
    PARTITIONS_ORPHANED("outbox.partitions.orphaned"),
    MAINTENANCE_TRANSACTION_POOLING("outbox.maintenance.transaction_pooling"),
    MAINTENANCE_SINCE_SUCCESS("outbox.maintenance.since_success"),
    INBOX_MAINTENANCE_TRANSACTION_POOLING("outbox.inbox.maintenance.transaction_pooling"),
    INBOX_PARTITIONS_ORPHANED("outbox.inbox.partitions.orphaned");

    private final String metricName;

    OutboxGauge(String metricName) {
        this.metricName = metricName;
    }

    public String metricName() {
        return metricName;
    }
}
