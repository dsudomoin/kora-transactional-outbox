package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxConfig;
import jakarta.annotation.Nullable;

/**
 * The cursors' shard count as this instance's {@link Heartbeat} last read it, one per {@link OutboxSchema}: every write
 * of the instance ({@link PgOutbox}, {@link Resender}) refuses while it differs from the count the write shards by, as
 * after another instance's reshard: a row of a shard without a cursor would never be relayed, and one of a shard that
 * has a cursor would break its key's order. A failed business transaction beats a silently stranded message.
 */
final class ShardGuard {

    /** For components built from a config that no bootstrap resolved, as tests build them: checks nothing. */
    static final ShardGuard NONE = new ShardGuard();

    @Nullable
    private volatile Integer cursorShards;

    /** The guard of the {@link OutboxSchema#config()} {@code config} is, else {@link #NONE}. */
    static ShardGuard of(OutboxConfig config) {
        return config instanceof OutboxSchema.Resolved r ? r.guard() : NONE;
    }

    /** {@code shards}: the cursors' count just read; 0 (no cursors) checks nothing. */
    void observed(int shards) {
        if (this != NONE) {
            cursorShards = shards == 0 ? null : shards;
        }
    }

    /** Throws {@link IllegalStateException} while the cursors' count last read differs from {@code shards}. */
    void require(TableNames names, int shards) {
        var present = cursorShards;
        if (present != null && present != shards) {
            throw new IllegalStateException("outbox " + names.base() + " refuses this write: " + names.cursor()
                    + " holds cursors for " + present + " shards, but this instance writes with outbox.shards = "
                    + shards + " (another instance resharded it), so the row would be stranded or out of its key's"
                    + " order; restart this instance with outbox.shards = " + present + " or unset");
        }
    }
}
