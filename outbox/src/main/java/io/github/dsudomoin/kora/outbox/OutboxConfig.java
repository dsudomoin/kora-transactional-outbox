package io.github.dsudomoin.kora.outbox;

import java.time.Duration;
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/** HOCON path {@code outbox}; every key is optional. */
@ConfigValueExtractor
public interface OutboxConfig {

    default String table() {
        return "outbox";
    }

    default int shards() {
        return 32;
    }

    default RelayConfig relay() {
        return new RelayConfig() {};
    }

    default PartitionsConfig partitions() {
        return new PartitionsConfig() {};
    }

    default InboxConfig inbox() {
        return new InboxConfig() {};
    }

    @ConfigValueExtractor
    interface RelayConfig {

        default boolean enabled() {
            return true;
        }

        default int workers() {
            return 4;
        }

        /**
         * Rows per chunk read and per handler call. Keep {@code batchSize ×} the handler's time per record well below
         * {@link #batchTimeout()}: a longer call is cut at its soft deadline and the shard moves on only by the records
         * the handler finished.
         */
        default int batchSize() {
            return 1000;
        }

        default int maxTicksPerBatch() {
            return 100;
        }

        default Duration minTickInterval() {
            return Duration.ofMillis(50);
        }

        default Duration idlePollInterval() {
            return Duration.ofMillis(200);
        }

        /**
         * Bound of one handler call. At 80 % of it the handler thread is interrupted and the call's real outcomes are
         * kept (records it did not start are read again); a call still running at the full timeout is cancelled and
         * all its records are retried, delivered ones included. Per-record timeouts of the handler (HTTP read
         * timeouts, each {@code byTopic} route's own) must be well below it. See {@link OutboxHandler#handleBatch}.
         */
        default Duration batchTimeout() {
            return Duration.ofSeconds(30);
        }

        default Duration shutdownTimeout() {
            return Duration.ofSeconds(30);
        }

        default int headFailuresBeforeDivert() {
            return 3;
        }

        default RetryConfig retry() {
            return new RetryConfig() {};
        }

        default RequeueConfig requeue() {
            return new RequeueConfig() {};
        }
    }

    @ConfigValueExtractor
    interface RetryConfig {

        default Duration initialDelay() {
            return Duration.ofSeconds(1);
        }

        default Duration maxDelay() {
            return Duration.ofMinutes(5);
        }

        /** 0 = retry forever. */
        default int maxAttempts() {
            return 0;
        }
    }

    /** Records the handler set aside with {@link Outcome#requeue()}. */
    @ConfigValueExtractor
    interface RequeueConfig {

        /** A requeued record is due {@code delay} later, or at the handler's later {@code notBefore}. */
        default Duration delay() {
            return Duration.ofSeconds(1);
        }

        /** Requeues a record may take; the one past it moves the record to the dead table. 0 = unlimited. */
        default int maxRequeues() {
            return 0;
        }
    }

    @ConfigValueExtractor
    interface PartitionsConfig {

        default long xidsPerPartition() {
            return 16_777_216L;
        }

        default int premake() {
            return 64;
        }

        default Duration maintenanceInterval() {
            return Duration.ofSeconds(30);
        }

        /** Lock wait bound of the short tail-swap transaction, so of a writer's stall per attempt. Default 200 ms. */
        default Duration swapLockTimeout() {
            return Duration.ofMillis(200);
        }

        /** Move rows above the horizon in a range marked for drop back to the outbox; false only logs. Default true. */
        default boolean restoreOrphans() {
            return true;
        }
    }

    @ConfigValueExtractor
    interface InboxConfig {

        default String table() {
            return "inbox";
        }

        default Duration partitionInterval() {
            return Duration.ofHours(1);
        }

        default Duration premake() {
            return Duration.ofHours(24);
        }

        default Duration retention() {
            return Duration.ofHours(24);
        }

        /** Attach a range detached inside the dedup window back by its name's bounds; false only logs. Default true. */
        default boolean attachOrphans() {
            return true;
        }
    }
}
