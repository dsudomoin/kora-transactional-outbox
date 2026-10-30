package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.time.Duration;

/**
 * The outbox's settings; every one has a default: implement it and override what you change
 * ({@code new OutboxConfig() {}} is all defaults). The Kora adapter reads it from HOCON path {@code outbox}, keys named
 * as here.
 */
public interface OutboxConfig {

    default String table() {
        return "outbox";
    }

    /** The shard count of a new outbox when {@link #shards()} is not set. */
    int DEFAULT_SHARDS = 256;

    /**
     * Null when not set: an existing outbox keeps the shard count of its cursors, a new one gets
     * {@link #DEFAULT_SHARDS}. A set value that differs from the cursors' reshards at bootstrap, which refuses unless
     * the outbox is drained and no other instance runs ({@code OutboxSchema}).
     */
    @Nullable
    default Integer shards() {
        return null;
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

    default DeadLettersConfig deadLetters() {
        return new DeadLettersConfig() {};
    }

    default RecoveryConfig recovery() {
        return new RecoveryConfig() {};
    }

    default SchemaConfig schema() {
        return new SchemaConfig() {};
    }

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

        /**
         * How long an idle shard's latest cursor row may stay on a tick below its effective one before maintenance
         * appends one at the effective tick (an anchor): ticks are kept from the lowest latest cursor row's tick on,
         * as the previous release starts its steps there. At least {@code outbox.partitions.maintenanceInterval}.
         */
        default Duration cursorAnchorInterval() {
            return Duration.ofHours(1);
        }

        default RetryConfig retry() {
            return new RetryConfig() {};
        }

        default RequeueConfig requeue() {
            return new RequeueConfig() {};
        }

        default WaitConfig waiting() {
            return new WaitConfig() {};
        }
    }

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

    /**
     * Records waiting for an instance that can handle them ({@link Outcome.Waiting}): each is handed again after a
     * quarter of the time it has waited, at least {@code delay} and at most {@code maxDelay} later.
     */
    interface WaitConfig {

        default Duration delay() {
            return Duration.ofSeconds(1);
        }

        default Duration maxDelay() {
            return Duration.ofMinutes(5);
        }

        /** A record waiting longer turns the periodic WARN about waiting records into an ERROR. */
        default Duration alertAfter() {
            return Duration.ofMinutes(15);
        }
    }

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

        /**
         * How long the tail swap waits in the queue for its lock on the outbox once its tries without waiting failed,
         * writers queueing behind it meanwhile: keep it below their own {@code lock_timeout} and
         * {@code statement_timeout}. 0 (default): no wait, so writers wait only for the swap's hold.
         */
        default Duration swapLockTimeout() {
            return Duration.ZERO;
        }

        /**
         * Largest tail, in bytes ({@code pg_relation_size}), that the swap turns into a range when rows went to it,
         * which scans it under the swap's lock: writers wait for the scan. A larger tail stays hot, writes going on
         * into it, until {@link #tailConversion()}'s window. Default 16 MB.
         */
        default long maxTailScan() {
            return 16L * 1024 * 1024;
        }

        /**
         * Move rows at or above the horizon in a range marked for drop back to the outbox; false only logs. Default
         * true.
         */
        default boolean restoreOrphans() {
            return true;
        }

        default TailConversionConfig tailConversion() {
            return new TailConversionConfig() {};
        }
    }

    /** When a hot tail over {@code maxTailScan} is turned into a range all the same: writers wait for its scan. */
    interface TailConversionConfig {

        /**
         * {@code HH:mm-HH:mm} in the JVM's default time zone, crossing midnight when the end comes first; empty: never.
         * Default {@code 03:00-05:00}.
         */
        default String window() {
            return "03:00-05:00";
        }

        /**
         * The scan's {@code statement_timeout} in the window, so about the longest writers wait for its hold: keep it
         * below their own {@code lock_timeout} and {@code statement_timeout}. A scan that does not fit rolls back,
         * nothing left behind, and this instance tries again in the next window. Default 1 s.
         */
        default Duration maxStall() {
            return Duration.ofSeconds(1);
        }

        /** The IANA time zone {@link #window()} is read in. Default: the JVM's default zone. */
        default String zone() {
            return java.time.ZoneId.systemDefault().getId();
        }
    }

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

    interface DeadLettersConfig {

        /** Letters per call of the dead-letter hook, 1..1000 (a redrive batch holds a key lock each). Default 100. */
        default int batchSize() {
            return 100;
        }

        /** Bound of one hook call; a call cut at it leaves its letters pending, retried with backoff. Default 30 s. */
        default Duration timeout() {
            return Duration.ofSeconds(30);
        }

        /**
         * Kept letters older than this, by their {@code failed_at}, are deleted by maintenance; 0 = kept forever, the
         * default; at most 3650 days. Letters still pending for the hook are never deleted.
         */
        default Duration retention() {
            return Duration.ZERO;
        }

        /**
         * How long the count of a message's redrives is remembered, so a redriven message that dies again carries
         * it. Default 30 days, at most 3650 days.
         */
        default Duration redriveHistory() {
            return Duration.ofDays(30);
        }
    }

    /** What happens when the xid counter is found below a tick's xmax (a logical restore or migration). */
    interface RecoveryConfig {

        /**
         * {@code auto} (default): the instance holding the maintenance lock recovers by itself, when at most
         * {@link #maxRows()} rows are to be re-sent; {@code halt}: never by itself, the relay stays halted until
         * {@code OutboxAdmin.recoverXidRegression()}.
         */
        default String xidRegression() {
            return "auto";
        }

        /**
         * Rows to re-send above which the automatic recovery leaves the relay halted: writers wait while it copies
         * them. Default 1 000 000.
         */
        default long maxRows() {
            return 1_000_000L;
        }

        /**
         * Rows written since the newest pre-regression tick, less this margin for clock skew between the old and the
         * new server, are re-sent. Default 10 min.
         */
        default Duration clockMargin() {
            return Duration.ofMinutes(10);
        }

        /**
         * The whole recovery transaction's deadline, writers waiting for it all along; each statement gets what is
         * left. Default 10 min.
         */
        default Duration timeout() {
            return Duration.ofMinutes(10);
        }
    }

    interface SchemaConfig {

        /**
         * True (default): every instance creates and upgrades the base tables at startup. False: the application's
         * migrations own them ({@link OutboxSql}), and startup only checks them and the roles' rights, failing with
         * every problem named; partitions are the library's in both modes.
         */
        default boolean bootstrap() {
            return true;
        }
    }
}
