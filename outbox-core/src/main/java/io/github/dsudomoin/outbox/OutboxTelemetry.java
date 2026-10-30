package io.github.dsudomoin.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.DoubleSupplier;

/**
 * Metrics and traces of the outbox, implemented by the application's adapter: every method is a no-op by default,
 * and new events come as new default methods. Hooks run on the outbox's own threads, the enqueue ones on the
 * writer's; "the relay thread" below is the thread that steps: a relay worker, or the caller of
 * {@link OutboxRelay#drain()}. An exception a hook throws is logged at WARN (at most once per 10 s per hook) and
 * ignored, so a broken implementation never fails a send or a relay step.
 */
public interface OutboxTelemetry {

    OutboxTelemetry NONE = new OutboxTelemetry() {};

    /** One per {@code send} / {@code sendAll} call, after its transaction check. */
    default Enqueue enqueueStarted() {
        return Enqueue.NONE;
    }

    /** Per message, after its insert. */
    default void enqueued(String topic) {}

    /** One per handler call, on the relay thread, before the records are handed to the handler. */
    default Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
        return Delivery.NONE;
    }

    /** A record delivered, {@code delay} after its {@code createdAt}. */
    default void delivered(String topic, Duration delay) {}

    /** A record failed and is retried later. */
    default void retried(String topic) {}

    /** A record moved to the dead table. */
    default void deadLettered(String topic) {}

    /** A record set aside with {@link Outcome#requeue()} and parked. */
    default void requeued(String topic) {}

    /**
     * A record waits for an instance that can handle it ({@link Outcome.Waiting}): neither a delivery nor a failure,
     * it spends no attempt. {@code kind}, a closed set, fits a metric label; the reason stays in the record's error.
     */
    default void waited(String topic, Outcome.Waiting.Kind kind) {}

    /**
     * A send refused with {@link OutboxIncompatibleContractException}, on the writer's thread: some live relay
     * instance cannot read {@code version} of {@code topic}. {@code type}: the class name it was written as, {@code ?}
     * when unknown.
     */
    default void contractRejected(String topic, String type, int version) {}

    /**
     * A send refused with {@link OutboxMissingRouteException}, on the writer's thread: a route of this instance claims
     * {@code topic}, but some live relay instance does not route it.
     */
    default void routeRejected(String topic) {}

    /**
     * The writer guard let a message to {@code topic} through unchecked, on the writer's thread, once per message: this
     * instance has not read the live relay instances' reports lately, and the last read, which the route or the version
     * check took, allowed it. Counted at the guard, before the insert, so the message may still not be written: a
     * later message of the same {@code sendAll} refused, or the insert failing.
     */
    default void routeUnchecked(String topic) {}

    /**
     * A relay step found work, on the thread that steps: called at the first locked shard with work, so only for the
     * steps {@link #stepFinished} measures; a step that locks nothing or finds nothing to do calls neither.
     * {@code shards} is a copy of the shards whose locks the step holds, those without work included. The step's
     * cursor read, its target choice and the retry and parked lookups up to that shard come before this call; its
     * further database work and handler calls run on this thread (the handler itself on a handler thread) until
     * {@link RelayStep#finished()} or {@link RelayStep#failed}.
     */
    default RelayStep stepStarted(List<Integer> shards) {
        return RelayStep.NONE;
    }

    /** A relay step of one or more shards ended. */
    default void stepFinished(Duration duration) {}

    /**
     * A relay pass, or a step of a single shard, failed and is retried later; the relay logs it at WARN. Never while
     * stopping.
     */
    default void relayFailed(Throwable error) {}

    /** A shard was paused after a failed call. */
    default void shardPaused() {}

    /** The dead-letter hook decided a letter: {@code keep}, {@code discard}, {@code redrive} or {@code redrive_at}. */
    default void deadLetterDecision(String decision) {}

    /** A dead-letter hook call failed or timed out. */
    default void deadLetterHookFailed() {}

    /** The inbox saw a message older than its dedup window. */
    default void inboxOutsideWindow() {}

    /**
     * On a handler or dead-letter hook thread after each call there, which then serves other calls: drop state the
     * call may have bound to the thread.
     */
    default void releaseThread() {}

    /** Registers a gauge, once per {@code gauge}; {@code value} is read on the caller's schedule, NaN when unknown. */
    default void gauge(OutboxGauge gauge, DoubleSupplier value) {}

    /** One {@code send} / {@code sendAll} call. */
    interface Enqueue {

        Enqueue NONE = new Enqueue() {};

        /** The headers to store for a message with {@code headers}; a message with trace context keeps it. */
        default List<OutboxHeader> headers(List<OutboxHeader> headers) {
            return headers;
        }

        /** Every message of the call is inserted. */
        default void finished() {}

        /** The call failed with {@code error}, which reaches the caller. */
        default void failed(Throwable error) {}
    }

    /** One relay step, ended once on the thread that started it. */
    interface RelayStep {

        RelayStep NONE = new RelayStep() {};

        /** The step's work is done; its transaction commits next, so a commit that fails still ends the step here. */
        default void finished() {}

        /** The step failed with {@code error}, which rolls its transaction back. */
        default void failed(Throwable error) {}
    }

    /** One handler call. */
    interface Delivery {

        Delivery NONE = new Delivery() {};

        /** The records the handler gets: the same size and order, with headers replaced as needed. */
        default List<OutboxRecord> handed(List<OutboxRecord> records) {
            return records;
        }

        /**
         * Runs {@code call} on the handler thread in the scope of {@code record}, one of the handed records; see
         * {@link OutboxHandler#inRecordScope}.
         */
        default <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            return call.call();
        }

        /** The call returned {@code outcomes}, one per record in order (a timed-out call's are retries). */
        default void finished(List<Outcome> outcomes) {}

        /** The call ended without outcomes: interrupted, or the relay is closing. */
        default void failed(Throwable error) {}
    }
}
