package io.github.dsudomoin.kora.outbox;

import io.github.dsudomoin.kora.outbox.internal.BatchClassifier;
import io.github.dsudomoin.kora.outbox.internal.OutboxTracing;
import io.github.dsudomoin.kora.outbox.internal.ShardProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;

/** Delivers relayed records to their target. The application graph holds exactly one. */
public interface OutboxHandler {

    /**
     * Delivers one record. Throw {@link OutboxPermanentException} for a record that can never succeed; anything else
     * thrown, an {@link Error} included, means "retry later". Runs in a Kora context carrying the record's trace, so
     * Kora clients called from here continue it.
     */
    void handle(OutboxRecord record) throws Exception;

    /**
     * Delivers records in list order and returns one {@link Outcome} per record, in the same order. Contract: never
     * deliver a record whose (topic, key) already failed earlier in this call.
     *
     * <p>{@link Outcome#requeue()} sets a record aside without blocking its key. The default never returns it: a
     * handler that needs it overrides {@code handleBatch}; {@link #byTopic} passes it through like any outcome.
     *
     * <p>Report failures as outcomes. A call that throws (an {@link Error} included) or returns an invalid list (null,
     * wrong size, a null element) fails every record of the call transiently ({@code Retry}), records it already
     * delivered included, which are delivered again; only a call of a single record that throws
     * {@link OutboxPermanentException} moves that record to the dead table. The relay then pauses the shard and halves
     * the records it hands per call after each such failure (doubling again after each delivery) until the failing
     * record is alone, and diverts it, so the rest of the shard goes on. Records already in the retry table lose no
     * attempt to such a call: each key is called again on its own, and then its first record alone, which bears the
     * attempt. An Error or an invalid list is logged at ERROR.
     *
     * <p>A call runs on a relay thread bounded by {@code outbox.relay.batchTimeout}. At 80 % of it the relay asks it to
     * wind down ({@link #windDownRequested()} turns true and the thread is interrupted): stop starting records then and
     * return {@code NotAttempted} for the rest, as the default does; the relay keeps the outcomes and reads the
     * trailing not-attempted records again in its next step. A call still
     * running at {@code batchTimeout} is cancelled and all its records are retried, those it delivered included. So
     * keep per-record timeouts (HTTP read timeouts, each {@link #byTopic} route's own) well below
     * {@code batchTimeout}.
     *
     * <p>Default: sequential {@link #handle}. After a failure its (topic, key) is skipped ({@code NotAttempted});
     * unkeyed records never block each other. While nothing has succeeded yet, it stops after failures on 3 distinct
     * (topic, key) pairs (the target looks down; each failing unkeyed record counts as a pair) and returns
     * {@code NotAttempted} for the rest; so does a call asked to wind down. {@link OutboxPermanentException} gives
     * {@code Permanent} and does not block the key; anything else {@code handle} throws, an {@link Error} included,
     * gives {@code Retry} and blocks the key (the relay logs a {@code Retry} carrying an Error at ERROR, at most once
     * per 10 s). So the default never fails a whole call.
     */
    default List<Outcome> handleBatch(List<OutboxRecord> records) {
        var outcomes = new ArrayList<Outcome>(records.size());
        var failed = new HashSet<Object>();
        boolean anyDelivered = false;
        for (var record : records) {
            var key = BatchClassifier.orderingKey(record);
            if (failed.contains(key) || (!anyDelivered && failed.size() >= 3) || windDownRequested()) {
                outcomes.add(Outcome.notAttempted());
                continue;
            }
            try {
                OutboxTracing.withKoraContext(OutboxTracing.extract(record.headers()), () -> {
                    handle(record);
                    return null;
                });
                outcomes.add(Outcome.delivered());
                anyDelivered = true;
            } catch (OutboxPermanentException e) {
                outcomes.add(Outcome.permanent(e));
            } catch (Throwable e) { // an Error too: one record's bug must not fail the records around it
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                outcomes.add(Outcome.retry(e));
                failed.add(key);
            }
        }
        return outcomes;
    }

    /**
     * True once the relay asked the {@link #handleBatch} call running on this thread to wind down (its soft deadline
     * at 80 % of {@code batchTimeout}, or the hard timeout), and whenever the thread is interrupted. Check this before
     * starting each record, not just the interrupt flag: a client may clear the flag (a blocking Kora HTTP client on
     * the JDK transport wraps the {@link InterruptedException} without restoring it).
     */
    static boolean windDownRequested() {
        return Thread.currentThread().isInterrupted() || ShardProcessor.windDownRequested();
    }

    /**
     * Routes records by topic to {@code routes}, unknown topics to {@code fallback}. Each route's
     * {@link #handleBatch} gets its records in their original order; outcomes come back in the input positions. A
     * route whose {@code handleBatch} throws an exception, or returns an invalid list (null, wrong size, a null
     * element; logged at ERROR, at most once per 10 s), gets {@code Retry} for all its records; an {@link Error}
     * propagates and fails the whole call. Routes run one after another on one thread: once the soft deadline asks
     * the call to wind down, the routes that follow return {@code NotAttempted} (default {@code handleBatch}). A route
     * that ignores it holds the call until the hard {@code batchTimeout}, and then every record of the call is
     * retried, those other routes delivered included; each route's own timeout must stay below {@code batchTimeout}.
     */
    static OutboxHandler byTopic(Map<String, OutboxHandler> routes, OutboxHandler fallback) {
        var table = Map.copyOf(routes);
        Objects.requireNonNull(fallback, "fallback");
        return new OutboxHandler() {
            private final long reportInterval = TimeUnit.SECONDS.toNanos(10);
            /** When an invalid route answer was last logged ({@code nanoTime}), and how many went unlogged since. */
            private final AtomicLong reported = new AtomicLong(System.nanoTime() - reportInterval);
            private final AtomicInteger suppressed = new AtomicInteger();

            @Override
            public void handle(OutboxRecord record) throws Exception {
                table.getOrDefault(record.topic(), fallback).handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                var positions = new LinkedHashMap<OutboxHandler, List<Integer>>();
                for (int i = 0; i < records.size(); i++) {
                    var route = table.getOrDefault(records.get(i).topic(), fallback);
                    positions.computeIfAbsent(route, r -> new ArrayList<>()).add(i);
                }
                var outcomes = new Outcome[records.size()];
                positions.forEach((route, indexes) -> {
                    List<Outcome> routed;
                    try {
                        routed = route.handleBatch(indexes.stream().map(records::get).toList());
                    } catch (Exception e) { // one broken route must not block the others; Errors propagate
                        routed = indexes.stream().map(i -> Outcome.retry(e)).toList();
                    }
                    if (routed == null || routed.size() != indexes.size()
                            || routed.stream().anyMatch(Objects::isNull)) { // a handler bug: Retry, not the call
                        var error = new IllegalStateException(route + " returned an invalid outcome list for "
                                + indexes.size() + " records: " + routed);
                        report(error);
                        routed = indexes.stream().map(i -> Outcome.retry(error)).toList();
                    }
                    for (int i = 0; i < indexes.size(); i++) {
                        outcomes[indexes.get(i)] = routed.get(i);
                    }
                });
                return Arrays.asList(outcomes);
            }

            private void report(IllegalStateException error) {
                long now = System.nanoTime();
                long last = reported.get();
                if (now - last < reportInterval || !reported.compareAndSet(last, now)) {
                    suppressed.incrementAndGet();
                    return;
                }
                LoggerFactory.getLogger(OutboxHandler.class).error("outbox byTopic route failed; its records are"
                        + " retried ({} more since the previous report not logged)", suppressed.getAndSet(0), error);
            }
        };
    }
}
