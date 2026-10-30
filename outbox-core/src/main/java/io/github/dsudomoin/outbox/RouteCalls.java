package io.github.dsudomoin.outbox;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;

/**
 * The calls a handler that hands its records on ({@link OutboxHandler#byTopic}, {@link LocalOnly}) makes to the
 * handlers behind it: a call that fails fails only the records it was handed. One per such handler, for its log limit.
 */
final class RouteCalls {

    private static final long REPORT_INTERVAL = TimeUnit.SECONDS.toNanos(10);

    /** What the ERROR calls the handlers behind it. */
    private final String what;
    /** When an invalid answer was last logged ({@code nanoTime}), and how many went unlogged since. */
    private final AtomicLong reported = new AtomicLong(System.nanoTime() - REPORT_INTERVAL);
    private final AtomicInteger suppressed = new AtomicInteger();

    RouteCalls(String what) {
        this.what = what;
    }

    /**
     * {@code route}'s outcomes for {@code records}, one per record. A route that throws {@link OutboxWaitException}
     * gives {@code Waiting} (its kind and message) for all of them; one that throws another exception, or returns an
     * invalid list (null, wrong size, a null element; logged at ERROR, at most once per 10 s), gives {@code Retry} for
     * all of them. An {@link Error} propagates.
     */
    List<Outcome> handleBatch(OutboxHandler route, List<OutboxRecord> records) {
        List<Outcome> outcomes;
        try {
            outcomes = route.handleBatch(records);
        } catch (OutboxWaitException e) { // the route cannot take any of them now
            Outcome waiting = new Outcome.Waiting(e.kind(), e.getMessage(), null);
            outcomes = records.stream().map(r -> waiting).toList();
        } catch (Exception e) { // one broken route must not fail the records of others; Errors propagate
            outcomes = records.stream().map(r -> Outcome.retry(e)).toList();
        }
        if (outcomes == null || outcomes.size() != records.size()
                || outcomes.stream().anyMatch(Objects::isNull)) { // a handler bug: Retry, not the call
            var error = new IllegalStateException(route + " returned an invalid outcome list for " + records.size()
                    + " records: " + outcomes);
            report(error);
            outcomes = records.stream().map(r -> Outcome.retry(error)).toList();
        }
        return outcomes;
    }

    private void report(IllegalStateException error) {
        long now = System.nanoTime();
        long last = reported.get();
        if (now - last < REPORT_INTERVAL || !reported.compareAndSet(last, now)) {
            suppressed.incrementAndGet();
            return;
        }
        LoggerFactory.getLogger(OutboxHandler.class).error("outbox " + what + " failed; its records are retried ({}"
                + " more since the previous report not logged)", suppressed.getAndSet(0), error);
    }
}
