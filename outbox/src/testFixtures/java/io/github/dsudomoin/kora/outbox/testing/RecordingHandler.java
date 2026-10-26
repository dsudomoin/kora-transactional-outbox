package io.github.dsudomoin.kora.outbox.testing;

import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Thread-safe scriptable handler over the default {@link OutboxHandler#handleBatch}: logs delivered records and the
 * outcomes of every completed batch call.
 */
public final class RecordingHandler implements OutboxHandler {

    private record Failure(Predicate<OutboxRecord> matches, Supplier<Exception> error, boolean once) {}

    private final List<OutboxRecord> delivered = new ArrayList<>();
    private final List<List<Outcome>> batches = new ArrayList<>();
    private final List<Failure> failures = new CopyOnWriteArrayList<>();
    private final Queue<Supplier<Throwable>> batchThrows = new ConcurrentLinkedQueue<>();
    private volatile Duration delay = Duration.ZERO;

    @Override
    public void handle(OutboxRecord record) throws Exception {
        for (var f : failures) {
            if (f.matches().test(record) && (!f.once() || failures.remove(f))) {
                throw f.error().get();
            }
        }
        synchronized (delivered) {
            delivered.add(record);
        }
    }

    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        var toThrow = batchThrows.poll();
        if (toThrow != null) {
            var t = toThrow.get();
            if (t instanceof RuntimeException e) {
                throw e;
            }
            if (t instanceof Error e) {
                throw e;
            }
            throw new RuntimeException(t);
        }
        if (!delay.isZero()) {
            try {
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // the default handleBatch then attempts nothing
            }
        }
        var outcomes = OutboxHandler.super.handleBatch(records);
        synchronized (batches) {
            batches.add(outcomes);
        }
        return outcomes;
    }

    /** The next record matching {@code matches} fails once with {@code error}. */
    public RecordingHandler failNext(Predicate<OutboxRecord> matches, Supplier<Exception> error) {
        failures.add(new Failure(matches, error, true));
        return this;
    }

    /** Every record matching {@code matches} fails with {@code error} until {@link #reset()}. */
    public RecordingHandler failAlways(Predicate<OutboxRecord> matches, Supplier<Exception> error) {
        failures.add(new Failure(matches, error, false));
        return this;
    }

    /** The next {@code handleBatch} call throws {@code error} itself; calls queue up, one throw per batch call. */
    public RecordingHandler throwFromBatch(Supplier<Throwable> error) {
        batchThrows.add(error);
        return this;
    }

    /** Every {@code handleBatch} call sleeps {@code delay} first (interruptible). */
    public RecordingHandler delay(Duration delay) {
        this.delay = delay;
        return this;
    }

    public void reset() {
        failures.clear();
        batchThrows.clear();
        delay = Duration.ZERO;
        synchronized (delivered) {
            delivered.clear();
        }
        synchronized (batches) {
            batches.clear();
        }
    }

    /** Successfully handled records in handling order. */
    public List<OutboxRecord> delivered() {
        synchronized (delivered) {
            return List.copyOf(delivered);
        }
    }

    /** Outcomes of each completed {@code handleBatch} call, in call order. */
    public List<List<Outcome>> batches() {
        synchronized (batches) {
            return List.copyOf(batches);
        }
    }
}
