package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Turns the outcomes of a handler call into retry/dead/parked actions or a shard pause. */
public final class BatchClassifier {

    /**
     * A call row: {@code dbBlocked} rows were not handed to the handler and carry no outcome; {@code parked} rows come
     * from the parked table.
     */
    public record Item(OutboxRecord record, boolean dbBlocked, @Nullable Outcome outcome, boolean parked) {
        public Item {
            if (dbBlocked == (outcome != null)) {
                throw new IllegalArgumentException("outcome must be null exactly when the row is blocked");
            }
            if (dbBlocked && record.key() == null) {
                throw new IllegalArgumentException("unkeyed rows are never blocked"); // no head would promote them
            }
            if (dbBlocked && parked) {
                throw new IllegalArgumentException("parked rows are never blocked");
            }
        }

        public Item(OutboxRecord record, boolean dbBlocked, @Nullable Outcome outcome) {
            this(record, dbBlocked, outcome, false);
        }
    }

    public sealed interface Action {
        OutboxRecord r();
    }

    /**
     * First retry row of its (topic, key), or an unkeyed retry row; due at {@code nextAttemptAt}. {@code waiting}: why
     * a waiting head waits, null for one that failed or was not attempted.
     */
    public record Head(OutboxRecord r, int attempts, Instant nextAttemptAt, String error,
            @Nullable Outcome.Waiting.Kind waiting) implements Action {

        /** A head that failed or was not attempted. */
        public Head(OutboxRecord r, int attempts, Instant nextAttemptAt, String error) {
            this(r, attempts, nextAttemptAt, error, null);
        }
    }

    /** A retry row waiting behind its head ({@code next_attempt_at} null). */
    public record Follower(OutboxRecord r) implements Action {}

    public record Dead(OutboxRecord r, int attempts, String error) implements Action {}

    /** Parks a main-stream record, or re-parks a parked one, with {@code requeues}, due at {@code dueAt}. */
    public record Park(OutboxRecord r, int requeues, Instant dueAt) implements Action {}

    /** A parked record not attempted: stays parked, due at the later of its due and {@code dueAt}. */
    public record Keep(OutboxRecord r, Instant dueAt) implements Action {}

    /** A parked record delivered: leaves the parked table. */
    public record Unpark(OutboxRecord r) implements Action {}

    /** {@code pause}: no actions, cursor unchanged; otherwise apply {@code actions} and advance. */
    public record Decision(boolean pause, List<Action> actions) {}

    static final String NOT_ATTEMPTED = "not attempted";
    static final String REQUEUE_LIMIT = "requeue limit reached";
    /** The furthest a handler's {@code notBefore} may push a retry. */
    static final Duration MAX_NOT_BEFORE = Duration.ofHours(24);

    private BatchClassifier() {}

    /**
     * Ordering identity of a record: equal for equal topic and byte-equal key; a fresh object for an unkeyed record,
     * so unkeyed records never block or follow each other.
     */
    public static Object orderingKey(OutboxRecord r) {
        return r.key() == null ? new Object() : List.of(r.topic(), ByteBuffer.wrap(r.key()));
    }

    /**
     * Any delivery → per-key handling: the first Retry/NotAttempted/Waiting row of a (topic, key) becomes its
     * head, every later row of that pair and every row of a {@code dbBlocked} pair a follower, Permanent rows
     * dead. Nothing delivered and 2+ {@linkplain #failingKeys failing} pairs → pause. Exactly one such pair
     * → pause until {@code priorFailures + 1 >= headFailuresBeforeDivert}, then per-key handling with this attempt
     * counted {@code priorFailures + 1} times instead of once. No such pair at all → advance. Retry heads and
     * Permanent dead rows add this attempt to the record's {@code attempts}; NotAttempted heads always start at 0
     * attempts (never-attempted rows keep their {@code maxAttempts} budget). A head is due at
     * {@code max(now + backoff(max(attempts - 1, 0)), notBefore)}, with {@code notBefore} capped at {@code now + 24h}.
     *
     * <p>Waiting is neither a delivery nor a failure: the row becomes a waiting head with its attempts unchanged, due
     * at {@code max(now + waitBackoff(0), notBefore)} ({@link #waitBackoff}), {@code notBefore} capped the same way,
     * and blocks its key, which is not failing, the NotAttempted rows after it included.
     *
     * <p>Requeue is neither a delivery nor a failure: the row is parked ({@code requeues + 1}, due at
     * {@code max(now + requeueDelay, notBefore)}) without blocking its key, or, the requeue past a positive
     * {@code maxRequeues}, dead with its attempts unchanged; behind a failed row of its key it is a follower like any
     * row. A {@code parked} row delivered is unparked; one not attempted stays parked (due no earlier than
     * {@code max(now + requeueDelay, notBefore)}) without blocking its key; any other outcome treats it as any row.
     */
    public static Decision classify(List<Item> items, int priorFailures, int headFailuresBeforeDivert, Instant now,
            Duration retryInitial, Duration retryMax, Duration requeueDelay, int maxRequeues, Duration waitDelay,
            Duration waitMax) {
        boolean anyDelivered = items.stream().anyMatch(item -> item.outcome() instanceof Outcome.Delivered);
        var failing = failingKeys(items);
        int attempts = 1; // added by a Retry head or dead row: this attempt plus the paused batches before it
        if (!anyDelivered && !failing.isEmpty()) {
            if (failing.size() > 1 || priorFailures + 1 < headFailuresBeforeDivert) {
                return new Decision(true, List.of());
            }
            attempts = priorFailures + 1;
        }
        var notAttemptedDue = now.plus(Backoff.delay(0, retryInitial, retryMax)); // attempts 0
        var requeueDue = now.plus(requeueDelay);
        var waitDue = now.plus(waitBackoff(Duration.ZERO, waitDelay, waitMax)); // a first wait
        var blocked = new HashSet<Object>();
        var actions = new ArrayList<Action>();
        for (var item : items) {
            var r = item.record();
            var key = orderingKey(r);
            var outcome = item.outcome();
            if (item.dbBlocked() || blocked.contains(key)) {
                actions.add(new Follower(r));
                blocked.add(key);
            } else if (outcome instanceof Outcome.Delivered) {
                if (item.parked()) {
                    actions.add(new Unpark(r));
                }
            } else if (outcome instanceof Outcome.Requeue requeue) {
                actions.add(requeueLimitReached(r, maxRequeues)
                        ? new Dead(r, r.attempts(), REQUEUE_LIMIT)
                        : new Park(r, r.requeues() + 1, dueAt(requeueDue, requeue.notBefore(), now)));
            } else if (outcome instanceof Outcome.Permanent p) {
                actions.add(new Dead(r, r.attempts() + attempts, p.error().toString()));
            } else if (outcome instanceof Outcome.Retry retry) {
                int total = r.attempts() + attempts;
                var due = now.plus(Backoff.delay(total - 1, retryInitial, retryMax));
                actions.add(new Head(r, total, dueAt(due, retry.notBefore(), now), retry.error().toString()));
                blocked.add(key);
            } else if (outcome instanceof Outcome.NotAttempted skipped && item.parked()) {
                actions.add(new Keep(r, dueAt(requeueDue, skipped.notBefore(), now)));
            } else if (outcome instanceof Outcome.NotAttempted skipped) {
                actions.add(new Head(r, 0, dueAt(notAttemptedDue, skipped.notBefore(), now), NOT_ATTEMPTED));
                blocked.add(key);
            } else if (outcome instanceof Outcome.Waiting waiting) {
                actions.add(new Head(r, r.attempts(), dueAt(waitDue, waiting.notBefore(), now), waitingError(waiting),
                        waiting.kind()));
                blocked.add(key);
            } else {
                throw new IllegalStateException("unknown outcome " + outcome);
            }
        }
        return new Decision(false, actions);
    }

    /**
     * The (topic, key) pairs of {@code items} that failed: those whose first Retry, NotAttempted or Waiting row is a
     * Retry or a NotAttempted. A pair whose first such row waits is not failing: a wait says nothing about the target,
     * and the rows after it were not attempted because of it.
     */
    static Set<Object> failingKeys(List<Item> items) {
        var seen = new HashSet<Object>();
        var failing = new HashSet<Object>();
        for (var item : items) {
            var outcome = item.outcome();
            if (item.dbBlocked() || outcome instanceof Outcome.Delivered || outcome instanceof Outcome.Permanent
                    || outcome instanceof Outcome.Requeue) {
                continue; // not handed, or its key goes on
            } else if (outcome instanceof Outcome.Retry || outcome instanceof Outcome.NotAttempted) {
                var key = orderingKey(item.record());
                if (seen.add(key)) {
                    failing.add(key);
                }
            } else if (outcome instanceof Outcome.Waiting) {
                seen.add(orderingKey(item.record()));
            } else {
                throw new IllegalStateException("unknown outcome " + outcome);
            }
        }
        return failing;
    }

    /**
     * Until a waiting record is handed again: a quarter of the time it has {@code waited}, at least {@code delay} and
     * at most {@code maxDelay}, so a long wait is asked about less often.
     */
    static Duration waitBackoff(Duration waited, Duration delay, Duration maxDelay) {
        var quarter = waited.dividedBy(4);
        return quarter.compareTo(maxDelay) > 0 ? maxDelay : quarter.compareTo(delay) < 0 ? delay : quarter;
    }

    /** The {@code last_error} of a waiting head. */
    static String waitingError(Outcome.Waiting waiting) {
        return "waiting (" + waiting.kind().label() + "): " + waiting.reason();
    }

    /** A requeue of {@code r} now goes past a positive {@code maxRequeues}: dead instead of parked. */
    static boolean requeueLimitReached(OutboxRecord r, int maxRequeues) {
        return maxRequeues > 0 && r.requeues() >= maxRequeues;
    }

    /** {@code max(due, notBefore)}, with {@code notBefore} capped at {@code now + 24h}. */
    static Instant dueAt(Instant due, @Nullable Instant notBefore, Instant now) {
        if (notBefore == null) {
            return due;
        }
        var cap = now.plus(MAX_NOT_BEFORE);
        var capped = notBefore.isAfter(cap) ? cap : notBefore;
        return capped.isAfter(due) ? capped : due;
    }
}
