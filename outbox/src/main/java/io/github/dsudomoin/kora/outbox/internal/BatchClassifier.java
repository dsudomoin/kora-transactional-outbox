package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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

    /** First retry row of its (topic, key), or an unkeyed retry row; due at {@code nextAttemptAt}. */
    public record Head(OutboxRecord r, int attempts, Instant nextAttemptAt, String error) implements Action {}

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
     * Any delivery → per-key handling: the first Retry/NotAttempted row of a (topic, key) becomes its
     * head, every later row of that pair and every row of a {@code dbBlocked} pair a follower, Permanent rows
     * dead. Nothing delivered and Retry/NotAttempted rows on 2+ pairs → pause. Exactly one such pair
     * → pause until {@code priorFailures + 1 >= headFailuresBeforeDivert}, then per-key handling with this attempt
     * counted {@code priorFailures + 1} times instead of once. No such pair at all → advance. Retry heads and
     * Permanent dead rows add this attempt to the record's {@code attempts}; NotAttempted heads always start at 0
     * attempts (never-attempted rows keep their {@code maxAttempts} budget). A head is due at
     * {@code max(now + backoff(max(attempts - 1, 0)), notBefore)}, with {@code notBefore} capped at {@code now + 24h}.
     *
     * <p>Requeue is neither a delivery nor a failure: the row is parked ({@code requeues + 1}, due at
     * {@code max(now + requeueDelay, notBefore)}) without blocking its key, or, the requeue past a positive
     * {@code maxRequeues}, dead with its attempts unchanged; behind a failed row of its key it is a follower like any
     * row. A {@code parked} row delivered is unparked; one not attempted stays parked (due no earlier than
     * {@code max(now + requeueDelay, notBefore)}) without blocking its key; any other outcome treats it as any row.
     */
    public static Decision classify(List<Item> items, int priorFailures, int headFailuresBeforeDivert, Instant now,
            Duration retryInitial, Duration retryMax, Duration requeueDelay, int maxRequeues) {
        boolean anyDelivered = false;
        var failing = new HashSet<Object>();
        for (var item : items) {
            if (item.outcome() instanceof Outcome.Delivered) {
                anyDelivered = true;
            } else if (item.outcome() instanceof Outcome.Retry || item.outcome() instanceof Outcome.NotAttempted) {
                failing.add(orderingKey(item.record()));
            }
        }
        int attempts = 1; // added by a Retry head or dead row: this attempt plus the paused batches before it
        if (!anyDelivered && !failing.isEmpty()) {
            if (failing.size() > 1 || priorFailures + 1 < headFailuresBeforeDivert) {
                return new Decision(true, List.of());
            }
            attempts = priorFailures + 1;
        }
        var notAttemptedDue = now.plus(Backoff.delay(0, retryInitial, retryMax)); // attempts 0
        var requeueDue = now.plus(requeueDelay);
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
                actions.add(maxRequeues > 0 && r.requeues() >= maxRequeues
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
            }
        }
        return new Decision(false, actions);
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
