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

/** Turns the outcomes of a main-stream batch into retry/dead actions or a shard pause. */
public final class BatchClassifier {

    /** A batch row: {@code dbBlocked} rows were not handed to the handler and carry no outcome. */
    public record Item(OutboxRecord record, boolean dbBlocked, @Nullable Outcome outcome) {
        public Item {
            if (dbBlocked == (outcome != null)) {
                throw new IllegalArgumentException("outcome must be null exactly when the row is blocked");
            }
            if (dbBlocked && record.key() == null) {
                throw new IllegalArgumentException("unkeyed rows are never blocked"); // no head would promote them
            }
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

    /** {@code pause}: no actions, cursor unchanged; otherwise apply {@code actions} and advance. */
    public record Decision(boolean pause, List<Action> actions) {}

    static final String NOT_ATTEMPTED = "not attempted";
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
     * dead (attempts 1). Nothing delivered and Retry/NotAttempted rows on 2+ pairs → pause. Exactly one such pair
     * → pause until {@code priorFailures + 1 >= headFailuresBeforeDivert}, then per-key handling with Retry heads
     * and dead rows at {@code priorFailures + 1} attempts. No such pair at all → advance. NotAttempted heads always
     * start at 0 attempts (never-attempted rows keep their {@code maxAttempts} budget). A head is due at
     * {@code max(now + backoff(max(attempts - 1, 0)), notBefore)}, with {@code notBefore} capped at {@code now + 24h}.
     */
    public static Decision classify(List<Item> items, int priorFailures, int headFailuresBeforeDivert, Instant now,
            Duration retryInitial, Duration retryMax) {
        boolean anyDelivered = false;
        var failing = new HashSet<Object>();
        for (var item : items) {
            if (item.outcome() instanceof Outcome.Delivered) {
                anyDelivered = true;
            } else if (item.outcome() instanceof Outcome.Retry || item.outcome() instanceof Outcome.NotAttempted) {
                failing.add(orderingKey(item.record()));
            }
        }
        int attempts = 1; // of a Retry head or dead row: this attempt plus the paused batches before it
        if (!anyDelivered && !failing.isEmpty()) {
            if (failing.size() > 1 || priorFailures + 1 < headFailuresBeforeDivert) {
                return new Decision(true, List.of());
            }
            attempts = priorFailures + 1;
        }
        var retryDue = now.plus(Backoff.delay(attempts - 1, retryInitial, retryMax));
        var notAttemptedDue = now.plus(Backoff.delay(0, retryInitial, retryMax)); // attempts 0
        var blocked = new HashSet<Object>();
        var actions = new ArrayList<Action>();
        for (var item : items) {
            var r = item.record();
            var key = orderingKey(r);
            var outcome = item.outcome();
            if (item.dbBlocked() || blocked.contains(key)) {
                actions.add(new Follower(r));
                blocked.add(key);
            } else if (outcome instanceof Outcome.Permanent p) {
                actions.add(new Dead(r, attempts, p.error().toString()));
            } else if (outcome instanceof Outcome.Retry retry) {
                actions.add(new Head(r, attempts, dueAt(retryDue, retry.notBefore(), now), retry.error().toString()));
                blocked.add(key);
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
