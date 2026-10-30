package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Action;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Decision;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Follower;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Head;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Item;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Keep;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Park;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Unpark;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class BatchClassifierTest {

    private static final Instant NOW = Instant.parse("2026-10-25T12:00:00Z");
    private static final Duration INITIAL = Duration.ofSeconds(1);
    private static final Duration MAX = Duration.ofMinutes(5);
    private static final int THRESHOLD = 3;
    private static final Duration DELAY = Duration.ofSeconds(1);
    private static final Duration WAIT = Duration.ofSeconds(2);
    private static final Duration WAIT_MAX = Duration.ofMinutes(1);
    private static final RuntimeException DOWN = new RuntimeException("down");
    private static final Decision PAUSE = new Decision(true, List.of());

    private final AtomicLong ids = new AtomicLong();

    @Test
    void anySuccessDivertsFailuresAndFollowers() {
        var k1a = record("t", "k1");
        var k2 = record("t", "k2");
        var k1b = record("t", "k1");
        var k3a = record("t", "k3");
        var k1c = record("t", "k1");
        var u = record("t", null);
        var u2 = record("t", null);
        var k3b = record("t", "k3");

        var decision = classify(0, retry(k1a), delivered(k2), notAttempted(k1b), notAttempted(k3a),
                delivered(k1c), retry(u), delivered(u2), delivered(k3b));

        assertThat(decision).isEqualTo(advance(
                head(k1a, 1),
                new Follower(k1b),
                new Head(k3a, 0, NOW.plus(INITIAL), "not attempted"), // never attempted: keeps its budget
                new Follower(k1c), // delivered after its key failed: diverted all the same, order wins
                head(u, 1),
                new Follower(k3b)));
    }

    @Test
    void dbBlockedRowsBecomeFollowers() {
        var k1a = record("t", "k1");
        var k2 = record("t", "k2");
        var k1b = record("t", "k1");
        var k3 = record("t", "k3");

        assertThat(classify(0, blocked(k1a), delivered(k2), blocked(k1b), retry(k3)))
                .isEqualTo(advance(new Follower(k1a), new Follower(k1b), head(k3, 1)));
    }

    @Test
    void allFailedTwoKeysPauses() {
        assertThat(classify(0, retry(record("t", "k1")), notAttempted(record("t", "k2")))).isEqualTo(PAUSE);
        assertThat(classify(10, retry(record("t", "k1")), retry(record("t", "k2")), blocked(record("t", "k3")),
                permanent(record("t", "k4"))))
                .as("past the divert threshold")
                .isEqualTo(PAUSE);
        assertThat(classify(10, retry(record("t", "k")), retry(record("u", "k"))))
                .as("same key on two topics is two pairs")
                .isEqualTo(PAUSE);
    }

    @Test
    void allFailedSingleKeyPausesUntilThreshold() {
        var k1a = record("t", "k1");
        var k1b = record("t", "k1");
        var k2 = record("t", "k2");
        var k3 = record("t", "k3");
        var items = new Item[] {retry(k1a), notAttempted(k1b), blocked(k2), permanent(k3)};

        assertThat(classify(0, items)).isEqualTo(PAUSE);
        assertThat(classify(1, items)).isEqualTo(PAUSE);
        assertThat(classify(2, items)).isEqualTo(advance(
                new Head(k1a, 3, NOW.plus(Duration.ofSeconds(4)), DOWN.toString()),
                new Follower(k1b),
                new Follower(k2),
                new Dead(k3, 3, new OutboxPermanentException("bad").toString())));
    }

    @Test
    void singleKeyDivertOfNotAttemptedKeyKeepsAttemptBudget() {
        var k1a = record("t", "k1");
        var k1b = record("t", "k1");
        var k2 = record("t", "k2");

        assertThat(classify(2, notAttempted(k1a), notAttempted(k1b), permanent(k2))).isEqualTo(advance(
                new Head(k1a, 0, NOW.plus(INITIAL), "not attempted"),
                new Follower(k1b),
                new Dead(k2, 3, new OutboxPermanentException("bad").toString())));
    }

    @Test
    void permanentBehindABlockedKeyFollows() {
        var k1a = record("t", "k1");
        var k2 = record("t", "k2");
        var k1b = record("t", "k1");
        var k3a = record("t", "k3");
        var k3b = record("t", "k3");

        assertThat(classify(0, retry(k1a), delivered(k2), permanent(k1b), blocked(k3a), permanent(k3b)))
                .isEqualTo(advance(head(k1a, 1), new Follower(k1b), new Follower(k3a), new Follower(k3b)));
    }

    @Test
    void permanentBecomesDeadAndKeyContinues() {
        var k1a = record("t", "k1");
        var k1b = record("t", "k1");
        var k2 = record("t", "k2");
        var error = new OutboxPermanentException("bad");

        assertThat(classify(0, new Item(k1a, false, Outcome.permanent(error)), delivered(k1b), delivered(k2)))
                .isEqualTo(advance(new Dead(k1a, 1, error.toString())));
        assertThat(classify(0, new Item(k1a, false, Outcome.permanent(error))))
                .as("nothing delivered, nothing failing")
                .isEqualTo(advance(new Dead(k1a, 1, error.toString())));
    }

    @Test
    void unkeyedFailuresCountAsDistinct() {
        var u1 = record("t", null);
        var u2 = record("t", null);

        assertThat(classify(10, retry(u1), retry(u2))).isEqualTo(PAUSE);
        assertThat(classify(1, retry(u1))).isEqualTo(PAUSE);
        assertThat(classify(2, retry(u1))).as("a lone unkeyed poison row is diverted").isEqualTo(advance(head(u1, 3)));
        assertThat(classify(0, retry(u1), delivered(record("t", "k")), retry(u2)))
                .as("unkeyed rows never follow each other")
                .isEqualTo(advance(head(u1, 1), head(u2, 1)));
    }

    @Test
    void notBeforeIsRespected() {
        var late = NOW.plus(Duration.ofHours(1));
        var early = NOW.plusMillis(100);
        var k1 = record("t", "k1");
        var k2 = record("t", "k2");
        var k3 = record("t", "k3");
        var k4 = record("t", "k4");
        var k5 = record("t", "k5");
        var k6 = record("t", "k6");
        var farAway = NOW.plus(Duration.ofDays(100));

        var decision = classify(0, new Item(k1, false, Outcome.retry(DOWN, late)),
                new Item(k2, false, Outcome.notAttempted(late)), new Item(k3, false, Outcome.retry(DOWN, early)),
                delivered(k4), new Item(k5, false, Outcome.retry(DOWN, farAway)),
                new Item(k6, false, Outcome.notAttempted(farAway)));

        assertThat(decision).isEqualTo(advance(
                new Head(k1, 1, late, DOWN.toString()),
                new Head(k2, 0, late, "not attempted"),
                new Head(k3, 1, NOW.plus(INITIAL), DOWN.toString()),
                new Head(k5, 1, NOW.plus(Duration.ofHours(24)), DOWN.toString()), // notBefore capped at 24 h
                new Head(k6, 0, NOW.plus(Duration.ofHours(24)), "not attempted")));
    }

    @Test
    void allDbBlockedAdvancesWithFollowersOnly() {
        var k1 = record("t", "k1");
        var k2 = record("t", "k2");

        assertThat(classify(0, blocked(k1), blocked(k2))).isEqualTo(advance(new Follower(k1), new Follower(k2)));
    }

    @Test
    void emptyBatchAdvances() {
        assertThat(classify(7)).isEqualTo(advance());
    }

    @Test
    void itemOutcomeIsNullIffDbBlocked() {
        var keyed = record("t", "k");

        assertThatThrownBy(() -> new Item(keyed, true, Outcome.delivered()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Item(keyed, false, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Item(record("t", null), true, null))
                .as("unkeyed rows are never blocked")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requeueParksTheRecordAndLetsItsKeyGo() {
        var k1 = record("t", "k");
        var k2 = record("t", "k");
        var k3 = record("t", "k");

        assertThat(classify(0, requeue(k1), delivered(k2), retry(k3)))
                .isEqualTo(advance(new Park(k1, 1, NOW.plus(DELAY)), head(k3, 1)));
    }

    @Test
    void requeueOnlyAdvancesWithoutPause() {
        var a = record("t", "a");
        var b = record("t", "b");

        assertThat(classify(0, requeue(a), requeue(b)))
                .isEqualTo(advance(new Park(a, 1, NOW.plus(DELAY)), new Park(b, 1, NOW.plus(DELAY))));
    }

    @Test
    void requeueAfterItsKeyFailedIsAFollower() {
        var k1 = record("t", "k");
        var k2 = record("t", "k");

        assertThat(classify(THRESHOLD - 1, retry(k1), requeue(k2)))
                .as("parked, k2 would overtake its failed predecessor")
                .isEqualTo(advance(head(k1, THRESHOLD), new Follower(k2)));
    }

    @Test
    void requeueInAPausedCallIsNotApplied() {
        assertThat(classify(0, requeue(record("t", "a")), retry(record("t", "b")), retry(record("t", "c"))))
                .isEqualTo(PAUSE);
    }

    @Test
    void requeueDueTime() {
        var plain = record("t", "a");
        var later = record("t", "b");
        var past = record("t", "c");
        var early = record("t", "d");
        var farAway = record("t", "e");

        assertThat(classify(0, requeue(plain), requeue(later, NOW.plusSeconds(10)),
                requeue(past, NOW.minusSeconds(1)), requeue(early, NOW.plusMillis(100)),
                requeue(farAway, NOW.plus(Duration.ofHours(30))))).isEqualTo(advance(
                new Park(plain, 1, NOW.plus(DELAY)),
                new Park(later, 1, NOW.plusSeconds(10)),
                new Park(past, 1, NOW.plus(DELAY)), // never before the delay: no busy loop
                new Park(early, 1, NOW.plus(DELAY)),
                new Park(farAway, 1, NOW.plus(Duration.ofHours(24))))); // notBefore capped at 24 h
    }

    @Test
    void requeueLimitGoesToDead() {
        var atLimit = record("t", "a", 4, 2);
        var belowLimit = record("t", "b", 0, 1);
        var veteran = record("t", "c", 0, 1_000);

        assertThat(classify(0, 2, requeue(atLimit), requeue(belowLimit))).isEqualTo(advance(
                new Dead(atLimit, 4, "requeue limit reached"), // attempts unchanged: a requeue is no attempt
                new Park(belowLimit, 2, NOW.plus(DELAY))));
        assertThat(classify(0, requeue(veteran)))
                .as("maxRequeues 0 never dead-letters")
                .isEqualTo(advance(new Park(veteran, 1_001, NOW.plus(DELAY))));
    }

    @Test
    void parkedItemOutcomes() {
        var delivered = record("t", "a", 0, 1);
        var skipped = record("t", "b", 0, 1);
        var afterSkipped = record("t", "b");
        var failed = record("t", "c", 2, 1);
        var afterFailed = record("t", "c", 0, 1);
        var bad = record("t", "d", 2, 1);
        var error = new OutboxPermanentException("bad");

        assertThat(classify(0, parked(delivered, Outcome.delivered()), parked(skipped, Outcome.notAttempted()),
                delivered(afterSkipped), parked(failed, Outcome.retry(DOWN)), parked(afterFailed, Outcome.delivered()),
                parked(bad, Outcome.permanent(error)))).isEqualTo(advance(
                new Unpark(delivered),
                new Keep(skipped, NOW.plus(DELAY)), // stays parked and lets its key go: afterSkipped is no follower
                head(failed, 3), // its 2 earlier attempts count: due after backoff(2)
                new Follower(afterFailed),
                new Dead(bad, 3, error.toString())));
    }

    @Test
    void requeueOfAParkedRowParksItAgain() {
        var veteran = record("t", "k", 1, 3);

        assertThat(classify(0, parked(veteran, Outcome.requeue())))
                .isEqualTo(advance(new Park(veteran, 4, NOW.plus(DELAY))));
    }

    @Test
    void anUnkeyedRecordIsParkedToo() {
        var unkeyed = record("t", null);

        assertThat(classify(0, requeue(unkeyed))).isEqualTo(advance(new Park(unkeyed, 1, NOW.plus(DELAY))));
    }

    @Test
    void parkedItemsCannotBeDbBlocked() {
        assertThatThrownBy(() -> new Item(record("t", "k"), true, null, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void waitingIsNeitherADeliveryNorAFailure() {
        var k1a = record("t", "k1");
        var k1b = record("t", "k1");
        var k2 = record("t", "k2");

        assertThat(classify(0, waiting(k1a), notAttempted(k1b), waiting(k2)))
                .as("no pause: the rows a wait held back count nowhere")
                .isEqualTo(advance(waitingHead(k1a), new Follower(k1b), waitingHead(k2)));
        assertThat(classify(0, waiting(k1a), delivered(k2))).isEqualTo(advance(waitingHead(k1a)));
    }

    @Test
    void aWaitingKeyIsNotFailingNextToARetriedOne() {
        var k1 = record("t", "k1");
        var k2 = record("t", "k2");
        var items = new Item[] {waiting(k1), retry(k2)};

        assertThat(BatchClassifier.failingKeys(List.of(items))).containsExactly(BatchClassifier.orderingKey(k2));
        assertThat(classify(0, items)).as("a single failing key pauses until the threshold").isEqualTo(PAUSE);
        assertThat(classify(THRESHOLD - 1, items)).isEqualTo(advance(waitingHead(k1), head(k2, THRESHOLD)));
    }

    @Test
    void aKeyIsDecidedByItsFirstRetryNotAttemptedOrWaitingRow() {
        var a1 = record("t", "a");
        var a2 = record("t", "a");
        var a3 = record("t", "a");
        var c1 = record("t", "c");

        assertThat(BatchClassifier.failingKeys(List.of(permanent(a1), waiting(a2), notAttempted(a3),
                requeue(record("t", "b")), waiting(record("t", "b")), notAttempted(record("t", "b")),
                retry(c1), waiting(record("t", "c")), waiting(record("t", "d")), retry(record("t", "d")))))
                .containsExactly(BatchClassifier.orderingKey(c1));
        assertThat(classify(0, permanent(a1), waiting(a2), notAttempted(a3)))
                .as("a dead row before the wait does not make its key failing")
                .isEqualTo(advance(new Dead(a1, 1, new OutboxPermanentException("bad").toString()), waitingHead(a2),
                        new Follower(a3)));
    }

    @Test
    void aParkedRowThatWaitsBecomesAWaitingHead() {
        var veteran = record("t", "k", 2, 3);
        var after = record("t", "k");

        assertThat(classify(0, parked(veteran, Outcome.waitFor("not yet")), notAttempted(after)))
                .isEqualTo(advance(waitingHead(veteran), new Follower(after)));
    }

    @Test
    void aWaitSpendsNoAttempt() {
        var retried = record("t", "k1", 4, 0);
        var failing = record("t", "k2");

        assertThat(classify(THRESHOLD - 1, waiting(retried), retry(failing))).isEqualTo(advance(
                new Head(retried, 4, NOW.plus(WAIT), "waiting (handler): not yet", Outcome.Waiting.Kind.HANDLER),
                head(failing, THRESHOLD)));
    }

    @Test
    void firstWaitDueTime() {
        var plain = record("t", "a");
        var later = record("t", "b");
        var early = record("t", "c");
        var farAway = record("t", "d");
        var contract = record("t", "e");

        assertThat(classify(0, waiting(plain),
                new Item(later, false, Outcome.waitFor("not yet", NOW.plusSeconds(30))),
                new Item(early, false, Outcome.waitFor("not yet", NOW.plusMillis(100))),
                new Item(farAway, false, Outcome.waitFor("not yet", NOW.plus(Duration.ofHours(30)))),
                new Item(contract, false, new Outcome.Waiting(Outcome.Waiting.Kind.CONTRACT, "v2", null))))
                .isEqualTo(advance(
                        waitingHead(plain),
                        new Head(later, 0, NOW.plusSeconds(30), "waiting (handler): not yet",
                                Outcome.Waiting.Kind.HANDLER),
                        waitingHead(early), // never before the delay
                        new Head(farAway, 0, NOW.plus(Duration.ofHours(24)), "waiting (handler): not yet",
                                Outcome.Waiting.Kind.HANDLER), // notBefore capped at 24 h
                        new Head(contract, 0, NOW.plus(WAIT), "waiting (contract): v2",
                                Outcome.Waiting.Kind.CONTRACT)));
    }

    @Test
    void waitBackoffIsAQuarterOfTheWaitWithinBounds() {
        assertThat(BatchClassifier.waitBackoff(Duration.ZERO, WAIT, WAIT_MAX)).isEqualTo(WAIT);
        assertThat(BatchClassifier.waitBackoff(Duration.ofSeconds(20), WAIT, WAIT_MAX))
                .isEqualTo(Duration.ofSeconds(5));
        assertThat(BatchClassifier.waitBackoff(Duration.ofHours(1), WAIT, WAIT_MAX)).isEqualTo(WAIT_MAX);
    }

    private static Decision classify(int priorFailures, Item... items) {
        return classify(priorFailures, 0, items);
    }

    private static Decision classify(int priorFailures, int maxRequeues, Item... items) {
        return BatchClassifier.classify(List.of(items), priorFailures, THRESHOLD, NOW, INITIAL, MAX, DELAY,
                maxRequeues, WAIT, WAIT_MAX);
    }

    private static Decision advance(Action... actions) {
        return new Decision(false, List.of(actions));
    }

    /** Head of a {@link #retry} item: due after the backoff of its attempts. */
    private static Head head(OutboxRecord r, int attempts) {
        return new Head(r, attempts, NOW.plus(Backoff.delay(Math.max(attempts - 1, 0), INITIAL, MAX)),
                DOWN.toString());
    }

    /** Head of a {@link #waiting} item: attempts unchanged, due after the first wait. */
    private static Head waitingHead(OutboxRecord r) {
        return new Head(r, r.attempts(), NOW.plus(WAIT), "waiting (handler): not yet", Outcome.Waiting.Kind.HANDLER);
    }

    private static Item delivered(OutboxRecord r) {
        return new Item(r, false, Outcome.delivered());
    }

    private static Item retry(OutboxRecord r) {
        return new Item(r, false, Outcome.retry(DOWN));
    }

    private static Item notAttempted(OutboxRecord r) {
        return new Item(r, false, Outcome.notAttempted());
    }

    private static Item permanent(OutboxRecord r) {
        return new Item(r, false, Outcome.permanent(new OutboxPermanentException("bad")));
    }

    private static Item blocked(OutboxRecord r) {
        return new Item(r, true, null);
    }

    private static Item requeue(OutboxRecord r) {
        return new Item(r, false, Outcome.requeue());
    }

    private static Item requeue(OutboxRecord r, Instant notBefore) {
        return new Item(r, false, Outcome.requeue(notBefore));
    }

    private static Item waiting(OutboxRecord r) {
        return new Item(r, false, Outcome.waitFor("not yet"));
    }

    private static Item parked(OutboxRecord r, Outcome outcome) {
        return new Item(r, false, outcome, true);
    }

    private OutboxRecord record(String topic, @Nullable String key) {
        return record(topic, key, 0, 0);
    }

    private OutboxRecord record(String topic, @Nullable String key, int attempts, int requeues) {
        return new OutboxRecord(ids.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), null, List.of(), NOW, attempts, requeues);
    }
}
