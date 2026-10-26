package io.github.dsudomoin.kora.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxPermanentException;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Action;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Decision;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Follower;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Head;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Item;
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

    private static Decision classify(int priorFailures, Item... items) {
        return BatchClassifier.classify(List.of(items), priorFailures, THRESHOLD, NOW, INITIAL, MAX);
    }

    private static Decision advance(Action... actions) {
        return new Decision(false, List.of(actions));
    }

    /** Head of a {@link #retry} item: due after the backoff of its attempts. */
    private static Head head(OutboxRecord r, int attempts) {
        return new Head(r, attempts, NOW.plus(Backoff.delay(Math.max(attempts - 1, 0), INITIAL, MAX)),
                DOWN.toString());
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

    private OutboxRecord record(String topic, @Nullable String key) {
        return new OutboxRecord(ids.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), null, List.of(), NOW, 0);
    }
}
