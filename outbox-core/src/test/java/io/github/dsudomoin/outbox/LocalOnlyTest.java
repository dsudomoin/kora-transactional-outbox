package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class LocalOnlyTest {

    private static final AtomicLong IDS = new AtomicLong();
    private static final OutboxHeader MARKER = new OutboxHeader("outbox-local", null);
    /** A header of the marker's name with a value: it marks nothing. */
    private static final OutboxHeader VALUED = new OutboxHeader("outbox-local", "x".getBytes(UTF_8));

    private final RecordingHandler delegate = new RecordingHandler();
    private final LocalOnly handler = new LocalOnly(delegate);

    @Test
    void aLocalRecordWaitsAndBlocksItsKeyWhileTheRestGoToTheDelegateInOneCall() {
        var k1Local = record("billing", "k1", MARKER);
        var k1 = record("billing", "k1");
        var k2 = record("billing", "k2");
        var unkeyedLocal = record("billing", null, MARKER);
        var unkeyed = record("billing", null);

        var outcomes = handler.handleBatch(List.of(k1Local, k1, k2, unkeyedLocal, unkeyed));

        assertThat(outcomes).containsExactly(waiting("billing"), Outcome.notAttempted(), Outcome.delivered(),
                waiting("billing"), Outcome.delivered());
        assertThat(delegate.delivered()).containsExactly(k2, unkeyed);
        assertThat(delegate.batches()).as("one call").hasSize(1);

        assertThat(handler.handleBatch(List.of(record("audit", "k", MARKER), record("audit", null, MARKER))))
                .containsExactly(waiting("audit"), waiting("audit"));
        assertThat(delegate.batches()).as("no call without a record for it").hasSize(1);
    }

    @Test
    void aCallWithoutALocalRecordIsTheDelegatesOwn() {
        var records = List.of(record("billing", "k"), record("billing", "k", VALUED), record("billing", null));
        var seen = new AtomicReference<List<OutboxRecord>>();
        var answer = List.of(Outcome.delivered(), Outcome.requeue(), Outcome.delivered());
        var wrapped = new LocalOnly(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> batch) {
                seen.set(batch);
                return answer;
            }
        });

        assertThat(wrapped.handleBatch(records)).isSameAs(answer);
        assertThat(seen.get()).isSameAs(records);
    }

    @Test
    void aThrowingDelegateFailsOnlyItsRecordsAndTheLocalOnesStillWait() {
        var records = List.of(record("billing", "k1", MARKER), record("billing", "k2"), record("billing", null));
        var down = new IllegalStateException("kafka down");
        delegate.throwFromBatch(() -> down);

        assertThat(handler.handleBatch(records))
                .containsExactly(waiting("billing"), Outcome.retry(down), Outcome.retry(down));

        delegate.throwFromBatch(() -> new OutboxWaitException("not now"));
        var notNow = new Outcome.Waiting(Outcome.Waiting.Kind.HANDLER, "not now", null);
        assertThat(handler.handleBatch(records)).containsExactly(waiting("billing"), notNow, notNow);

        var fatal = new AssertionError("fatal");
        delegate.throwFromBatch(() -> fatal);
        assertThatThrownBy(() -> handler.handleBatch(records)).as("errors propagate").isSameAs(fatal);
    }

    @Test
    void anInvalidDelegateListFailsOnlyItsRecordsLoggedOncePer10Seconds() {
        var answers = List.<Function<List<OutboxRecord>, List<Outcome>>>of(
                records -> Collections.emptyList(),
                records -> null,
                records -> Arrays.asList(new Outcome[records.size()]));
        for (var answer : answers) {
            var wrapped = new LocalOnly(new OutboxHandler() {
                @Override
                public void handle(OutboxRecord record) {}

                @Override
                public List<Outcome> handleBatch(List<OutboxRecord> records) {
                    return answer.apply(records);
                }
            });
            var records = List.of(record("billing", "k"), record("billing", "j", MARKER), record("billing", null));

            try (var err = new CapturedStderr()) {
                var outcomes = wrapped.handleBatch(records);
                wrapped.handleBatch(records);

                assertThat(outcomes.get(1)).isEqualTo(waiting("billing"));
                assertThat(List.of(outcomes.get(0), outcomes.get(2))).allSatisfy(o -> assertThat(o)
                        .isInstanceOfSatisfying(Outcome.Retry.class, r -> assertThat(r.error())
                                .hasMessageContaining("invalid outcome list for 2 records")));
                assertThat(err.text().split("ERROR " + OutboxHandler.class.getName(), -1))
                        .as("one ERROR per 10 s").hasSize(2);
            }
        }
    }

    @Test
    void handleThrowsAWaitForALocalRecordAndHandsOnAnyOther() throws Exception {
        var plain = record("billing", "k");
        var valued = record("billing", "k", VALUED);

        assertThatThrownBy(() -> handler.handle(record("billing", "k", MARKER)))
                .isInstanceOfSatisfying(OutboxWaitException.class,
                        e -> assertThat(e.kind()).isEqualTo(Outcome.Waiting.Kind.NO_ROUTE))
                .hasMessage(waiting("billing").reason());
        handler.handle(plain);
        handler.handle(valued);

        assertThat(delegate.delivered()).containsExactly(plain, valued);
    }

    private static Outcome.Waiting waiting(String topic) {
        return new Outcome.Waiting(Outcome.Waiting.Kind.NO_ROUTE,
                "a record for a local handler of topic " + topic + ", which no route on this instance claims", null);
    }

    private static OutboxRecord record(String topic, @Nullable String key, OutboxHeader... headers) {
        return new OutboxRecord(IDS.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), new byte[] {1}, List.of(headers), Instant.now(), 0, 0);
    }
}
