package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.TestTracing.TRACEPARENT;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACE_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.header;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

class OutboxHandlerTest {

    private static final AtomicLong IDS = new AtomicLong();

    @AfterEach
    void clear() {
        Context.clear();
        Thread.interrupted();
    }

    @Test
    void sequentialDeliversInOrder() {
        var handler = new RecordingHandler();
        var records = List.of(record("t", "a"), record("t", "b"), record("u", null), record("t", "a"));

        var outcomes = handler.handleBatch(records);

        assertThat(outcomes).containsExactly(
                Outcome.delivered(), Outcome.delivered(), Outcome.delivered(), Outcome.delivered());
        assertThat(outcomes.get(0)).isSameAs(Outcome.Delivered.INSTANCE);
        assertThat(handler.delivered()).containsExactlyElementsOf(records);
    }

    @Test
    void failureBlocksOnlyThatTopicKey() {
        var handler = new RecordingHandler();
        var k1 = record("t", "k1");
        var k2 = record("t", "k2");
        var k1Again = record("t", "k1");
        var k3 = record("t", "k3");
        var otherTopic = record("u", "k1");
        var error = new IllegalStateException("down");
        handler.failNext(r -> r == k1, () -> error);

        var outcomes = handler.handleBatch(List.of(k1, k2, k1Again, k3, otherTopic));

        assertThat(outcomes).containsExactly(
                Outcome.retry(error),
                Outcome.delivered(),
                Outcome.notAttempted(),
                Outcome.delivered(),
                Outcome.delivered());
        assertThat(handler.delivered()).containsExactly(k2, k3, otherTopic);
    }

    @Test
    void probesThreeKeysThenStopsWhenNothingSucceeded() {
        var attempted = new ArrayList<OutboxRecord>();
        OutboxHandler handler = r -> {
            attempted.add(r);
            throw new RuntimeException("down");
        };
        var records = List.of(
                record("t", "1"), record("t", null), record("t", "3"), record("t", "4"), record("t", "5"),
                record("t", "6"));

        var outcomes = handler.handleBatch(records);

        assertThat(outcomes).extracting(Object::getClass).containsExactly(
                Outcome.Retry.class, Outcome.Retry.class, Outcome.Retry.class,
                Outcome.NotAttempted.class, Outcome.NotAttempted.class, Outcome.NotAttempted.class);
        assertThat(outcomes.subList(3, 6)).containsOnly(Outcome.notAttempted());
        assertThat(attempted).containsExactlyElementsOf(records.subList(0, 3));
    }

    @Test
    void probeLimitCountsDistinctPairsAndStopsOnlyBeforeAnySuccess() {
        var attempted = new ArrayList<OutboxRecord>();
        var ok = record("t", "ok");
        OutboxHandler handler = r -> {
            attempted.add(r);
            if (r != ok) {
                throw new RuntimeException("down");
            }
        };
        // k1 fails twice (one pair), then 2 more pairs fail, then a success: never 3 distinct pairs before success
        var noStop = List.of(record("t", "k1"), record("t", "k1"), record("t", "k2"), ok, record("t", "k3"),
                record("t", "k4"), record("t", "k5"));

        var outcomes = handler.handleBatch(noStop);

        assertThat(outcomes).extracting(Object::getClass).containsExactly(
                Outcome.Retry.class, Outcome.NotAttempted.class, Outcome.Retry.class, Outcome.Delivered.class,
                Outcome.Retry.class, Outcome.Retry.class, Outcome.Retry.class);
        assertThat(attempted).hasSize(6);
    }

    @Test
    void permanentDoesNotBlockKey() {
        var handler = new RecordingHandler();
        var poison = record("t", "k");
        var next = record("t", "k");
        var error = new OutboxPermanentException("bad payload");
        handler.failAlways(r -> r == poison, () -> error);
        var others = new ArrayList<OutboxRecord>();
        for (int i = 0; i < 5; i++) { // permanent failures never count towards the probe limit
            var p = record("t", "p" + i);
            handler.failAlways(r -> r == p, () -> new OutboxPermanentException("bad"));
            others.add(p);
        }
        var records = new ArrayList<OutboxRecord>(others);
        records.add(poison);
        records.add(next);

        var outcomes = handler.handleBatch(records);

        assertThat(outcomes.get(5)).isEqualTo(Outcome.permanent(error));
        assertThat(outcomes.get(6)).isEqualTo(Outcome.delivered());
        assertThat(outcomes.subList(0, 5)).allMatch(o -> o instanceof Outcome.Permanent);
        assertThat(handler.delivered()).containsExactly(next);
    }

    @Test
    void errorFailsOnlyItsRecordAndBlocksItsKey() {
        var poison = record("t", "k");
        var errors = List.<Error>of(new AssertionError("fatal"), new StackOverflowError("deep"),
                new OutOfMemoryError("big"));
        for (var error : errors) {
            var handler = new RecordingHandler();
            OutboxHandler throwing = r -> {
                if (r == poison) {
                    throw error;
                }
                handler.handle(r);
            };
            var other = record("t", "j");
            var next = record("t", "k");
            var outer = Context.current();

            assertThat(throwing.handleBatch(List.of(poison, other, next)))
                    .containsExactly(Outcome.retry(error), Outcome.delivered(), Outcome.notAttempted());
            assertThat(handler.delivered()).containsExactly(other);
            assertThat(Context.current()).isSameAs(outer);
        }
    }

    @Test
    void interruptedThreadAttemptsNothingFurther() {
        var attempted = new ArrayList<OutboxRecord>();
        OutboxHandler handler = r -> {
            attempted.add(r);
            throw new InterruptedException();
        };
        var records = List.of(record("t", "a"), record("t", "b"), record("t", null));

        var outcomes = handler.handleBatch(records);

        assertThat(outcomes.get(0)).isInstanceOf(Outcome.Retry.class);
        assertThat(outcomes.subList(1, 3)).containsOnly(Outcome.notAttempted());
        assertThat(attempted).hasSize(1);
        assertThat(Thread.currentThread().isInterrupted()).as("interrupt flag restored").isTrue();
    }

    @Test
    void byTopicRoutesAndKeepsPositions() {
        var a = new RecordingHandler();
        var b = new RecordingHandler();
        var fallback = new RecordingHandler();
        var a1 = record("a", "k");
        var b1 = record("b", "k");
        var c1 = record("c", "k");
        var a2 = record("a", "k");
        var b2 = record("b", "x");
        var b3 = record("b", "x");
        var error = new RuntimeException("b down");
        b.failNext(r -> r == b2, () -> error);
        var handler = OutboxHandler.byTopic(Map.of("a", a, "b", b), fallback);

        var outcomes = handler.handleBatch(List.of(a1, b1, c1, a2, b2, b3));

        assertThat(outcomes).containsExactly(
                Outcome.delivered(), Outcome.delivered(), Outcome.delivered(), Outcome.delivered(),
                Outcome.retry(error), Outcome.notAttempted());
        assertThat(a.delivered()).containsExactly(a1, a2);
        assertThat(b.delivered()).containsExactly(b1);
        assertThat(fallback.delivered()).containsExactly(c1);
        assertThat(a.batches()).hasSize(1);
        assertThat(b.batches())
                .containsExactly(List.of(Outcome.delivered(), Outcome.retry(error), Outcome.notAttempted()));
    }

    @Test
    void throwingRouteDoesNotBlockOtherRoutes() {
        var a = new RecordingHandler();
        var b = new RecordingHandler();
        var a1 = record("a", "k");
        var b1 = record("b", "k");
        var a2 = record("a", null);
        var error = new IllegalStateException("route a broken");
        a.throwFromBatch(() -> error);
        var handler = OutboxHandler.byTopic(Map.of("a", a, "b", b), new RecordingHandler());

        assertThat(handler.handleBatch(List.of(a1, b1, a2)))
                .containsExactly(Outcome.retry(error), Outcome.delivered(), Outcome.retry(error));
        assertThat(b.delivered()).containsExactly(b1);

        var fatal = new AssertionError("fatal");
        a.throwFromBatch(() -> fatal);
        assertThatThrownBy(() -> handler.handleBatch(List.of(a1, b1))).as("errors propagate").isSameAs(fatal);
    }

    @Test
    void byTopicRouteReturningAnInvalidListFailsOnlyItsRecords() {
        var answers = List.<Function<List<OutboxRecord>, List<Outcome>>>of(
                records -> Collections.emptyList(),
                records -> null,
                records -> Arrays.asList(new Outcome[records.size()]));
        for (var answer : answers) {
            OutboxHandler broken = new OutboxHandler() {
                @Override
                public void handle(OutboxRecord record) {}

                @Override
                public List<Outcome> handleBatch(List<OutboxRecord> records) {
                    return answer.apply(records);
                }
            };
            var handler = OutboxHandler.byTopic(Map.of("ok", new RecordingHandler()), broken);
            var records = List.of(record("t", "k"), record("ok", "k"), record("t", null));

            try (var err = new CapturedStderr()) {
                var outcomes = handler.handleBatch(records);
                handler.handleBatch(records);

                assertThat(outcomes.get(1)).isEqualTo(Outcome.delivered());
                assertThat(List.of(outcomes.get(0), outcomes.get(2))).allSatisfy(o -> assertThat(o)
                        .isInstanceOfSatisfying(Outcome.Retry.class, r -> assertThat(r.error())
                                .hasMessageContaining("invalid outcome list for 2 records")));
                assertThat(err.text().split("ERROR " + OutboxHandler.class.getName(), -1))
                        .as("one ERROR per 10 s").hasSize(2);
            }
        }
    }

    @Test
    void byTopicPassesRequeueThrough() {
        OutboxHandler requeueing = new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                return records.stream().map(r -> Outcome.requeue()).toList();
            }
        };
        var handler = OutboxHandler.byTopic(Map.of("q", requeueing), new RecordingHandler());

        assertThat(handler.handleBatch(List.of(record("t", "k"), record("q", "k"), record("t", null))))
                .containsExactly(Outcome.delivered(), Outcome.requeue(), Outcome.delivered());
    }

    @Test
    void handleSeesTraceContextOfRecord() {
        var traceIds = new ArrayList<String>();
        OutboxHandler handler = r -> traceIds.add(OpentelemetryContext.getSpan() == null
                ? null
                : OpentelemetryContext.getSpan().getSpanContext().getTraceId());
        var traced = record("t", "k", List.of(header("traceparent", TRACEPARENT)));
        var untraced = record("t", "k", List.of());

        handler.handleBatch(List.of(traced, untraced));

        assertThat(traceIds.get(0)).isEqualTo(TRACE_ID);
        assertThat(traceIds.get(1)).as("no span leaks from the previous record").isNotEqualTo(TRACE_ID);
        assertThat(OpentelemetryContext.getSpan()).isNull();
    }

    @Test
    void outcomeFactories() {
        var e = new RuntimeException();
        var at = Instant.parse("2026-10-25T00:00:00Z");
        assertThat(Outcome.retry(e)).isEqualTo(new Outcome.Retry(e, null));
        assertThat(Outcome.retry(e, at)).isEqualTo(new Outcome.Retry(e, at));
        assertThat(Outcome.permanent(e)).isEqualTo(new Outcome.Permanent(e));
        assertThat(Outcome.notAttempted()).isEqualTo(new Outcome.NotAttempted(null));
        assertThat(Outcome.notAttempted(at)).isEqualTo(new Outcome.NotAttempted(at));
        assertThat(Outcome.requeue()).isEqualTo(new Outcome.Requeue(null));
        assertThat(Outcome.requeue(at)).isEqualTo(new Outcome.Requeue(at));
        assertThatThrownBy(() -> Outcome.requeue(null)).isInstanceOf(NullPointerException.class);
        assertThat(new OutboxPermanentException("m", e)).hasMessage("m").hasCause(e);
    }

    private static OutboxRecord record(String topic, @Nullable String key) {
        return record(topic, key, List.of());
    }

    private static OutboxRecord record(String topic, @Nullable String key, List<OutboxHeader> headers) {
        return new OutboxRecord(IDS.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), new byte[] {1}, headers, Instant.now(), 0, 0);
    }
}
