package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.NOT_LOCKED;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.NO_WORK;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.PAUSED;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.PROGRESS;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxPermanentException;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardResult;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.kora.outbox.testing.TestTracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import jakarta.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

class ShardProcessorIT {

    /** {@link TestOutboxConfig}'s retry initial delay. */
    private static final Duration INITIAL = Duration.ofMillis(50);
    /** {@link TestOutboxConfig}'s requeue delay. */
    private static final Duration REQUEUE_DELAY = Duration.ofMillis(100);

    private final JdbcDatabase db = TestDb.newDatabase(4);
    private final TestOutboxConfig config = new TestOutboxConfig().shards(1);
    private final RecordingHandler handler = new RecordingHandler();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final List<Connection> open = new ArrayList<>();
    /** SQL that {@link #process()} prepared, in order, and when ({@code nanoTime}). */
    private final List<String> statements = new CopyOnWriteArrayList<>();
    private final List<Long> statementTimes = new CopyOnWriteArrayList<>();
    /** The records of each {@code handleBatch} call, in call order. */
    private final List<List<OutboxRecord>> calls = new CopyOnWriteArrayList<>();
    /** Test clock passed to {@code tryProcess}; Postgres keeps microseconds. */
    private Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    /** Answers {@code handleBatch} instead of {@link #handler} while set. */
    private volatile @Nullable Function<List<OutboxRecord>, List<Outcome>> script;
    private @Nullable Tracer tracer;
    private TableNames names;
    private Ticks ticks;
    private Cursors cursors;
    private PgOutbox outbox;
    private ShardProcessor processor;

    @AfterEach
    void release() throws SQLException {
        if (processor != null) {
            processor.close();
        }
        for (var c : open) {
            c.close();
        }
        db.release();
        Context.clear();
    }

    @Test
    void deliversCommittedRowsAndAdvancesCursor() throws Exception {
        bootstrap();
        assertThat(process()).as("no tick since bootstrap").isEqualTo(new ShardResult(NO_WORK, 0, true));
        var ids = send(msg("t", "a", "a1"), msg("t", "b", "b1"), msg("u", null, "u1"), msg("t", "a", "a2"));
        long t = tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 4, true));

        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
        assertThat(handler.delivered()).extracting(OutboxRecord::attempts).containsOnly(0);
        assertThat(cursor()).isEqualTo(new Cursor(0, t, null, 0, null, 0, null, null));
        assertThat(process()).isEqualTo(new ShardResult(NO_WORK, 0, true));
        assertThat(handler.delivered()).hasSize(4);
        assertThat(counter("outbox.messages.delivered", "topic", "t")).isEqualTo(3);
        assertThat(counter("outbox.messages.delivered", "topic", "u")).isEqualTo(1);
        assertThat(meters.find("outbox.batch.duration").timer().count()).isEqualTo(1);
    }

    @Test
    void chunkedBatchDoesNotLoseLongTransaction() throws Exception {
        config.batchSize(3);
        bootstrap();
        long t0 = cursor().tickId();
        var longTx = begin();
        var late = outbox.send(longTx, msg("t", "late", "late")); // the smallest id, committed after t1
        var others = send(IntStream.range(0, 6).mapToObj(i -> msg("t", "k" + i, "k" + i))
                .toArray(OutboxMessage[]::new));
        long t1 = tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 3, false));
        long lastId = handler.delivered().get(2).id();
        assertThat(cursor()).isEqualTo(new Cursor(0, t0, t1, lastId, null, 0, null, null));
        longTx.commit();
        long t2 = tick();
        processUntilNoWork();

        var expected = new ArrayList<>(others);
        expected.add(late);
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(expected);
        assertThat(cursor()).isEqualTo(new Cursor(0, t2, null, 0, null, 0, null, null));
    }

    @Test
    void giantTransactionIsDeliveredCompletelyInOrder() throws Exception {
        config.batchSize(100);
        bootstrap();
        var ids = send(IntStream.range(0, 5000).mapToObj(i -> msg("t", "k", "m" + i)).toArray(OutboxMessage[]::new));
        tick();

        var results = processUntilNoWork();

        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
        assertThat(results).extracting(ShardResult::delivered).allMatch(n -> n <= 100);
    }

    @Test
    void laggingRelayBoundsBatchesByDataVolume() throws Exception {
        config.batchSize(1000); // maxTicksPerBatch 100 would merge all 40 ticks into one 80 000-row batch
        bootstrap();
        var ids = new ArrayList<UUID>();
        for (int t = 0; t < 40; t++) {
            int first = t * 2000;
            ids.addAll(send(IntStream.range(first, first + 2000).mapToObj(i -> msg("t", "k" + i % 50, "m" + i))
                    .toArray(OutboxMessage[]::new)));
            tick();
        }

        var results = processUntilNoWork();

        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
        assertThat(handler.delivered()).extracting(OutboxRecord::id).isSorted();
        // each call with a batch reads one chunk; a batch ends with the call that finishes it
        var chunksPerBatch = new ArrayList<Integer>();
        int chunks = 0;
        for (var result : results) {
            chunks++;
            if (result.batchFinished()) {
                chunksPerBatch.add(chunks);
                chunks = 0;
            }
        }
        assertThat(chunks).isZero();
        assertThat(chunksPerBatch).as("one tick per batch, 2000 rows in chunks of 1000").hasSize(40)
                .allMatch(n -> n <= 3);
    }

    @Test
    void failingKeyIsDivertedOthersFlow() throws Exception {
        bootstrap();
        handler.failNext(payload("k1#1"), () -> new RuntimeException("k1 down"));
        send(msg("t", "k1", "k1#1"), msg("t", "k2", "k2#1"), msg("t", "k1", "k1#2"), msg("t", "k3", "k3#1"),
                msg("t", "k1", "k1#3"));
        long t = tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, false)); // k1#3 ends it unattempted: read again
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        var due = now.plus(INITIAL);
        assertThat(delivered()).containsExactly("k2#1", "k3#1");
        assertThat(retryRows()).containsExactly("k1#1:1:" + due, "k1#2:0:-", "k1#3:0:-");
        assertThat(cursor()).isEqualTo(new Cursor(0, t, null, 0, null, 0, due, null));
        assertThat(counter("outbox.messages.failed", "topic", "t", "outcome", "retry")).isEqualTo(1);

        send(msg("t", "k1", "k1#4"), msg("t", "k2", "k2#2")); // k1 stays blocked by its retry rows
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(retryRows()).containsExactly("k1#1:1:" + due, "k1#2:0:-", "k1#3:0:-", "k1#4:0:-");

        now = due; // the head, then each promoted follower, in the same pass
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 4, true));

        assertThat(delivered()).containsExactly("k2#1", "k3#1", "k2#2", "k1#1", "k1#2", "k1#3", "k1#4");
        assertThat(handler.delivered().get(3).attempts()).isEqualTo(1);
        assertThat(retryRows()).isEmpty();
        assertThat(cursor().retryDueAt()).isNull();
        assertThat(process().status()).isEqualTo(NO_WORK);
    }

    @Test
    void perTopicIsolation() throws Exception {
        bootstrap();
        handler.failAlways(r -> r.topic().equals("a"), () -> new RuntimeException("a down"));
        send(msg("a", "k", "a#1"), msg("b", "k", "b#1"), msg("a", "k", "a#2"), msg("b", "k", "b#2"));
        tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(retryRows()).containsExactly("a#1:1:" + now.plus(INITIAL), "a#2:0:-");

        send(msg("b", "k", "b#3"), msg("a", "k", "a#3"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(delivered()).containsExactly("b#1", "b#2", "b#3");
        assertThat(retryRows()).containsExactly("a#1:1:" + now.plus(INITIAL), "a#2:0:-", "a#3:0:-");
    }

    @Test
    void permanentGoesToDeadAndKeyContinues() throws Exception {
        bootstrap();
        handler.failNext(payload("k#1"), () -> new OutboxPermanentException("bad k#1"));
        handler.failNext(payload("j#1"), () -> new RuntimeException("j busy"));
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"), msg("t", "j", "j#1"), msg("t", "j", "j#2"),
                msg("t", "k", "k#3"));
        tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true, true));
        assertThat(delivered()).containsExactly("k#2", "k#3");
        assertThat(deadRows()).containsExactly("k#1:1");
        assertThat(retryRows()).containsExactly("j#1:1:" + now.plus(INITIAL), "j#2:0:-");

        // in the retry pass a permanent failure moves the head to dead and its follower goes on in the same pass
        handler.failNext(payload("j#1"), () -> new OutboxPermanentException("bad j#1"));
        now = now.plus(INITIAL);
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true, true));

        assertThat(delivered()).containsExactly("k#2", "k#3", "j#2");
        assertThat(deadRows()).containsExactly("k#1:1", "j#1:2");
        assertThat(retryRows()).isEmpty();
        assertThat(counter("outbox.messages.failed", "topic", "t", "outcome", "dead")).isEqualTo(2);
    }

    @Test
    void targetDownPausesWithoutWritingRows() throws Exception {
        bootstrap();
        handler.failAlways(r -> true, () -> new RuntimeException("target down"));
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "c", "c#1"), msg("t", "a", "a#2"));
        tick();
        long t0 = cursor().tickId();

        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
        assertThat(retryRows()).isEmpty();
        assertThat(deadRows()).isEmpty();
        var paused = cursor();
        assertThat(paused).isEqualTo(new Cursor(0, t0, null, 0, paused.pausedUntil(), 1, null, null));
        assertPausedFor(paused, INITIAL);
        assertThat(process().status()).as("before pausedUntil").isEqualTo(NO_WORK);

        now = paused.pausedUntil();
        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
        paused = cursor();
        assertThat(paused).isEqualTo(new Cursor(0, t0, null, 0, paused.pausedUntil(), 2, null, null));
        assertPausedFor(paused, INITIAL.multipliedBy(2));
        assertThat(counter("outbox.shard.paused")).isEqualTo(2);

        handler.reset(); // the target is back
        now = cursor().pausedUntil();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 4, true));
        assertThat(delivered()).containsExactly("a#1", "b#1", "c#1", "a#2");
        assertThat(cursor().failures()).isZero();
        assertThat(cursor().pausedUntil()).isNull();
    }

    @Test
    void singlePoisonHeadIsDivertedAfterThreshold() throws Exception {
        config.headFailuresBeforeDivert(3);
        bootstrap();
        handler.failAlways(r -> r.topic().equals("t"), () -> new RuntimeException("poison"));
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"));
        long t0 = cursor().tickId();
        long t = tick();

        for (int failures = 1; failures <= 2; failures++) {
            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
            assertThat(cursor().failures()).isEqualTo(failures);
            assertThat(retryRows()).isEmpty();
            now = cursor().pausedUntil();
        }
        // diverted at the third failure and advanced to it (k#2, not attempted after it, is read again); still a
        // failed attempt, so the shard keeps backing off
        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));

        var due = now.plus(Backoff.delay(2, INITIAL, config.maxDelay()));
        assertThat(retryRows()).containsExactly("k#1:3:" + due);
        var cursor = cursor();
        assertThat(cursor).isEqualTo(new Cursor(0, t0, t, retryIds().get(0), cursor.pausedUntil(), 3, due, null));
        assertPausedFor(cursor, Backoff.delay(2, INITIAL, config.maxDelay()));
    }

    @Test
    void maxAttemptsMovesToDead() throws Exception {
        config.retry(INITIAL, Duration.ofMillis(500), 2);
        bootstrap();
        var attempts = new AtomicInteger();
        handler.failAlways(payload("k#1"), () -> {
            attempts.incrementAndGet();
            return new RuntimeException("k#1 rejected");
        });
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(retryRows()).containsExactly("k#1:1:" + now.plus(INITIAL), "k#2:0:-");

        // never-attempted heads keep their attempts and last error and are no failure: due at
        // max(now + initialDelay, notBefore)
        script = records -> records.stream().map(r -> Outcome.notAttempted()).toList();
        now = now.plus(INITIAL);
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(retryRows()).containsExactly("k#1:1:" + now.plus(INITIAL), "k#2:0:-");
        var notBefore = now.plusSeconds(60);
        script = records -> records.stream().map(r -> Outcome.notAttempted(notBefore)).toList();
        now = now.plus(INITIAL);
        process();
        assertThat(retryRows()).containsExactly("k#1:1:" + notBefore, "k#2:0:-");
        assertThat(rows("select last_error from " + names.retry() + " where next_attempt_at is not null",
                rs -> rs.getString(1))).singleElement().asString().contains("k#1 rejected");
        assertThat(counter("outbox.messages.failed", "topic", "t", "outcome", "retry")).isEqualTo(1);

        script = null;
        now = notBefore;
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true, true));
        assertThat(attempts).as("maxAttempts = 2: two delivery attempts in all").hasValue(2);
        assertThat(deadRows()).containsExactly("k#1:2");
        // k#2 was handed with k#1 and skipped after its failure: the next head, not attempted, attempts 0
        assertThat(retryRows()).containsExactly("k#2:0:" + now.plus(INITIAL));

        now = now.plus(INITIAL);
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(retryRows()).isEmpty();
        assertThat(delivered()).containsExactly("j#1", "k#2");
    }

    @Test
    void batchTimeoutCountsAsFailure() throws Exception {
        config.batchTimeout(Duration.ofMillis(300));
        bootstrap();
        handler.delay(Duration.ofSeconds(2));
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        tick();

        long started = System.nanoTime();
        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(1500));
        assertThat(retryRows()).isEmpty();
        assertThat(cursor().failures()).isEqualTo(1);
        assertThat(cursor().pausedUntil()).as("the backoff starts after the call, cut at 80 % of batchTimeout")
                .isAfterOrEqualTo(now.plus(Duration.ofMillis(240)).plus(INITIAL));

        // the call was interrupted, so the handler attempted nothing behind the relay's back
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (handler.batches().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(handler.batches()).singleElement().satisfies(outcomes ->
                assertThat(outcomes).allMatch(o -> o instanceof Outcome.NotAttempted));
        assertThat(handler.delivered()).isEmpty();
    }

    @Test
    void callCutAtTheSoftDeadlineKeepsItsOutcomesAndRereadsTheRest() throws Exception {
        config.batchTimeout(Duration.ofSeconds(2)); // soft deadline at 1.6 s: 400 ms for the call to return
        bootstrap();
        var hang = new AtomicBoolean(true);
        script = hangingOn("k#1", hang)::handleBatch;
        send(msg("t", "a", "a#1"), msg("t", "k", "k#1"), msg("t", "b", "b#1"), msg("t", "k", "k#2"),
                msg("t", "c", "c#1"));
        long t0 = cursor().tickId();
        long t = tick();

        // a#1 delivered, k#1 interrupted (a failed attempt), the rest never started: unread, the batch goes on
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, false));
        hang.set(false);
        var due = now.plus(INITIAL);
        assertThat(retryRows()).containsExactly("k#1:1:" + due);
        assertThat(cursor()).isEqualTo(new Cursor(0, t0, t, retryIds().get(0), null, 0, due, null));

        // read again: k#2 is blocked by k#1's retry row and follows it
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(retryRows()).containsExactly("k#1:1:" + due, "k#2:0:-");
        now = due;
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(delivered()).containsExactly("a#1", "b#1", "c#1", "k#1", "k#2");
        assertThat(retryRows()).isEmpty();
    }

    @Test
    void trailingNotAttemptedWithNotBeforeIsDivertedNotReread() throws Exception {
        bootstrap();
        var until = now.plusSeconds(10); // e.g. their Kafka partition cools down
        script = records -> records.stream()
                .map(r -> payload("a#1").test(r) ? Outcome.delivered() : Outcome.notAttempted(until)).toList();
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "c", "c#1"));
        long t = tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(retryRows()).containsExactly("b#1:0:" + until, "c#1:0:" + until);
        assertThat(cursor()).isEqualTo(new Cursor(0, t, null, 0, null, 0, until, null));
    }

    @Test
    void trailingNotAttemptedRowsAreRereadBlockedBehindTheirKey() throws Exception {
        bootstrap();
        handler.failNext(payload("k#1"), () -> new RuntimeException("k down"));
        send(msg("t", "k", "k#1"), msg("t", "a", "a#1"), msg("t", "k", "k#2"));
        long t0 = cursor().tickId();
        long t = tick();

        // k#2 not attempted (its key failed in the call) ends the batch: unread, read again behind k#1's retry row
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, false));
        var due = now.plus(INITIAL);
        assertThat(retryRows()).containsExactly("k#1:1:" + due);
        assertThat(cursor()).isEqualTo(new Cursor(0, t0, t, handler.delivered().get(0).id(), null, 0, due, null));
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(retryRows()).containsExactly("k#1:1:" + due, "k#2:0:-");
    }

    @Test
    void retryPassCutAtTheSoftDeadlineKeepsItsOutcomes() throws Exception {
        config.batchTimeout(Duration.ofSeconds(2)); // 400 ms between the soft deadline and the hard one
        bootstrap();
        for (var head : List.of("a#1", "b#1", "c#1")) { // diverted while d#1 is delivered
            handler.failNext(payload(head), () -> new RuntimeException(head + " failed"));
        }
        send(msg("t", "d", "d#1"), msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "c", "c#1"),
                msg("t", "a", "a#2"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, false));
        assertThat(process()).as("a#2 read again, behind a#1").isEqualTo(new ShardResult(PROGRESS, 0, true));
        var due = now.plus(INITIAL);
        assertThat(retryRows()).containsExactly("a#1:1:" + due, "b#1:1:" + due, "c#1:1:" + due, "a#2:0:-");

        var hang = new AtomicBoolean(true);
        script = hangingOn("b#1", hang)::handleBatch;
        now = due;
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        hang.set(false);

        // one call [a#1, b#1, c#1, a#2]: a#1 delivered; b#1 failed an attempt; c#1 and a#2 never started: no attempt
        // counted, c#1's last error kept, a#2 now the head of a
        assertThat(delivered()).containsExactly("d#1", "a#1");
        assertThat(retryRows()).containsExactly("b#1:2:" + now.plus(Backoff.delay(1, INITIAL, config.maxDelay())),
                "c#1:1:" + now.plus(INITIAL), "a#2:0:" + now.plus(INITIAL));
        assertThat(rows("select last_error from " + names.retry() + " where payload = 'c#1'::bytea",
                rs -> rs.getString(1))).singleElement().asString().contains("c#1 failed");
    }

    @Test
    void twoHeadsOfOneKeyAreHandedOnce() throws Exception {
        bootstrap(); // the relay keeps one head per key; rows written outside it may not
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + names.retry() + " (id, shard,"
                + " message_id, topic, key, payload, created_at, next_attempt_at) values (?, 0, gen_random_uuid(),"
                + " 't', 'k'::bytea, ?, now(), ?)")) {
            for (int i = 1; i <= 3; i++) {
                ps.setLong(1, i);
                ps.setBytes(2, ("k#" + i).getBytes(UTF_8));
                ps.setObject(3, i == 3 ? null : now.atOffset(ZoneOffset.UTC));
                ps.executeUpdate();
            }
        }

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 3, true));
        assertThat(delivered()).containsExactly("k#1", "k#2", "k#3");
        assertThat(retryRows()).isEmpty();
    }

    @Test
    void divertedHotKeyDrainsInAFewCalls() throws Exception {
        config.batchSize(1000);
        bootstrap();
        handler.failNext(payload("k#0"), () -> new RuntimeException("k busy once"));
        var keyed = IntStream.rangeClosed(0, 2000).mapToObj(i -> "k#" + i).toList();
        var messages = new ArrayList<OutboxMessage>();
        messages.add(msg("t", "a", "a#1")); // delivered, so k#0 alone is diverted at once
        keyed.forEach(p -> messages.add(msg("t", "k", p)));
        send(messages.toArray(OutboxMessage[]::new));
        tick();
        processUntilNoWork(); // k#0 diverted, the 2000 rows after it follow it
        assertThat(retryRows()).hasSize(2001).first().asString().startsWith("k#0:1:");
        int calls = handler.batches().size();

        now = now.plus(INITIAL);
        processUntilNoWork();

        assertThat(retryRows()).isEmpty();
        assertThat(handler.batches().size() - calls).as("retry-pass calls, 1000 rows each").isEqualTo(3);
        var expected = new ArrayList<>(List.of("a#1"));
        expected.addAll(keyed);
        assertThat(delivered()).containsExactlyElementsOf(expected);
        assertThat(cursor().retryDueAt()).isNull();
    }

    @Test
    void retryPassDeliversAKeysPrefixAndStopsAtItsFirstFailure() throws Exception {
        bootstrap();
        handler.failNext(payload("k#1"), () -> new RuntimeException("k busy"));
        var messages = new ArrayList<>(List.of(msg("t", "a", "a#1")));
        IntStream.rangeClosed(1, 10).forEach(i -> messages.add(msg("t", "k", "k#" + i)));
        send(messages.toArray(OutboxMessage[]::new));
        tick();
        processUntilNoWork();
        assertThat(retryRows()).hasSize(10);
        handler.failNext(payload("k#3"), () -> new OutboxPermanentException("bad k#3"));
        handler.failNext(payload("k#6"), () -> new RuntimeException("k#6 rejected"));
        int calls = handler.batches().size();

        now = now.plus(INITIAL);
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 4, true, true));

        assertThat(handler.batches()).as("the head and its followers in one call").hasSize(calls + 1);
        assertThat(delivered()).containsExactly("a#1", "k#1", "k#2", "k#4", "k#5");
        assertThat(deadRows()).as("a permanent failure does not stop the key").containsExactly("k#3:1");
        var due = now.plus(INITIAL);
        assertThat(retryRows()).containsExactly("k#6:1:" + due, "k#7:0:-", "k#8:0:-", "k#9:0:-", "k#10:0:-");
        assertThat(cursor().retryDueAt()).isEqualTo(due);
    }

    @Test
    void retryCallFailingAsAWholeIsRetriedHeadByHeadSoOnlyThePoisonIsDead() throws Exception {
        config.retry(INITIAL, Duration.ofMillis(500), 3);
        bootstrap();
        var heads = List.of("p#1", "i1#1", "i2#1", "i3#1", "i4#1", "i5#1");
        heads.forEach(head -> handler.failNext(payload(head), () -> new RuntimeException(head + " busy")));
        var messages = new ArrayList<>(List.of(msg("t", "d", "d#1")));
        heads.forEach(head -> messages.add(msg("t", head.substring(0, head.indexOf('#')), head)));
        messages.add(msg("t", "i1", "i1#2"));
        send(messages.toArray(OutboxMessage[]::new));
        tick();
        processUntilNoWork(); // all six heads diverted together, due together
        assertThat(retryRows()).hasSize(7);
        script = records -> {
            if (records.stream().anyMatch(payload("p#1"))) {
                throw new IllegalStateException("handleBatch cannot take p#1");
            }
            return handler.handleBatch(records);
        };

        for (int pass = 0; pass < 5 && !retryRows().isEmpty(); pass++) {
            now = cursor().retryDueAt();
            process();
        }

        assertThat(deadRows()).containsExactly("p#1:3");
        assertThat(retryRows()).isEmpty();
        assertThat(delivered()).containsExactly("d#1", "i1#1", "i1#2", "i2#1", "i3#1", "i4#1", "i5#1");
    }

    @Test
    void aDownTargetCostsEachRetryHeadOneAttemptPerPass() throws Exception {
        bootstrap();
        for (var head : List.of("a#1", "b#1", "c#1")) {
            handler.failNext(payload(head), () -> new RuntimeException(head + " busy"));
        }
        send(msg("t", "d", "d#1"), msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "c", "c#1"),
                msg("t", "a", "a#2"));
        tick();
        processUntilNoWork();
        var calls = new AtomicInteger();
        script = records -> {
            calls.incrementAndGet();
            throw new IllegalStateException("target down");
        };

        for (int attempts = 2; attempts <= 3; attempts++) {
            now = cursor().retryDueAt();
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
            var due = now.plus(Backoff.delay(attempts - 1, INITIAL, config.maxDelay()));
            assertThat(retryRows()).containsExactly("a#1:" + attempts + ":" + due, "b#1:" + attempts + ":" + due,
                    "c#1:" + attempts + ":" + due, "a#2:0:-");
        }
        assertThat(calls).as("per pass: all heads, then a with a#2, a alone, b, c").hasValue(10);
    }

    @Test
    void retryCallFailingAsAWholeOverAFollowerDoesNotChargeTheHead() throws Exception {
        config.retry(INITIAL, Duration.ofMillis(500), 3);
        bootstrap();
        handler.failNext(payload("k#1"), () -> new RuntimeException("k busy"));
        send(msg("t", "d", "d#1"), msg("t", "k", "k#1"), msg("t", "k", "k#2"), msg("t", "k", "k#3"));
        tick();
        processUntilNoWork();
        assertThat(retryRows()).hasSize(3);
        script = records -> {
            if (records.stream().anyMatch(payload("k#2"))) {
                throw new IllegalStateException("handleBatch cannot take k#2");
            }
            return handler.handleBatch(records);
        };

        for (int pass = 0; pass < 5 && !retryRows().isEmpty(); pass++) {
            now = cursor().retryDueAt();
            process();
        }

        assertThat(deadRows()).containsExactly("k#2:3");
        assertThat(retryRows()).isEmpty();
        assertThat(delivered()).containsExactly("d#1", "k#1", "k#3");
    }

    @Test
    void retryCallsOutrunningBatchTimeoutStopTheReRunsOnceThePassSpentIt() throws Exception {
        var timeout = Duration.ofMillis(500);
        config.batchTimeout(timeout);
        bootstrap();
        var heads = List.of("a#1", "b#1", "c#1", "d#1", "e#1");
        heads.forEach(head -> handler.failNext(payload(head), () -> new RuntimeException(head + " busy")));
        var messages = new ArrayList<>(List.of(msg("t", "z", "z#1")));
        heads.forEach(head -> messages.add(msg("t", head.substring(0, 1), head)));
        send(messages.toArray(OutboxMessage[]::new));
        tick();
        processUntilNoWork();
        var due = now.plus(INITIAL);
        var calls = new AtomicInteger();
        script = records -> { // ignores interrupts and outlives batchTimeout: every call fails as a whole
            calls.incrementAndGet();
            long until = System.nanoTime() + timeout.plusMillis(200).toNanos();
            while (System.nanoTime() < until) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {
                    // keeps going
                }
            }
            return records.stream().map(r -> Outcome.delivered()).toList();
        };
        now = due;

        for (int pass = 1; pass <= 2; pass++) {
            long started = System.nanoTime();
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).as("pass %d", pass)
                    .isLessThan(timeout.multipliedBy(3));
            // pass 1: the call of all due heads, then a#1 alone; pass 2 probes: b#1 alone, the rest wait for time
            assertThat(calls).as("pass %d", pass).hasValue(pass == 1 ? 2 : 3);
        }

        // a#1, then b#1 (a#1 no longer due) bore an attempt; the rest wait, uncharged and still due
        var backoff = now.plus(Backoff.delay(1, INITIAL, config.maxDelay()));
        assertThat(retryRows()).containsExactly("a#1:2:" + backoff, "b#1:2:" + backoff, "c#1:1:" + due,
                "d#1:1:" + due, "e#1:1:" + due);
        assertThat(cursor().retryDueAt()).isEqualTo(due);
    }

    @Test
    void aPassOutOfTimeStartsNoFurtherRound() throws Exception {
        var timeout = Duration.ofMillis(500);
        config.batchTimeout(timeout).retry(INITIAL, Duration.ofMillis(500), 3);
        bootstrap();
        // b#1 and c#1 due with one attempt; a#1 due and out of attempts (diverted at maxAttempts), a#2 behind it
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + names.retry() + " (id, shard,"
                + " message_id, topic, key, payload, created_at, attempts, next_attempt_at) values (?, 0,"
                + " gen_random_uuid(), 't', ?, ?, now(), ?, ?)")) {
            var rows = List.of("b#1:1", "a#1:3", "a#2:0", "c#1:1");
            for (int i = 0; i < rows.size(); i++) {
                var row = rows.get(i).split(":");
                ps.setLong(1, i + 1);
                ps.setBytes(2, row[0].substring(0, 1).getBytes(UTF_8));
                ps.setBytes(3, row[0].getBytes(UTF_8));
                ps.setInt(4, Integer.parseInt(row[1]));
                ps.setObject(5, row[0].equals("a#2") ? null : now.atOffset(ZoneOffset.UTC));
                ps.executeUpdate();
            }
        }
        var calls = new AtomicInteger();
        script = records -> { // ignores interrupts and outlives batchTimeout: every call fails as a whole
            calls.incrementAndGet();
            long until = System.nanoTime() + timeout.plusMillis(200).toNanos();
            while (System.nanoTime() < until) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {
                    // keeps going
                }
            }
            return records.stream().map(r -> Outcome.delivered()).toList();
        };

        long started = System.nanoTime();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true, true));

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(timeout.multipliedBy(3));
        assertThat(calls).as("[b#1, a#2, c#1], then b#1 alone").hasValue(2);
        assertThat(deadRows()).containsExactly("a#1:3");
        // a#2 is a's head now, due at once: the next pass goes on with it
        assertThat(retryRows()).containsExactly("b#1:2:" + now.plus(Backoff.delay(1, INITIAL, config.maxDelay())),
                "a#2:0:" + now, "c#1:1:" + now);
    }

    @Test
    void noTwoHandlerCallsOfARetryPassRunWithoutAStatementBetweenThem() throws Exception {
        var timeout = Duration.ofMillis(500);
        config.batchTimeout(timeout);
        bootstrap();
        for (var head : List.of("a#1", "b#1")) {
            handler.failNext(payload(head), () -> new RuntimeException(head + " busy"));
        }
        send(msg("t", "z", "z#1"), msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "a", "a#2"));
        tick();
        processUntilNoWork();
        var due = now.plus(INITIAL);
        var calls = new AtomicInteger();
        script = hangingPast(timeout, records -> calls.incrementAndGet() > 0, records -> List.of());
        now = due;

        var gaps = new ArrayList<Duration>();
        assertThat(process(gaps)).isEqualTo(new ShardResult(PROGRESS, 0, true));

        // the idle_in_transaction_session_timeout of a step (batchTimeout + 30 s) outlasts one call, not two
        assertThat(gaps).singleElement().satisfies(gap -> assertThat(gap).isLessThan(timeout.plusMillis(250)));
        assertThat(calls).as("[a#1, b#1, a#2], then a#1 alone").hasValue(2);
        assertThat(retryRows()).containsExactly(
                "a#1:2:" + now.plus(Backoff.delay(1, INITIAL, config.maxDelay())), "b#1:1:" + due, "a#2:0:-");
    }

    @Test
    void aPoisonThatHangsIsDeadLetteredWhileTheShardGoesOn() throws Exception {
        var timeout = Duration.ofMillis(500);
        config.batchTimeout(timeout).retry(INITIAL, Duration.ofMillis(500), 3);
        bootstrap();
        for (var head : List.of("p#1", "i#1")) {
            handler.failNext(payload(head), () -> new RuntimeException(head + " busy"));
        }
        send(msg("t", "z", "z#1"), msg("t", "p", "p#1"), msg("t", "i", "i#1"), msg("t", "p", "p#2"),
                msg("t", "i", "i#2"));
        tick();
        processUntilNoWork();
        assertThat(retryRows()).hasSize(4);
        script = hangingPast(timeout, records -> records.stream().anyMatch(payload("p#1")), handler::handleBatch);
        send(msg("t", "m", "m#1")); // the main stream of the shard
        tick();

        var gaps = new ArrayList<Duration>();
        for (int pass = 0; pass < 10 && !retryRows().isEmpty(); pass++) {
            var due = cursor().retryDueAt();
            now = due.isAfter(now) ? due : now;
            process(gaps);
            if (pass == 0) {
                assertThat(delivered()).as("the main stream, in the step of the first pass").contains("m#1");
            }
        }

        assertThat(deadRows()).containsExactly("p#1:3");
        assertThat(retryRows()).isEmpty();
        assertThat(delivered()).containsExactly("z#1", "m#1", "i#1", "i#2", "p#2");
        assertThat(gaps).allSatisfy(gap -> assertThat(gap).isLessThan(timeout.plusMillis(250)));
    }

    @Test
    void aPoisonThatHangsBehindHealthyRowsIsProbedAloneInTheNextStep() throws Exception {
        var timeout = Duration.ofMillis(500);
        config.batchTimeout(timeout).retry(INITIAL, Duration.ofMillis(500), 3);
        bootstrap();
        for (var head : List.of("k#1", "j#1", "p#1")) {
            handler.failNext(payload(head), () -> new RuntimeException(head + " busy"));
        }
        send(msg("t", "z", "z#1"), msg("t", "k", "k#1"), msg("t", "k", "k#2"), msg("t", "k", "k#3"),
                msg("t", "j", "j#1"), msg("t", "k", "k#4"), msg("t", "k", "k#5"), msg("t", "p", "p#1"));
        tick();
        processUntilNoWork(); // heads k#1, j#1, p#1; k#2..k#5 follow k#1, all below p#1
        assertThat(retryRows()).hasSize(7);
        OutboxHandler hangsOnP = record -> { // delivers the rows before p#1, then hangs past batchTimeout on it
            if (payload("p#1").test(record)) {
                long until = System.nanoTime() + timeout.plusMillis(200).toNanos();
                while (System.nanoTime() < until) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                        // keeps going
                    }
                }
                throw new RuntimeException("p#1 hung");
            }
            handler.handle(record);
        };
        script = hangsOnP::handleBatch;

        var gaps = new ArrayList<Duration>();
        int kDrainedAt = 0;
        for (int step = 1; step <= 10 && !retryRows().isEmpty(); step++) {
            var due = cursor().retryDueAt();
            now = due.isAfter(now) ? due : now;
            process(gaps);
            if (step == 2) {
                assertThat(retryRows()).as("p#1 is charged in the second step").anyMatch(r -> r.startsWith("p#1:2:"));
            }
            if (kDrainedAt == 0 && retryRows().stream().noneMatch(r -> r.startsWith("k#"))) {
                kDrainedAt = step;
            }
        }

        assertThat(deadRows()).containsExactly("p#1:3");
        assertThat(retryRows()).isEmpty();
        assertThat(kDrainedAt).as("the step k drained in").isBetween(1, 3);
        assertThat(delivered()).doesNotContain("p#1")
                .containsAll(List.of("z#1", "k#1", "k#2", "k#3", "k#4", "k#5", "j#1"));
        var firstK = delivered().stream().filter(p -> p.startsWith("k#")).distinct().toList();
        assertThat(firstK).as("k in order").containsExactly("k#1", "k#2", "k#3", "k#4", "k#5");
        assertThat(gaps).allSatisfy(gap -> assertThat(gap).isLessThan(timeout.plusMillis(250)));
    }

    @Test
    void stepsOfAShardWithoutRetryRowsLeaveTheRetryTableAlone() throws Exception {
        bootstrap();
        process(); // a shard's first step checks the retry table in full
        var horizon = begin();
        horizon.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        execute(horizon, "select 1"); // holds the xmin horizon: the dead retry versions below stay
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at,"
                    + " next_attempt_at) select g, 0, gen_random_uuid(), 't', int4send(g::int), now(), now()"
                    + " from generate_series(1, 100000) g");
            execute(c, "delete from " + names.retry());
        }
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        tick();
        statements.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(process()).isEqualTo(new ShardResult(NO_WORK, 0, true));

        assertThat(statements).isNotEmpty().noneMatch(sql -> sql.contains(names.retry()));
        assertThat(delivered()).containsExactly("a#1", "b#1");
    }

    @Test
    void retryRowsChangedOutsideTheRelayAreFoundWithinMaintenanceInterval() throws Exception {
        config.maintenanceInterval(Duration.ofMillis(100));
        bootstrap();
        handler.failNext(payload("k#1"), () -> new RuntimeException("k busy"));
        send(msg("t", "a", "a#1"), msg("t", "k", "k#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor().retryDueAt()).isEqualTo(now.plus(INITIAL));
        try (var c = TestDb.connect()) { // an operator drops k#1 and re-queues another message as a due head
            execute(c, "delete from " + names.retry());
            try (var ps = c.prepareStatement("insert into " + names.retry() + " (id, shard, message_id, topic, key,"
                    + " payload, created_at, next_attempt_at) values (1000000, 0, gen_random_uuid(), 't', 'r'::bytea,"
                    + " 'redriven'::bytea, now(), ?)")) {
                ps.setObject(1, now.atOffset(ZoneOffset.UTC));
                ps.executeUpdate();
            }
        }

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (handler.delivered().size() < 2 && System.nanoTime() < deadline) {
            process();
            Thread.sleep(10);
        }
        assertThat(delivered()).containsExactly("a#1", "redriven");
        assertThat(retryRows()).isEmpty();
        assertThat(cursor().retryDueAt()).isNull();

        try (var c = TestDb.connect()) { // a stale retry_due_at is corrected too
            execute(c, "insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at,"
                    + " next_attempt_at) values (1000001, 0, gen_random_uuid(), 't', 'r'::bytea, now(),"
                    + " now() + interval '1 hour')");
        }
        while (cursor().retryDueAt() == null && System.nanoTime() < deadline) {
            process();
            Thread.sleep(10);
        }
        assertThat(cursor().retryDueAt()).isNotNull();
        try (var c = TestDb.connect()) {
            execute(c, "delete from " + names.retry());
        }
        while (cursor().retryDueAt() != null && System.nanoTime() < deadline) {
            assertThat(process().status()).isEqualTo(NO_WORK);
            Thread.sleep(10);
        }
        assertThat(cursor().retryDueAt()).isNull();
    }

    @Test
    void notLockedWhenAnotherTransactionHoldsShard() throws Exception {
        bootstrap();
        send(msg("t", "a", "a#1"));
        tick();
        var holder = begin();
        try (var ps = holder.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_SHARD_BASE);
            ps.execute();
        }

        assertThat(process()).isEqualTo(new ShardResult(NOT_LOCKED, 0, false));
        assertThat(handler.delivered()).isEmpty();

        holder.commit();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
    }

    @Test
    void stepDoesNotRunWhileTheSwapLockIsHeld() throws Exception {
        bootstrap();
        script = answering(Map.of("p#1", Outcome.requeue()));
        send(msg("t", "p", "p#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true)); // p#1 parked
        script = answering(Map.of());
        send(msg("t", "a", "a#1"));
        tick();
        now = now.plus(REQUEUE_DELAY); // p#1 due as well
        var swap = TestDb.connect();
        open.add(swap);
        swapLock(swap, "pg_advisory_lock");
        var cursor = cursor();
        long cursorRows = cursorRows();
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(NOT_LOCKED, 0, false));
        assertThat(calls).isEmpty();
        assertThat(cursorRows()).isEqualTo(cursorRows);
        assertThat(cursor()).isEqualTo(cursor);

        swapLock(swap, "pg_advisory_unlock");
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(delivered()).containsExactly("p#1", "a#1");
    }

    @Test
    void swapLockWaitsForTheStepInFlightWithoutBlockingWriters() throws Exception {
        bootstrap();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        script = records -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return records.stream().map(r -> Outcome.delivered()).toList();
        };
        send(msg("t", "a", "a#1"));
        tick();
        var swap = TestDb.connect();
        open.add(swap);
        var step = new FutureTask<>(this::process);
        var quiesce = new FutureTask<Void>(() -> {
            swapLock(swap, "pg_advisory_lock");
            return null;
        });
        try {
            new Thread(step).start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("the handler is called").isTrue();
            new Thread(quiesce).start();

            assertThatThrownBy(() -> quiesce.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
            var insert = new FutureTask<>(() -> send(msg("t", "b", "b#1")));
            new Thread(insert).start();
            assertThat(insert.get(1, TimeUnit.SECONDS)).as("a writer commits meanwhile").hasSize(1);

            release.countDown();
            assertThat(step.get(5, TimeUnit.SECONDS)).isEqualTo(new ShardResult(PROGRESS, 1, true));
            quiesce.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
        swapLock(swap, "pg_advisory_unlock");
    }

    @Test
    void aQueuedSwapLockTurnsNewStepsAway() throws Exception {
        config.shards(2);
        bootstrap();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        script = records -> {
            if (payloads(records).contains("a#1")) { // shard 0's step stays in flight
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return records.stream().map(r -> Outcome.delivered()).toList();
        };
        send(msg("t", keyOnShard(0), "a#1"), msg("t", keyOnShard(1), "b#1"));
        tick();
        var swap = TestDb.connect();
        open.add(swap);
        var step = new FutureTask<>(this::process);
        var quiesce = new FutureTask<Void>(() -> {
            swapLock(swap, "pg_advisory_lock");
            return null;
        });
        try {
            new Thread(step).start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("the handler is called").isTrue();
            new Thread(quiesce).start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!swapLockQueued()) {
                assertThat(System.nanoTime()).as("the swap lock queued within 5 s").isLessThan(deadline);
                Thread.sleep(5);
            }

            // no step holds shard 1 or waits for it: only the queued exclusive request turns this one away, so a
            // stream of new steps cannot keep the swap waiting
            assertThat(process(processor, 1)).isEqualTo(new ShardResult(NOT_LOCKED, 0, false));
            assertThat(calls).extracting(ShardProcessorIT::payloads).containsExactly(List.of("a#1"));

            release.countDown();
            assertThat(step.get(5, TimeUnit.SECONDS)).isEqualTo(new ShardResult(PROGRESS, 1, true));
            quiesce.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
        swapLock(swap, "pg_advisory_unlock");
        assertThat(process(processor, 1)).isEqualTo(new ShardResult(PROGRESS, 1, true));
    }

    /** A session waits for {@code LOCK_SWAP} exclusively: the swap's quiesce request is queued. */
    private boolean swapLockQueued() throws SQLException {
        return Long.parseLong(rows("select count(*) from pg_locks where locktype = 'advisory' and classid::bigint = "
                + Integer.toUnsignedLong(names.internalNamespace()) + " and objid = " + TableNames.LOCK_SWAP
                + " and objsubid = 2 and mode = 'ExclusiveLock' and not granted", rs -> rs.getString(1)).get(0)) > 0;
    }

    /** A key of topic {@code t} on {@code shard}. */
    private String keyOnShard(int shard) {
        for (int i = 0; ; i++) {
            var key = "k" + i;
            if (Hashing.shard(Hashing.keyHash("t", key.getBytes(UTF_8)), config.shards()) == shard) {
                return key;
            }
        }
    }

    /** {@code function(internalNs, LOCK_SWAP)} on {@code c}, as maintenance quiesces the relay for a tail swap. */
    private void swapLock(Connection c, String function) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_SWAP);
            ps.execute();
        }
    }

    @Test
    void deliverySpansAreParentedByWriterTrace() throws Exception {
        var exporter = InMemorySpanExporter.create();
        try (var provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build()) {
            tracer = provider.get("test");
            bootstrap();
            TestTracing.activateSpan();
            var ids = send(msg("t", "k", "traced"), msg("t", "j", "failing"));
            Context.clear();
            handler.failNext(payload("failing"), () -> new RuntimeException("j down"));
            tick();

            process();

            var spans = exporter.getFinishedSpanItems();
            assertThat(spans).hasSize(2);
            var ok = spans.get(0);
            assertThat(ok.getName()).isEqualTo("outbox deliver t");
            assertThat(ok.getKind()).isEqualTo(SpanKind.PRODUCER);
            assertThat(ok.getTraceId()).isEqualTo(TestTracing.TRACE_ID);
            assertThat(ok.getParentSpanId()).isEqualTo(TestTracing.SPAN_ID);
            assertThat(ok.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
            var attributes = ok.getAttributes();
            assertThat(attributes.get(AttributeKey.stringKey("messaging.destination.name"))).isEqualTo("t");
            assertThat(attributes.get(AttributeKey.stringKey("outbox.message_id"))).isEqualTo(ids.get(0).toString());
            assertThat(attributes.get(AttributeKey.longKey("outbox.attempt"))).isEqualTo(1);
            assertThat(attributes.get(AttributeKey.longKey("outbox.delay_ms"))).isNotNull().isNotNegative();
            assertThat(attributes.get(AttributeKey.stringKey("outbox.outcome"))).isEqualTo("delivered");
            var received = handler.delivered().get(0);
            assertThat(received.messageId()).isEqualTo(ids.get(0));
            assertThat(traceparents(received))
                    .containsExactly("00-" + TestTracing.TRACE_ID + "-" + ok.getSpanId() + "-01");

            var failed = spans.get(1);
            assertThat(failed.getAttributes().get(AttributeKey.stringKey("outbox.message_id")))
                    .isEqualTo(ids.get(1).toString());
            assertThat(failed.getParentSpanId()).isEqualTo(TestTracing.SPAN_ID);
            assertThat(failed.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(failed.getEvents()).extracting(EventData::getName).contains("exception");
            assertThat(failed.getAttributes().get(AttributeKey.stringKey("outbox.outcome"))).isEqualTo("retry");

            // the retry row keeps the writer's trace: the next attempt is again a child of the writer span
            exporter.reset();
            now = now.plus(INITIAL);
            assertThat(process().delivered()).isEqualTo(1);
            var retried = exporter.getFinishedSpanItems();
            assertThat(retried).singleElement().satisfies(span -> {
                assertThat(span.getParentSpanId()).isEqualTo(TestTracing.SPAN_ID);
                assertThat(span.getAttributes().get(AttributeKey.longKey("outbox.attempt"))).isEqualTo(2);
            });
        }
    }

    @Test
    void shardProcessingIsCorrectUnderRepeatableReadSessionDefault() throws Exception {
        bootstrap();
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        long latest = tick();
        try (var first = TestDb.connect(); var second = TestDb.connect()) {
            for (var c : List.of(first, second)) {
                execute(c, "set session characteristics as transaction isolation level repeatable read");
            }
            // second's transaction runs a query before first commits, as a relay worker does when it tries other
            // shards first; under repeatable read that query would fix its snapshot
            ShardProcessor.prepareTransaction(second, config.batchTimeout());
            assertThat(queryString(second, "select current_setting('transaction_isolation')"))
                    .isEqualTo("read committed");
            ShardProcessor.prepareTransaction(first, config.batchTimeout());
            assertThat(processor.tryProcess(first, 0, latest, now))
                    .isEqualTo(new ShardResult(PROGRESS, 2, true));
            first.commit();

            assertThat(processor.tryProcess(second, 0, latest, now))
                    .isEqualTo(new ShardResult(NO_WORK, 0, true));
            second.commit();
        }
        assertThat(delivered()).containsExactly("a#1", "b#1");
    }

    @Test
    void handlerFailuresFailEveryRecordOfTheCallTransiently() throws Exception {
        config.headFailuresBeforeDivert(1000); // pauses only: no probe, no divert
        bootstrap();
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        tick();
        var failures = List.<Function<List<OutboxRecord>, List<Outcome>>>of(
                records -> {
                    throw new IllegalArgumentException("handler bug");
                },
                records -> {
                    throw new StackOverflowError("handler recursion");
                },
                records -> {
                    throw new OutboxPermanentException("a call of two records is not dead");
                },
                records -> List.of(Outcome.delivered()),
                records -> Arrays.asList(Outcome.delivered(), null),
                records -> null,
                records -> {
                    records.clear(); // read-only
                    return List.of();
                });

        try (var err = new CapturedStderr()) {
            for (int i = 0; i < failures.size(); i++) {
                script = failures.get(i);
                assertThat(process()).as("failure %d", i).isEqualTo(new ShardResult(PAUSED, 0, false));
                assertThat(cursor().failures()).isEqualTo(i + 1);
                now = cursor().pausedUntil();
            }
            // an Error or an invalid outcome list is a handler bug: ERROR, at most one per 10 s
            assertThat(err.text()).contains("ERROR " + ShardProcessor.class.getName()).contains("handler recursion");
        }
        assertThat(retryRows()).isEmpty();
        assertThat(deadRows()).isEmpty();

        script = null;
        // seven failed calls halved the rows per step from 100 to 1; each delivery doubles them again
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, false));
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(delivered()).containsExactly("a#1", "b#1");
    }

    @Test
    void outcomeListIsReadOnTheHandlerThread() throws Exception {
        bootstrap();
        var readers = new CopyOnWriteArrayList<String>();
        script = records -> new AbstractList<>() { // a lazy view runs handler code whenever it is read
            @Override
            public Outcome get(int index) {
                readers.add(Thread.currentThread().getName());
                return Outcome.delivered();
            }

            @Override
            public int size() {
                readers.add(Thread.currentThread().getName());
                return records.size();
            }
        };
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(readers).isNotEmpty().allMatch(name -> name.startsWith("outbox-handler-"));
    }

    @Test
    void singleRecordCallThrowingPermanentExceptionIsDead() throws Exception {
        bootstrap();
        send(msg("t", "k", "k#1"));
        tick();
        script = records -> {
            throw new OutboxPermanentException("bad k#1");
        };

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true, true));
        assertThat(deadRows()).containsExactly("k#1:1");
    }

    @Test
    void wrongSizeOutcomeListIsRetriedWithBackoffAndOtherKeysFlow() throws Exception {
        bootstrap();
        script = records -> records.stream().anyMatch(payload("bad#1")) ? List.of() : handler.handleBatch(records);
        send(msg("t", "bad", "bad#1"), msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "a", "a#2"));
        tick();

        for (int failures = 1; failures <= 3; failures++) {
            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
            assertThat(cursor().failures()).isEqualTo(failures);
            now = cursor().pausedUntil();
        }
        // probe: bad#1 alone is a single failing key, diverted with backoff
        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
        assertThat(retryRows()).containsExactly("bad#1:3:" + now.plus(Backoff.delay(2, INITIAL, config.maxDelay())));
        now = cursor().pausedUntil();

        processUntilNoWork();
        assertThat(delivered()).containsExactly("a#1", "b#1", "a#2");
        assertThat(retryRows()).singleElement().asString().startsWith("bad#1:4:"); // retried by the retry pass
        assertThat(cursor().failures()).isZero();
    }

    @Test
    void poisonDeepInAChunkIsIsolatedInLogarithmicallyManyPauses() throws Exception {
        config.batchSize(1000);
        bootstrap();
        var messages = new ArrayList<OutboxMessage>();
        for (int i = 0; i < 1000; i++) {
            messages.add(i == 400 ? msg("t", "poison", "poison") : msg("t", "k" + i % 50, "m" + i));
        }
        var ids = new ArrayList<>(send(messages.toArray(OutboxMessage[]::new)));
        ids.remove(400);
        tick();
        script = records -> {
            if (records.stream().anyMatch(payload("poison"))) {
                throw new IllegalStateException("cannot handle the poison");
            }
            return handler.handleBatch(records);
        };

        int steps = 0;
        int pauses = 0;
        while (handler.delivered().size() < 999 || cursor().targetTickId() != null) {
            assertThat(steps++).as("steps so far, %d pauses, %d delivered", pauses, handler.delivered().size())
                    .isLessThan(200);
            if (process().status() == PAUSED) {
                pauses++;
                now = cursor().pausedUntil();
            }
        }

        // the limit halves after each failed call and doubles after each delivery: about 2 pauses per halving
        // (one where the poison is in the chunk, one bouncing back after a delivery), not 3 per row before it
        assertThat(pauses).as("pauses").isLessThanOrEqualTo(2 * 9 + config.headFailuresBeforeDivert());
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
        assertThat(retryRows()).singleElement().asString().startsWith("poison:");
    }

    @Test
    void errorFromHandleFailsOnlyItsKey() throws Exception {
        bootstrap();
        script = ((OutboxHandler) record -> {
            if (payload("k#1").test(record)) {
                throw new StackOverflowError("deep k#1");
            }
            handler.handle(record);
        })::handleBatch;
        send(msg("t", "a", "a#1"), msg("t", "k", "k#1"), msg("t", "b", "b#1"), msg("t", "k", "k#2"),
                msg("t", "c", "c#1"));
        long t = tick();

        try (var err = new CapturedStderr()) {
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 3, true));
            // an Error is a handler bug, not a routine retry: ERROR, at most one per 10 s
            assertThat(err.text()).contains("ERROR " + ShardProcessor.class.getName())
                    .contains("StackOverflowError: deep k#1");
        }

        var due = now.plus(INITIAL);
        assertThat(delivered()).containsExactly("a#1", "b#1", "c#1");
        assertThat(retryRows()).containsExactly("k#1:1:" + due, "k#2:0:-");
        assertThat(rows("select last_error from " + names.retry() + " where payload = 'k#1'::bytea",
                rs -> rs.getString(1))).singleElement().asString().contains("StackOverflowError: deep k#1");
        assertThat(cursor()).isEqualTo(new Cursor(0, t, null, 0, null, 0, due, null));
    }

    @Test
    void maxAttemptsBoundsSingleKeyDivert() throws Exception {
        config.retry(INITIAL, Duration.ofMillis(500), 2); // below headFailuresBeforeDivert = 3
        bootstrap();
        var attempts = new AtomicInteger();
        handler.failAlways(payload("k#1"), () -> {
            attempts.incrementAndGet();
            return new RuntimeException("poison");
        });
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"));
        tick();

        assertThat(process().status()).isEqualTo(PAUSED);
        now = cursor().pausedUntil();
        assertThat(process().status()).as("diverted at maxAttempts").isEqualTo(PAUSED);
        assertThat(retryRows()).as("k#2, not attempted after k#1, is read again")
                .containsExactly("k#1:2:" + now.plus(Backoff.delay(1, INITIAL, config.maxDelay())));
        now = cursor().pausedUntil(); // the head is due by then

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true, true));
        assertThat(attempts).as("dead-lettered without a third attempt").hasValue(2);
        assertThat(deadRows()).containsExactly("k#1:2");
        assertThat(rows("select error from " + names.dead(), rs -> rs.getString(1)))
                .singleElement().asString().contains("poison");
        assertThat(delivered()).containsExactly("k#2");
    }

    @Test
    void slowHeadIsDivertedViaProbe() throws Exception {
        config.batchTimeout(Duration.ofMillis(200));
        bootstrap();
        var calls = new CopyOnWriteArrayList<List<String>>();
        script = records -> {
            var payloads = records.stream().map(r -> new String(r.payload(), UTF_8)).toList();
            calls.add(payloads);
            if (payloads.get(0).equals("slow")) { // ignores interrupts past batchTimeout: every record of the call fails
                long until = System.nanoTime() + Duration.ofMillis(400).toNanos();
                while (System.nanoTime() < until) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                        // keeps going, as a handler that ignores cancellation does
                    }
                }
                return records.stream().map(r -> Outcome.notAttempted()).toList();
            }
            return handler.handleBatch(records);
        };
        send(msg("t", "s", "slow"), msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "c", "c#1"));
        tick();

        for (int failures = 1; failures <= 3; failures++) {
            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
            assertThat(cursor().failures()).isEqualTo(failures);
            now = cursor().pausedUntil();
        }
        assertThat(retryRows()).isEmpty();

        // probe: the head row alone, so the timeout fails a single key, which is diverted
        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
        assertThat(calls.get(calls.size() - 1)).containsExactly("slow");
        assertThat(retryRows()).singleElement().asString().startsWith("slow:3:");
        now = cursor().pausedUntil();

        // the next probe succeeds, which restores full batches
        processUntilNoWork();
        assertThat(delivered()).containsExactly("a#1", "b#1", "c#1");
        assertThat(calls.subList(calls.size() - 2, calls.size()))
                .containsExactly(List.of("a#1"), List.of("b#1", "c#1"));
        assertThat(retryRows()).singleElement().asString().startsWith("slow:");
        var cursor = cursor();
        assertThat(cursor.failures()).isZero();
        assertThat(cursor.pausedUntil()).isNull();
        assertThat(cursor.targetTickId()).isNull();
    }

    @Test
    void deadTargetProbesOneRowAtATime() throws Exception {
        bootstrap();
        handler.failAlways(r -> true, () -> new RuntimeException("target down"));
        send(IntStream.range(0, 5).mapToObj(i -> msg("t", "k" + i, "k" + i)).toArray(OutboxMessage[]::new));
        tick();

        for (int failures = 1; failures <= 3; failures++) {
            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
            now = cursor().pausedUntil();
        }
        assertThat(retryRows()).isEmpty();

        for (int probe = 1; probe <= 3; probe++) {
            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
            var batches = handler.batches();
            assertThat(batches.get(batches.size() - 1)).as("the probe hands one row").hasSize(1);
            var retry = retryRows();
            assertThat(retry).as("one row diverted per probe").hasSize(probe);
            assertThat(retry.get(probe - 1)).startsWith("k" + (probe - 1) + ":3:");
            var cursor = cursor();
            assertThat(cursor.failures()).isEqualTo(3 + probe);
            assertPausedFor(cursor, Backoff.delay(2 + probe, INITIAL, config.maxDelay()));
            now = cursor.pausedUntil();
        }
        assertThat(deadRows()).isEmpty();
        assertThat(handler.delivered()).isEmpty();
    }

    @Test
    void probeOfABlockedRowKeepsTheBackoffGrowing() throws Exception {
        bootstrap();
        var later = now.plus(Duration.ofHours(1));
        script = records -> records.stream() // k is diverted, its head not due for an hour
                .map(r -> payload("k#1").test(r) ? Outcome.retry(new RuntimeException("k busy"), later)
                        : Outcome.delivered())
                .toList();
        send(msg("t", "k", "k#1"), msg("t", "a", "a#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        script = null;
        handler.failAlways(r -> true, () -> new RuntimeException("target down"));
        send(msg("t", "k", "k#2"), msg("t", "b", "b#1"), msg("t", "c", "c#1"));
        tick();
        for (int failures = 1; failures <= 3; failures++) {
            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
            now = cursor().pausedUntil();
        }

        // the probe's head row k#2 follows k#1 unhanded: no delivery, so no reset to full batches
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, false));
        assertThat(retryRows()).containsExactly("k#1:1:" + later, "k#2:0:-");
        assertThat(cursor().failures()).isEqualTo(3);

        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
        var batches = handler.batches();
        assertThat(batches.get(batches.size() - 1)).as("still probing").hasSize(1);
        var cursor = cursor();
        assertThat(cursor.failures()).isEqualTo(4);
        assertPausedFor(cursor, Backoff.delay(3, INITIAL, config.maxDelay()));
    }

    @Test
    void longStepOutlivesARoleTransactionTimeoutOnPostgres17() throws Exception {
        try (var pg17 = new PostgreSQLContainer("postgres:17-alpine")) {
            pg17.start();
            try (var c = DriverManager.getConnection(pg17.getJdbcUrl(), pg17.getUsername(), pg17.getPassword())) {
                execute(c, "set transaction_timeout = '200ms'"); // as a role or database default would
                ShardProcessor.prepareTransaction(c, Duration.ofSeconds(5));
                execute(c, "select pg_sleep(0.5)"); // a handler call far longer than that
                c.commit();
            }
        }
    }

    @Test
    void pauseMidBatchKeepsPositionAndTarget() throws Exception {
        config.batchSize(3);
        bootstrap();
        long t0 = cursor().tickId();
        var longTx = begin();
        var late = outbox.send(longTx, msg("t", "late", "late")); // the smallest id, committed after t1
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"), msg("t", "c", "c#1"), msg("t", "d", "d#1"),
                msg("t", "e", "e#1"), msg("t", "d", "d#2"));
        long t1 = tick();
        var down = new AtomicBoolean(true);
        handler.failAlways(r -> down.get() && List.of("d", "e").contains(new String(r.key(), UTF_8)),
                () -> new RuntimeException("d and e down"));

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 3, false));
        long lastId = handler.delivered().get(2).id();
        assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
        var paused = cursor();
        assertThat(paused).isEqualTo(new Cursor(0, t0, t1, lastId, paused.pausedUntil(), 1, null, null));
        assertThat(retryRows()).isEmpty();

        longTx.commit();
        long t2 = tick();
        down.set(false);
        now = paused.pausedUntil();
        processUntilNoWork();

        assertThat(delivered()).containsExactly("a#1", "b#1", "c#1", "d#1", "e#1", "d#2", "late");
        assertThat(handler.delivered().get(6).messageId()).isEqualTo(late);
        assertThat(cursor()).isEqualTo(new Cursor(0, t2, null, 0, null, 0, null, null));
    }

    @Test
    void everyStepWithATargetAppends() throws Exception {
        config.maxTicksPerBatch(1);
        bootstrap();
        long t1 = tick();
        long appends = cursorRows();

        // nothing to read: the cursor still moves to the target, so the shard is not behind it afterwards
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(cursorRows()).isEqualTo(appends + 1);
        assertThat(cursor()).isEqualTo(new Cursor(0, t1, null, 0, null, 0, null, null));

        // on the latest tick: no target, no write
        assertThat(process()).isEqualTo(new ShardResult(NO_WORK, 0, true));
        assertThat(cursorRows()).isEqualTo(appends + 1);

        // ticks that do not mark the shard are skipped: one step to the latest, however many there are
        tick();
        long t3 = tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(cursor().tickId()).isEqualTo(t3);
        assertThat(cursorRows()).isEqualTo(appends + 2);

        // ticks that mark it: maxTicksPerBatch (1) a step, each appended
        send(msg("t", "a", "a#1"));
        long t4 = tick();
        send(msg("t", "b", "b#1"));
        long t5 = tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor().tickId()).isEqualTo(t4);
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor().tickId()).isEqualTo(t5);
        assertThat(cursorRows()).isEqualTo(appends + 4);
    }

    @Test
    void stepStartsFromTheEffectiveTick() throws Exception {
        config.maxTicksPerBatch(1);
        bootstrap();
        long t0 = cursor().tickId();
        tick();
        long t2 = tick();
        try (var c = TestDb.connect()) {
            // a checkpoint at t2 that found the shard idle, and cleanup below it: the cursor's own tick is gone
            checkpointIdleAt(c, t2, t0);
            execute(c, "delete from " + names.tick() + " where id < " + t2);
        }
        assertThat(cursor().tickId()).isEqualTo(t0);
        long appends = cursorRows();

        assertThat(process()).as("the effective tick is the latest").isEqualTo(new ShardResult(NO_WORK, 0, true));
        assertThat(cursorRows()).isEqualTo(appends);

        send(msg("t", "a", "a#1"));
        long t3 = tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor()).isEqualTo(new Cursor(0, t3, null, 0, null, 0, null, null));
        assertThat(delivered()).containsExactly("a#1");
    }

    @Test
    void aSparseShardsFirstRowFarPastItsEffectiveTickIsDeliveredInOneStep() throws Exception {
        bootstrap(); // maxTicksPerBatch 100
        try (var c = TestDb.connect()) { // 150 idle ticks, none marking the shard
            execute(c, "insert into " + names.tick() + " (snapshot, max_id, dirty) select pg_current_snapshot(),"
                    + " 0, ''::varbit from generate_series(1, 150)");
        }
        send(msg("t", "a", "a#1"));
        long t = tick();
        long appends = cursorRows();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursorRows()).isEqualTo(appends + 1);
        assertThat(cursor().tickId()).isEqualTo(t);
        assertThat(process()).isEqualTo(new ShardResult(NO_WORK, 0, true));
    }

    @Test
    void aBatchInProgressGoesOnFromTheEffectiveTick() throws Exception {
        bootstrap();
        long t0 = cursor().tickId();
        long idle = tick();
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        long t1 = tick();
        try (var c = TestDb.connect()) {
            // a step from t0, which read its cursor before the checkpoint below, left the batch (t0, t1] after a#1;
            // the checkpoint found the shard idle at the tick before the rows, and cleanup deleted t0
            long first = queryLong(c, "select min(id) from " + names.base());
            cursors.append(c, cursor().withProgress(t0, t1, first));
            checkpointIdleAt(c, idle, t0);
            execute(c, "delete from " + names.tick() + " where id = " + t0);
        }

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(delivered()).containsExactly("b#1");
        assertThat(cursor()).isEqualTo(new Cursor(0, t1, null, 0, null, 0, null, null));
    }

    @Test
    void aStepWithoutATargetAppendsAMovedRetryDueAt() throws Exception {
        config.maintenanceInterval(Duration.ofNanos(1)); // every step checks the retry table in full
        bootstrap();
        var later = now.plus(Duration.ofHours(1));
        script = records -> records.stream()
                .map(r -> payload("k#1").test(r) ? Outcome.retry(new RuntimeException("k busy"), later)
                        : Outcome.delivered())
                .toList();
        send(msg("t", "a", "a#1"), msg("t", "k", "k#1"));
        long t = tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor().retryDueAt()).isEqualTo(later);
        var moved = later.plus(Duration.ofHours(1));
        try (var c = TestDb.connect(); var ps = c.prepareStatement("update " + names.retry()
                + " set next_attempt_at = ? where next_attempt_at is not null")) { // changed outside the relay
            ps.setObject(1, moved.atOffset(ZoneOffset.UTC));
            ps.executeUpdate();
        }
        long appends = cursorRows();

        // nothing to read and no head due, but retry_due_at changes: the step appends it
        assertThat(process()).isEqualTo(new ShardResult(NO_WORK, 0, true));
        assertThat(cursorRows()).isEqualTo(appends + 1);
        assertThat(cursor()).isEqualTo(new Cursor(0, t, null, 0, null, 0, moved, null));

        // and once nothing would change, no write
        assertThat(process()).isEqualTo(new ShardResult(NO_WORK, 0, true));
        assertThat(cursorRows()).isEqualTo(appends + 1);
    }

    @Test
    void doublingTheLimitAfterADeliveryStaysWithinAHugeBatchSize() throws Exception {
        config.batchSize(Integer.MAX_VALUE); // 2 × batchSize would overflow an int
        bootstrap();
        for (var payload : List.of("a#1", "a#2")) {
            send(msg("t", "a", payload));
            tick();
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        }
        assertThat(delivered()).containsExactly("a#1", "a#2");
    }

    @Test
    void tickIdGapDoesNotStopTheShard() throws Exception {
        config.maxTicksPerBatch(1);
        bootstrap();
        send(msg("t", "a", "a#1"));
        long gap = tick();
        long t2 = tick();
        try (var c = TestDb.connect()) {
            execute(c, "delete from " + names.tick() + " where id = " + gap);
        }

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor().tickId()).isEqualTo(t2);
    }

    @Test
    void aCleanProbeCutByTimeStillLetsTheShardBatchAgain() throws Exception {
        var timeout = Duration.ofMillis(300);
        config.batchTimeout(timeout);
        bootstrap();
        var heads = List.of("x#1", "h1#1", "h2#1", "h3#1", "h4#1", "h5#1", "h6#1", "h7#1", "h8#1");
        heads.forEach(head -> handler.failNext(payload(head), () -> new RuntimeException(head + " busy")));
        var messages = new ArrayList<>(List.of(msg("t", "z", "z#1")));
        heads.forEach(head -> messages.add(msg("t", head.substring(0, head.indexOf('#')), head)));
        send(messages.toArray(OutboxMessage[]::new));
        tick();
        processUntilNoWork();
        var sizes = new CopyOnWriteArrayList<Integer>();
        var latency = new AtomicLong();
        script = hangingPast(timeout, records -> { // x#1 hangs; every other call takes `latency`
            sizes.add(records.size());
            return records.stream().anyMatch(payload("x#1"));
        }, records -> {
            try {
                Thread.sleep(latency.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return handler.handleBatch(records);
        });
        now = now.plus(INITIAL);
        process(); // the call of all heads hangs, x#1 alone hangs and is charged: the shard probes
        assertThat(sizes).containsExactly(9, 1);

        latency.set(60); // eight healthy heads, one call each: more than batchTimeout holds
        process();
        assertThat(sizes.subList(2, sizes.size())).as("the probe, cut by time").allMatch(size -> size == 1)
                .hasSizeLessThan(8);
        int probed = sizes.size();

        process(); // no call of the probe failed as a whole: batched again
        assertThat(sizes.subList(probed, sizes.size())).as("one call of the heads left").singleElement()
                .satisfies(size -> assertThat(size).isGreaterThan(1));
        assertThat(retryRows()).singleElement().asString().startsWith("x#1:2:");
    }

    @Test
    void aRetryPassThatFindsThePoolFullSkipsTheShardHereLongerEachTime() throws Exception {
        // pool of max(4, 2 * workers) = 4 threads; no main-pass target: only retry heads are due
        config.workers(1).batchTimeout(Duration.ofMillis(100));
        bootstrap();
        var hung = List.of("f1", "f2", "f3", "f4");
        var healthy = List.of("h1", "h2", "h3", "h4", "h5");
        var messages = new ArrayList<>(List.of(msg("t", "z", "z#1")));
        for (var key : concat(hung, healthy)) {
            handler.failNext(payload(key + "#1"), () -> new RuntimeException(key + " busy"));
            messages.add(msg("t", key, key + "#1"));
        }
        send(messages.toArray(OutboxMessage[]::new));
        tick();
        processUntilNoWork();
        assertThat(cursor().targetTickId()).isNull();
        var due = now.plus(INITIAL);
        var stuck = new CountDownLatch(1);
        var calls = new AtomicInteger();
        script = records -> {
            calls.incrementAndGet();
            if (records.stream().noneMatch(r -> new String(r.payload(), UTF_8).startsWith("f"))) {
                return handler.handleBatch(records);
            }
            while (stuck.getCount() > 0) { // a call holding an f row keeps its thread until released
                try {
                    stuck.await();
                } catch (InterruptedException ignored) {
                    // a handler that ignores cancellation keeps its thread
                }
            }
            return records.stream().map(r -> Outcome.delivered()).toList();
        };
        now = due;
        try {
            // the call of all heads, then f1 alone; then f2 alone; then f3 alone: four threads held
            for (int step = 1; step <= 3; step++) {
                assertThat(process().status()).isEqualTo(PROGRESS);
            }
            assertThat(calls).hasValue(4);

            assertThat(process()).as("f4 finds the pool full").isEqualTo(new ShardResult(PAUSED, 0, false));

            assertThat(calls).as("no call ran, none followed the rejected one").hasValue(4);
            var charged = "2:" + due.plus(Backoff.delay(1, INITIAL, config.maxDelay()));
            var expected = new ArrayList<>(List.of("f1#1:" + charged, "f2#1:" + charged, "f3#1:" + charged,
                    "f4#1:1:" + now.plus(INITIAL)));
            healthy.forEach(key -> expected.add(key + "#1:1:" + due));
            assertThat(retryRows()).as("f4 not charged, the rest untouched").containsExactlyElementsOf(expected);
            var paused = cursor();
            assertPausedFor(paused, INITIAL); // from the rejection
            assertThat(paused.failures()).isZero();
            assertThat(process().status()).as("paused: no step before pausedUntil").isEqualTo(NO_WORK);

            now = paused.pausedUntil(); // still full: the shard pauses initialDelay again, still no failure
            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
            var again = cursor();
            assertPausedFor(again, INITIAL);
            assertThat(again.failures()).isZero();
            now = again.pausedUntil(); // ... and this processor skips it for twice as long
            assertThat(process()).as("skipped here").isEqualTo(new ShardResult(NO_WORK, 0, false));
            assertThat(cursor()).isEqualTo(again);
            assertThat(calls).hasValue(4);
        } finally {
            stuck.countDown();
        }

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!retryRows().isEmpty() && System.nanoTime() < deadline) { // the threads come back
            var cursor = cursor();
            for (var at : Arrays.asList(cursor.pausedUntil(), cursor.retryDueAt())) {
                now = at != null && at.isAfter(now) ? at : now;
            }
            if (process().status() == NO_WORK) {
                now = now.plus(INITIAL); // skipped here, or its pool still full a moment ago
            }
            Thread.sleep(10);
        }
        assertThat(retryRows()).isEmpty();
        assertThat(deadRows()).isEmpty();
        assertThat(delivered()).containsAll(healthy.stream().map(key -> key + "#1").toList());
    }

    private static List<String> concat(List<String> a, List<String> b) {
        var all = new ArrayList<>(a);
        all.addAll(b);
        return all;
    }

    @Test
    void aStepThatIsNotBusyResetsTheBusySkip() throws Exception {
        // pool of max(4, 2 * workers) = 4 threads; no probe or divert while the pool fills
        config.workers(1).batchTimeout(Duration.ofMillis(100)).headFailuresBeforeDivert(1000);
        bootstrap();
        var latches = IntStream.range(0, 8).mapToObj(i -> new CountDownLatch(1)).toList();
        var calls = new AtomicInteger();
        script = records -> { // each call holds its thread until its own latch is released
            var latch = latches.get(calls.getAndIncrement());
            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                    // a handler that ignores cancellation keeps its thread
                }
            }
            return records.stream().map(r -> Outcome.notAttempted()).toList();
        };
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        tick();
        try {
            for (int i = 1; i <= 4; i++) { // four calls hold the four threads
                assertThat(process().status()).isEqualTo(PAUSED);
                now = cursor().pausedUntil();
            }
            for (int busy = 1; busy <= 2; busy++) { // the second busy step in a row skips the shard here longer
                assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
                now = cursor().pausedUntil();
            }
            assertThat(process()).as("skipped here").isEqualTo(new ShardResult(NO_WORK, 0, false));

            latches.get(0).countDown(); // one thread comes back; the next call runs on it (and holds it again)
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (calls.get() < 5 && System.nanoTime() < deadline) {
                now = now.plus(Duration.ofSeconds(1)); // past any pause or skip
                process();
            }
            assertThat(calls).as("a step that was not busy").hasValue(5);

            now = cursor().pausedUntil();
            assertThat(process()).as("busy again").isEqualTo(new ShardResult(PAUSED, 0, false));
            now = cursor().pausedUntil();
            assertThat(process()).as("not skipped: the count started over")
                    .isEqualTo(new ShardResult(PAUSED, 0, false));
            assertThat(calls).hasValue(5);
        } finally {
            latches.forEach(CountDownLatch::countDown);
        }
    }

    @Test
    void onlyAStepThatCallsTheHandlerEndsTheBusySkip() throws Exception {
        // pool of max(4, 2 * workers) = 4 threads; no probe or divert while the pool fills
        config.workers(1).batchTimeout(Duration.ofMillis(100)).headFailuresBeforeDivert(1000);
        bootstrap();
        var stuck = new CountDownLatch(1);
        var sick = new ShardProcessor(names, config, record -> { // every call holds its thread until released
            while (stuck.getCount() > 0) {
                try {
                    stuck.await();
                } catch (InterruptedException ignored) {
                    // a handler that ignores cancellation keeps its thread
                }
            }
        }, new OutboxMetrics(null), null, false);
        try {
            send(msg("t", "a", "a#1"));
            tick();
            for (int i = 1; i <= 4; i++) { // four calls of the sick relay hold its four threads
                assertThat(process(sick).status()).isEqualTo(PAUSED);
                now = cursor().pausedUntil();
            }
            for (int busy = 1; busy <= 2; busy++) { // skipped here for initialDelay, then twice that
                assertThat(process(sick)).isEqualTo(new ShardResult(PAUSED, 0, false));
                now = cursor().pausedUntil();
            }
            assertThat(process()).as("another relay delivers").isEqualTo(new ShardResult(PROGRESS, 1, true));
            tick();
            now = now.plus(Duration.ofSeconds(1)); // past the sick relay's skip
            assertThat(process(sick)).as("read nothing, called nothing").isEqualTo(new ShardResult(PROGRESS, 0, true));
            send(msg("t", "b", "b#1"));
            tick();
            assertThat(process(sick)).isEqualTo(new ShardResult(PAUSED, 0, false));
            now = cursor().pausedUntil();

            assertThat(process(sick)).as("the empty step kept the count: skipped here 4 × initialDelay")
                    .isEqualTo(new ShardResult(NO_WORK, 0, false));

            var skipped = now;
            stuck.countDown(); // the threads come back; the next call resets the count
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            ShardResult result;
            do {
                now = now.plus(Duration.ofSeconds(1)); // past any pause or skip
                result = process(sick);
            } while (result.status() != PROGRESS && System.nanoTime() < deadline);
            assertThat(result).isEqualTo(new ShardResult(PROGRESS, 1, true));
            now = skipped; // the wall clock steps back into a skip of before the reset
            send(msg("t", "c", "c#1"));
            tick();

            assertThat(process(sick)).as("that skip went with the count")
                    .isEqualTo(new ShardResult(PROGRESS, 1, true));
        } finally {
            stuck.countDown();
            sick.close();
        }
    }

    @Test
    void aRelayWhoseHandlerThreadsAreAllHeldLeavesTheShardToAnother() throws Exception {
        // pool of max(4, 2 * workers) = 4 threads; a long maxDelay, so a shared pause would show
        config.workers(1).batchTimeout(Duration.ofMillis(100)).headFailuresBeforeDivert(1000)
                .retry(INITIAL, Duration.ofSeconds(30), 0);
        bootstrap();
        var stuck = new CountDownLatch(1);
        var sick = new ShardProcessor(names, config, records -> { // every call holds its thread
            while (stuck.getCount() > 0) {
                try {
                    stuck.await();
                } catch (InterruptedException ignored) {
                    // a handler that ignores cancellation keeps its thread
                }
            }
        }, new OutboxMetrics(null), null, false);
        try {
            send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
            tick();
            for (int i = 1; i <= 4; i++) { // four calls of the sick relay hold its four threads
                assertThat(process(sick).status()).isEqualTo(PAUSED);
                now = cursor().pausedUntil();
            }
            for (int busy = 1; busy <= 6; busy++) { // its skip grows to initialDelay * 32; the shard's pause does not
                assertThat(process(sick)).isEqualTo(new ShardResult(PAUSED, 0, false));
                var paused = cursor().pausedUntil();
                assertThat(paused).as("busy step %d", busy).isAfterOrEqualTo(now.plus(INITIAL))
                        .isBefore(now.plus(INITIAL).plusMillis(500));
                now = busy < 6 ? now.plus(Duration.ofSeconds(30)) : paused; // past the sick relay's skip, but last
            }
            assertThat(process(sick)).as("skipped by the sick relay")
                    .isEqualTo(new ShardResult(NO_WORK, 0, false));

            assertThat(process()).as("the healthy relay delivers").isEqualTo(new ShardResult(PROGRESS, 2, true));
            send(msg("t", "c", "c#1"));
            tick();
            assertThat(process(sick).status()).isEqualTo(NO_WORK);
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
            assertThat(delivered()).containsExactly("a#1", "b#1", "c#1");
        } finally {
            stuck.countDown();
            sick.close();
        }
    }

    @Test
    void aFullHandlerPoolChargesNoAttempt() throws Exception {
        // pool of max(4, 2 * workers) = 4 threads; no probe or divert while the pool fills
        config.workers(1).batchTimeout(Duration.ofMillis(100)).headFailuresBeforeDivert(1000);
        bootstrap();
        var later = now.plus(Duration.ofHours(1));
        script = records -> records.stream() // r#1 diverted, due in an hour
                .map(r -> payload("r#1").test(r) ? Outcome.retry(new RuntimeException("r busy"), later)
                        : Outcome.delivered())
                .toList();
        send(msg("t", "z", "z#1"), msg("t", "r", "r#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        var stuck = new CountDownLatch(1);
        var calls = new AtomicInteger();
        script = records -> {
            calls.incrementAndGet();
            if (records.stream().anyMatch(payload("r#1"))) {
                return handler.handleBatch(records);
            }
            while (stuck.getCount() > 0) {
                try {
                    stuck.await();
                } catch (InterruptedException ignored) {
                    // a handler that ignores cancellation keeps its thread
                }
            }
            return records.stream().map(r -> Outcome.notAttempted()).toList();
        };
        send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
        tick();
        try {
            for (int failures = 1; failures <= 4; failures++) { // each step leaves one more thread hung
                assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));
                assertThat(cursor().failures()).isEqualTo(failures);
                now = cursor().pausedUntil();
            }
            var before = cursor();
            now = later; // r#1 due as well

            assertThat(process()).isEqualTo(new ShardResult(PAUSED, 0, false));

            assertThat(calls).as("every handler thread busy: no call ran").hasValue(4);
            assertThat(retryRows()).as("r#1 keeps its attempt; nothing diverted")
                    .containsExactly("r#1:1:" + now.plus(INITIAL));
            assertThat(deadRows()).isEmpty();
            var after = cursor();
            assertThat(after.withPause(before.pausedUntil(), before.failures()).withRetryDueAt(before.retryDueAt()))
                    .as("position kept").isEqualTo(before);
            assertThat(after.failures()).as("no failure counted").isEqualTo(4);
            assertPausedFor(after, INITIAL);
        } finally {
            stuck.countDown();
        }

        script = null; // the threads come back
        now = cursor().pausedUntil(); // also when this processor's skip ends (one busy step: initialDelay)
        processUntilNoWork();
        assertThat(delivered()).containsExactly("r#1", "a#1", "b#1"); // z#1 went out through the script
        assertThat(retryRows()).isEmpty();
    }

    @Test
    void requeuedRecordLetsItsKeyGo() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));

        var due = now.plus(REQUEUE_DELAY);
        assertThat(delivered()).containsExactly("k#2", "j#1");
        assertThat(parkedRows()).containsExactly("k#1:1:" + due);
        assertThat(cursor().parkedDueAt()).isEqualTo(due);
        assertThat(retryRows()).isEmpty();
        assertThat(cursor().failures()).isZero();
        assertThat(counter("outbox.messages.requeued", "topic", "t")).isEqualTo(1);
    }

    @Test
    void parkedRowsComeFirstInTheCallOfTheNextRecordOfTheirKey() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        send(msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));

        assertThat(calls).singleElement().satisfies(call -> {
            assertThat(payloads(call)).containsExactly("k#1", "k#2", "j#1");
            assertThat(call.get(0).requeues()).isEqualTo(1);
        });
        assertThat(delivered()).containsExactly("k#2", "j#1");
        assertThat(parkedRows()).containsExactly("k#1:2:" + now.plus(REQUEUE_DELAY));
    }

    @Test
    void parkedRowsOfAKeyAreDeliveredInTheirOrderBeforeItsNextRecord() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue(), "k#2", Outcome.requeue()));
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"));
        tick();
        process();
        assertThat(parkedRows()).hasSize(2);
        script = answering(Map.of());
        send(msg("t", "k", "k#3"));
        tick();
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 3, true));

        assertThat(calls).singleElement()
                .satisfies(call -> assertThat(payloads(call)).containsExactly("k#1", "k#2", "k#3"));
        assertThat(delivered()).containsExactly("k#1", "k#2", "k#3");
        assertThat(parkedRows()).isEmpty();
        assertThat(cursor().parkedDueAt()).isNull();
    }

    @Test
    void requeueIsNoFailure() throws Exception {
        config.batchSize(5);
        bootstrap();
        script = records -> records.stream().map(r -> Outcome.requeue()).toList();
        send(IntStream.range(0, 10).mapToObj(i -> msg("t", "k" + i, "k" + i)).toArray(OutboxMessage[]::new));
        tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, false));

        assertThat(cursor().pausedUntil()).isNull();
        assertThat(cursor().failures()).isZero();
        assertThat(parkedRows()).hasSize(5);
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, false));
        assertThat(calls).as("the chunk limit is not halved").singleElement()
                .satisfies(call -> assertThat(call).hasSize(5));
    }

    @Test
    void requeueAfterTheKeyFailedInTheCallIsNotParked() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.retry(new RuntimeException("k down")), "k#2", Outcome.requeue()));
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        assertThat(retryRows()).containsExactly("k#1:1:" + now.plus(INITIAL), "k#2:0:-");
        assertThat(parkedRows()).isEmpty();
        assertThat(cursor().parkedDueAt()).isNull();
        assertThat(counter("outbox.messages.requeued", "topic", "t")).isZero();
    }

    @Test
    void aParkedRowFailingTransientlyBlocksItsKeyWithTheNewerParkedRows() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue(), "k#2", Outcome.requeue()));
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"));
        tick();
        process();
        script = answering(Map.of("k#1", Outcome.retry(new RuntimeException("k down")),
                "k#2", Outcome.notAttempted(), "k#3", Outcome.notAttempted()));
        send(msg("t", "k", "k#3"), msg("t", "j", "j#1"));
        tick();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        var due = now.plus(INITIAL);
        assertThat(retryRows()).containsExactly("k#1:1:" + due, "k#2:0:-", "k#3:0:-");
        assertThat(rows("select requeues from " + names.retry() + " order by id", rs -> rs.getString(1)))
                .containsExactly("1", "1", "0");
        assertThat(parkedRows()).isEmpty();
        assertThat(cursor().retryDueAt()).isEqualTo(due);
        assertThat(cursor().parkedDueAt()).isNull();
        assertThat(delivered()).containsExactly("j#1");
    }

    @Test
    void mixedOutcomesInOneCall() throws Exception {
        bootstrap();
        script = answering(Map.of("a#1", Outcome.requeue()));
        send(msg("t", "a", "a#1"));
        tick();
        process();
        script = answering(Map.of("a#1", Outcome.requeue(), "b#1", Outcome.retry(new RuntimeException("b down")),
                "c#1", Outcome.permanent(new OutboxPermanentException("c bad")), "d#1", Outcome.notAttempted(),
                "e#1", Outcome.requeue()));
        send(msg("t", "a", "a#2"), msg("t", "b", "b#1"), msg("t", "c", "c#1"), msg("t", "d", "d#1"),
                msg("t", "e", "e#1"));
        tick();
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true, true));

        var requeueDue = now.plus(REQUEUE_DELAY);
        var retryDue = now.plus(INITIAL);
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call))
                .containsExactly("a#1", "a#2", "b#1", "c#1", "d#1", "e#1"));
        assertThat(parkedRows()).containsExactly("a#1:2:" + requeueDue, "e#1:1:" + requeueDue);
        assertThat(retryRows()).containsExactly("b#1:1:" + retryDue, "d#1:0:" + retryDue);
        assertThat(deadRows()).containsExactly("c#1:1");
        assertThat(delivered()).containsExactly("a#2");
        assertThat(cursor().parkedDueAt()).isEqualTo(requeueDue);
        assertThat(cursor().retryDueAt()).isEqualTo(retryDue);
        assertThat(counter("outbox.messages.requeued", "topic", "t")).isEqualTo(3);
    }

    @Test
    void chunkRowsNotAttemptedAfterADeliveredParkedRowAreReadAgain() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        var before = cursor();
        script = answering(Map.of("k#2", Outcome.notAttempted())); // e.g. a call cut after the parked row
        send(msg("t", "k", "k#2"));
        tick();

        // k#1 left the parked table: progress, so the chunk is read again rather than failed
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        assertThat(delivered()).containsExactly("k#1");
        assertThat(parkedRows()).isEmpty();
        assertThat(retryRows()).isEmpty();
        assertThat(cursor()).as("k#2 unread, the position kept").isEqualTo(before.withParkedDueAt(null));
        script = answering(Map.of());
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("k#2"));
    }

    @Test
    void aCallOfReParkedRowsAndAnUnattemptedChunkFailsAsAWhole() throws Exception {
        // k#1 is parked again on every call and the rest is never attempted, e.g. the call is cut right after it
        failsAsAWholeWhileItsParkedRowStays(Outcome::requeue);
    }

    @Test
    void aCallOfKeptParkedRowsAndAnUnattemptedChunkFailsAsAWhole() throws Exception {
        // k#1 is not attempted with a notBefore of its own (it counts as attempted), the rest not attempted at all
        failsAsAWholeWhileItsParkedRowStays(Outcome::notAttempted);
    }

    /**
     * {@code k#1} parked until {@code later}, then answered {@code answer(later)} in every call while {@code k#2} and
     * {@code j#1} are never attempted.
     */
    private void failsAsAWholeWhileItsParkedRowStays(Function<Instant, Outcome> answer) throws Exception {
        bootstrap();
        var later = now.plus(Duration.ofHours(1)); // k#1 is due long after this test
        script = answering(Map.of("k#1", Outcome.requeue(later)));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        script = answering(Map.of("k#1", answer.apply(later), "k#2", Outcome.notAttempted(),
                "j#1", Outcome.notAttempted()));
        send(msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();

        // k#1 stays parked and no chunk row is attempted: two keys fail, the call pauses and k#1's answer is not
        // applied
        for (int failures = 1; failures <= config.headFailuresBeforeDivert(); failures++) {
            assertThat(process().status()).isEqualTo(PAUSED);
            assertThat(cursor().failures()).isEqualTo(failures);
            assertThat(parkedRows()).containsExactly("k#1:1:" + later);
            now = cursor().pausedUntil();
        }
        // the probe reads k#2 alone and merges no parked row: a single failing key, diverted
        calls.clear();
        assertThat(process().status()).isEqualTo(PAUSED);
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("k#2"));
        assertThat(retryRows()).containsExactly("k#2:0:" + now.plus(INITIAL));
        assertThat(parkedRows()).containsExactly("k#1:1:" + later);

        // k blocked in the retry table hands no record, so k#1 is no longer merged, and the shard goes on with j#1
        script = answering(Map.of("k#2", Outcome.notAttempted()));
        now = cursor().pausedUntil();
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, false));
        assertThat(calls).extracting(call -> payloads(call)).containsExactly(List.of("k#2"), List.of("j#1"));
        assertThat(delivered()).containsExactly("j#1");
        assertThat(parkedRows()).containsExactly("k#1:1:" + later);
    }

    @Test
    void theProbeCallHoldsTheHeadAloneWithoutTheParkedRowsOfItsKey() throws Exception {
        bootstrap();
        var later = now.plus(Duration.ofHours(1)); // k#1 is due long after this test
        script = answering(Map.of("k#1", Outcome.requeue(later)));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        script = records -> {
            if (records.stream().anyMatch(payload("k#2"))) {
                throw new IllegalStateException("cannot handle k#2");
            }
            return handler.handleBatch(records);
        };
        send(msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();

        // k#1, k#2 and j#1 fail as a whole: two keys, so the shard pauses until it probes
        for (int failures = 1; failures <= config.headFailuresBeforeDivert(); failures++) {
            assertThat(process().status()).isEqualTo(PAUSED);
            now = cursor().pausedUntil();
        }
        // the probe hands the poison alone: it is diverted and charged, k#1 stays parked as it was
        calls.clear();
        assertThat(process().status()).isEqualTo(PAUSED);
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("k#2"));
        assertThat(retryRows()).containsExactly("k#2:3:" + now.plus(Backoff.delay(2, INITIAL, config.maxDelay())));
        assertThat(parkedRows()).containsExactly("k#1:1:" + later);
        assertThat(rows("select attempts from " + names.parked(), rs -> rs.getString(1))).containsExactly("0");
    }

    @Test
    void requeueIsAtomicWithTheStep() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        var before = cursor();
        var refuse = names.base() + "_refuse";
        try (var c = TestDb.connect()) {
            execute(c, "create function " + refuse + "() returns trigger language plpgsql"
                    + " as $$ begin raise exception 'cursor append refused'; end $$");
            execute(c, "create trigger refuse before insert on " + names.cursor() + " for each row execute function "
                    + refuse + "()");
        }

        assertThatThrownBy(this::process).isInstanceOf(SQLException.class).hasMessageContaining("append refused");

        assertThat(parkedRows()).isEmpty();
        assertThat(cursor()).isEqualTo(before);
        try (var c = TestDb.connect()) {
            execute(c, "drop trigger refuse on " + names.cursor());
        }
        script = answering(Map.of());
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(calls).singleElement().satisfies(call -> {
            assertThat(payloads(call)).containsExactly("k#1");
            assertThat(call.get(0).requeues()).isZero();
        });
    }

    @Test
    void lostShardLockMidStepLeavesNoParkedCopy() throws Exception {
        bootstrap();
        var terminated = new AtomicInteger();
        script = records -> { // the step's backend dies after the handler answered, as when its lock is lost
            try (var c = TestDb.connect(); var ps = c.prepareStatement("select pg_terminate_backend(pid, 5000)"
                    + " from pg_locks where locktype = 'advisory' and classid = ?::int4::oid"
                    + " and objid = ?::int4::oid and objsubid = 2 and granted")) {
                ps.setInt(1, names.internalNamespace());
                ps.setInt(2, TableNames.LOCK_SHARD_BASE);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        terminated.addAndGet(rs.getBoolean(1) ? 1 : 0);
                    }
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
            return records.stream().map(r -> Outcome.requeue()).toList();
        };
        send(msg("t", "k", "k#1"));
        tick();

        assertThatThrownBy(this::process).isInstanceOf(SQLException.class);

        assertThat(terminated).hasValue(1);
        assertThat(parkedRows()).isEmpty();
        script = answering(Map.of());
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(calls).singleElement().satisfies(call -> {
            assertThat(payloads(call)).containsExactly("k#1");
            assertThat(call.get(0).requeues()).isZero();
        });
    }

    @Test
    void aHotKeyMergesAtMostTheChunkLimitOfParkedRows() throws Exception {
        config.batchSize(5);
        bootstrap();
        script = records -> records.stream().map(r -> Outcome.requeue()).toList();
        send(IntStream.rangeClosed(1, 12).mapToObj(i -> msg("t", "k", "k#" + i)).toArray(OutboxMessage[]::new));
        tick();
        processUntilNoWork(); // chunks of 5, each with the 5 oldest parked rows of k merged in
        assertThat(parkedRows()).hasSize(12);
        script = answering(Map.of());
        calls.clear();

        send(msg("t", "k", "k#new"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 6, true));
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call))
                .containsExactly("k#1", "k#2", "k#3", "k#4", "k#5", "k#new"));

        send(msg("t", "k", "k#newer"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 6, true));
        send(msg("t", "k", "k#newest"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 3, true));
        assertThat(delivered()).containsExactly("k#1", "k#2", "k#3", "k#4", "k#5", "k#new", "k#6", "k#7", "k#8",
                "k#9", "k#10", "k#newer", "k#11", "k#12", "k#newest");
        assertThat(parkedRows()).isEmpty();
    }

    @Test
    void stepsOfAShardWithoutParkedRowsLeaveTheParkedTableAlone() throws Exception {
        bootstrap();
        send(msg("t", "a", "a#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(statements).as("a shard's first step checks in full").anyMatch(sql -> sql.contains(names.parked()));
        handler.failNext(payload("k#1"), () -> new RuntimeException("k busy"));
        send(msg("t", "b", "b#1"), msg("t", "k", "k#1"));
        tick();
        statements.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true)); // k#1 diverted
        now = now.plus(INITIAL);
        assertThat(process()).as("a retry pass").isEqualTo(new ShardResult(PROGRESS, 1, true));
        send(msg("t", "c", "c#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        assertThat(statements).anyMatch(sql -> sql.contains(names.retry()))
                .noneMatch(sql -> sql.contains(names.parked()));
        assertThat(delivered()).containsExactly("a#1", "b#1", "k#1", "c#1");
    }

    @Test
    void requeueLimitMovesTheRecordToDead() throws Exception {
        config.maxRequeues(2);
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        send(msg("t", "k", "k#2"));
        tick();
        process();
        assertThat(parkedRows()).containsExactly("k#1:2:" + now.plus(REQUEUE_DELAY));

        try (var err = new CapturedStderr()) {
            send(msg("t", "k", "k#3"));
            tick();
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true, true));
            assertThat(err.text()).contains("ERROR " + ShardProcessor.class.getName())
                    .contains("moved to the dead table").contains("requeue limit reached");
        }

        assertThat(deadRows()).containsExactly("k#1:0");
        assertThat(rows("select error from " + names.dead(), rs -> rs.getString(1)))
                .containsExactly("requeue limit reached");
        assertThat(parkedRows()).isEmpty();
        assertThat(cursor().parkedDueAt()).isNull();
        assertThat(delivered()).containsExactly("k#2", "k#3");
        assertThat(counter("outbox.messages.failed", "topic", "t", "outcome", "dead")).isEqualTo(1);
    }

    @Test
    void requeuedDeliverySpanIsNoError() throws Exception {
        var exporter = InMemorySpanExporter.create();
        try (var provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build()) {
            tracer = provider.get("test");
            bootstrap();
            script = answering(Map.of("k#1", Outcome.requeue()));
            send(msg("t", "k", "k#1"));
            tick();

            process();

            assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span -> {
                assertThat(span.getAttributes().get(AttributeKey.stringKey("outbox.outcome"))).isEqualTo("requeued");
                assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
            });
        }
    }

    @Test
    void aParkedRecordComesBackAtItsDueTime() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        var due = now.plus(REQUEUE_DELAY);
        script = answering(Map.of());
        calls.clear();

        now = due.minus(1, ChronoUnit.MICROS);
        assertThat(process()).isEqualTo(new ShardResult(NO_WORK, 0, true));
        assertThat(calls).isEmpty();

        now = due; // nothing else of k arrives: the retry pass hands it
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(calls).singleElement().satisfies(call -> {
            assertThat(payloads(call)).containsExactly("k#1");
            assertThat(call.get(0).requeues()).isEqualTo(1);
        });
        assertThat(delivered()).containsExactly("k#1");
        assertThat(parkedRows()).isEmpty();
        assertThat(cursor().parkedDueAt()).isNull();
        assertThat(process().status()).isEqualTo(NO_WORK);
    }

    @Test
    void requeueWithNotBefore() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        process();

        now = now.plus(REQUEUE_DELAY);
        var notBefore = now.plusSeconds(10);
        script = answering(Map.of("k#1", Outcome.requeue(notBefore)));
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(parkedRows()).containsExactly("k#1:2:" + notBefore);

        now = notBefore; // a notBefore in the past is due after the requeue delay, not at once
        script = answering(Map.of("k#1", Outcome.requeue(now.minusSeconds(1))));
        process();
        assertThat(parkedRows()).containsExactly("k#1:3:" + now.plus(REQUEUE_DELAY));

        now = now.plus(REQUEUE_DELAY); // one beyond 24 h is cut to 24 h
        script = answering(Map.of("k#1", Outcome.requeue(now.plus(Duration.ofHours(48)))));
        process();
        assertThat(parkedRows()).containsExactly("k#1:4:" + now.plus(Duration.ofHours(24)));
        assertThat(cursor().parkedDueAt()).isEqualTo(now.plus(Duration.ofHours(24)));
    }

    @Test
    void anUnkeyedRecordIsRequeuedAndComesBackOnlyAtItsDueTime() throws Exception {
        bootstrap();
        script = answering(Map.of("u", Outcome.requeue()));
        send(msg("t", null, "u"));
        tick();
        process();
        var due = now.plus(REQUEUE_DELAY);
        send(msg("t", null, "v"));
        tick();
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("v"));
        assertThat(parkedRows()).containsExactly("u:1:" + due);

        script = answering(Map.of());
        now = due;
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("u"));
        assertThat(delivered()).containsExactly("v", "u");
        assertThat(parkedRows()).isEmpty();
    }

    @Test
    void requeueOfARetryHeadParksItAndItsFollowersGoOn() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.retry(new RuntimeException("k busy")), "k#2", Outcome.notAttempted(),
                "k#3", Outcome.notAttempted()));
        send(msg("t", "k", "k#1"), msg("t", "k", "k#2"), msg("t", "k", "k#3"), msg("t", "j", "j#1"));
        tick();
        process(); // j#1 delivered, so k#1 is diverted at once, k#2 and k#3 behind it
        var due = now.plus(INITIAL);
        assertThat(retryRows()).containsExactly("k#1:1:" + due, "k#2:0:-", "k#3:0:-");
        script = answering(Map.of("k#1", Outcome.requeue()));
        now = due;
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));

        assertThat(calls).singleElement()
                .satisfies(call -> assertThat(payloads(call)).containsExactly("k#1", "k#2", "k#3"));
        assertThat(retryRows()).isEmpty();
        assertThat(parkedRows()).containsExactly("k#1:1:" + now.plus(REQUEUE_DELAY));
        assertThat(delivered()).containsExactly("j#1", "k#2", "k#3");
        assertThat(cursor().retryDueAt()).isNull();
        assertThat(cursor().parkedDueAt()).isEqualTo(now.plus(REQUEUE_DELAY));
        assertThat(counter("outbox.messages.requeued", "topic", "t")).isEqualTo(1);

        script = answering(Map.of()); // back at its due with the attempt it was charged
        now = now.plus(REQUEUE_DELAY);
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(calls).singleElement().satisfies(call -> {
            assertThat(payloads(call)).containsExactly("k#1");
            assertThat(call.get(0).attempts()).isEqualTo(1);
            assertThat(call.get(0).requeues()).isEqualTo(1);
        });
        assertThat(parkedRows()).isEmpty();
    }

    @Test
    void aDueParkedRowFailingTransientlyBecomesTheHeadAndDemotesANewerHead() throws Exception {
        now = parkedBelowARetryHeadOfItsKey(); // k#2's head is due as well
        script = answering(Map.of("k#1", Outcome.retry(new RuntimeException("k#1 down")),
                "k#2", Outcome.retry(new RuntimeException("k#2 down"))));
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));

        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("k#1", "k#2"));
        var due = now.plus(INITIAL);
        assertThat(retryRows()).as("one head: k#1").containsExactly("k#1:1:" + due, "k#2:2:-");
        assertThat(parkedRows()).isEmpty();
        assertThat(cursor().retryDueAt()).isEqualTo(due);
        assertThat(cursor().parkedDueAt()).isNull();

        script = answering(Map.of());
        now = due;
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(delivered()).containsExactly("j#1", "k#1", "k#2");
        assertThat(retryRows()).isEmpty();
    }

    @Test
    void aDueParkedRowFailingAfterTheRetryHeadOfItsKeyWasDeliveredKeepsItsBackoff() throws Exception {
        now = parkedBelowARetryHeadOfItsKey();
        script = answering(Map.of("k#1", Outcome.retry(new RuntimeException("k#1 down")))); // k#2 is delivered

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        // k goes on after k#2 (its next retry row is promoted) before k#1 becomes its head: a promote after it would
        // make k#1, then the lowest retry row of k, due at once
        assertThat(retryRows()).containsExactly("k#1:1:" + now.plus(INITIAL));
        assertThat(parkedRows()).isEmpty();
        assertThat(delivered()).containsExactly("j#1", "k#2");
    }

    /**
     * {@code k#1} parked, then {@code k#2} failing in the call that merges {@code k#1}, which is parked again: k#1
     * parked below the retry head k#2; returns k#1's due, by which k#2 is due too.
     */
    private Instant parkedBelowARetryHeadOfItsKey() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        script = answering(Map.of("k#1", Outcome.requeue(), "k#2", Outcome.retry(new RuntimeException("k#2 down"))));
        send(msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();
        process();
        var due = now.plus(REQUEUE_DELAY);
        assertThat(parkedRows()).containsExactly("k#1:2:" + due);
        assertThat(retryRows()).containsExactly("k#2:1:" + now.plus(INITIAL));
        return due;
    }

    @Test
    void aKeyGoingOnInARetryCallFailingAsAWholeIsPromotedBeforeItsParkedRowBecomesTheHead() throws Exception {
        config.batchSize(3).retry(INITIAL, Duration.ofMillis(500), 2);
        bootstrap();
        // as the relay leaves them (no parked row has a lower-id retry row of its key): k#2 a head out of attempts,
        // k#3 its follower, k#1 and x#1 parked; all due
        var due = "'" + now + "'::timestamptz";
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.retry() + " (id, shard, message_id, topic, key, payload, created_at,"
                    + " attempts, next_attempt_at, last_error) values (2, 0, gen_random_uuid(), 't', 'k'::bytea,"
                    + " 'k#2'::bytea, now(), 2, " + due + ", 'k#2 down'), (3, 0, gen_random_uuid(), 't', 'k'::bytea,"
                    + " 'k#3'::bytea, now(), 0, null, null)");
            execute(c, "insert into " + names.parked() + " (id, shard, message_id, topic, key, payload, created_at,"
                    + " requeues, due_at) values (1, 0, gen_random_uuid(), 't', 'k'::bytea, 'k#1'::bytea, now(), 1, "
                    + due + "), (4, 0, gen_random_uuid(), 't', 'x'::bytea, 'x#1'::bytea, now(), 1, " + due + ")");
        }
        // k#2 goes to dead before the call and the budget leaves k#3 out: k goes on without a call of its own; the
        // call of k#1 and x#1 fails as a whole, then k#1 alone fails transiently
        script = records -> {
            if (records.stream().anyMatch(payload("x#1"))) {
                throw new IllegalStateException("handleBatch cannot take x#1");
            }
            return records.stream().map(r -> Outcome.retry(new RuntimeException("k#1 down"))).toList();
        };

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true, true));

        assertThat(calls).extracting(call -> payloads(call))
                .containsExactly(List.of("k#1", "x#1"), List.of("k#1"), List.of("x#1"));
        assertThat(deadRows()).containsExactly("k#2:2");
        var next = now.plus(INITIAL);
        assertThat(retryRows()).as("k#3 promoted, then demoted under k#1")
                .containsExactly("k#1:1:" + next, "k#3:0:-", "x#1:1:" + next);
        assertThat(parkedRows()).isEmpty();
    }

    @Test
    void aParkedRowFailingInTheRetryPassBlocksTheNewerRecordsOfItsKeyInTheSameStep() throws Exception {
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        assertThat(cursor().retryDueAt()).isNull();
        script = answering(Map.of("k#1", Outcome.retry(new RuntimeException("k#1 down"))));
        send(msg("t", "k", "k#2"));
        tick();
        now = now.plus(REQUEUE_DELAY);
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));

        assertThat(calls).as("k#2 is read blocked behind k#1, which the main pass no longer merges")
                .singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("k#1"));
        assertThat(retryRows()).containsExactly("k#1:1:" + now.plus(INITIAL), "k#2:0:-");
        assertThat(delivered()).isEmpty();
    }

    @Test
    void requeueLimitInTheRetryPass() throws Exception {
        config.maxRequeues(1);
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue(), "j#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"), msg("t", "j", "j#1"));
        tick();
        process();
        assertThat(parkedRows()).hasSize(2);

        now = now.plus(REQUEUE_DELAY); // j#1 is requeued past the limit, k#1 fails and becomes a retry head
        script = answering(Map.of("k#1", Outcome.retry(new RuntimeException("k#1 down")), "j#1", Outcome.requeue()));
        try (var err = new CapturedStderr()) {
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true, true));
            assertThat(err.text()).contains("moved to the dead table").contains("requeue limit reached");
        }
        assertThat(deadRows()).containsExactly("j#1:0");
        assertThat(retryRows()).containsExactly("k#1:1:" + now.plus(INITIAL));
        assertThat(parkedRows()).isEmpty();

        now = now.plus(INITIAL); // k#1 is requeued as a retry head, past the limit too
        script = answering(Map.of("k#1", Outcome.requeue()));
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true, true));

        assertThat(deadRows()).containsExactly("k#1:1", "j#1:0");
        assertThat(rows("select error from " + names.dead() + " order by id", rs -> rs.getString(1)))
                .containsExactly("requeue limit reached", "requeue limit reached");
        assertThat(retryRows()).isEmpty();
        assertThat(parkedRows()).isEmpty();
        assertThat(cursor().retryDueAt()).isNull();
        assertThat(cursor().parkedDueAt()).isNull();
        assertThat(counter("outbox.messages.failed", "topic", "t", "outcome", "dead")).isEqualTo(2);
    }

    @Test
    void parkedRowsAreFoundWithinMaintenanceIntervalWhenTheCursorLostTheSignal() throws Exception {
        config.maintenanceInterval(Duration.ofMillis(100));
        bootstrap();
        script = answering(Map.of("k#1", Outcome.requeue()));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        var due = now.plus(REQUEUE_DELAY);
        assertThat(cursor().parkedDueAt()).isEqualTo(due);
        try (var c = TestDb.connect()) { // an instance of the previous version appends a cursor without the column
            cursors.append(c, cursor().withParkedDueAt(null));
        }
        calls.clear();

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (cursor().parkedDueAt() == null && System.nanoTime() < deadline) { // not due yet: the signal comes back
            assertThat(process().status()).isEqualTo(NO_WORK);
            Thread.sleep(10);
        }
        assertThat(cursor().parkedDueAt()).isEqualTo(due);
        assertThat(calls).isEmpty();

        try (var c = TestDb.connect()) {
            cursors.append(c, cursor().withParkedDueAt(null));
        }
        now = due; // due: the full check hands it, and it is parked once more
        while (calls.isEmpty() && System.nanoTime() < deadline) {
            process();
            Thread.sleep(10);
        }
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("k#1"));
        assertThat(parkedRows()).containsExactly("k#1:2:" + now.plus(REQUEUE_DELAY));
        assertThat(cursor().parkedDueAt()).isEqualTo(now.plus(REQUEUE_DELAY));
    }

    @Test
    void aParkedGroupFailingAsAWholeIsIsolated() throws Exception {
        bootstrap();
        script = answering(Map.of("p1", Outcome.requeue(), "p2", Outcome.requeue()));
        send(msg("t", "a", "p1"), msg("t", "b", "p2"));
        tick();
        process();
        script = records -> {
            if (records.stream().anyMatch(payload("p1"))) {
                throw new IllegalStateException("handleBatch cannot take p1");
            }
            return handler.handleBatch(records);
        };
        now = now.plus(REQUEUE_DELAY);
        calls.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        assertThat(calls).extracting(call -> payloads(call))
                .containsExactly(List.of("p1", "p2"), List.of("p1"), List.of("p2"));
        assertThat(delivered()).containsExactly("p2");
        assertThat(retryRows()).containsExactly("p1:1:" + now.plus(INITIAL));
        assertThat(parkedRows()).isEmpty();
    }

    @Test
    void theRetryPassWritesItsParkedRowsInBatches() throws Exception {
        bootstrap();
        script = records -> records.stream().map(r -> Outcome.requeue()).toList();
        send(Stream.of("a", "b", "c", "d", "e", "f").map(key -> msg("t", key, key)).toArray(OutboxMessage[]::new));
        tick();
        process();
        now = now.plus(REQUEUE_DELAY);
        script = answering(Map.of("a", Outcome.requeue(), "b", Outcome.requeue(), "c", Outcome.requeue(),
                "d", Outcome.notAttempted(), "e", Outcome.notAttempted()));
        statements.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        var due = now.plus(REQUEUE_DELAY);
        assertThat(parkedRows()).containsExactly("a:2:" + due, "b:2:" + due, "c:2:" + due, "d:1:" + due,
                "e:1:" + due);
        assertThat(delivered()).containsExactly("f");
        assertThat(statements).as("one re-park and one keep statement, each a JDBC batch")
                .filteredOn(sql -> sql.startsWith("update " + names.parked())).hasSize(2);
        assertThat(statements).filteredOn(sql -> sql.startsWith("delete from " + names.parked())).hasSize(1);
    }

    @Test
    void invalidRelayConfigIsRejected() {
        var cases = List.<Consumer<TestOutboxConfig>>of(
                c -> c.batchSize(0),
                c -> c.maxTicksPerBatch(0),
                c -> c.batchTimeout(Duration.ZERO),
                c -> c.headFailuresBeforeDivert(0),
                c -> c.retry(Duration.ZERO, Duration.ofSeconds(1), 0),
                c -> c.retry(Duration.ofSeconds(2), Duration.ofSeconds(1), 0),
                c -> c.retry(Duration.ofSeconds(1), Duration.ofSeconds(1), -1));
        var keys = List.of("outbox.relay.batchSize", "outbox.relay.maxTicksPerBatch", "outbox.relay.batchTimeout",
                "outbox.relay.headFailuresBeforeDivert", "outbox.relay.retry.initialDelay",
                "outbox.relay.retry.maxDelay", "outbox.relay.retry.maxAttempts");
        for (int i = 0; i < cases.size(); i++) {
            var invalid = new TestOutboxConfig();
            cases.get(i).accept(invalid);
            assertThatThrownBy(() -> new ShardProcessor(TableNames.of("outbox"), invalid, handler,
                    new OutboxMetrics(null), null, false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith(keys.get(i) + " ");
        }
    }

    @Test
    void groupStepDeliversEveryShardInKeyOrder() throws Exception {
        config.shards(4);
        bootstrap();
        var a = keyOnShard(0);
        var b = keyOnShard(1);
        var c = keyOnShard(2);
        var d = keyOnShard(3);
        send(msg("t", c, "c#1"), msg("t", a, "a#1"), msg("t", d, "d#1"), msg("t", c, "c#2"), msg("t", a, "a#2"),
                msg("t", b, "b#1"));
        long t = tick();

        assertThat(processGroup(2, 0, 3, 1)).containsExactly(new ShardResult(PROGRESS, 2, true),
                new ShardResult(PROGRESS, 2, true), new ShardResult(PROGRESS, 1, true),
                new ShardResult(PROGRESS, 1, true));

        assertThat(calls).as("one call: shard by shard in group order, id order inside")
                .extracting(ShardProcessorIT::payloads)
                .containsExactly(List.of("c#1", "c#2", "a#1", "a#2", "d#1", "b#1"));
        for (int s = 0; s < 4; s++) {
            assertThat(cursor(s)).isEqualTo(new Cursor(s, t, null, 0, null, 0, null, null));
        }
        assertThat(processGroup(0, 1, 2, 3)).containsOnly(new ShardResult(NO_WORK, 0, true));
    }

    @Test
    void groupStepCommitsAsOneTransaction() throws Exception {
        config.shards(3);
        bootstrap();
        send(msg("t", keyOnShard(0), "a#1"), msg("t", keyOnShard(1), "b#1"), msg("t", keyOnShard(2), "c#1"));
        tick();
        long seq;
        try (var c = TestDb.connect()) {
            seq = queryLong(c, "select max(seq) from " + names.cursor());
        }
        statements.clear();

        assertThat(processGroup(0, 1, 2)).containsOnly(new ShardResult(PROGRESS, 1, true));

        assertThat(rows("select count(*) || ':' || count(distinct xmin::text) from " + names.cursor()
                + " where seq > " + seq, rs -> rs.getString(1))).as("three rows of one transaction")
                .containsExactly("3:1");
        assertThat(statements).as("one cursor append")
                .filteredOn(sql -> sql.startsWith("insert into " + names.cursor())).hasSize(1);
        assertThat(statements).as("one lock statement").filteredOn(sql -> sql.contains("advisory")).hasSize(1);
    }

    @Test
    void aFailingShardDoesNotPauseItsNeighbours() throws Exception {
        config.shards(2);
        bootstrap();
        var down = keysOnShard(0, 3);
        var b = keyOnShard(1);
        handler.failAlways(r -> new String(r.payload(), UTF_8).startsWith("f"), () -> new RuntimeException("down"));
        send(msg("t", down.get(0), "f1"), msg("t", down.get(1), "f2"), msg("t", down.get(2), "f3"),
                msg("t", b, "b#1"), msg("t", b, "b#2"));
        long t = tick();
        var before = cursor(1);
        long rows = cursorRows(1);

        var results = processGroup(0, 1);

        // the default handler stops after three failing keys: shard 1's rows are not attempted
        assertThat(calls).extracting(ShardProcessorIT::payloads)
                .containsExactly(List.of("f1", "f2", "f3", "b#1", "b#2"));
        assertThat(results.get(0)).isEqualTo(new ShardResult(PAUSED, 0, false));
        assertThat(cursor(0).failures()).isEqualTo(1);
        assertThat(results.get(1)).as("untouched").isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(cursorRows(1)).as("no cursor row").isEqualTo(rows);
        assertThat(cursor(1)).isEqualTo(before);

        var next = processGroup(0, 1);
        assertThat(next.get(0).status()).as("paused").isEqualTo(NO_WORK);
        assertThat(next.get(1)).as("its limit unchanged: both rows").isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(delivered()).containsExactly("b#1", "b#2");
        assertThat(cursor(1)).isEqualTo(new Cursor(1, t, null, 0, null, 0, null, null));
    }

    @Test
    void cutReadLeavesLaterShardsUntouched() throws Exception {
        config.shards(5).batchSize(10);
        bootstrap();
        long t0 = cursor(0).tickId();
        for (int s = 0; s < 5; s++) {
            var key = keyOnShard(s);
            int shard = s;
            send(IntStream.range(1, 6).mapToObj(i -> msg("t", key, "s" + shard + "#" + i))
                    .toArray(OutboxMessage[]::new));
        }
        long t = tick();

        // 10 rows in all: shards 0 and 1 fill the read; 2..4 are not read, or read empty, and stay as they were
        assertThat(processGroup(0, 1, 2, 3, 4)).containsExactly(new ShardResult(PROGRESS, 5, false),
                new ShardResult(PROGRESS, 5, false), new ShardResult(PROGRESS, 0, true),
                new ShardResult(PROGRESS, 0, true), new ShardResult(PROGRESS, 0, true));
        var ids = handler.delivered().stream().map(OutboxRecord::id).toList();
        assertThat(cursor(0)).isEqualTo(new Cursor(0, t0, t, ids.get(4), null, 0, null, null));
        assertThat(cursor(1)).isEqualTo(new Cursor(1, t0, t, ids.get(9), null, 0, null, null));
        for (int s = 2; s < 5; s++) {
            assertThat(cursorRows(s)).as("shard %d untouched", s).isEqualTo(1);
        }

        // shards 0 and 1 read nothing more, but the read is cut again by 2 and 3: they stay mid-batch
        assertThat(processGroup(0, 1, 2, 3, 4)).containsExactly(new ShardResult(PROGRESS, 0, false),
                new ShardResult(PROGRESS, 0, false), new ShardResult(PROGRESS, 5, false),
                new ShardResult(PROGRESS, 5, false), new ShardResult(PROGRESS, 0, true));
        assertThat(cursor(0)).isEqualTo(new Cursor(0, t0, t, ids.get(4), null, 0, null, null));

        assertThat(processGroup(0, 1, 2, 3, 4)).containsExactly(new ShardResult(PROGRESS, 0, true),
                new ShardResult(PROGRESS, 0, true), new ShardResult(PROGRESS, 0, true),
                new ShardResult(PROGRESS, 0, true), new ShardResult(PROGRESS, 5, true));
        for (int s = 0; s < 5; s++) {
            assertThat(cursor(s)).isEqualTo(new Cursor(s, t, null, 0, null, 0, null, null));
        }
        assertThat(delivered()).hasSize(25).isSortedAccordingTo((x, y) -> {
            int shard = Integer.compare(x.charAt(1), y.charAt(1));
            return shard != 0 ? shard : Integer.compare(x.charAt(3), y.charAt(3));
        });
    }

    @Test
    void probeModeShardInAGroup() throws Exception {
        config.shards(2);
        bootstrap();
        var keys = keysOnShard(0, 2);
        script = records -> records.stream().anyMatch(payload("bad#1")) ? List.of() : handler.handleBatch(records);
        send(msg("t", keys.get(0), "bad#1"), msg("t", keys.get(1), "a#1"));
        tick();
        for (int failures = 1; failures <= 3; failures++) {
            assertThat(process(processor, 0)).isEqualTo(new ShardResult(PAUSED, 0, false));
            now = cursor(0).pausedUntil();
        }
        send(msg("t", keyOnShard(1), "b#1"));
        tick();
        calls.clear();

        // shard 0 probes: its head alone, in a call of its own; shard 1 in the group's call
        assertThat(processGroup(0, 1)).containsExactly(new ShardResult(PAUSED, 0, false),
                new ShardResult(PROGRESS, 1, true));

        assertThat(calls).extracting(ShardProcessorIT::payloads).containsExactly(List.of("b#1"), List.of("bad#1"));
        assertThat(retryRows()).containsExactly("bad#1:3:" + now.plus(Backoff.delay(2, INITIAL, config.maxDelay())));
        now = cursor(0).pausedUntil();
        script = null;
        for (int i = 0; i < 100 && processGroup(0, 1).get(0).status() != NO_WORK; i++) {
            now = now.plusSeconds(1);
        }
        assertThat(delivered()).startsWith("b#1").containsExactlyInAnyOrder("b#1", "a#1", "bad#1");
    }

    @Test
    void retryHeadsOfOneShardInAGroup() throws Exception {
        config.shards(2);
        bootstrap();
        var keys = keysOnShard(0, 2);
        script = answering(Map.of("a#1", Outcome.retry(new RuntimeException("once"))));
        send(msg("t", keys.get(0), "a#1"), msg("t", keys.get(1), "c#1"));
        tick();
        assertThat(process(processor, 0)).isEqualTo(new ShardResult(PROGRESS, 1, true)); // a#1 diverted
        assertThat(retryRows()).containsExactly("a#1:1:" + now.plus(INITIAL));
        script = null;
        send(msg("t", keyOnShard(1), "b#1"));
        tick();
        now = now.plus(INITIAL);
        calls.clear();

        assertThat(processGroup(0, 1)).containsExactly(new ShardResult(PROGRESS, 1, true),
                new ShardResult(PROGRESS, 1, true));

        assertThat(calls).as("shard 0's retry pass, then the group's call")
                .extracting(ShardProcessorIT::payloads).containsExactly(List.of("a#1"), List.of("b#1"));
        assertThat(retryRows()).isEmpty();
        assertThat(cursor(0).retryDueAt()).isNull();
        assertThat(delivered()).containsExactly("c#1", "a#1", "b#1");
    }

    @Test
    void busyPoolPausesTheWholeGroup() throws Exception {
        // pool of max(4, 2 * workers) = 4 threads; no probe or divert while shard 2's calls fill it
        config.shards(3).workers(1).batchTimeout(Duration.ofMillis(100)).headFailuresBeforeDivert(1000);
        bootstrap();
        var release = new CountDownLatch(1);
        script = records -> {
            if (payloads(records).contains("h#1")) {
                while (release.getCount() > 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // a handler that ignores cancellation keeps its thread
                    }
                }
            }
            return records.stream().map(r -> Outcome.delivered()).toList();
        };
        send(msg("t", keyOnShard(2), "h#1"), msg("t", keyOnShard(0), "a#1"), msg("t", keyOnShard(1), "b#1"));
        tick();
        try {
            for (int i = 1; i <= 4; i++) { // four calls of shard 2 hold the four threads
                assertThat(process(processor, 2).status()).isEqualTo(PAUSED);
                now = cursor(2).pausedUntil();
            }
            int held = calls.size();

            assertThat(processGroup(0, 1)).containsExactly(new ShardResult(PAUSED, 0, false),
                    new ShardResult(PAUSED, 0, false));

            assertThat(calls).as("no call ran").hasSize(held);
            for (int s = 0; s < 2; s++) {
                assertPausedFor(cursor(s), INITIAL);
                assertThat(cursor(s).failures()).isZero();
                assertThat(cursor(s).targetTickId()).isNull();
            }
        } finally {
            release.countDown();
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        var together = List.of("a#1", "b#1");
        while (calls.stream().map(ShardProcessorIT::payloads).noneMatch(together::equals)
                && System.nanoTime() < deadline) {
            now = now.plus(Duration.ofSeconds(1)); // past any pause or skip, once the threads come back
            processGroup(0, 1);
        }
        assertThat(calls).as("the group is called once threads are free").extracting(ShardProcessorIT::payloads)
                .contains(together);
    }

    @Test
    void swapLockStopsTheWholeGroup() throws Exception {
        config.shards(3);
        bootstrap();
        send(msg("t", keyOnShard(0), "a#1"), msg("t", keyOnShard(1), "b#1"), msg("t", keyOnShard(2), "c#1"));
        tick();
        var swap = TestDb.connect();
        open.add(swap);
        swapLock(swap, "pg_advisory_lock");

        assertThat(processGroup(0, 1, 2)).hasSize(3).containsOnly(new ShardResult(NOT_LOCKED, 0, false));
        assertThat(calls).isEmpty();

        swapLock(swap, "pg_advisory_unlock");
        var holder = begin(); // another worker holds shard 1
        try (var ps = holder.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_SHARD_BASE + 1);
            ps.execute();
        }
        assertThat(processGroup(0, 1, 2)).containsExactly(new ShardResult(PROGRESS, 1, true),
                new ShardResult(NOT_LOCKED, 0, false), new ShardResult(PROGRESS, 1, true));
        holder.commit();
        assertThat(processGroup(0, 1, 2)).containsExactly(new ShardResult(NO_WORK, 0, true),
                new ShardResult(PROGRESS, 1, true), new ShardResult(NO_WORK, 0, true));
        assertThat(delivered()).containsExactly("a#1", "c#1", "b#1");
    }

    @Test
    void aGroupCallFailingAsAWholeChargesNoShard() throws Exception {
        config.shards(2);
        bootstrap();
        script = records -> {
            if (payloads(records).contains("bad#1")) {
                throw new IllegalStateException("poison");
            }
            return handler.handleBatch(records);
        };
        send(msg("t", keyOnShard(0), "bad#1"), msg("t", keyOnShard(1), "b#1"));
        tick();
        long rows = cursorRows(0) + cursorRows(1);

        assertThat(processGroup(0, 1)).as("says nothing about either shard")
                .containsOnly(new ShardResult(PROGRESS, 0, true));
        assertThat(cursorRows(0) + cursorRows(1)).isEqualTo(rows);

        // each shard is called alone now: the failure is shard 0's own
        assertThat(processGroup(0, 1)).containsExactly(new ShardResult(PAUSED, 0, false),
                new ShardResult(PROGRESS, 1, true));
        assertThat(calls).extracting(ShardProcessorIT::payloads)
                .containsExactly(List.of("bad#1", "b#1"), List.of("bad#1"), List.of("b#1"));
        assertThat(cursor(0).failures()).isEqualTo(1);
        assertThat(cursor(1).failures()).isZero();
    }

    @Test
    void aGroupStepStartsNoRetryPassOnceItSpentBatchTimeout() throws Exception {
        var timeout = Duration.ofMillis(300);
        config.shards(2).batchTimeout(timeout);
        bootstrap();
        var a = keysOnShard(0, 2);
        var b = keysOnShard(1, 2);
        var once = Outcome.retry(new RuntimeException("once"));
        script = answering(Map.of("a#1", once, "b#1", once));
        send(msg("t", a.get(0), "a#1"), msg("t", a.get(1), "a2#1"), msg("t", b.get(0), "b#1"),
                msg("t", b.get(1), "b2#1"));
        tick();
        assertThat(process(processor, 0)).isEqualTo(new ShardResult(PROGRESS, 1, true)); // a#1 diverted
        assertThat(process(processor, 1)).isEqualTo(new ShardResult(PROGRESS, 1, true)); // b#1 diverted
        now = now.plus(INITIAL); // both heads due
        var before = cursor(1);
        long rows = cursorRows(1);
        script = hangingPast(timeout, records -> payloads(records).contains("a#1"), handler::handleBatch);
        calls.clear();

        // shard 0's retry call outlives batchTimeout: shard 1's retry pass waits for its next step
        assertThat(processGroup(0, 1)).containsExactly(new ShardResult(PROGRESS, 0, true),
                new ShardResult(PROGRESS, 0, true));

        assertThat(calls).extracting(ShardProcessorIT::payloads).containsExactly(List.of("a#1"));
        assertThat(cursor(1)).as("deferred: its cursor as it was").isEqualTo(before);
        assertThat(cursorRows(1)).isEqualTo(rows);
        assertThat(retryRows()).hasSize(2).last().asString().startsWith("b#1:1:");

        script = null;
        assertThat(processGroup(0, 1).get(1)).as("the next step hands it")
                .isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(delivered()).endsWith("b#1");
        assertThat(cursor(1).retryDueAt()).isNull();
    }

    @Test
    void aGroupOfRepeatedShardsIsRefused() throws Exception {
        config.shards(2);
        bootstrap();

        assertThatThrownBy(() -> processGroup(0, 1, 0)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not distinct");
    }

    private void bootstrap() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
        ticks = new Ticks(names, config.shards());
        cursors = new Cursors(names, config.shards());
        outbox = new PgOutbox(db, schema, config.shards(), () -> {}, new OutboxMetrics(null));
        var scripted = new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                handler.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                calls.add(List.copyOf(records));
                var s = script;
                return s == null ? handler.handleBatch(records) : s.apply(records);
            }
        };
        processor = new ShardProcessor(names, config, scripted, new OutboxMetrics(meters), tracer, false);
    }

    /** One worker {@code tryProcess} of shard 0 at the test clock in its own transaction, rolled back on failure. */
    private ShardResult process() throws Exception {
        return process(processor);
    }

    private ShardResult process(ShardProcessor processor) throws Exception {
        return process(processor, 0);
    }

    private ShardResult process(ShardProcessor processor, int shard) throws Exception {
        long latest;
        try (var c = TestDb.connect()) {
            latest = ticks.latest(c).id();
        }
        try (var c = recording(TestDb.connect())) {
            ShardProcessor.prepareTransaction(c, config.batchTimeout());
            try {
                var result = processor.tryProcess(c, shard, latest, now);
                c.commit();
                return result;
            } catch (Throwable t) {
                c.rollback();
                throw t;
            }
        }
    }

    /** One worker group step of {@code shards} at the test clock in its own transaction, rolled back on failure. */
    private List<ShardResult> processGroup(Integer... shards) throws Exception {
        long latest;
        try (var c = TestDb.connect()) {
            latest = ticks.latest(c).id();
        }
        try (var c = recording(TestDb.connect())) {
            ShardProcessor.prepareTransaction(c, config.batchTimeout());
            try {
                var results = processor.tryProcess(c, List.of(shards), latest, now);
                c.commit();
                return results;
            } catch (Throwable t) {
                c.rollback();
                throw t;
            }
        }
    }

    private Cursor cursor(int shard) throws SQLException {
        try (var c = TestDb.connect()) {
            return cursors.latest(c, shard).cursor();
        }
    }

    /** Cursor rows of {@code shard} ever appended. */
    private long cursorRows(int shard) throws SQLException {
        return Long.parseLong(rows("select count(*) from " + names.cursor() + " where shard = " + shard,
                rs -> rs.getString(1)).get(0));
    }

    /** {@code n} keys of topic {@code t} on {@code shard}. */
    private List<String> keysOnShard(int shard, int n) {
        var keys = new ArrayList<String>();
        for (int i = 0; keys.size() < n; i++) {
            var key = "k" + i;
            if (Hashing.shard(Hashing.keyHash("t", key.getBytes(UTF_8)), config.shards()) == shard) {
                keys.add(key);
            }
        }
        return keys;
    }

    /** {@code c}, adding the SQL of each statement it prepares to {@link #statements}. */
    private Connection recording(Connection c) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().startsWith("prepare")) {
                        statements.add((String) args[0]);
                        statementTimes.add(System.nanoTime());
                    }
                    try {
                        return method.invoke(c, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /** One {@link #process()}; the longest time between two statements it prepared is added to {@code gaps}. */
    private ShardResult process(List<Duration> gaps) throws Exception {
        statementTimes.clear();
        var result = process();
        long max = 0;
        for (int i = 1; i < statementTimes.size(); i++) {
            max = Math.max(max, statementTimes.get(i) - statementTimes.get(i - 1));
        }
        gaps.add(Duration.ofNanos(max));
        return result;
    }

    /** A handler that ignores interrupts and outlives {@code timeout} whenever {@code hangs} matches the call. */
    private static Function<List<OutboxRecord>, List<Outcome>> hangingPast(Duration timeout,
            Predicate<List<OutboxRecord>> hangs, Function<List<OutboxRecord>, List<Outcome>> otherwise) {
        return records -> {
            if (!hangs.test(records)) {
                return otherwise.apply(records);
            }
            long until = System.nanoTime() + timeout.plusMillis(200).toNanos();
            while (System.nanoTime() < until) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {
                    // keeps going
                }
            }
            return records.stream().map(r -> Outcome.delivered()).toList();
        };
    }

    /** Steps until a step finds nothing to do ({@code NO_WORK}). */
    private List<ShardResult> processUntilNoWork() throws Exception {
        var results = new ArrayList<ShardResult>();
        for (int i = 0; i < 1000; i++) {
            var result = process();
            if (result.status() == NO_WORK) {
                return results;
            }
            results.add(result);
        }
        throw new AssertionError("still working after 1000 calls: " + results.get(results.size() - 1));
    }

    private long tick() throws SQLException {
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            return ticks.latest(c).id();
        }
    }

    /** A checkpoint at {@code tickId} that read shard 0 idle on its cursor's tick {@code cursorTick}. */
    private void checkpointIdleAt(Connection c, long tickId, long cursorTick) throws SQLException {
        cursors.checkpoint(c, tickId, List.of(new Cursors.Position(
                new Cursor(0, cursorTick, null, 0, null, 0, null, null), false, cursorTick)));
    }

    /** Cursor rows of shard 0 ever appended: the log only grows while maintenance does not run. */
    private long cursorRows() throws SQLException {
        return Long.parseLong(rows("select count(*) from " + names.cursor() + " where shard = 0",
                rs -> rs.getString(1)).get(0));
    }

    /** Tick {@code id} made exactly {@code by} before tick {@code later}, whatever time passed between them. */
    private void madeBefore(long id, long later, Duration by) throws SQLException {
        try (var c = TestDb.connect()) {
            execute(c, "update " + names.tick() + " set created_at = (select created_at from " + names.tick()
                    + " where id = " + later + ") - interval '" + by.toMillis() + " milliseconds' where id = " + id);
        }
    }

    private Cursor cursor() throws SQLException {
        try (var c = TestDb.connect()) {
            return cursors.latest(c, 0).cursor();
        }
    }

    /** A transaction left open until the test commits it. */
    private Connection begin() throws SQLException {
        var c = TestDb.connect();
        open.add(c);
        c.setAutoCommit(false);
        return c;
    }

    /** One committed transaction; rows get ids in argument order. */
    private List<UUID> send(OutboxMessage... messages) throws SQLException {
        return db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(messages)));
    }

    private static OutboxMessage msg(String topic, @Nullable String key, String payload) {
        return key == null
                ? OutboxMessage.of(topic, payload.getBytes(UTF_8))
                : OutboxMessage.of(topic, key, payload.getBytes(UTF_8));
    }

    /** Default {@code handleBatch} over {@link #handler}; {@code payload} hangs until interrupted while {@code hang}. */
    private OutboxHandler hangingOn(String payload, AtomicBoolean hang) {
        return record -> {
            if (hang.get() && payload(payload).test(record)) {
                Thread.sleep(10_000);
            }
            handler.handle(record);
        };
    }

    private static Predicate<OutboxRecord> payload(String payload) {
        return r -> payload.equals(new String(r.payload(), UTF_8));
    }

    /** Payloads the handler delivered, in delivery order. */
    private List<String> delivered() {
        return handler.delivered().stream().map(r -> new String(r.payload(), UTF_8)).toList();
    }

    /** {@code payload:attempts:next_attempt_at} in id order; {@code -} for followers. */
    private List<String> retryRows() throws SQLException {
        return rows("select payload, attempts, next_attempt_at from " + names.retry() + " order by id", rs -> {
            var next = rs.getObject(3, OffsetDateTime.class);
            return new String(rs.getBytes(1), UTF_8) + ":" + rs.getInt(2) + ":"
                    + (next == null ? "-" : next.toInstant().toString());
        });
    }

    /** {@code payload:requeues:due_at} in id order. */
    private List<String> parkedRows() throws SQLException {
        return rows("select payload, requeues, due_at from " + names.parked() + " order by id",
                rs -> new String(rs.getBytes(1), UTF_8) + ":" + rs.getInt(2) + ":"
                        + rs.getObject(3, OffsetDateTime.class).toInstant());
    }

    /** Answers each record by its payload from {@code answers}; any other record goes to {@link #handler}. */
    private Function<List<OutboxRecord>, List<Outcome>> answering(Map<String, Outcome> answers) {
        return records -> records.stream().map(r -> {
            var answer = answers.get(new String(r.payload(), UTF_8));
            return answer != null ? answer : handler.handleBatch(List.of(r)).get(0);
        }).toList();
    }

    private static List<String> payloads(List<OutboxRecord> records) {
        return records.stream().map(r -> new String(r.payload(), UTF_8)).toList();
    }

    private List<Long> retryIds() throws SQLException {
        return rows("select id from " + names.retry() + " order by id", rs -> rs.getString(1)).stream()
                .map(Long::valueOf).toList();
    }

    /** {@code payload:attempts} in id order. */
    private List<String> deadRows() throws SQLException {
        return rows("select payload, attempts from " + names.dead() + " order by id",
                rs -> new String(rs.getBytes(1), UTF_8) + ":" + rs.getInt(2));
    }

    private interface RowMapper {
        String map(ResultSet rs) throws SQLException;
    }

    private static List<String> rows(String sql, RowMapper mapper) throws SQLException {
        try (var c = TestDb.connect(); var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            var rows = new ArrayList<String>();
            while (rs.next()) {
                rows.add(mapper.map(rs));
            }
            return rows;
        }
    }

    /** Paused for {@code backoff} counted from the end of the call, which took less than 5 s. */
    private void assertPausedFor(Cursor cursor, Duration backoff) {
        assertThat(cursor.pausedUntil()).isBetween(now.plus(backoff), now.plus(backoff).plusSeconds(5));
    }

    private double counter(String name, String... tags) {
        var counter = meters.find(name).tags(tags).counter();
        return counter == null ? 0 : counter.count();
    }

    private static List<String> traceparents(OutboxRecord r) {
        return r.headers().stream()
                .filter(h -> h.name().equals(OutboxTracing.TRACEPARENT))
                .map(h -> new String(h.value(), UTF_8))
                .toList();
    }
}
