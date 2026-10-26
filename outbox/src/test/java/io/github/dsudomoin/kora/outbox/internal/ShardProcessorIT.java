package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.IDLE;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.NOT_LOCKED;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.NO_WORK;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.PAUSED;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.PROGRESS;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
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

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 2, true));
        assertThat(delivered()).containsExactly("k#2", "k#3");
        assertThat(deadRows()).containsExactly("k#1:1");
        assertThat(retryRows()).containsExactly("j#1:1:" + now.plus(INITIAL), "j#2:0:-");

        // in the retry pass a permanent failure moves the head to dead and its follower goes on in the same pass
        handler.failNext(payload("j#1"), () -> new OutboxPermanentException("bad j#1"));
        now = now.plus(INITIAL);
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

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
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
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
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 4, true));

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
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));

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
            assertThat(processor.tryProcess(first, 0, latest, now, false))
                    .isEqualTo(new ShardResult(PROGRESS, 2, true));
            first.commit();

            assertThat(processor.tryProcess(second, 0, latest, now, false))
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

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
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

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
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
    void emptyStepAppendsNoCursorWhileTheCursorTickIsYoungerThanFiveSeconds() throws Exception {
        config.maxTicksPerBatch(1);
        bootstrap();
        long t0 = cursor().tickId();
        long t1 = tick();
        long appends = cursorRows();

        // nothing to read, nothing in retry: no write at all
        assertThat(process()).isEqualTo(new ShardResult(IDLE, 0, true));
        assertThat(cursorRows()).isEqualTo(appends);
        assertThat(cursor().tickId()).isEqualTo(t0);

        // still idle just below 5 s
        madeBefore(t0, t1, Duration.ofMillis(4900));
        assertThat(process()).isEqualTo(new ShardResult(IDLE, 0, true));
        assertThat(cursorRows()).isEqualTo(appends);

        // 5 s old: the cursor moves on, for tick cleanup, partition drops and the lag gauge
        madeBefore(t0, t1, Duration.ofSeconds(5));
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(cursor()).isEqualTo(new Cursor(0, t1, null, 0, null, 0, null, null));

        // a target below the latest tick (maxTicksPerBatch 1) appends however young, or maxTicksPerBatch ticks
        // within 5 s would hold the cursor on the same young target forever
        long t2 = tick();
        long t3 = tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(cursor().tickId()).isEqualTo(t2);
        assertThat(process()).isEqualTo(new ShardResult(IDLE, 0, true));
        assertThat(cursor().tickId()).isEqualTo(t2);

        // drain appends every step, so the cursors reach its tick
        assertThat(process(true)).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(cursor().tickId()).isEqualTo(t3);

        // rows read: appended as before
        send(msg("t", "a", "a#1"));
        long t4 = tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor().tickId()).isEqualTo(t4);
    }

    @Test
    void emptyStepWhoseRetryDueAtMovedStillAppends() throws Exception {
        config.maintenanceInterval(Duration.ofNanos(1)); // every step checks the retry table in full
        bootstrap();
        var later = now.plus(Duration.ofHours(1));
        script = records -> records.stream()
                .map(r -> payload("k#1").test(r) ? Outcome.retry(new RuntimeException("k busy"), later)
                        : Outcome.delivered())
                .toList();
        send(msg("t", "a", "a#1"), msg("t", "k", "k#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        assertThat(cursor().retryDueAt()).isEqualTo(later);
        var moved = later.plus(Duration.ofHours(1));
        try (var c = TestDb.connect(); var ps = c.prepareStatement("update " + names.retry()
                + " set next_attempt_at = ? where next_attempt_at is not null")) { // changed outside the relay
            ps.setObject(1, moved.atOffset(ZoneOffset.UTC));
            ps.executeUpdate();
        }
        long t = tick();
        long appends = cursorRows();

        // nothing to read and no head due, but retry_due_at changes: not idle, the step appends
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 0, true));
        assertThat(cursorRows()).isEqualTo(appends + 1);
        assertThat(cursor()).isEqualTo(new Cursor(0, t, null, 0, null, 0, moved, null));

        tick(); // and once nothing but the tick would change, it is idle again
        assertThat(process()).isEqualTo(new ShardResult(IDLE, 0, true));
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
        }, new OutboxMetrics(null), null);
        try {
            send(msg("t", "a", "a#1"));
            tick();
            for (int i = 1; i <= 4; i++) { // four calls of the sick relay hold its four threads
                assertThat(process(sick, false).status()).isEqualTo(PAUSED);
                now = cursor().pausedUntil();
            }
            for (int busy = 1; busy <= 2; busy++) { // skipped here for initialDelay, then twice that
                assertThat(process(sick, false)).isEqualTo(new ShardResult(PAUSED, 0, false));
                now = cursor().pausedUntil();
            }
            assertThat(process()).as("another relay delivers").isEqualTo(new ShardResult(PROGRESS, 1, true));
            tick();
            now = now.plus(Duration.ofSeconds(1)); // past the sick relay's skip
            assertThat(process(sick, false).status()).as("read nothing, called nothing").isEqualTo(IDLE);
            send(msg("t", "b", "b#1"));
            tick();
            assertThat(process(sick, false)).isEqualTo(new ShardResult(PAUSED, 0, false));
            now = cursor().pausedUntil();

            assertThat(process(sick, false)).as("the idle step kept the count: skipped here 4 × initialDelay")
                    .isEqualTo(new ShardResult(NO_WORK, 0, false));

            var skipped = now;
            stuck.countDown(); // the threads come back; the next call resets the count
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            ShardResult result;
            do {
                now = now.plus(Duration.ofSeconds(1)); // past any pause or skip
                result = process(sick, false);
            } while (result.status() != PROGRESS && System.nanoTime() < deadline);
            assertThat(result).isEqualTo(new ShardResult(PROGRESS, 1, true));
            now = skipped; // the wall clock steps back into a skip of before the reset
            send(msg("t", "c", "c#1"));
            tick();

            assertThat(process(sick, false)).as("that skip went with the count")
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
        }, new OutboxMetrics(null), null);
        try {
            send(msg("t", "a", "a#1"), msg("t", "b", "b#1"));
            tick();
            for (int i = 1; i <= 4; i++) { // four calls of the sick relay hold its four threads
                assertThat(process(sick, false).status()).isEqualTo(PAUSED);
                now = cursor().pausedUntil();
            }
            for (int busy = 1; busy <= 6; busy++) { // its skip grows to initialDelay * 32; the shard's pause does not
                assertThat(process(sick, false)).isEqualTo(new ShardResult(PAUSED, 0, false));
                var paused = cursor().pausedUntil();
                assertThat(paused).as("busy step %d", busy).isAfterOrEqualTo(now.plus(INITIAL))
                        .isBefore(now.plus(INITIAL).plusMillis(500));
                now = busy < 6 ? now.plus(Duration.ofSeconds(30)) : paused; // past the sick relay's skip, but last
            }
            assertThat(process(sick, false)).as("skipped by the sick relay")
                    .isEqualTo(new ShardResult(NO_WORK, 0, false));

            assertThat(process()).as("the healthy relay delivers").isEqualTo(new ShardResult(PROGRESS, 2, true));
            send(msg("t", "c", "c#1"));
            tick();
            assertThat(process(sick, false).status()).isEqualTo(NO_WORK);
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

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

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
        bootstrap();
        var later = now.plus(Duration.ofHours(1)); // k#1 is due long after this test
        script = answering(Map.of("k#1", Outcome.requeue(later)));
        send(msg("t", "k", "k#1"));
        tick();
        process();
        // k#1 is parked again on every call and the rest is never attempted, e.g. the call is cut right after it
        script = answering(Map.of("k#1", Outcome.requeue(later), "k#2", Outcome.notAttempted(),
                "j#1", Outcome.notAttempted()));
        send(msg("t", "k", "k#2"), msg("t", "j", "j#1"));
        tick();

        // nothing attempted but a requeue: two keys fail, the call pauses and its requeue is not applied
        for (int failures = 1; failures <= config.headFailuresBeforeDivert(); failures++) {
            assertThat(process().status()).isEqualTo(PAUSED);
            assertThat(cursor().failures()).isEqualTo(failures);
            assertThat(parkedRows()).containsExactly("k#1:1:" + later);
            now = cursor().pausedUntil();
        }
        // the probe reads k#2 alone, merged with k#1: a single failing key, diverted
        calls.clear();
        assertThat(process().status()).isEqualTo(PAUSED);
        assertThat(calls).singleElement().satisfies(call -> assertThat(payloads(call)).containsExactly("k#1", "k#2"));
        assertThat(retryRows()).containsExactly("k#2:0:" + now.plus(INITIAL));
        assertThat(parkedRows()).containsExactly("k#1:2:" + later);

        // k blocked in the retry table hands no record, so k#1 is no longer merged, and the shard goes on with j#1
        script = answering(Map.of("k#2", Outcome.notAttempted()));
        now = cursor().pausedUntil();
        calls.clear();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, false));
        assertThat(calls).extracting(call -> payloads(call)).containsExactly(List.of("k#2"), List.of("j#1"));
        assertThat(delivered()).containsExactly("j#1");
        assertThat(parkedRows()).containsExactly("k#1:2:" + later);
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
        send(msg("t", "b", "b#1"));
        tick();
        statements.clear();

        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
        send(msg("t", "c", "c#1"));
        tick();
        assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));

        assertThat(statements).isNotEmpty().noneMatch(sql -> sql.contains(names.parked()));
        assertThat(delivered()).containsExactly("a#1", "b#1", "c#1");
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
            assertThat(process()).isEqualTo(new ShardResult(PROGRESS, 1, true));
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
                    new OutboxMetrics(null), null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith(keys.get(i) + " ");
        }
    }

    private void bootstrap() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
        ticks = new Ticks(names, config.shards());
        cursors = new Cursors(names);
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
        processor = new ShardProcessor(names, config, scripted, new OutboxMetrics(meters), tracer);
    }

    /** One worker {@code tryProcess} of shard 0 at the test clock in its own transaction, rolled back on failure. */
    private ShardResult process() throws Exception {
        return process(false);
    }

    private ShardResult process(boolean draining) throws Exception {
        return process(processor, draining);
    }

    private ShardResult process(ShardProcessor processor, boolean draining) throws Exception {
        long latest;
        try (var c = TestDb.connect()) {
            latest = ticks.latest(c).id();
        }
        try (var c = recording(TestDb.connect())) {
            ShardProcessor.prepareTransaction(c, config.batchTimeout());
            try {
                var result = processor.tryProcess(c, 0, latest, now, draining);
                c.commit();
                return result;
            } catch (Throwable t) {
                c.rollback();
                throw t;
            }
        }
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

    /** Steps until a step finds nothing to do: {@code NO_WORK}, or {@code IDLE} (nothing up to the latest tick). */
    private List<ShardResult> processUntilNoWork() throws Exception {
        var results = new ArrayList<ShardResult>();
        for (int i = 0; i < 1000; i++) {
            var result = process();
            if (result.status() == NO_WORK || result.status() == IDLE) {
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
            return cursors.latest(c, 0);
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
