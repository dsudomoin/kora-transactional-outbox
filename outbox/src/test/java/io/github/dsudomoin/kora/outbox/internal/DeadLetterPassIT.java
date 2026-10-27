package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.DeadLetterDecision.discard;
import static io.github.dsudomoin.kora.outbox.DeadLetterDecision.keep;
import static io.github.dsudomoin.kora.outbox.DeadLetterDecision.redrive;
import static io.github.dsudomoin.kora.outbox.DeadLetterDecision.redriveAt;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.DeadLetterDecision;
import io.github.dsudomoin.kora.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxRelay;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.kora.outbox.testing.TestSql;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

class DeadLetterPassIT {

    private final JdbcDatabase db = TestDb.newDatabase(8);
    private final TestOutboxConfig config = new TestOutboxConfig().relayEnabled(false);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final List<DeadLetterPass> passes = new ArrayList<>();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final AtomicLong ids = new AtomicLong();
    private OutboxSchema schema;
    private TableNames names;

    @BeforeEach
    void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
    }

    @AfterEach
    void release() {
        passes.forEach(DeadLetterPass::close);
        relays.forEach(OutboxRelay::release);
        pool.shutdownNow();
        db.release();
    }

    @Test
    void keepLeavesTheLetterForHumans() throws Exception {
        var headers = List.of(new OutboxHeader("h", bytes("v")), new OutboxHeader("n", null));
        long id = letter("t", "k", "p", headers);
        var handed = new CopyOnWriteArrayList<DeadLetter>();
        var pass = pass(letters -> {
            handed.addAll(letters);
            return all(letters, keep());
        });

        assertThat(pass.runOnce()).isZero();
        assertThat(pass.runOnce()).isZero();

        assertThat(handed).as("called once").hasSize(1);
        var letter = handed.get(0);
        assertThat(letter.id()).isEqualTo(id);
        assertThat(letter.messageId()).isEqualTo(messageId(id));
        assertThat(letter.topic()).isEqualTo("t");
        assertThat(letter.key()).isEqualTo(bytes("k"));
        assertThat(letter.payload()).isEqualTo(bytes("p"));
        assertThat(letter.headers()).isEqualTo(headers);
        assertThat(letter.attempts()).isEqualTo(3);
        assertThat(letter.error()).isEqualTo("boom");
        assertThat(letter.status()).isEqualTo(DeadLetter.Status.PENDING);
        assertThat(letter.redrives()).isZero();
        assertThat(letter.handlerAttempts()).isZero();
        assertThat(letter.dueAt()).isNotNull();
        assertThat(status(id)).isEqualTo("kept");
        assertThat(dueAt(id)).isNull();
        assertThat(decisions("keep")).isEqualTo(1);
    }

    @Test
    void discardDeletesIt() throws Exception {
        long id = letter("t", "k", "p", List.of());

        assertThat(pass(letters -> all(letters, discard())).runOnce()).isZero();

        assertThat(status(id)).isNull();
        assertThat(decisions("discard")).isEqualTo(1);
    }

    @Test
    void redriveSendsItAgainWithTheSameMessageId() throws Exception {
        var headers = List.of(new OutboxHeader("h", bytes("v")));
        long id = letter("t", "k", "p", headers);
        var messageId = messageId(id);

        assertThat(pass(letters -> all(letters, redrive())).runOnce()).isEqualTo(1);

        assertThat(status(id)).as("the dead row is gone").isNull();
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery(
                        "select shard, message_id, topic, key, payload, headers from " + names.base())) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(Hashing.shard(Hashing.keyHash("t", bytes("k")), config.shards()));
            assertThat(rs.getObject(2, UUID.class)).isEqualTo(messageId);
            assertThat(rs.getString(3)).isEqualTo("t");
            assertThat(rs.getBytes(4)).isEqualTo(bytes("k"));
            assertThat(rs.getBytes(5)).isEqualTo(bytes("p"));
            assertThat(rs.getBytes(6)).isEqualTo(HeaderCodec.encode(headers));
            assertThat(rs.next()).isFalse();
            assertThat(queryLong(c, "select redrives from " + names.redriven() + " where message_id = '"
                    + messageId + "'")).isEqualTo(1);
        }
        var handler = new RecordingHandler();
        assertThat(relay(handler).drain()).isEqualTo(1);
        assertThat(handler.delivered()).singleElement().satisfies(r -> {
            assertThat(r.messageId()).isEqualTo(messageId);
            assertThat(r.headers()).isEqualTo(headers);
            assertThat(r.attempts()).isZero();
        });
        assertThat(decisions("redrive")).isEqualTo(1);
    }

    @Test
    void redriveAtSchedulesIt() throws Exception {
        long soon = letter("t", "a", "soon", List.of());
        long later = letter("t", "b", "later", List.of());
        long never = letter("t", "c", "never", List.of());
        var at = Instant.now().plusSeconds(3).truncatedTo(ChronoUnit.MICROS);
        var pass = pass(letters -> letters.stream()
                .map(l -> redriveAt(l.id() == soon ? at
                        : l.id() == later ? Instant.now().plus(30, ChronoUnit.DAYS) : Instant.MAX))
                .toList());

        assertThat(pass.runOnce()).isZero();
        assertThat(pass.runOnce()).as("not redriven before it is due").isZero();

        assertThat(status(soon)).isEqualTo("scheduled");
        assertThat(dueAt(soon)).isEqualTo(at);
        assertThat(status(later)).isEqualTo("scheduled");
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select max(abs(extract(epoch from due_at - (now() + interval '7 days'))))"
                    + "::bigint from " + names.dead() + " where id in (" + later + ", " + never + ")"))
                    .as("clamped to now + 7 days").isLessThan(60);
            assertThat(queryLong(c, "select count(*) from " + names.base())).isZero();
        }
        assertThat(pass.untilNextDue()).as("by the database clock")
                .isPositive().isLessThanOrEqualTo(Duration.ofSeconds(3));

        await(Duration.ofSeconds(10), () -> pass.runOnce() == 1);

        assertThat(status(soon)).isNull();
        assertThat(status(later)).isEqualTo("scheduled");
        assertThat(status(never)).isEqualTo("scheduled");
        var handler = new RecordingHandler();
        relay(handler).drain();
        assertThat(handler.delivered()).extracting(r -> new String(r.payload(), UTF_8)).containsExactly("soon");
        assertThat(decisions("redrive_at")).isEqualTo(3);
    }

    @Test
    void aThrowingHookLeavesLettersPendingWithBackoff() throws Exception {
        long a = letter("t", "a", "p", List.of());
        long b = letter("t", "b", "p", List.of());
        var pass = pass(letters -> {
            throw new IllegalStateException("alerting is down");
        });

        for (int attempt = 1; attempt <= 2; attempt++) {
            var before = dbNow();
            assertThat(pass.runOnce()).isZero();
            var after = dbNow();

            var backoff = Duration.ofMillis(50L << (attempt - 1));
            for (long id : List.of(a, b)) {
                assertThat(status(id)).isEqualTo("pending");
                assertThat(handlerAttempts(id)).isEqualTo(attempt);
                assertThat(lastHandlerError(id)).contains("alerting is down");
                assertThat(dueAt(id)).isBetween(before.plus(backoff), after.plus(backoff));
            }
            execute("update " + names.dead() + " set due_at = now()");
        }
        assertThat(registry.get("outbox.dead.hook.failures").counter().count()).isEqualTo(2);
    }

    @Test
    void aMalformedAnswerIsAFailure() throws Exception {
        long a = letter("t", "a", "p", List.of());
        long b = letter("t", "b", "p", List.of());
        List<OutboxDeadLetterHandler> malformed = List.of(
                letters -> null,
                letters -> List.of(discard()),
                letters -> Arrays.asList(discard(), null));

        int attempts = 0;
        for (var hook : malformed) {
            pass(hook).runOnce();
            attempts++;

            for (long id : List.of(a, b)) {
                assertThat(status(id)).isEqualTo("pending");
                assertThat(handlerAttempts(id)).isEqualTo(attempts);
                assertThat(lastHandlerError(id)).contains("for 2 letters");
            }
            execute("update " + names.dead() + " set due_at = now()");
        }
    }

    @Test
    void aHangingHookIsCutAtTheTimeout() throws Exception {
        config.deadLettersTimeout(Duration.ofMillis(500));
        long id = letter("t", "k", "p", List.of());
        var pass = pass(letters -> {
            Thread.sleep(10_000);
            return all(letters, discard());
        });

        long started = System.nanoTime();
        pass.runOnce();

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
        assertThat(status(id)).isEqualTo("pending");
        assertThat(handlerAttempts(id)).isEqualTo(1);
        assertThat(lastHandlerError(id)).contains("timeout");
    }

    @Test
    void aCrashBetweenHookAndCommitCallsTheHookAgain() throws Exception {
        long id = letter("t", "k", "p", List.of());
        var calls = new CopyOnWriteArrayList<List<Long>>();
        var terminated = new AtomicLong();
        var pass = pass(letters -> {
            calls.add(letters.stream().map(DeadLetter::id).toList());
            if (calls.size() == 1) { // the pass's backend dies while it waits for this answer
                try (var c = TestDb.connect()) {
                    terminated.set(queryLong(c, "select count(*) filter (where pg_terminate_backend(pid, 5000))"
                            + " from pg_stat_activity where state = 'idle in transaction'"
                            + " and query like '%" + names.dead() + "%'"));
                }
            }
            return all(letters, discard());
        });

        assertThatThrownBy(pass::runOnce).isInstanceOf(RuntimeSqlException.class);
        assertThat(terminated).hasValue(1);
        assertThat(status(id)).isEqualTo("pending");
        assertThat(handlerAttempts(id)).isZero();

        pass.runOnce();

        assertThat(calls).containsExactly(List.of(id), List.of(id));
        assertThat(status(id)).isNull();
    }

    @Test
    void aRedriveOfAHeldKeyWaitsScheduledWithoutAnotherCall() throws Exception {
        long id = letter("t", "k", "dead", List.of());
        var messageId = messageId(id);
        var commit = new CountDownLatch(1);
        var sender = openSend("k", "live", commit);
        var calls = new AtomicInteger();
        var pass = pass(letters -> {
            calls.incrementAndGet();
            return all(letters, redrive());
        });

        long started = System.nanoTime();
        assertThat(pass.runOnce()).isZero();

        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("no longer than a writer waits for the swap")
                .isLessThan(config.swapLockTimeout().plusSeconds(1));
        assertThat(status(id)).isEqualTo("scheduled");
        assertThat(handlerAttempts(id)).as("not the hook's failure").isZero();

        commit.countDown();
        var live = sender.get(10, SECONDS);
        await(Duration.ofSeconds(5), () -> pass.runOnce() == 1);

        assertThat(calls).as("decided once").hasValue(1);
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select message_id from " + names.base() + " order by id")) {
            var order = new ArrayList<UUID>();
            while (rs.next()) {
                order.add(rs.getObject(1, UUID.class));
            }
            assertThat(order).as("behind the send").containsExactly(live, messageId);
        }
        var handler = new RecordingHandler();
        relay(handler).drain();
        assertThat(handler.delivered()).extracting(r -> new String(r.payload(), UTF_8))
                .containsExactly("live", "dead");
    }

    @Test
    void aHeldKeyDoesNotHoldBackOtherRedrives() throws Exception {
        long held = letter("t", "k", "held", List.of());
        long free = letter("t", "j", "free", List.of());
        execute("update " + names.dead() + " set status = 'scheduled'");
        var commit = new CountDownLatch(1);
        var sender = openSend("k", "live", commit);
        var pass = pass(null);

        assertThat(pass.runOnce()).as("the other key, in the same pass").isEqualTo(1);
        assertThat(status(free)).isNull();
        assertThat(status(held)).isEqualTo("scheduled");

        commit.countDown();
        sender.get(10, SECONDS);
        await(Duration.ofSeconds(5), () -> pass.runOnce() == 1);
        assertThat(status(held)).isNull();
    }

    @Test
    void aLockTimeoutDefersTheBatchToTheNextPass() throws Exception {
        long id = letter("t", "k", "p", List.of());
        execute("update " + names.dead() + " set status = 'scheduled'");
        var pass = pass(null);

        try (var locker = TestDb.connect()) {
            locker.setAutoCommit(false);
            TestSql.execute(locker, "lock table " + names.base() + " in access exclusive mode");
            long started = System.nanoTime();
            assertThat(pass.runOnce()).isZero();
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(config.swapLockTimeout().plusSeconds(1));
            assertThat(status(id)).isEqualTo("scheduled");
            locker.rollback();
        }

        await(Duration.ofSeconds(5), () -> pass.runOnce() == 1);
        assertThat(status(id)).isNull();
    }

    @Test
    void oneRunAtATimePerPass() throws Exception {
        // serialized, each call has 2 s to spare; queued behind the other, the second would need 5 s
        config.deadLettersBatchSize(1).deadLettersTimeout(Duration.ofMillis(4500));
        long a = letter("t", "a", "p", List.of());
        long b = letter("t", "b", "p", List.of());
        var pass = pass(letters -> {
            Thread.sleep(2500);
            return all(letters, keep());
        });
        var start = new CountDownLatch(1);
        var runs = new ArrayList<Future<Integer>>();
        for (int i = 0; i < 2; i++) {
            runs.add(pool.submit(() -> {
                start.await();
                return pass.runOnce();
            }));
        }
        start.countDown();
        for (var run : runs) {
            run.get(20, SECONDS);
        }

        assertThat(registry.find("outbox.dead.hook.failures").counter())
                .as("no call timed out waiting for the other run's").isNull();
        assertThat(status(a)).isEqualTo("kept");
        assertThat(status(b)).isEqualTo("kept");
    }

    @Test
    void aFailedBatchNeverOverwritesADecisionMadeSinceItsRollback() throws Exception {
        long id = letter("t", "k", "p", List.of());
        var later = Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MICROS);
        var other = pass(letters -> all(letters, redriveAt(later))); // another instance's pass
        var failing = new DeadLetterPass(commitFailsOnce(other::runOnce), names, config,
                letters -> all(letters, keep()), new OutboxMetrics(registry));
        passes.add(failing);

        assertThatThrownBy(failing::runOnce).isInstanceOf(RuntimeSqlException.class)
                .hasMessageContaining("injected");

        assertThat(status(id)).isEqualTo("scheduled");
        assertThat(dueAt(id)).as("the other pass's decision").isEqualTo(later);
        assertThat(handlerAttempts(id)).isZero();
    }

    @Test
    void aBatchThatDeferredAHeldKeyEndsTheRedrivePhase() throws Exception {
        // a deferred letter is due again at once: a phase that looped on full batches would claim it until the send
        // ends, holding the run lock drain() waits for
        config.deadLettersBatchSize(1).retry(Duration.ofMillis(1), Duration.ofMillis(500), 0);
        letter("t", "k", "first", List.of());
        letter("t", "k", "second", List.of());
        execute("update " + names.dead() + " set status = 'scheduled'");
        var commit = new CountDownLatch(1);
        var sender = openSend("k", "live", commit);
        var pass = pass(null);
        try {
            assertThat(pool.submit(pass::runOnce).get(5, SECONDS)).isZero();
        } finally {
            commit.countDown();
        }
        sender.get(10, SECONDS);

        var redriven = new AtomicInteger();
        await(Duration.ofSeconds(5), () -> redriven.addAndGet(pass.runOnce()) == 2);
    }

    @Test
    void aRedriveHoldsAKeyLockNoLongerThanSwapLockTimeoutWhileItWaits() throws Exception {
        config.swapLockTimeout(Duration.ofSeconds(1));
        letter("t", "k", "held", List.of());
        letter("t", "j", "free", List.of());
        execute("update " + names.dead() + " set status = 'scheduled'");
        var commit = new CountDownLatch(1);
        var sender = openSend("k", "live", commit);
        var pass = pass(letters -> all(letters, keep())); // without a hook, the update of pending letters would wait
        long heldFor;
        try (var locker = TestDb.connect()) {
            locker.setAutoCommit(false);
            // the claim passes it; the deferral of the held key's letter waits for it, holding the lock of key j
            TestSql.execute(locker, "lock table " + names.dead() + " in share mode");
            var run = pool.submit(pass::runOnce);
            await(Duration.ofSeconds(5), () -> keyLockGranted("j"));
            long taken = System.nanoTime();
            await(Duration.ofSeconds(10), () -> !keyLockGranted("j"));
            heldFor = System.nanoTime() - taken;
            locker.rollback();
            assertThat(run.get(10, SECONDS)).isZero();
        } finally {
            commit.countDown();
        }
        sender.get(10, SECONDS);

        // writers of key j waited this long; the step's own lock_timeout is 5 s
        assertThat(Duration.ofNanos(heldFor)).isLessThan(Duration.ofSeconds(3));
    }

    @Test
    void closeInterruptsTheHookWithoutCostingAnAttempt() throws Exception {
        long id = letter("t", "k", "p", List.of());
        var entered = new CountDownLatch(1);
        var pass = pass(letters -> {
            entered.countDown();
            Thread.sleep(60_000);
            return all(letters, keep());
        });
        var run = pool.submit(pass::runOnce);
        assertThat(entered.await(10, SECONDS)).isTrue();

        pass.close();

        assertThatThrownBy(() -> run.get(10, SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(status(id)).isEqualTo("pending");
        assertThat(handlerAttempts(id)).isZero();
        assertThat(registry.find("outbox.dead.hook.failures").counter()).isNull();
    }

    @Test
    void aRunQueuedBehindAnotherDoesNothingOnceClosed() throws Exception {
        config.deadLettersBatchSize(1);
        letter("t", "a", "p", List.of());
        long b = letter("t", "b", "p", List.of());
        var bDue = dueAt(b);
        var entered = new CountDownLatch(1);
        var pass = pass(letters -> {
            entered.countDown();
            Thread.sleep(60_000);
            return all(letters, keep());
        });
        var first = pool.submit(pass::runOnce); // claims a, its hook call hangs
        assertThat(entered.await(10, SECONDS)).isTrue();
        var queued = new CompletableFuture<Integer>();
        var interrupted = new CompletableFuture<Throwable>();
        var queuedThread = new Thread(() -> queued.complete(pass.runOnce()));
        var interruptedThread = new Thread(() -> {
            try {
                pass.runOnce();
            } catch (Throwable t) {
                interrupted.complete(t);
            }
        });
        queuedThread.start();
        interruptedThread.start();
        awaitWaiting(queuedThread);
        awaitWaiting(interruptedThread);

        interruptedThread.interrupt();
        assertThat(interrupted.get(5, SECONDS)).as("its wait for the run lock is interruptible")
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("interrupted");
        pass.close();

        assertThatThrownBy(() -> first.get(10, SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(queued.get(10, SECONDS)).as("started after close(): no claim").isZero();
        assertThat(dueAt(b)).as("b neither claimed nor deferred").isEqualTo(bDue);
        assertThat(handlerAttempts(b)).isZero();
    }

    @Test
    void aLockTimeoutWritingTheDecisionsFailsTheRun() throws Exception {
        long id = letter("t", "k", "p", List.of());
        var calls = new AtomicInteger();
        try (var locker = TestDb.connect()) {
            locker.setAutoCommit(false);
            var pass = pass(letters -> { // answers, then the write of its decision waits for a table lock
                calls.incrementAndGet();
                TestSql.execute(locker, "lock table " + names.dead() + " in share mode");
                pool.submit(() -> {
                    Thread.sleep(6000); // past the 5 s lock_timeout of the hand-over
                    locker.rollback();
                    return null;
                });
                return all(letters, keep());
            });

            // not swallowed like a redrive's: the hook answered, so the caller backs off before it is called again
            assertThatThrownBy(pass::runOnce).isInstanceOf(RuntimeSqlException.class)
                    .cause().satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("55P03"));
        }

        assertThat(calls).hasValue(1);
        assertThat(status(id)).isEqualTo("pending");
        assertThat(handlerAttempts(id)).as("no failure of the hook").isZero();
        assertThat(registry.find("outbox.dead.hook.failures").counter()).isNull();
    }

    @Test
    void hookFailuresAndLockTimeoutsWarnApart() throws Exception {
        letter("t", "a", "p", List.of());
        var calls = new AtomicInteger();
        var pass = pass(letters -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("alerting is down");
            }
            return all(letters, keep());
        });
        try (var err = new CapturedStderr()) {
            pass.runOnce();
            long scheduled = letter("t", "b", "p", List.of());
            execute("update " + names.dead() + " set status = 'scheduled' where id = " + scheduled);
            try (var locker = TestDb.connect()) {
                locker.setAutoCommit(false);
                TestSql.execute(locker, "lock table " + names.base() + " in access exclusive mode");
                pass.runOnce();
                locker.rollback();
            }

            assertThat(err.text()).contains("dead-letter hook failed").contains("timed out waiting for a lock");
        }
    }

    @Test
    void withoutAHookPendingLettersBecomeKept() throws Exception {
        long pending = letter("t", "a", "p", List.of());
        long scheduled = letter("t", "b", "due", List.of());
        execute("update " + names.dead() + " set status = 'scheduled' where id = " + scheduled);

        var none = pass(null);
        assertThat(none.active()).isFalse();
        assertThat(none.runOnce()).as("a due scheduled letter is redriven all the same").isEqualTo(1);
        assertThat(status(pending)).isEqualTo("kept");
        assertThat(dueAt(pending)).isNull();
        assertThat(status(scheduled)).isNull();

        long another = letter("t", "c", "p", List.of());
        var explicit = pass(OutboxDeadLetterHandler.NONE);
        assertThat(explicit.active()).isFalse();
        assertThat(explicit.runOnce()).isZero();
        assertThat(status(another)).isEqualTo("kept");
    }

    @Test
    void anInactiveHookIsNeverCalledAndReadOnce() throws Exception {
        long pending = letter("t", "a", "p", List.of());
        var calls = new AtomicInteger();
        var reads = new AtomicInteger();
        var inactive = new OutboxDeadLetterHandler() {
            @Override
            public List<DeadLetterDecision> handle(List<DeadLetter> letters) {
                calls.incrementAndGet();
                return all(letters, keep());
            }

            @Override
            public boolean active() {
                reads.incrementAndGet();
                return false;
            }
        };

        var pass = pass(inactive);
        assertThat(pass.active()).isFalse();
        assertThat(pass.runOnce()).isZero();
        assertThat(pass.active()).isFalse();

        assertThat(status(pending)).isEqualTo("kept");
        assertThat(calls).hasValue(0);
        assertThat(reads).as("read once, when the pass is built").hasValue(1);
    }

    @Test
    void twoPassesNeverHandTheSameLetterAtOnce() throws Exception {
        config.deadLettersBatchSize(10);
        for (int i = 0; i < 200; i++) {
            letter("t", "k" + i, "p", List.of());
        }
        var handed = new ConcurrentLinkedQueue<Long>();
        OutboxDeadLetterHandler hook = letters -> {
            letters.forEach(l -> handed.add(l.id()));
            Thread.sleep(5);
            return all(letters, keep());
        };
        var first = pass(hook);
        var second = pass(hook);
        var start = new CountDownLatch(1);

        var a = pool.submit(() -> {
            start.await();
            return first.runOnce();
        });
        var b = pool.submit(() -> {
            start.await();
            return second.runOnce();
        });
        start.countDown();
        a.get(30, SECONDS);
        b.get(30, SECONDS);

        assertThat(handed).hasSize(200).doesNotHaveDuplicates();
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + names.dead() + " where status = 'kept'")).isEqualTo(200);
        }
    }

    /** A transaction that has sent {@code payload} with {@code key} on topic t and stays open until {@code commit}. */
    private Future<UUID> openSend(String key, String payload, CountDownLatch commit) throws Exception {
        var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, new OutboxMetrics(null));
        var sent = new CountDownLatch(1);
        var sender = pool.submit(() -> db.inTx((SqlFunction0<UUID>) () -> {
            var messageId = outbox.send(OutboxMessage.of("t", key, bytes(payload)));
            sent.countDown();
            awaitLatch(commit);
            return messageId;
        }));
        assertThat(sent.await(10, SECONDS)).as("the sender holds the key lock").isTrue();
        return sender;
    }

    /**
     * {@link #db} whose first commit fails; right after the rollback of that transaction, {@code afterRollback} runs,
     * as another instance's pass would, before the failed one goes on.
     */
    private JdbcConnectionFactory commitFailsOnce(Runnable afterRollback) {
        var failed = new AtomicBoolean();
        var ran = new AtomicBoolean();
        return new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                return db.withConnection(callback);
            }

            @Override
            public Connection currentConnection() {
                return db.currentConnection();
            }

            @Override
            public Connection newConnection() {
                var c = db.newConnection();
                return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                            if (method.getName().equals("commit") && failed.compareAndSet(false, true)) {
                                throw new SQLException("injected commit failure");
                            }
                            Object result;
                            try {
                                result = method.invoke(c, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                            if (method.getName().equals("rollback") && failed.get() && ran.compareAndSet(false, true)) {
                                afterRollback.run();
                            }
                            return result;
                        });
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return db.telemetry();
            }
        };
    }

    /** Until {@code thread} parks, waiting for the pass's run lock. */
    private static void awaitWaiting(Thread thread) throws Exception {
        await(Duration.ofSeconds(5), () -> thread.getState() == Thread.State.WAITING);
    }

    /** Some transaction holds the key lock of {@code key} on topic t. */
    private boolean keyLockGranted(String key) throws SQLException {
        try (var c = TestDb.connect(); var ps = c.prepareStatement("select count(*) from pg_locks"
                + " where locktype = 'advisory' and granted and classid::bigint = ? and objid::bigint = ?"
                + " and objsubid = 2")) {
            ps.setLong(1, Integer.toUnsignedLong(names.keyNamespace()));
            ps.setLong(2, Integer.toUnsignedLong(Hashing.keyHash("t", bytes(key))));
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1) > 0;
            }
        }
    }

    private DeadLetterPass pass(@Nullable OutboxDeadLetterHandler hook) {
        var pass = new DeadLetterPass(db, names, config, hook, new OutboxMetrics(registry));
        passes.add(pass);
        return pass;
    }

    private OutboxRelay relay(RecordingHandler handler) {
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null,
                new Maintenance(db, names, config, new OutboxMetrics(null)), pass(null));
        relays.add(relay);
        return relay;
    }

    /** A pending letter as the relay writes it, due now. */
    private long letter(String topic, String key, String payload, List<OutboxHeader> headers) throws SQLException {
        long id = ids.incrementAndGet();
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + names.dead()
                + " (id, shard, message_id, topic, key, payload, headers, created_at, attempts, error, status, due_at)"
                + " values (?, 0, gen_random_uuid(), ?, ?, ?, ?, now(), 3, 'boom', 'pending', now())")) {
            ps.setLong(1, id);
            ps.setString(2, topic);
            ps.setBytes(3, bytes(key));
            ps.setBytes(4, bytes(payload));
            ps.setBytes(5, HeaderCodec.encode(headers));
            ps.executeUpdate();
        }
        return id;
    }

    private static List<DeadLetterDecision> all(List<DeadLetter> letters, DeadLetterDecision decision) {
        return letters.stream().map(l -> decision).toList();
    }

    private double decisions(String decision) {
        return registry.get("outbox.dead.decisions").tag("decision", decision).counter().count();
    }

    @Nullable
    private String status(long id) throws SQLException {
        return column(id, "status");
    }

    @Nullable
    private Instant dueAt(long id) throws SQLException {
        try (var c = TestDb.connect();
                var ps = c.prepareStatement("select due_at from " + names.dead() + " where id = ?")) {
            ps.setLong(1, id);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                var at = rs.getObject(1, OffsetDateTime.class);
                return at == null ? null : at.toInstant();
            }
        }
    }

    private int handlerAttempts(long id) throws SQLException {
        return Integer.parseInt(column(id, "handler_attempts"));
    }

    @Nullable
    private String lastHandlerError(long id) throws SQLException {
        return column(id, "last_handler_error");
    }

    private UUID messageId(long id) throws SQLException {
        return UUID.fromString(column(id, "message_id"));
    }

    /** The column as text, null when the letter is gone. */
    @Nullable
    private String column(long id, String column) throws SQLException {
        try (var c = TestDb.connect();
                var ps = c.prepareStatement("select " + column + "::text from " + names.dead() + " where id = ?")) {
            ps.setLong(1, id);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private Instant dbNow() throws SQLException {
        try (var c = TestDb.connect(); var st = c.createStatement(); var rs = st.executeQuery("select now()")) {
            rs.next();
            return rs.getObject(1, OffsetDateTime.class).toInstant();
        }
    }

    private void execute(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, sql);
        }
    }

    private interface Condition {
        boolean met() throws Exception;
    }

    private static void await(Duration timeout, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.met()) {
            assertThat(System.nanoTime()).as("condition met within " + timeout).isLessThan(deadline);
            Thread.sleep(20);
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
