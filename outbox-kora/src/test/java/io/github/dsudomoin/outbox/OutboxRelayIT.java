package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.outbox.internal.Cursors;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.internal.ShardProcessor;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.internal.Ticks;
import io.github.dsudomoin.outbox.kora.KoraOutboxTelemetry;
import io.github.dsudomoin.outbox.kora.KoraOutboxTransactions;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;

class OutboxRelayIT {

    /** The owner's priorities: the event types of an account that a running process of that account blocks. */
    private static final Map<String, Set<String>> BLOCKS = Map.of(
            "WITHDRAW", Set.of("FEE_RELEASE", "SYNCHRONIZE"),
            "FEE_RELEASE", Set.of("FEE_RELEASE", "SYNCHRONIZE"),
            "SYNCHRONIZE", Set.of("SYNCHRONIZE"));

    private final TestDatabase db = TestDb.newDatabase(30);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private OutboxSchema schema;

    @AfterEach
    void release() {
        for (var relay : relays) {
            relay.close();
        }
        db.release();
    }

    @Test
    void deliversWithinMillisecondsAfterCommit() throws Exception {
        config.idlePollInterval(Duration.ofSeconds(30)); // only the wake-up can deliver in time
        bootstrap();
        var handler = new RecordingHandler();
        var relay = start(handler);
        var outbox = outbox(relay::wake);
        awaitParked();

        long committed = System.nanoTime();
        var ids = send(outbox, msg("t", "k", "p"));
        await(Duration.ofMillis(500), () -> handler.delivered().size() == 1);

        assertThat(Duration.ofNanos(System.nanoTime() - committed)).isLessThan(Duration.ofMillis(500));
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
    }

    @Test
    void wakeUpThrottledByAnOlderTickStillDeliversBeforeTheIdlePoll() throws Exception {
        config.idlePollInterval(Duration.ofSeconds(30)).minTickInterval(Duration.ofSeconds(1));
        bootstrap();
        var handler = new RecordingHandler();
        var relay = start(handler);
        var outbox = outbox(relay::wake);
        awaitParked();
        try (var c = TestDb.connect()) { // another instance ticks just before the commit
            assertThat(new Ticks(schema.names(), config.shards()).make(c, Duration.ZERO)).isTrue();
        }

        send(outbox, msg("t", "k", "p")); // the wake-up's tick is skipped: that tick is younger than 1 s

        // a further attempt within 2 × minTickInterval covers it, not the 30 s idle poll
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == 1);
    }

    @Test
    void wakeUpLatencyStaysLowWithManyShards() throws Exception {
        config.shards(32).workers(4).idlePollInterval(Duration.ofMillis(200));
        bootstrap();
        var delivered = new AtomicInteger();
        var cursorReads = new AtomicInteger();
        var lockingWorkers = ConcurrentHashMap.<String>newKeySet();
        var slowTicks = new AtomicBoolean();
        var relay = relay(observed(sql -> {
            if (sql.contains("pg_current_snapshot")) { // a tick insert
                if (slowTicks.get()) { // as if contended
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } else if (sql.contains("generate_series")) { // Cursors.positions
                cursorReads.incrementAndGet();
            } else if (sql.contains("pg_try_advisory_xact_lock")) { // a shard step
                lockingWorkers.add(Thread.currentThread().getName());
            }
        }), new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                delivered.incrementAndGet();
            }
        });
        relay.start();
        var outbox = outbox(relay::wake);
        awaitParked();
        long ticksBefore = ticksMade();
        int readsBefore = cursorReads.get();

        for (int i = 0; i < 25; i++) {
            send(outbox, msg("t", "k" + i, "m" + i));
            int n = i + 1;
            await(Duration.ofSeconds(5), () -> delivered.get() == n);
        }
        double readsPerTick = (double) (cursorReads.get() - readsBefore) / (ticksMade() - ticksBefore);
        // another instance's commits: no wake-up; the idle poll's (slow) tick must wake the workers that polled before
        // it committed, so they spread the tick's 16 shards with new rows
        var silent = outbox(() -> {});
        slowTicks.set(true);
        int workersPerTick = 0;
        for (int i = 0; i < 5; i++) {
            awaitParked(); // the previous tick's shards are all stepped
            lockingWorkers.clear();
            int round = i;
            send(silent, IntStream.range(0, 16).mapToObj(shard -> msg("t", keyOnShard(2 * shard, "r" + round + "-"),
                    "r" + round)).toArray(OutboxMessage[]::new));
            int n = 25 + 16 * (i + 1);
            await(Duration.ofSeconds(5), () -> delivered.get() == n);
            workersPerTick += lockingWorkers.size();
        }

        // what keeps a wake-up's latency low with many shards (latency bounds themselves pass on the old relay and
        // fail on a saturated machine): a pass steps every candidate of one cursor read (not one shard per read),
        // and a tick wakes every worker (not just the one that made it)
        assertThat(readsPerTick).as("cursor reads per tick").isLessThan(config.shards() / 2.0);
        assertThat(workersPerTick / 5.0).as("workers stepping shards per idle tick").isGreaterThanOrEqualTo(3);
    }

    @Test
    void tickAttemptsUnderLoadAreBoundedByMinTickInterval() throws Exception {
        config.minTickInterval(Duration.ofMillis(50)).idlePollInterval(Duration.ofMillis(200));
        bootstrap();
        var attempts = new AtomicInteger();
        var handler = new RecordingHandler();
        var relay = relay(observed(sql -> {
            if (sql.contains("pg_current_snapshot")) { // a tick insert
                attempts.incrementAndGet();
            }
        }), handler);
        relay.start();
        var outbox = outbox(relay::wake);
        awaitParked();

        int before = attempts.get();
        long started = System.nanoTime();
        int sent = 0;
        while (System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1)) { // a wake-up every commit
            send(outbox, msg("t", "k" + sent % 10, "m" + sent));
            sent++;
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        int made = attempts.get() - before;

        assertThat(sent).isGreaterThan(100);
        assertThat(made).isBetween(1, (int) (elapsedMillis / 50) + 2);
        int total = sent;
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == total);
    }

    @Test
    void maintenanceRunsRightAfterStartOnlyWhenEnabled() throws Exception {
        bootstrap(); // maintenanceInterval is 1 h
        var runs = new AtomicInteger();
        relays.add(new OutboxRelay(db, config, schema, new RecordingHandler(), TestTelemetry.kora(),
                maintenance(observed(sql -> {
                    if (sql.contains("pg_try_advisory_lock(")) { // a maintenance run
                        runs.incrementAndGet();
                    }
                })), noHook(db)));
        relays.get(0).start();
        await(Duration.ofSeconds(5), () -> runs.get() == 1);

        config.relayEnabled(false);
        var disabled = new AtomicInteger();
        relays.add(new OutboxRelay(db, config, schema, new RecordingHandler(), TestTelemetry.kora(),
                maintenance(observed(sql -> disabled.incrementAndGet())), noHook(db)));
        relays.get(1).start();
        relays.get(1).close();
        assertThat(disabled).hasValue(0);
    }

    @Test
    void fatalErrorInMaintenanceIsLogged() throws Exception {
        bootstrap();
        var connections = new AtomicInteger();
        var failing = new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Connection currentConnection() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Connection newConnection() {
                if (connections.incrementAndGet() == 1) { // the halt check before the workers start
                    return db.newConnection();
                }
                throw new OutOfMemoryError("maintenance out of memory (test)");
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return db.telemetry();
            }
        };
        try (var err = new CapturedStderr()) {
            relays.add(new OutboxRelay(db, config, schema, new RecordingHandler(), TestTelemetry.kora(),
                    maintenance(failing), noHook(db)));
            relays.get(0).start();

            // the scheduler swallows what a run throws and silently cancels all later runs: only the log tells
            await(Duration.ofSeconds(5), () -> err.text().contains("maintenance out of memory (test)"));
            assertThat(err.text()).contains("ERROR " + OutboxRelay.class.getName());
        }
    }

    @Test
    void wakeUpsUnderHighCommitRateDoNotMultiplyCursorReads() throws Exception {
        config.minTickInterval(Duration.ofMillis(50)).idlePollInterval(Duration.ofMillis(200));
        bootstrap();
        var reads = new AtomicInteger();
        var attempts = new AtomicInteger();
        var handler = new RecordingHandler();
        var relay = relay(observed(sql -> {
            if (sql.contains("pg_current_snapshot")) { // a tick insert
                attempts.incrementAndGet();
            } else if (sql.contains("generate_series")) { // Cursors.positions
                reads.incrementAndGet();
            }
        }), handler);
        relay.start();
        var outbox = outbox(relay::wake);
        awaitParked();

        int readsBefore = reads.get();
        int attemptsBefore = attempts.get();
        long started = System.nanoTime();
        int sent = 0;
        while (System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1)) { // a wake-up every commit
            send(outbox, msg("t", "k" + sent % 10, "m" + sent));
            sent++;
        }
        int read = reads.get() - readsBefore;
        int tickAttempts = attempts.get() - attemptsBefore;

        assertThat(sent).isGreaterThan(100);
        // per tick attempt a worker passes when it is due, when it made a tick and after a pass that delivered (2-3
        // observed, 4 allowed); one pass per commit and worker before the fix (~4300 reads for ~2500 commits)
        assertThat(read).as("cursor reads for %d tick attempts, %d commits", tickAttempts, sent)
                .isLessThanOrEqualTo((tickAttempts + 2) * config.workers() * 4);
        int total = sent;
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == total);
    }

    @Test
    void otherInstanceDeliversViaIdlePoll() throws Exception {
        config.idlePollInterval(Duration.ofMillis(200)); // the default
        bootstrap();
        var handler = new RecordingHandler();
        start(handler); // another instance: nothing wakes it
        var outbox = outbox(() -> {});

        var ids = send(outbox, msg("t", "k", "p"), msg("t", null, "q"));

        await(Duration.ofSeconds(1), () -> handler.delivered().size() == 2);
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void drainDeliversEverythingWhenRelayDisabled() throws Exception {
        config.relayEnabled(false);
        bootstrap();
        var handler = new RecordingHandler();
        var relay = relay(handler);
        relay.start();
        var outbox = outbox(relay::wake);
        var ids = new ArrayList<UUID>();
        for (int tx = 0; tx < 10; tx++) {
            int t = tx;
            ids.addAll(send(outbox, IntStream.range(0, 50)
                    .mapToObj(i -> msg("t", i % 10 == 0 ? null : "k" + i % 7, t + "/" + i))
                    .toArray(OutboxMessage[]::new)));
        }

        assertThat(relay.running()).isFalse();
        assertThat(relayThreads()).isEmpty();
        assertThat(relay.drain()).isEqualTo(500);
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(relay.drain()).isZero();
    }

    @Test
    void drainFailsWhenNothingAdvances() throws Exception {
        config.relayEnabled(false).batchTimeout(Duration.ofMillis(500)).headFailuresBeforeDivert(1000);
        bootstrap();
        var handler = new RecordingHandler().failAlways(r -> true, () -> new RuntimeException("target down"));
        var relay = relay(handler);
        send(outbox(relay::wake), msg("t", "a", "a"), msg("t", "b", "b"), msg("t", "c", "c"));

        assertThatThrownBy(relay::drain)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no progress");
        assertThat(handler.delivered()).isEmpty();
    }

    @Test
    void drainFailsOnAStuckShardWhileOtherShardsMove() throws Exception {
        config.batchTimeout(Duration.ofMillis(500)).headFailuresBeforeDivert(1000);
        bootstrap();
        var stuck = keyOnShard(0, "s");
        var handler = new RecordingHandler().failAlways(r -> stuck.equals(key(r)), () -> new RuntimeException("down"));
        var relay = start(handler); // its idle ticks keep moving the cursors of the other shards
        send(outbox(relay::wake), msg("t", stuck, "stuck"));

        var drain = CompletableFuture.supplyAsync(relay::drain);

        assertThatThrownBy(() -> drain.get(20, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no progress");
    }

    @Test
    void hookReceivesTheRelaysDeadLetters() throws Exception {
        bootstrap(); // maintenanceInterval is 1 h: only the wake-up of the step that wrote the letter is in time
        var handed = new CopyOnWriteArrayList<DeadLetter>();
        var relay = relay(poisonous(), keeping(handed));
        relay.start();
        awaitParked();

        var ids = send(outbox(relay::wake), msg("t", "poison", "p"));

        await(Duration.ofSeconds(2), () -> handed.size() == 1);
        assertThat(handed.get(0).messageId()).isEqualTo(ids.get(0));
        assertThat(handed.get(0).error()).contains("bad payload");
        await(Duration.ofSeconds(2), () -> "kept".equals(deadStatus(ids.get(0))));
    }

    @Test
    void anInactiveHookIsNeverCalledAndTheRelayWritesKept() throws Exception {
        bootstrap();
        var handed = new CopyOnWriteArrayList<DeadLetter>();
        var keeping = keeping(handed);
        var relay = relay(poisonous(), new OutboxDeadLetterHandler() {
            @Override
            public List<DeadLetterDecision> handle(List<DeadLetter> letters) throws Exception {
                return keeping.handle(letters);
            }

            @Override
            public boolean active() {
                return false;
            }
        });
        relay.start();
        awaitParked();

        var ids = send(outbox(relay::wake), msg("t", "poison", "p"));

        await(Duration.ofSeconds(2), () -> {
            assertThat(handed).as("no hook call while the letter is written").isEmpty();
            return deadStatus(ids.get(0)) != null;
        });
        assertThat(deadStatus(ids.get(0))).as("written kept by the step").isEqualTo("kept");
        relay.drain(); // runs the dead-letter pass once more
        assertThat(handed).isEmpty();
    }

    @Test
    void aHangingHookDoesNotDelayDelivery() throws Exception {
        config.deadLettersTimeout(Duration.ofMinutes(1)); // the call is never cut while the test runs
        bootstrap();
        var delivered = new AtomicReference<Map<String, Long>>();
        OutboxHandler handler = record -> {
            if ("poison".equals(key(record))) { // its step wakes the dead-letter thread
                throw new OutboxPermanentException("bad payload");
            }
            delivered.get().put(payload(record), System.nanoTime());
        };
        var plain = relay(handler);
        plain.start();
        awaitParked();
        deliveryDelays(outbox(plain::wake), delivered, "warm-up");
        var baseline = p99(deliveryDelays(outbox(plain::wake), delivered, "baseline"));
        plain.close();

        var entered = new CountDownLatch(1);
        var never = new CountDownLatch(1);
        pendingLetter("dead");
        var relay = relay(handler, letters -> {
            entered.countDown();
            never.await(); // until release() interrupts it
            return List.of();
        });
        relay.start();
        assertThat(entered.await(5, TimeUnit.SECONDS)).as("the hook hangs").isTrue();
        awaitParked();
        var hanging = p99(deliveryDelays(outbox(relay::wake), delivered, "hanging"));

        assertThat(rows(schema.names().dead() + " where status = 'pending' and key = 'poison'::bytea"))
                .as("the run's own letter, waiting behind the hanging call").isEqualTo(1);
        // 20 ms on top of 2×: scheduler noise of a run of a few seconds on a shared machine
        assertThat(hanging).as("p99 with a hanging hook, %s without", baseline)
                .isLessThanOrEqualTo(baseline.multipliedBy(2).plusMillis(20));
    }

    @Test
    void drainRunsTheHook() throws Exception {
        config.relayEnabled(false);
        bootstrap();
        var handed = new CopyOnWriteArrayList<DeadLetter>();
        var relay = relay(poisonous(), keeping(handed));
        relay.start();
        var ids = send(outbox(relay::wake), msg("t", "poison", "p"), msg("t", "ok", "q"));

        assertThat(relay.drain()).isOne();

        assertThat(handed).extracting(DeadLetter::messageId).containsExactly(ids.get(0));
        assertThat(deadStatus(ids.get(0))).isEqualTo("kept");
        assertThat(deadLetterThreads()).as("no thread of its own while the relay is disabled").isEmpty();
    }

    @Test
    void theDeadLetterThreadBacksOffWhileAPassFailsAndResetsOnSuccess() throws Exception {
        config.maintenanceInterval(Duration.ofSeconds(5)); // the backoff's cap
        bootstrap();
        var dead = schema.names().dead();
        try (var c = TestDb.connect()) { // the hook's decisions cannot be written: each pass fails after its call
            execute(c, "create function " + dead + "_fail() returns trigger language plpgsql as"
                    + " $$ begin raise exception 'decisions cannot be written (test)'; end $$");
            execute(c, "create trigger fail before update on " + dead + " for each row when (new.status = 'kept')"
                    + " execute function " + dead + "_fail()");
        }
        pendingLetter("a");
        var calls = new AtomicInteger();
        var keeping = keeping(new CopyOnWriteArrayList<>());
        var relay = relay(poisonous(), letters -> {
            calls.incrementAndGet();
            return keeping.handle(letters);
        });
        relay.start();

        Thread.sleep(3000);
        // initialDelay is 50 ms: about 60 calls in 3 s without a growing backoff, 7 with one doubling from it
        assertThat(calls.get()).isBetween(2, 12);

        try (var c = TestDb.connect()) {
            execute(c, "alter table " + dead + " disable trigger fail");
        }
        await(Duration.ofSeconds(10), () -> rows(dead + " where status = 'kept'") == 1); // the next run, by 5 s
        try (var c = TestDb.connect()) {
            execute(c, "alter table " + dead + " enable trigger fail");
        }
        int before = calls.get();

        send(outbox(relay::wake), msg("t", "poison", "p")); // its step wakes the thread

        // the success reset the backoff: the failure of that call is retried after initialDelay, not 5 s
        await(Duration.ofSeconds(2), () -> calls.get() >= before + 2);
    }

    @Test
    void releaseStopsTheDeadLetterThreadWhileTheHookIgnoresInterrupts() throws Exception {
        config.deadLettersTimeout(Duration.ofMinutes(1)).shutdownTimeout(Duration.ofMillis(300));
        bootstrap();
        pendingLetter("a");
        var entered = new CountDownLatch(1);
        var done = new CountDownLatch(1); // ends the hook, which no interrupt does, before later tests run
        var relay = relay(new RecordingHandler(), letters -> {
            entered.countDown();
            while (true) {
                try {
                    if (done.await(1, TimeUnit.MINUTES)) {
                        return List.of();
                    }
                } catch (InterruptedException ignored) {
                    // a hook that will not stop
                }
            }
        });
        try {
            relay.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

            long releasing = System.nanoTime();
            relay.close();

            assertThat(Duration.ofNanos(System.nanoTime() - releasing)).isLessThan(Duration.ofSeconds(2));
            assertThat(deadLetterThreads()).isEmpty();
            try (var c = TestDb.connect()) {
                assertThat(queryString(c, "select status || '/' || handler_attempts from " + schema.names().dead()))
                        .as("no attempt spent").isEqualTo("pending/0");
            }
        } finally {
            done.countDown();
        }
    }

    @Test
    void drainFitsThePoolBudgetBesideAHangingHookAndBusyWorkers() throws Exception {
        config.workers(2).deadLettersTimeout(Duration.ofSeconds(2)); // each hanging call is cut after 2 s
        bootstrap();
        // README: relay.workers + 3 (the two maintenance runs, the dead-letter thread's run), + 1 while drain() runs
        var tight = TestDb.newDatabase(config.workers() + 3 + 1);
        var peaks = new ConcurrentHashMap<Thread, Integer>();
        var factory = tracking(tight, peaks);
        var entered = new CountDownLatch(1);
        var done = new CountDownLatch(1);
        var metrics = new OutboxMetrics(null);
        OutboxHandler slow = record -> {
            if ("poison".equals(key(record))) {
                throw new OutboxPermanentException("bad payload");
            }
            Thread.sleep(20);
        };
        var relay = new OutboxRelay(new KoraOutboxTransactions(factory), config, schema, slow,
                new KoraOutboxTelemetry(metrics, null), maintenance(db),
                new DeadLetterPass(new KoraOutboxTransactions(factory), schema.names(), config, letters -> {
                    entered.countDown();
                    done.await(); // until the call is cut
                    return List.of();
                }, metrics));
        relays.add(relay);
        // stand-ins for an outbox and an inbox maintenance run, at the same time
        try (var outboxMaintenance = tight.newConnection(); var inboxMaintenance = tight.newConnection()) {
            relay.start();
            var outbox = outbox(relay::wake);
            send(outbox, msg("t", "poison", "p"));
            assertThat(entered.await(10, TimeUnit.SECONDS)).as("the hook hangs, holding a connection").isTrue();
            send(outbox, IntStream.range(0, 100).mapToObj(i -> msg("t", "k" + i % 20, "m" + i))
                    .toArray(OutboxMessage[]::new)); // the workers are busy for about a second

            var drainer = new AtomicReference<Thread>();
            var drain = CompletableFuture.supplyAsync(() -> {
                drainer.set(Thread.currentThread());
                return relay.drain();
            });

            drain.get(30, TimeUnit.SECONDS);
            assertThat(peaks.get(drainer.get())).as("connections drain() held at once").isOne();
            assertThat(peaks.values()).as("no thread of the relay holds two").allMatch(n -> n == 1);
        } finally {
            done.countDown();
            relay.close();
            tight.release();
        }
    }

    @Test
    void drainOutlastsTheQuiesceOfATailSwap() throws Exception {
        config.relayEnabled(false).batchTimeout(Duration.ofMillis(500));
        bootstrap();
        var handler = new RecordingHandler();
        var relay = relay(handler);
        var ids = send(outbox(relay::wake), msg("t", "k", "p"));
        try (var swap = TestDb.connect()) {
            var lock = "(" + schema.names().internalNamespace() + ", " + TableNames.LOCK_SWAP + ")";
            execute(swap, "select pg_advisory_lock" + lock); // as the swap holds it: every step is NOT_LOCKED

            var drain = CompletableFuture.supplyAsync(relay::drain);
            Thread.sleep(config.batchTimeout().plusSeconds(5).toMillis()); // the longest quiesce wait
            assertThat(drain).isNotDone();
            execute(swap, "select pg_advisory_unlock" + lock);

            assertThat(drain.get(10, TimeUnit.SECONDS)).isOne();
        }
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
    }

    @Test
    void drainHandsDueParkedRowsAndDoesNotWaitForTheRest() throws Exception {
        config.relayEnabled(false).requeueDelay(Duration.ofSeconds(1));
        bootstrap();
        var relay = relay(new OutboxHandler() { // k: requeued for an hour every time; j: requeued once
            @Override
            public void handle(OutboxRecord record) {
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                return records.stream().map(r -> "k".equals(key(r))
                        ? Outcome.requeue(Instant.now().plus(Duration.ofHours(1)))
                        : r.requeues() == 0 ? Outcome.requeue() : Outcome.delivered()).toList();
            }
        });
        send(outbox(relay::wake), msg("t", "k", "k#1"), msg("t", "j", "j#1"));

        long started = System.nanoTime();
        assertThat(relay.drain()).isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(1500));
        assertThat(parked()).isEqualTo("k#1,j#1");

        Thread.sleep(1100); // j#1 is due
        started = System.nanoTime();
        assertThat(relay.drain()).isEqualTo(1);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(1500));
        assertThat(parked()).isEqualTo("k#1");
    }

    @Test
    void aShardWhoseParkedRowIsDueIsSteppedWithoutNewRows() throws Exception {
        bootstrap();
        var delivered = new CopyOnWriteArrayList<OutboxRecord>();
        var relay = start(new OutboxHandler() { // requeues each record once, then delivers it
            @Override
            public void handle(OutboxRecord record) {
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                return records.stream().map(r -> {
                    if (r.requeues() == 0) {
                        return Outcome.requeue();
                    }
                    delivered.add(r);
                    return Outcome.delivered();
                }).toList();
            }
        });
        var outbox = outbox(relay::wake);
        awaitParked();

        send(outbox, msg("t", "k", "k#1")); // parked at once, due 100 ms later; nothing else is written

        // well before the shard's idle append (5 s), which would step it as well
        await(Duration.ofSeconds(2), () -> delivered.size() == 1 && parked() == null);
        assertThat(delivered.get(0).requeues()).isEqualTo(1);
    }

    @Test
    void activeSynchronizeLetsFeeThroughAndTheSecondSynchronizeWaits() throws Exception {
        config.requeueDelay(Duration.ofSeconds(1));
        bootstrap();
        var handler = new Coordinator();
        var relay = start(handler);
        var outbox = outbox(relay::wake);
        handler.blocked = whileRunning("SYNCHRONIZE");

        send(outbox, msg("t", "acc42", "SYNCHRONIZE#2"));
        await(Duration.ofSeconds(1), () -> "SYNCHRONIZE#2".equals(parked()));
        Thread.sleep(3000);
        assertThat(handler.calls).as("handed again at each due time (about 1 s and 2 s later)")
                .filteredOn(List.of("SYNCHRONIZE#2")::equals).hasSizeGreaterThanOrEqualTo(3);
        assertThat(handler.delivered()).isEmpty();
        assertThat(parked()).isEqualTo("SYNCHRONIZE#2");
        send(outbox, msg("t", "acc42", "FEE_RELEASE"));
        await(Duration.ofSeconds(1), () -> handler.delivered().size() == 1); // FEE_RELEASE did not wait

        assertThat(handler.calls)
                .anyMatch(call -> Collections.indexOfSubList(call, List.of("SYNCHRONIZE#2", "FEE_RELEASE")) >= 0);
        assertThat(parked()).isEqualTo("SYNCHRONIZE#2");

        handler.blocked = r -> false; // the running SYNCHRONIZE ends
        await(Duration.ofSeconds(2), () -> handler.delivered().size() == 2); // its due time at the latest

        assertThat(handler.delivered()).extracting(OutboxRelayIT::payload)
                .containsExactly("FEE_RELEASE", "SYNCHRONIZE#2");
        long handed = handler.calls.stream().filter(call -> call.contains("SYNCHRONIZE#2")).count();
        assertThat(handler.delivered().get(1).requeues()).as("requeued in each call before the last")
                .isEqualTo(handed - 1);
    }

    @Test
    void sameTypeOrderIsKeptWhenTheBlockLifts() throws Exception {
        config.requeueDelay(Duration.ofMinutes(1)); // only the next record of the key brings FEE_RELEASE#1 back
        bootstrap();
        var handler = new Coordinator();
        var relay = start(handler);
        var outbox = outbox(relay::wake);
        handler.blocked = whileRunning("FEE_RELEASE");

        send(outbox, msg("t", "acc42", "FEE_RELEASE#1"));
        await(Duration.ofSeconds(1), () -> "FEE_RELEASE#1".equals(parked()));
        handler.blocked = r -> false; // the running FEE_RELEASE ends
        send(outbox, msg("t", "acc42", "FEE_RELEASE#2"));
        await(Duration.ofSeconds(1), () -> handler.delivered().size() == 2 && parked() == null);

        assertThat(handler.calls)
                .containsExactly(List.of("FEE_RELEASE#1"), List.of("FEE_RELEASE#1", "FEE_RELEASE#2"));
        assertThat(handler.delivered()).extracting(OutboxRelayIT::payload)
                .containsExactly("FEE_RELEASE#1", "FEE_RELEASE#2");
    }

    @Test
    void multipleRelaysDeliverEachMessageOnceInSteadyState() throws Exception {
        config.shards(4).workers(2);
        bootstrap();
        var handler = new RecordingHandler();
        var started = List.of(start(handler), start(handler), start(handler));
        var ids = sendConcurrently(started);

        await(Duration.ofSeconds(60), () -> handler.delivered().size() >= 5000);
        assertThat(started.get(0).drain()).isZero(); // everything is committed, nothing left

        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(handler.delivered());
    }

    @Test
    void severalInstancesRequeueAndDeliverEveryMessage() throws Exception {
        config.shards(4).workers(2);
        bootstrap();
        var handler = new Coordinator();
        handler.blocked = r -> r.requeues() == 0 && ThreadLocalRandom.current().nextBoolean();
        var started = List.of(start(handler), start(handler), start(handler));
        var ids = sendConcurrently(started);

        await(Duration.ofSeconds(60), () -> handler.delivered().size() >= 5000);
        assertThat(started.get(0).drain()).isZero(); // everything is committed, nothing left
        await(Duration.ofSeconds(5), () -> rows(schema.names().parked()) == 0 && rows(schema.names().retry()) == 0);

        var delivered = handler.delivered();
        assertThat(delivered).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(delivered).filteredOn(r -> r.requeues() > 0).hasSizeGreaterThan(1000); // about half
        assertThat(delivered).allMatch(r -> r.requeues() <= 1, "requeued at most once");
        var perKey = new LinkedHashMap<String, List<OutboxRecord>>();
        delivered.forEach(r -> perKey.computeIfAbsent(key(r), k -> new ArrayList<>()).add(r));
        perKey.forEach((key, records) -> {
            assertThat(records).filteredOn(r -> r.requeues() == 0).extracting(OutboxRecord::id)
                    .as("never-requeued ids of key %s in delivery order", key).isSorted();
            long laterNeverRequeued = Long.MAX_VALUE; // lowest id of a never-requeued record delivered after r
            for (int i = records.size() - 1; i >= 0; i--) {
                var r = records.get(i);
                if (r.requeues() == 0) {
                    laterNeverRequeued = Math.min(laterNeverRequeued, r.id());
                } else {
                    assertThat(laterNeverRequeued).as("key %s: never-requeued id delivered after requeued id %d",
                            key, r.id()).isGreaterThan(r.id());
                }
            }
        });
    }

    @Test
    void handlerThrowingFromHandleBatchRedeliversAndWorkerSurvives() throws Exception {
        bootstrap();
        var handler = new RecordingHandler()
                .throwFromBatch(() -> new RuntimeException("handler bug"))
                .throwFromBatch(() -> new AssertionError("handler error"));
        var calls = new AtomicInteger();
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                handler.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                calls.incrementAndGet();
                return handler.handleBatch(records);
            }
        });
        var outbox = outbox(relay::wake);

        var ids = new ArrayList<>(send(outbox, IntStream.range(0, 20)
                .mapToObj(i -> msg("t", "k" + i % 4, "m" + i))
                .toArray(OutboxMessage[]::new)));
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 20);
        ids.addAll(send(outbox, msg("t", "k0", "after"))); // the workers still deliver
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 21);

        assertThat(calls.get() - handler.batches().size()).as("calls that threw").isEqualTo(2);
        assertThat(relay.running()).isTrue();
        assertThat(relayThreads()).hasSize(config.workers());
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(handler.delivered());
    }

    @Test
    void handleBatchThrowingOnAPoisonKeyDivertsItWhileEverythingElseFlows() throws Exception {
        config.shards(4).workers(2);
        bootstrap();
        var poison = keyOnShard(0, "poison");
        var handler = new RecordingHandler();
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                handler.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                if (records.stream().anyMatch(r -> poison.equals(key(r)))) {
                    throw new IllegalStateException("cannot handle " + poison);
                }
                return handler.handleBatch(records);
            }
        });
        var messages = new ArrayList<OutboxMessage>();
        messages.add(msg("t", poison, "poison"));
        messages.addAll(messagesOnShards(200, 0, 0));
        messages.addAll(messagesOnShards(200, 1, 3));

        var ids = new ArrayList<>(send(outbox(relay::wake), messages.toArray(OutboxMessage[]::new)));
        ids.remove(0);
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 400 && retryRows(poison) == 1);

        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(handler.delivered());
        assertThat(relayThreads()).hasSize(config.workers());
    }

    @Test
    void stackOverflowInHandleKillsNoWorker() throws Exception {
        config.shards(4).workers(2);
        bootstrap();
        var handler = new RecordingHandler();
        try (var err = new CapturedStderr()) {
            var relay = start(new OutboxHandler() {
                @Override
                public void handle(OutboxRecord record) throws Exception {
                    if ("deep".equals(new String(record.payload(), UTF_8))) {
                        throw new StackOverflowError("deep payload (test)");
                    }
                    handler.handle(record);
                }
            });
            var deep = keyOnShard(0, "deep");
            var messages = new ArrayList<OutboxMessage>();
            messages.add(msg("t", deep, "deep"));
            messages.addAll(messagesOnShards(100, 0, 3));

            var ids = new ArrayList<>(send(outbox(relay::wake), messages.toArray(OutboxMessage[]::new)));
            ids.remove(0);
            await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 100 && retryRows(deep) == 1);

            assertThat(relayThreads()).hasSize(config.workers());
            assertThat(handler.delivered()).extracting(OutboxRecord::messageId)
                    .containsExactlyInAnyOrderElementsOf(ids);
            // the default handleBatch makes the Error that record's Retry: a diverted key, no failed call, and an
            // ERROR for the handler bug
            assertThat(err.text()).contains("streams diverted to retry")
                    .contains("StackOverflowError: deep payload (test)")
                    .contains("ERROR " + ShardProcessor.class.getName());
        }
    }

    @Test
    void aFailingShardStepDoesNotAbortThePass() throws Exception {
        config.shards(4).workers(1); // no sibling worker ever takes the failing shard out of the way
        bootstrap();
        try (var c = TestDb.connect()) { // undecodable headers: every step of shard 0 fails
            execute(c, "insert into " + schema.names().base() + " (shard, message_id, topic, key, payload, headers)"
                    + " values (0, gen_random_uuid(), 't', 'x', 'x', '\\xff')");
        }
        var handler = new RecordingHandler();
        try (var err = new CapturedStderr()) {
            var relay = start(handler);

            var ids = send(outbox(relay::wake), messagesOnShards(100, 1, 3).toArray(OutboxMessage[]::new));
            await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 100);

            assertThat(handler.delivered()).extracting(OutboxRecord::messageId)
                    .containsExactlyInAnyOrderElementsOf(ids);
            // its pass may step shard 0 after the others: the failure may come just after the last delivery
            await(Duration.ofSeconds(5), () -> err.text().contains("outbox shard 0 step failed"));
        }
    }

    @Test
    void anErrorInAShardStepDoesNotAbortThePass() throws Exception {
        config.shards(4).workers(1);
        bootstrap();
        var noop = TracerProvider.noop().get("test");
        Tracer failing = name -> { // a library Error inside the step of shard 0, as under -ea
            if (name.endsWith(" boom")) {
                throw new AssertionError("tracer assertion (test)");
            }
            return noop.spanBuilder(name);
        };
        send(outbox(() -> {}), msg("boom", keyOnShard(0, "boom", "x"), "boom")); // every step of shard 0 fails
        var handler = new RecordingHandler();
        try (var err = new CapturedStderr()) {
            var relay = new OutboxRelay(db, config, schema, handler, TestTelemetry.kora(null, failing), maintenance(db),
                    noHook(db));
            relays.add(relay);
            relay.start();

            var ids = send(outbox(relay::wake), messagesOnShards(100, 1, 3).toArray(OutboxMessage[]::new));
            await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 100);

            assertThat(handler.delivered()).extracting(OutboxRecord::messageId)
                    .containsExactlyInAnyOrderElementsOf(ids);
            await(Duration.ofSeconds(5), () -> err.text().contains("outbox shard 0 step failed")); // see above
            assertThat(err.text()).contains("tracer assertion (test)");
            assertThat(relayThreads()).hasSize(config.workers());
        }
    }

    @Test
    void slowHandlerDrainsABacklogDeliveringEachRecordOnce() throws Exception {
        config.shards(1).batchSize(100).batchTimeout(Duration.ofSeconds(2)); // 400 ms past the soft deadline
        bootstrap();
        var handler = new RecordingHandler();
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                Thread.sleep(20); // a call of 100 records needs over 2 s, well past the soft deadline at 1.6 s
                handler.handle(record);
            }
        });

        var ids = send(outbox(relay::wake), IntStream.range(0, 1000)
                .mapToObj(i -> msg("t", "k" + i % 100, "m" + i))
                .toArray(OutboxMessage[]::new));
        // the handler alone needs 1000 × ~25 ms (sleep overshoot included); before the fix 50 distinct records in 30 s
        await(Duration.ofSeconds(45), () -> handler.delivered().size() >= 1000);

        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(handler.delivered());
    }

    @Test
    void handlerSwallowingTheInterruptDrainsABacklogDeliveringEachRecordOnce() throws Exception {
        config.shards(1).batchSize(100).batchTimeout(Duration.ofSeconds(2));
        bootstrap();
        var handler = new RecordingHandler();
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                try {
                    Thread.sleep(40); // a call of 100 records needs 4 s, twice batchTimeout
                } catch (InterruptedException e) { // as Kora's blocking JDK @HttpClient does: flag cleared
                    throw new IllegalStateException("wrapped", e);
                }
                handler.handle(record);
            }
        });

        var ids = send(outbox(relay::wake), IntStream.range(0, 500)
                .mapToObj(i -> msg("t", "k" + i % 100, "m" + i))
                .toArray(OutboxMessage[]::new));
        // the handler alone needs 500 × ~45 ms
        await(Duration.ofSeconds(45), () -> handler.delivered().size() >= 500);

        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(handler.delivered());
    }

    @Test
    void byTopicRouteHangingPastBatchTimeoutDoesNotRedeliverOtherRoutes() throws Exception {
        config.shards(1).batchTimeout(Duration.ofSeconds(2)); // 400 ms past the soft deadline
        bootstrap();
        var healthy = new RecordingHandler();
        var hanging = new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                Thread.sleep(60_000); // interrupted at the soft deadline
            }
        };
        var relay = start(OutboxHandler.byTopic(Map.of("slow", hanging), healthy));
        var messages = new ArrayList<OutboxMessage>();
        IntStream.range(0, 10).forEach(i -> messages.add(msg("ok", "k" + i, "a" + i)));
        messages.add(msg("slow", "s", "stuck"));
        IntStream.range(0, 10).forEach(i -> messages.add(msg("ok", "k" + i, "b" + i)));

        var ids = new ArrayList<>(send(outbox(relay::wake), messages.toArray(OutboxMessage[]::new)));
        ids.remove(10);
        await(Duration.ofSeconds(10), () -> healthy.delivered().size() >= 20 && retryRows("s") == 1);

        assertThat(healthy.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(healthy.delivered());
    }

    @Test
    void idleRelayDoesNotSpin() throws Exception {
        config.idlePollInterval(Duration.ofMillis(200)); // the default; a spin would still burn ~2 s of CPU
        bootstrap();
        start(new RecordingHandler());
        await(Duration.ofSeconds(5), () -> ticksMade() >= 2);
        var threads = relayThreads();
        assertThat(threads).hasSize(config.workers());
        var mx = ManagementFactory.getThreadMXBean();
        long ticks = ticksMade();
        long cpu = threads.stream().mapToLong(t -> mx.getThreadCpuTime(t.getId())).sum();

        Thread.sleep(2000); // the measured idle period

        long cpuUsed = threads.stream().mapToLong(t -> mx.getThreadCpuTime(t.getId())).sum() - cpu;
        assertThat(ticksMade() - ticks).isLessThanOrEqualTo(2000 / config.idlePollInterval().toMillis() + 2);
        assertThat(Duration.ofNanos(cpuUsed)).isLessThan(Duration.ofMillis(300));
    }

    @Test
    void idleTicksAreRateLimitedAcrossInstancesWhileTheirCommitsStillFlow() throws Exception {
        config.idlePollInterval(Duration.ofMillis(200)); // minTickInterval 5 ms
        bootstrap();
        var handler = new RecordingHandler();
        start(handler);
        long first = ticksMade();
        await(Duration.ofSeconds(5), () -> ticksMade() > first);
        start(handler); // a few ms out of phase with the first
        long ticks = ticksMade();
        long started = System.nanoTime();

        Thread.sleep(3000); // the measured idle period

        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertThat(ticksMade() - ticks).as("idle ticks of two instances in %d ms", elapsedMillis)
                .isLessThanOrEqualTo(elapsedMillis / 150 + 2);

        // commits of an instance without a relay: neither relay is woken, an idle tick covers them
        var silent = outbox(() -> {});
        long slowest = 0;
        for (int i = 0; i < 10; i++) {
            long sent = System.nanoTime();
            send(silent, msg("t", "k" + i, "m" + i));
            int n = i + 1;
            await(Duration.ofSeconds(2), () -> handler.delivered().size() == n);
            slowest = Math.max(slowest, System.nanoTime() - sent);
        }
        System.out.printf("slowest delivery without a wake-up: %d ms%n", TimeUnit.NANOSECONDS.toMillis(slowest));
        // about one interval: a tick gap of up to 1.75 intervals plus the send and the step; 3 leave room for CI stalls
        assertThat(Duration.ofNanos(slowest)).isLessThan(config.idlePollInterval().multipliedBy(3));
    }

    @Test
    void onlyShardsWithNewRowsAreStepped() throws Exception {
        config.shards(32).workers(4).minTickInterval(Duration.ofMillis(50)).idlePollInterval(Duration.ofMillis(200));
        bootstrap(); // the defaults
        var steps = new ConcurrentHashMap<Integer, AtomicInteger>();
        var handler = new RecordingHandler();
        var relay = relay(countingSteps(steps), handler);
        relay.start();
        var outbox = outbox(relay::wake);
        var keys = List.of(keyOnShard(5, "a"), keyOnShard(5, "b"), keyOnShard(5, "c"));
        awaitParked();
        steps.clear();
        long ticks = ticksMade();
        long started = System.nanoTime();

        int sent = 0;
        while (System.nanoTime() - started < TimeUnit.SECONDS.toNanos(6)) { // rows on shard 5 only
            send(outbox, msg("t", keys.get(sent % keys.size()), "m" + sent));
            sent++;
            Thread.sleep(20);
        }
        int total = sent;
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == total);

        double seconds = (System.nanoTime() - started) / 1e9;
        long made = ticksMade() - ticks;
        int others = steps.entrySet().stream().filter(e -> e.getKey() != 5).mapToInt(e -> e.getValue().get())
                .max().orElse(0);
        System.out.printf("%d sends to shard 5 in %.1f s, %d ticks: shard 5 stepped %s times, the busiest other"
                + " shard %d times%n", total, seconds, made, steps.get(5), others);
        // the other shards never step: maintenance's checkpoint moves them on (before, every 5 s to move their
        // cursors along, and before that every tick stepped every shard behind it)
        assertThat(others).as("steps of the busiest other shard").isZero();
        assertThat(steps.get(5)).as("steps of shard 5").hasPositiveValue();
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).doesNotHaveDuplicates();
        assertIdOrderPerKey(handler.delivered());
    }

    @Test
    void workersShareOneCandidateReadPerTick() throws Exception {
        config.shards(64).workers(4).minTickInterval(Duration.ofMillis(50)).idlePollInterval(Duration.ofMillis(200));
        bootstrap();
        var reads = new AtomicInteger();
        var handler = new RecordingHandler();
        var relay = relay(observed(sql -> {
            if (sql.startsWith("with c0 as (select c.* from generate_series")) { // Cursors.positions, not a tick
                reads.incrementAndGet();
            }
        }), handler);
        relay.start();
        awaitParked();
        int readsBefore = reads.get();
        long ticks = ticksMade();

        var writers = Executors.newFixedThreadPool(4);
        var sent = new ConcurrentLinkedQueue<UUID>();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        try {
            var done = new ArrayList<Future<?>>();
            for (int w = 0; w < 4; w++) {
                var outbox = outbox(relay::wake);
                done.add(writers.submit(() -> {
                    while (System.nanoTime() < until) {
                        sent.addAll(send(outbox, IntStream.range(0, 20).mapToObj(i -> msg("t",
                                "k" + ThreadLocalRandom.current().nextInt(500), "m")).toArray(OutboxMessage[]::new)));
                    }
                    return null;
                }));
            }
            for (var f : done) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            writers.shutdown();
        }
        await(Duration.ofSeconds(10), () -> handler.delivered().size() == sent.size());

        long made = ticksMade() - ticks;
        int read = reads.get() - readsBefore;
        System.out.printf("%d messages, %d ticks, %d positions reads by %d workers%n", sent.size(), made, read,
                config.workers());
        // every tick wakes all workers; they share the read of it (before: one read per pass and worker)
        assertThat(made).isGreaterThan(10);
        assertThat((long) read).as("positions reads for %d ticks", made).isLessThanOrEqualTo(made + 1);
        assertIdOrderPerKey(handler.delivered());
    }

    @Test
    void aStuckShardDoesNotStopItsNeighbours() throws Exception {
        config.shards(4).workers(1); // one worker: all four candidates in one group
        bootstrap();
        var handler = new RecordingHandler();
        var bootstrapped = lastSuccess();
        var first = start(handler);
        // shard 0's cursor past the checkpoint's tick, which then stays valid for the other shards (deleting the
        // checkpoint's own tick would strand every shard); once its start run is over, maintenance does not run again
        // for an hour
        await(Duration.ofSeconds(5), () -> !lastSuccess().equals(bootstrapped));
        long stuckAt = 0;
        for (int a = 1; stuckAt <= checkpointTick(); a++) {
            assertThat(a).as("rows sent to shard 0").isLessThanOrEqualTo(10);
            send(outbox(first::wake), msg("t", keyOnShard(0, "a"), "a#" + a));
            int sent = a;
            await(Duration.ofSeconds(5), () -> handler.delivered().size() == sent);
            try (var c = TestDb.connect()) {
                stuckAt = queryLong(c, "select tick_id from " + schema.names().cursor()
                        + " where shard = 0 order by seq desc limit 1");
            }
        }
        first.close();
        int before = handler.delivered().size();
        try (var c = TestDb.connect()) { // by hand: the tick shard 0's cursor stands at
            execute(c, "delete from " + schema.names().tick() + " where id = " + stuckAt);
        }
        var groups = new CopyOnWriteArrayList<List<Integer>>();
        try (var err = new CapturedStderr()) {
            var relay = relay(steppedGroups(groups::add), handler);
            var outbox = outbox(relay::wake);
            send(outbox, IntStream.range(0, 4).mapToObj(shard -> msg("t", keyOnShard(shard, "b"), "b" + shard))
                    .toArray(OutboxMessage[]::new));
            relay.start();

            await(Duration.ofSeconds(10), () -> handler.delivered().size() == before + 3);
            assertThat(handler.delivered().subList(before, before + 3)).extracting(OutboxRelayIT::payload)
                    .containsExactlyInAnyOrder("b1", "b2", "b3");
            await(Duration.ofSeconds(5), () -> err.text().contains("outbox shard 0 step failed"));
            assertThat(err.text()).contains("no tick " + stuckAt + " for shard 0");
            // keeps failing, alone: one shared step at most found it out
            send(outbox, IntStream.range(1, 4).mapToObj(shard -> msg("t", keyOnShard(shard, "c"), "c" + shard))
                    .toArray(OutboxMessage[]::new));
            await(Duration.ofSeconds(10), () -> handler.delivered().size() == before + 6);
            int seen = groups.size();
            await(Duration.ofSeconds(5), () -> groups.size() > seen + 2);
            assertThat(groups.stream().filter(g -> g.contains(0) && g.size() > 1)).as("shared steps of shard 0")
                    .hasSizeLessThanOrEqualTo(1);
            assertThat(groups.stream().filter(g -> g.equals(List.of(0)))).as("steps of shard 0 alone")
                    .hasSizeGreaterThan(1);
        }
    }

    @Test
    void ticksKeepComingWhileAPassWorksThroughABacklog() throws Exception {
        config.shards(4).workers(1).batchSize(10).minTickInterval(Duration.ofMillis(50))
                .idlePollInterval(Duration.ofMillis(200));
        bootstrap();
        var delivered = new AtomicInteger();
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                Thread.sleep(2); // 2000 records: about 4 s of one pass, its round never running out
                delivered.incrementAndGet();
            }
        });
        var outbox = outbox(relay::wake);
        awaitParked();
        send(outbox, messagesOnShards(2000, 0, 3).toArray(OutboxMessage[]::new));
        await(Duration.ofSeconds(5), () -> delivered.get() > 0);
        long ticks = ticksMade();

        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        for (int i = 0; System.nanoTime() < until; i++) { // commits that wake the relay, every 50 ms
            send(outbox, msg("t", "w" + i, "w"));
            Thread.sleep(50);
        }

        // a tick attempt per minTickInterval of wake-ups: about 40; before, none until the backlog was through, so
        // the next tick held everything committed meanwhile, read again in full by each chunk of its batches
        assertThat(delivered.get()).as("the backlog is still being delivered").isLessThan(2000);
        assertThat(ticksMade() - ticks).as("ticks made in 2 s").isGreaterThanOrEqualTo(10);
    }

    @Test
    void busyShardsAreSteppedAloneAndQuietOnesTogether() throws Exception {
        config.shards(32).workers(1).batchSize(1000); // groups of up to 32
        bootstrap();
        var handler = new RecordingHandler();
        var groups = new CopyOnWriteArrayList<List<Integer>>();
        var relay = relay(steppedGroups(groups::add), handler);
        relay.start();
        var outbox = outbox(relay::wake);
        awaitParked();
        int sent = 0;
        var waves = new ArrayList<List<List<Integer>>>();
        for (int rows : new int[] {600, 600, 1, 1, 100, 100}) { // one transaction each, rows per shard
            int from = groups.size();
            send(outbox, IntStream.range(0, 32).boxed().flatMap(shard -> IntStream.range(0, rows)
                    .mapToObj(i -> msg("t", keyOnShard(shard, "k"), "m"))).toArray(OutboxMessage[]::new));
            sent += 32 * rows;
            int total = sent;
            await(Duration.ofSeconds(20), () -> handler.delivered().size() == total);
            awaitParked();
            waves.add(List.copyOf(groups.subList(from, groups.size())));
        }

        // the first wave, with nothing known: a step cut behind its first shards leaves the rest untouched, and those
        // then step alone (not in groups that are cut again, each progressing a shard or two)
        assertThat(waves.get(0).stream().mapToInt(List::size).sum()).as("shards stepped in the first wave")
                .isLessThanOrEqualTo(3 * 32);
        // 600 rows a shard: two do not fit in one batchSize read, which would be cut after the first (the rest of
        // the second read again in the next step, the shards after it untouched); one row a shard: all in one step
        assertThat(waves.get(1)).as("steps of the second wave of 600 rows a shard").isNotEmpty()
                .allSatisfy(group -> assertThat(group).hasSize(1));
        assertThat(waves.get(3).stream().mapToInt(List::size).max().orElse(0)).as("largest step of the second wave"
                + " of 1 row a shard").isGreaterThanOrEqualTo(16);
        // 100 rows a shard, more than batchSize / 16: ten would fit in one read, but each steps alone
        assertThat(waves.get(5)).as("steps of the second wave of 100 rows a shard").isNotEmpty()
                .allSatisfy(group -> assertThat(group).hasSize(1));
        assertIdOrderPerKey(handler.delivered());
    }

    @Test
    void aShardInFlightWhenANewerTickMarksItIsSteppedAgainAtOnce() throws Exception {
        config.shards(4).workers(2).idlePollInterval(Duration.ofSeconds(30)); // only wake-ups tick
        bootstrap();
        var handler = new RecordingHandler();
        var first = new CountDownLatch(1);
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                if (payload(record).equals("r#1")) {
                    first.countDown();
                    Thread.sleep(500); // its step in flight while the second row commits and is ticked
                }
                handler.handle(record);
            }
        });
        var outbox = outbox(relay::wake);
        awaitParked();
        var key = keyOnShard(1, "k");
        send(outbox, msg("t", key, "r#1"));
        assertThat(first.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(100); // past minTickInterval: the wake-up's tick is made by the other worker
        send(outbox, msg("t", key, "r#2"));

        // the newer round found the shard in flight; the step, caught up only to its own tick, puts it back
        await(Duration.ofSeconds(3), () -> handler.delivered().size() == 2);
        assertThat(handler.delivered()).extracting(OutboxRelayIT::payload).containsExactly("r#1", "r#2");
    }

    @Test
    void aShardCaughtUpWithItsStepsTickIsNotSteppedAgain() throws Exception {
        config.shards(32).workers(1).idlePollInterval(Duration.ofSeconds(30));
        bootstrap();
        var steps = new ConcurrentHashMap<Integer, AtomicInteger>();
        var handler = new RecordingHandler();
        var relay = relay(countingSteps(steps), handler);
        relay.start();
        var outbox = outbox(relay::wake);
        awaitParked();
        steps.clear();

        send(outbox, msg("t", keyOnShard(5, "k"), "m"));
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == 1);
        awaitParked();

        assertThat(steps).containsOnlyKeys(5);
        assertThat(steps.get(5)).as("steps of shard 5").hasValue(1);
    }

    @Test
    @Tag("timing") // a delay bound: a starved CI runner misses it
    void aSlowShardDoesNotHoldItsNeighboursBackAcrossRounds() throws Exception {
        // 8 shards, one worker: a group of up to 8. Shard 0's call of 3 slow records winds down at the soft deadline
        // (800 ms) after its last one, so its batch ends while the shards after it in the call are not attempted
        config.shards(8).workers(1).batchTimeout(Duration.ofSeconds(1));
        bootstrap();
        var slow = keyOnShard(0, "slow");
        var slowDone = new AtomicInteger();
        var delays = new CopyOnWriteArrayList<Long>();
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                if (payload(record).equals("slow")) { // 300 ms, deaf to the wind-down's interrupt
                    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
                    while (System.nanoTime() < end) {
                        Thread.onSpinWait();
                    }
                    slowDone.incrementAndGet();
                } else {
                    delays.add(Duration.between(record.createdAt(), Instant.now()).toMillis());
                }
            }
        });
        var outbox = outbox(relay::wake);
        awaitParked();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        int sent = 0;
        for (int round = 0; System.nanoTime() < until; round++) {
            if (round % 5 == 0) { // every second: 3 slow records, about one call of 900 ms
                send(outbox, IntStream.range(0, 3).mapToObj(i -> msg("t", slow, "slow"))
                        .toArray(OutboxMessage[]::new));
            }
            int r = round;
            send(outbox, IntStream.range(1, 8).mapToObj(shard -> msg("t", keyOnShard(shard, "n" + shard + "-"),
                    "n" + r)).toArray(OutboxMessage[]::new));
            sent += 7;
            Thread.sleep(200);
        }
        int total = sent;
        await(Duration.ofSeconds(20), () -> delays.size() == total);

        long max = delays.stream().mapToLong(Long::longValue).max().orElse(0);
        System.out.printf("neighbours: %d records, max delay %d ms; slow records done %d%n", total, max,
                slowDone.get());
        // a slow call of about 900 ms, then the neighbours' step: they never wait for two of them
        assertThat(max).as("longest delivery delay of a neighbour, ms").isLessThan(2000);
    }

    @Test
    void rowsCoveredByAnotherInstancesTicksAreDelivered() throws Exception {
        // this relay's idle attempts find a younger tick of the other instance and insert none
        config.shards(32).idlePollInterval(Duration.ofMillis(500));
        bootstrap();
        var handler = new RecordingHandler();
        start(handler);
        var other = new Ticks(schema.names(), config.shards()); // another instance's tick maker
        var silent = outbox(() -> {}); // commits on another instance: nothing wakes this relay
        awaitParked();
        long before = ticksMade();
        int otherTicks = 0;

        var ids = new ArrayList<UUID>();
        for (int i = 0; i < 50; i++) {
            ids.addAll(send(silent, msg("t", "k" + i % 7, "m" + i), msg("t", null, "u" + i)));
            try (var c = TestDb.connect()) {
                otherTicks += other.make(c, Duration.ZERO) ? 1 : 0;
            }
            Thread.sleep(20);
        }

        await(Duration.ofSeconds(5), () -> handler.delivered().size() >= ids.size());
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(handler.delivered());
        assertThat(ticksMade() - before - otherTicks).as("ticks this relay made").isLessThanOrEqualTo(2);
    }

    @Test
    void aLockedPartitionDoesNotStopTicks() throws Exception {
        bootstrap(); // 4 shards, 2 workers
        var handler = new RecordingHandler();
        var relay = start(handler);
        var outbox = outbox(relay::wake);
        awaitParked();
        try (var err = new CapturedStderr(); var other = TestDb.connect()) {
            other.setAutoCommit(false);
            long before = ticksMade(); // before the lock: a fallback tick may come right after it
            // e.g. an ALTER TABLE by hand on the tail. Shard steps wait for it (a batch read plans over every
            // partition), but ticks go on without shard sets and say why, so the lag gauge shows the stall
            execute(other, "lock table " + schema.names().tail() + " in access exclusive mode");
            var ids = send(outbox, messagesOnShards(40, 0, config.shards() - 1).toArray(OutboxMessage[]::new));

            await(Duration.ofSeconds(2), () -> ticksMade() > before
                    && err.text().contains("outbox tick made without its shard set"));
            other.rollback();

            await(Duration.ofSeconds(10), () -> handler.delivered().size() >= ids.size());
            assertThat(handler.delivered()).extracting(OutboxRecord::messageId)
                    .containsExactlyInAnyOrderElementsOf(ids);
        }
    }

    @Test
    void drainFailsWithTheHaltWhenTheNewestTickTurnsOutAboveTheXidCounterAndRecoverySavesTheRowsSentBefore()
            throws Exception {
        bootstrap();
        var handler = new RecordingHandler();
        var injected = new AtomicBoolean();
        // a stale instance's tick from a history ahead, committed after drain's own halt check, before its probe
        var relay = relay(observed(sql -> {
            if (sql.contains("pg_current_snapshot") && injected.compareAndSet(false, true)) {
                try (var c = TestDb.connect()) {
                    insertTickAhead(c);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }), handler);
        var ids = send(outbox(() -> {}), msg("t", "k", "m")); // its xid passes for visible in the tick ahead

        assertThatThrownBy(relay::drain).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("xid counter below a tick's xmax");
        assertThat(injected).isTrue();
        assertThat(handler.delivered()).isEmpty();

        assertThat(maintenance(db).runOnce()).isTrue(); // outbox.recovery.xidRegression = auto
        relay.drain();
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
    }

    @Test
    void aWorkerTickBelowTheNewestXmaxHaltsTheRelayBeforeMaintenanceRuns() throws Exception {
        bootstrap(); // maintenance runs at start, then not for an hour
        var maintenance = maintenance(db);
        var relay = new OutboxRelay(db, config, schema, new RecordingHandler(), TestTelemetry.kora(), maintenance,
                noHook(db));
        relays.add(relay);
        var bootstrapped = lastSuccess();
        relay.start();
        awaitParked();
        // its start run is over: one starting after the tick below would find the regression itself and recover it
        await(Duration.ofSeconds(5), () -> !lastSuccess().equals(bootstrapped));
        assertThat(maintenance.halted()).isFalse();
        try (var c = TestDb.connect()) {
            // under the tick lock, as every tick is: a tick of the running relay in flight would otherwise take its
            // snapshot before this insert and its id after it, so the workers' next ticks would compare with that one
            var lock = "(" + schema.names().internalNamespace() + ", " + TableNames.LOCK_TICK + ")";
            execute(c, "select pg_advisory_lock" + lock);
            insertTickAhead(c);
            execute(c, "select pg_advisory_unlock" + lock);
        }

        send(outbox(relay::wake), msg("t", "k", "m"));

        await(Duration.ofSeconds(5), maintenance::halted);
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + schema.names().halt())).isOne();
        }
    }

    /** A tick whose snapshot is 10 000 xids ahead of the counter, as a logical cutover onto a younger cluster. */
    private void insertTickAhead(Connection c) throws Exception {
        execute(c, "insert into " + schema.names().tick() + " (snapshot, max_id) select ("
                + "(pg_snapshot_xmin(s)::text::bigint + 10000) || ':' || (pg_snapshot_xmax(s)::text::bigint + 10000)"
                + " || ':' || array_to_string(array(select x::text::bigint + 10000 from pg_snapshot_xip(s) x), ','))"
                + "::pg_snapshot, 0 from (select pg_current_snapshot() s) q");
    }

    @Test
    void idleShardsAreSteppedAgainWhenTickIdsGoBackwardsAfterAHaltIsLifted() throws Exception {
        bootstrap(); // 4 shards, 2 workers
        var names = schema.names();
        long dumped = ticksMade();
        try (var c = TestDb.connect()) {
            // the dump: ticks, cursors and checkpoints as of now (the tick id sequence with them)
            for (var table : List.of(names.tick(), names.cursor(), names.checkpoint())) {
                execute(c, "create table " + table + "_dump as select * from " + table);
            }
            // then a while of idle relaying on the old database
            execute(c, "insert into " + names.tick() + " (snapshot, max_id)"
                    + " select pg_current_snapshot(), 0 from generate_series(1, 1000)");
        }
        var maintenance = maintenance(db);
        var handler = new RecordingHandler();
        var relay = new OutboxRelay(db, config, schema, handler, TestTelemetry.kora(), maintenance,
                noHook(db));
        relays.add(relay);
        relay.start();
        // then maintenance's checkpoint moves every idle shard on, past the ids the restore goes back to
        await(Duration.ofSeconds(10), () -> checkpointTick() > dumped + 900);
        awaitParked();

        // the restore: its xid regression halts the relay; the operator restores and lifts the halt
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.halt() + " (max_tick_xmax) values ('0')");
        }
        assertThat(maintenance.checkHalt()).isTrue();
        awaitParked();
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            for (var table : List.of(names.cursor(), names.checkpoint(), names.tick())) {
                execute(c, "delete from " + table);
                execute(c, "insert into " + table + " overriding system value select * from " + table + "_dump");
            }
            execute(c, "alter table " + names.tick() + " alter column id restart with " + (dumped + 1));
            execute(c, "delete from " + names.halt());
            c.commit();
        }
        assertThat(maintenance.checkHalt()).isFalse();

        // tick ids now run below those the shards were found idle at
        var ids = send(outbox(relay::wake), IntStream.range(0, config.shards())
                .mapToObj(shard -> msg("t", keyOnShard(shard, "k"), "m" + shard)).toArray(OutboxMessage[]::new));
        await(Duration.ofSeconds(3), () -> handler.delivered().size() == ids.size());
        assertThat(ticksMade()).as("tick ids still below the old ones").isLessThan(dumped + 900);
    }

    @Test
    void aCheckpointFromAnotherHistoryIsIgnoredAndReplaced() throws Exception {
        // only the wake-up ticks: an idle tick before the run at start would let its cleanup delete the cursors' tick,
        // which the restore this stands for keeps
        config.idlePollInterval(Duration.ofSeconds(30));
        bootstrap(); // 4 shards; maintenance runs at start, then not for an hour
        var handler = new RecordingHandler();
        var relay = start(handler);
        awaitParked();
        var names = schema.names();
        // the start's maintenance run is past its checkpoint step, so it cannot replace the one written below
        await(Duration.ofSeconds(5), () -> checkpointTick() > 0);
        long ahead = ticksMade() + 1000;
        try (var c = TestDb.connect()) { // as a partial restore leaves it: above every tick, every shard idle
            execute(c, "delete from " + names.checkpoint());
            execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + ahead
                    + ", B'0000', " + ahead + ")");
        }

        var ids = send(outbox(relay::wake), msg("t", keyOnShard(2, "k"), "m"));
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == ids.size()); // the relay alone delivers
        assertThat(checkpointTick()).as("no maintenance ran meanwhile").isEqualTo(ahead);

        assertThat(maintenance(db).runOnce()).isTrue();

        assertThat(checkpointTick()).as("replaced").isLessThanOrEqualTo(ticksMade());
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
    }

    @Test
    void aCheckpointMadeBeforeItsOwnTickIsIgnoredAndReplaced() throws Exception {
        config.idlePollInterval(Duration.ofSeconds(30)); // only the wake-up steps
        bootstrap(); // 4 shards; maintenance runs at start, then not for an hour
        var handler = new RecordingHandler();
        var relay = start(handler);
        awaitParked();
        var names = schema.names();
        // the start's maintenance run is past its checkpoint step, so it cannot replace the one written below
        await(Duration.ofSeconds(5), () -> checkpointTick() > 0);
        long next = ticksMade() + 1;
        try (var c = TestDb.connect()) { // as a partial restore leaves it once new ticks pass its id: every shard idle
            execute(c, "delete from " + names.checkpoint());
            execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + next
                    + ", B'0000', " + next + ")");
        }
        var ids = send(outbox(() -> {}), msg("t", keyOnShard(2, "k"), "m"));
        try (var c = TestDb.connect()) { // the tick it names, made after it, holds shard 2's row: its 0 bit hides it
            assertThat(new Ticks(names, config.shards()).make(c, Duration.ZERO)).isTrue();
        }
        assertThat(ticksMade()).as("the tick the checkpoint names").isEqualTo(next);

        relay.wake();
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == ids.size()); // the relay alone delivers
        assertThat(checkpointTick()).as("no maintenance ran meanwhile").isEqualTo(next);

        assertThat(maintenance(db).runOnce()).isTrue();

        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + names.checkpoint() + " k join " + names.tick()
                    + " t on t.id = k.tick_id and t.created_at <= k.created_at")).as("replaced").isOne();
            assertThat(queryLong(c, "select count(*) from " + names.checkpoint())).isOne();
        }
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
    }

    @Test
    void drainOnAnIdleRunningRelayReturnsAtOnce() throws Exception {
        config.shards(8);
        bootstrap();
        var relay = start(new RecordingHandler());
        awaitParked(); // idle shards leave their cursors on the bootstrap tick

        long started = System.nanoTime();
        assertThat(relay.drain()).isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(500));
    }

    @Test
    void drainOfAnIdleOutboxRightAfterAnotherDrainReturnsAtOnce() throws Exception {
        config.shards(8).relayEnabled(false);
        bootstrap();
        var relay = relay(new RecordingHandler());
        relay.start();
        assertThat(relay.drain()).isZero(); // the cursors are on its tick

        long started = System.nanoTime();
        assertThat(relay.drain()).as("no shard behind its tick").isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(500));
    }

    @Test
    void drainDoesNotWaitForIdleShards() throws Exception {
        config.shards(64).relayEnabled(false);
        bootstrap();
        var relay = relay(new RecordingHandler());
        relay.start();
        long seq = maxCursorSeq();
        send(outbox(relay::wake), msg("t", keyOnShard(7, "k"), "m"));

        long started = System.nanoTime();
        assertThat(relay.drain()).isOne();

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(500));
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(shard::text, ',') from " + schema.names().cursor()
                    + " where seq > " + seq)).as("only the shard with a row stepped, once").isEqualTo("7");
        }
        assertThat(relay.drain()).as("and nothing to step after it").isZero();
        assertThat(maxCursorSeq()).isEqualTo(seq + 1);
    }

    @Test
    void aTickWithoutAShardSetStepsEachShardOnce() throws Exception {
        aTickWithoutAShardSetStepsEachShardOnce(0);
    }

    @Test
    void aTickWithoutAShardSetFarPastTheCheckpointStepsEachShardOnce() throws Exception {
        aTickWithoutAShardSetStepsEachShardOnce(150); // 30 s of idle ticks, beyond maxTicksPerBatch (100)
    }

    private void aTickWithoutAShardSetStepsEachShardOnce(int idleTicksBefore) throws Exception {
        // no tick of the relay's own during the sweep: a cursor read racing a sibling's step of a shard (begun before
        // the read, committed after it) finds the shard behind, and a newer tick by then makes its second step a fresh
        // batch with a cursor row of its own. The wake-up below is throttled by the tick just inserted, so its attempt
        // follows minTickInterval later, inside a sweep slowed by a cold JVM or a loaded machine: the tick lock, held
        // until the sweep is over, keeps it from inserting one
        config.shards(32).workers(4).minTickInterval(Duration.ofMillis(50)).idlePollInterval(Duration.ofSeconds(30));
        bootstrap();
        var relay = start(new RecordingHandler());
        awaitParked();
        long seq = maxCursorSeq();
        try (var c = TestDb.connect(); var tickLock = TestDb.connect()) {
            var lock = "(" + schema.names().internalNamespace() + ", " + TableNames.LOCK_TICK + ")";
            execute(tickLock, "select pg_advisory_lock" + lock);
            execute(c, "insert into " + schema.names().tick() + " (snapshot, max_id, dirty) select"
                    + " pg_current_snapshot(), t.max_id, ''::varbit from (select max_id from " + schema.names().tick()
                    + " order by id desc limit 1) t, generate_series(1, " + idleTicksBefore + ")");
            // as a capped or failed probe leaves it: every shard may have rows
            execute(c, "insert into " + schema.names().tick() + " (snapshot, max_id) select pg_current_snapshot(),"
                    + " max_id from " + schema.names().tick() + " order by id desc limit 1");
            relay.wake();

            await(Duration.ofSeconds(5), () -> maxCursorSeq() >= seq + config.shards());
            execute(tickLock, "select pg_advisory_unlock" + lock);
        }
        awaitParked();
        for (int i = 0; i < 5; i++) { // idle ticks go on: no shard is behind them, so no further step
            try (var c = TestDb.connect()) {
                execute(c, "insert into " + schema.names().tick() + " (snapshot, max_id, dirty) select"
                        + " pg_current_snapshot(), max_id, ''::varbit from " + schema.names().tick()
                        + " order by id desc limit 1");
            }
            relay.wake();
            Thread.sleep(200);
        }
        awaitParked();
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select count(*) || ':' || count(distinct shard) from "
                    + schema.names().cursor() + " where seq > " + seq)).as("one cursor row per shard")
                    .isEqualTo(config.shards() + ":" + config.shards());
            assertThat(new Cursors(schema.names(), config.shards()).positions(c, ticksMade()))
                    .extracting(Cursors.Position::behind).containsOnly(false);
        }
    }

    @Test
    void restartResumesPausedAndChunkedShards() throws Exception {
        config.shards(2).batchSize(5).headFailuresBeforeDivert(1000)
                .retry(Duration.ofMillis(300), Duration.ofMillis(300), 0);
        bootstrap();
        var a = List.of(keyOnShard(0, "a"), keyOnShard(0, "b"));
        var b = List.of(keyOnShard(1, "c"), keyOnShard(1, "d"));
        var outbox = outbox(() -> {});

        // 1: the target is down, shard 0 pauses
        var down = new RecordingHandler().failAlways(r -> true, () -> new RuntimeException("target down"));
        var first = start(down);
        var ids = new ArrayList<>(send(outbox, IntStream.range(0, 10)
                .mapToObj(i -> msg("t", a.get(i % 2), "a" + i))
                .toArray(OutboxMessage[]::new)));
        await(Duration.ofSeconds(10), () -> cursor(0).failures() >= 1);
        first.close();
        assertThat(cursor(0).failures()).isPositive();
        assertThat(cursor(0).targetTickId()).isNull();
        assertThat(down.delivered()).isEmpty();

        // 2: shard 1 delivers one chunk, then every call fails until the instance stops
        ids.addAll(send(outbox, IntStream.range(0, 20)
                .mapToObj(i -> msg("t", b.get(i % 2), "b" + i))
                .toArray(OutboxMessage[]::new)));
        var chunk = new RecordingHandler();
        var chunkDelivered = new AtomicBoolean();
        var second = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                if (records.stream().anyMatch(r -> a.contains(key(r)))) { // shard 0 stays paused
                    return records.stream().map(r -> Outcome.retry(new RuntimeException("still down"))).toList();
                }
                if (chunkDelivered.compareAndSet(false, true)) {
                    return chunk.handleBatch(records);
                }
                throw new IllegalStateException("instance stops after one chunk");
            }
        });
        await(Duration.ofSeconds(10), () -> cursor(1).targetTickId() != null);
        second.close();
        assertThat(chunk.delivered()).hasSize(5);
        assertThat(cursor(1).lastId()).isEqualTo(chunk.delivered().get(4).id());
        assertThat(cursor(1).targetTickId()).isNotNull();
        assertThat(cursor(0).failures()).isPositive();
        assertThat(cursor(0).lastId()).isZero();

        // 3: a new instance with a working target resumes both shards from their cursors
        var fresh = new RecordingHandler();
        var third = start(fresh);
        await(Duration.ofSeconds(15), () -> fresh.delivered().size() >= 25);
        assertThat(third.drain()).isZero();

        var all = new ArrayList<>(chunk.delivered());
        all.addAll(fresh.delivered());
        assertThat(all).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertIdOrderPerKey(all);
        assertThat(cursor(0).failures()).isZero();
        assertThat(cursor(1).targetTickId()).isNull();
    }

    @Test
    void shutdownWaitsForInFlightBatch() throws Exception {
        bootstrap();
        var inFlight = new CountDownLatch(1);
        var recording = new RecordingHandler().delay(Duration.ofSeconds(1));
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) throws Exception {
                recording.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                inFlight.countDown();
                return recording.handleBatch(records);
            }
        });
        var ids = send(outbox(relay::wake), IntStream.range(0, 10)
                .mapToObj(i -> msg("t", "k", "m" + i))
                .toArray(OutboxMessage[]::new));
        assertThat(inFlight.await(5, TimeUnit.SECONDS)).isTrue();

        long releasing = System.nanoTime();
        relay.close();

        assertThat(Duration.ofNanos(System.nanoTime() - releasing)).isLessThan(Duration.ofSeconds(5)); // not interrupted
        assertThat(relay.running()).isFalse();
        assertThat(relayThreads()).isEmpty();
        assertThat(recording.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
        var again = new RecordingHandler();
        assertThat(relay(again).drain()).isZero(); // the batch was committed: nothing is redelivered
        assertThat(again.delivered()).isEmpty();
    }

    @Test
    void shutdownInterruptsBatchStillRunningAfterTimeoutAndItIsRedelivered() throws Exception {
        config.shutdownTimeout(Duration.ofMillis(300));
        bootstrap();
        var inFlight = new CountDownLatch(1);
        var relay = start(new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                inFlight.countDown();
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return records.stream().map(r -> Outcome.delivered()).toList(); // too late: the worker rolled back
            }
        });
        var ids = send(outbox(relay::wake), msg("t", "k", "a"), msg("t", "k", "b"));
        assertThat(inFlight.await(5, TimeUnit.SECONDS)).isTrue();

        long releasing = System.nanoTime();
        relay.close();

        assertThat(Duration.ofNanos(System.nanoTime() - releasing)).isLessThan(Duration.ofSeconds(3));
        assertThat(relayThreads()).isEmpty();
        var again = new RecordingHandler();
        assertThat(relay(again).drain()).isEqualTo(2);
        assertThat(again.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
    }

    @Test
    void wakeNeverThrows() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler());
        relay.wake(); // before init
        relay.start();
        relay.close();
        relay.wake(); // after release
        assertThat(relay.running()).isFalse();
    }

    private void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.start();
    }

    private OutboxRelay relay(OutboxHandler handler) {
        return relay(db, handler);
    }

    private OutboxRelay relay(JdbcConnectionFactory factory, OutboxHandler handler) {
        var relay = new OutboxRelay(new KoraOutboxTransactions(factory), config, schema, handler,
                TestTelemetry.kora(), maintenance(db), noHook(factory));
        relays.add(relay);
        return relay;
    }

    /** {@code pool}, recording per thread the most connections it held at once. */
    private static JdbcConnectionFactory tracking(JdbcDatabase pool, Map<Thread, Integer> peaks) {
        var open = new ConcurrentHashMap<Thread, AtomicInteger>();
        return new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                return pool.withConnection(callback);
            }

            @Override
            public Connection currentConnection() {
                return pool.currentConnection();
            }

            @Override
            public Connection newConnection() {
                var c = pool.newConnection();
                var thread = Thread.currentThread();
                var count = open.computeIfAbsent(thread, t -> new AtomicInteger());
                peaks.merge(thread, count.incrementAndGet(), Math::max);
                var closed = new AtomicBoolean();
                return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                            if (method.getName().equals("close") && closed.compareAndSet(false, true)) {
                                count.decrementAndGet();
                            }
                            return invoke(method, c, args);
                        });
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return pool.telemetry();
            }
        };
    }

    /** A dead-letter pass without a hook: every dead letter is kept. */
    private DeadLetterPass noHook(JdbcConnectionFactory factory) {
        return new DeadLetterPass(new KoraOutboxTransactions(factory), schema.names(), config, null,
                new OutboxMetrics(null));
    }

    private OutboxRelay relay(OutboxHandler handler, OutboxDeadLetterHandler hook) {
        var metrics = new OutboxMetrics(null);
        var relay = new OutboxRelay(db, config, schema, handler, new KoraOutboxTelemetry(metrics, null),
                maintenance(db),
                new DeadLetterPass(db, schema.names(), config, hook, metrics));
        relays.add(relay);
        return relay;
    }

    /** A handler failing every record of key {@code poison} permanently. */
    private static RecordingHandler poisonous() {
        return new RecordingHandler()
                .failAlways(r -> "poison".equals(key(r)), () -> new OutboxPermanentException("bad payload"));
    }

    /** A hook keeping every letter, which it records. */
    private static OutboxDeadLetterHandler keeping(List<DeadLetter> handed) {
        return letters -> {
            handed.addAll(letters);
            return letters.stream().map(l -> DeadLetterDecision.keep()).toList();
        };
    }

    /** The status of the dead letter of {@code messageId}, null without one. */
    private String deadStatus(UUID messageId) throws Exception {
        try (var c = TestDb.connect()) {
            // a scalar subquery: one row, null while the relay has not written the letter yet
            return queryString(c, "select (select status from " + schema.names().dead() + " where message_id = '"
                    + messageId + "')");
        }
    }

    /** A pending dead letter as the relay writes one with a hook, due now; its id is above any outbox row's. */
    private void pendingLetter(String key) throws Exception {
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + schema.names().dead() + " (id, shard, message_id, topic, key, payload,"
                    + " created_at, attempts, error, status, due_at) values ((select greatest(max(id), 1000000000)"
                    + " + 1 from " + schema.names().dead() + "), 0, gen_random_uuid(), 't', '" + key + "', 'p', now(),"
                    + " 1, 'boom', 'pending', now())");
        }
    }

    private static List<Thread> deadLetterThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().equals("outbox-dead-letters") && t.isAlive())
                .toList();
    }

    /** {@code delays} sorted: the 99th percentile. */
    private static Duration p99(List<Duration> delays) {
        var sorted = delays.stream().sorted().toList();
        return sorted.get((int) Math.ceil(sorted.size() * 0.99) - 1);
    }

    /**
     * 1000 messages over 50 keys in 100 transactions, 5 ms apart, and one {@code poison} message among them; returns
     * how long after its commit each of the 1000 reached {@code delivered} (a fresh map for this run).
     */
    private List<Duration> deliveryDelays(PgOutbox outbox, AtomicReference<Map<String, Long>> delivered, String run)
            throws Exception {
        var arrived = new ConcurrentHashMap<String, Long>();
        delivered.set(arrived);
        var committed = new ConcurrentHashMap<String, Long>();
        for (int tx = 0; tx < 100; tx++) {
            int t = tx;
            var messages = new ArrayList<OutboxMessage>();
            IntStream.range(0, 10)
                    .forEach(i -> messages.add(msg("t", "k" + (t * 10 + i) % 50, run + "/" + t + "/" + i)));
            if (tx == 50) {
                messages.add(msg("t", "poison", run + "/poison"));
            }
            send(outbox, messages.toArray(OutboxMessage[]::new));
            long now = System.nanoTime();
            for (var m : messages) {
                committed.put(new String(m.payload(), UTF_8), now);
            }
            Thread.sleep(5);
        }
        committed.remove(run + "/poison");
        await(Duration.ofSeconds(30), () -> arrived.size() == 1000);
        return committed.entrySet().stream()
                .map(e -> Duration.ofNanos(Math.max(0, arrived.get(e.getKey()) - e.getValue())))
                .toList();
    }

    /** Real maintenance; with the 1 h {@code maintenanceInterval} it runs once, right after the relay starts. */
    private Maintenance maintenance(JdbcConnectionFactory factory) {
        return new Maintenance(new KoraOutboxTransactions(factory), schema.names(), config, new OutboxMetrics(null));
    }

    /** {@link #db} whose new connections report the SQL of every statement they prepare. */
    private JdbcConnectionFactory observed(Consumer<String> prepared) {
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
                            if (method.getName().equals("prepareStatement")) {
                                prepared.accept((String) args[0]);
                            }
                            try {
                                return method.invoke(c, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return db.telemetry();
            }
        };
    }

    /** {@link #db} whose new connections count the shard steps (shard lock attempts) per shard. */
    private JdbcConnectionFactory countingSteps(Map<Integer, AtomicInteger> steps) {
        return steppedGroups(group -> group.forEach(shard -> steps.computeIfAbsent(shard, s -> new AtomicInteger())
                .incrementAndGet()));
    }

    /** {@link #db} whose new connections report the shards of every step (a shard lock attempt), in group order. */
    private JdbcConnectionFactory steppedGroups(Consumer<List<Integer>> stepped) {
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
                            var result = invoke(method, c, args);
                            if (method.getName().equals("prepareStatement")
                                    && ((String) args[0]).contains("pg_try_advisory_xact_lock")) {
                                var ps = (PreparedStatement) result;
                                return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                                        new Class<?>[] {PreparedStatement.class}, (p, m, a) -> {
                                            if (m.getName().equals("setArray") && (int) a[0] == 5) { // the shards
                                                stepped.accept(Arrays.stream((Object[]) ((java.sql.Array) a[1])
                                                        .getArray()).map(shard -> (Integer) shard).toList());
                                            }
                                            return invoke(m, ps, a);
                                        });
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

    private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private OutboxRelay start(OutboxHandler handler) {
        var relay = relay(handler);
        relay.start();
        assertThat(relay.running()).isTrue();
        return relay;
    }

    private PgOutbox outbox(Runnable wake) {
        return new PgOutbox(db, schema, config.shards(), wake, TestTelemetry.kora());
    }

    /**
     * 5000 messages over 20 keys from 4 writers, each waking one of {@code relays} in turn: 25 transactions of 50
     * messages each; returns their ids.
     */
    private List<UUID> sendConcurrently(List<OutboxRelay> relays) throws Exception {
        var writers = Executors.newFixedThreadPool(4);
        try {
            var sent = new ArrayList<Future<List<UUID>>>();
            for (int w = 0; w < 4; w++) {
                var outbox = outbox(relays.get(w % relays.size())::wake);
                sent.add(writers.submit(() -> {
                    var ids = new ArrayList<UUID>();
                    for (int tx = 0; tx < 25; tx++) {
                        ids.addAll(send(outbox, IntStream.range(0, 50)
                                .mapToObj(i -> msg("t", "k" + ThreadLocalRandom.current().nextInt(20), "m"))
                                .toArray(OutboxMessage[]::new)));
                    }
                    return ids;
                }));
            }
            var ids = new ArrayList<UUID>();
            for (var f : sent) {
                ids.addAll(f.get(60, TimeUnit.SECONDS));
            }
            assertThat(ids).hasSize(5000);
            return ids;
        } finally {
            writers.shutdown();
        }
    }

    /** One committed transaction; Kora runs the wake-up after the commit. */
    private List<UUID> send(PgOutbox outbox, OutboxMessage... messages) {
        return db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(messages)));
    }

    private static OutboxMessage msg(String topic, String key, String payload) {
        return key == null
                ? OutboxMessage.of(topic, payload.getBytes(UTF_8))
                : OutboxMessage.of(topic, key, payload.getBytes(UTF_8));
    }

    private static String key(OutboxRecord r) {
        return r.key() == null ? null : new String(r.key(), UTF_8);
    }

    private static String payload(OutboxRecord r) {
        return new String(r.payload(), UTF_8);
    }

    /** The records a running {@code process} of their account blocks; the event type is the payload up to '#'. */
    private static Predicate<OutboxRecord> whileRunning(String process) {
        return r -> BLOCKS.get(process).contains(payload(r).split("#")[0]);
    }

    /**
     * The owner's coordinator over {@code handleBatch} (key = account id): requeues each record {@link #blocked}
     * matches, delivers the rest; logs each call's payloads and the delivered records. The library knows none of the
     * priorities behind {@code blocked}.
     */
    private static final class Coordinator implements OutboxHandler {

        volatile Predicate<OutboxRecord> blocked = r -> false;
        final List<List<String>> calls = new CopyOnWriteArrayList<>();
        private final List<OutboxRecord> delivered = new ArrayList<>();

        @Override
        public void handle(OutboxRecord record) {
            throw new UnsupportedOperationException("handleBatch only");
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            calls.add(records.stream().map(OutboxRelayIT::payload).toList());
            var outcomes = new ArrayList<Outcome>(records.size());
            for (var r : records) {
                if (blocked.test(r)) {
                    outcomes.add(Outcome.requeue());
                } else {
                    synchronized (delivered) {
                        delivered.add(r);
                    }
                    outcomes.add(Outcome.delivered());
                }
            }
            return outcomes;
        }

        /** Delivered records in handling order. */
        List<OutboxRecord> delivered() {
            synchronized (delivered) {
                return List.copyOf(delivered);
            }
        }
    }

    private String keyOnShard(int shard, String prefix) {
        return keyOnShard(shard, "t", prefix);
    }

    private String keyOnShard(int shard, String topic, String prefix) {
        for (int i = 0; ; i++) {
            var key = prefix + i;
            if (Hashing.shard(Hashing.keyHash(topic, key.getBytes(UTF_8)), config.shards()) == shard) {
                return key;
            }
        }
    }

    /** {@code n} messages over 20 keys per shard of {@code first..last}, several per key, keys interleaved. */
    private List<OutboxMessage> messagesOnShards(int n, int first, int last) {
        var keys = new ArrayList<String>();
        for (int i = 0; keys.size() < 20 * (last - first + 1); i++) {
            var key = "k" + i;
            int shard = Hashing.shard(Hashing.keyHash("t", key.getBytes(UTF_8)), config.shards());
            if (shard >= first && shard <= last) {
                keys.add(key);
            }
        }
        return IntStream.range(0, n).mapToObj(i -> msg("t", keys.get(i % keys.size()), first + "/" + i)).toList();
    }

    /** Payloads of the parked table in id order, comma-separated; null when it is empty. */
    private String parked() throws Exception {
        try (var c = TestDb.connect()) {
            return queryString(c, "select string_agg(convert_from(payload, 'UTF8'), ',' order by id) from "
                    + schema.names().parked());
        }
    }

    private static long rows(String table) throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + table);
        }
    }

    /** Rows of {@code key} in the retry table. */
    private long retryRows(String key) throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + schema.names().retry() + " where key = '"
                    + key + "'::bytea");
        }
    }

    private Cursor cursor(int shard) throws Exception {
        try (var c = TestDb.connect()) {
            return new Cursors(schema.names(), config.shards()).latest(c, shard).cursor();
        }
    }

    /** The highest cursor {@code seq}: grows with every cursor row written, whatever maintenance deletes. */
    private long maxCursorSeq() throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select max(seq) from " + schema.names().cursor());
        }
    }

    /** {@code succeeded_at} of the maintenance marker, as sortable text. */
    private String lastSuccess() throws Exception {
        try (var c = TestDb.connect()) {
            return queryString(c, "select to_char(succeeded_at at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') from "
                    + schema.names().maintenance());
        }
    }

    /** The tick of maintenance's checkpoint, 0 without one. */
    private long checkpointTick() throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select coalesce(max(tick_id), 0) from " + schema.names().checkpoint());
        }
    }

    /** The newest tick id: counts ticks made, unlike a row count, which maintenance shrinks. */
    private long ticksMade() throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select max(id) from " + schema.names().tick());
        }
    }

    /** {@code pg_current_xact_id()}: consumes one xid itself. */
    private static long currentXid() throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select pg_current_xact_id()::text::bigint");
        }
    }

    /** Every worker parked: the relay is idle. */
    private void awaitParked() throws Exception {
        await(Duration.ofSeconds(5), () -> relayThreads().size() == config.workers()
                && relayThreads().stream().allMatch(t -> t.getState() == Thread.State.TIMED_WAITING));
    }

    private static List<Thread> relayThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("outbox-relay-") && t.isAlive())
                .toList();
    }

    /** Per key, delivery order is id (= commit) order. */
    private static void assertIdOrderPerKey(List<OutboxRecord> delivered) {
        var perKey = new LinkedHashMap<String, List<Long>>();
        for (var r : delivered) {
            if (r.key() != null) {
                perKey.computeIfAbsent(key(r), k -> new ArrayList<>()).add(r.id());
            }
        }
        perKey.forEach((key, ids) -> assertThat(ids).as("ids of key %s", key).isSorted());
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void await(Duration timeout, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(5);
        }
    }
}
