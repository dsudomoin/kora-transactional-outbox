package io.github.dsudomoin.kora.outbox;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.internal.Cursors;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.Hashing;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.PgOutbox;
import io.github.dsudomoin.kora.outbox.internal.ShardProcessor;
import io.github.dsudomoin.kora.outbox.internal.TableNames;
import io.github.dsudomoin.kora.outbox.internal.Ticks;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

class OutboxRelayIT {

    /** The owner's priorities: the event types of an account that a running process of that account blocks. */
    private static final Map<String, Set<String>> BLOCKS = Map.of(
            "WITHDRAW", Set.of("FEE_RELEASE", "SYNCHRONIZE"),
            "FEE_RELEASE", Set.of("FEE_RELEASE", "SYNCHRONIZE"),
            "SYNCHRONIZE", Set.of("SYNCHRONIZE"));

    private final JdbcDatabase db = TestDb.newDatabase(30);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private OutboxSchema schema;

    @AfterEach
    void release() {
        for (var relay : relays) {
            relay.release();
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
        relay.init();
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
        relay.init();
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
        relays.add(new OutboxRelay(db, config, schema, new RecordingHandler(), new OutboxMetrics(null), null,
                maintenance(observed(sql -> {
                    if (sql.contains("pg_try_advisory_lock(")) { // a maintenance run
                        runs.incrementAndGet();
                    }
                }))));
        relays.get(0).init();
        await(Duration.ofSeconds(5), () -> runs.get() == 1);

        config.relayEnabled(false);
        var disabled = new AtomicInteger();
        relays.add(new OutboxRelay(db, config, schema, new RecordingHandler(), new OutboxMetrics(null), null,
                maintenance(observed(sql -> disabled.incrementAndGet()))));
        relays.get(1).init();
        relays.get(1).release();
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
            relays.add(new OutboxRelay(db, config, schema, new RecordingHandler(), new OutboxMetrics(null), null,
                    maintenance(failing)));
            relays.get(0).init();

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
        relay.init();
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
        relay.init();
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

        assertThatThrownBy(() -> drain.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no progress");
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
        Thread.sleep(3000); // handed again at each due time, requeued each time
        send(outbox, msg("t", "acc42", "FEE_RELEASE"));
        await(Duration.ofSeconds(1), () -> handler.delivered().size() == 1); // FEE_RELEASE did not wait

        assertThat(handler.calls)
                .anyMatch(call -> Collections.indexOfSubList(call, List.of("SYNCHRONIZE#2", "FEE_RELEASE")) >= 0);
        assertThat(parked()).isEqualTo("SYNCHRONIZE#2");

        handler.blocked = r -> false; // the running SYNCHRONIZE ends
        await(Duration.ofSeconds(2), () -> handler.delivered().size() == 2); // its due time at the latest

        assertThat(handler.delivered()).extracting(OutboxRelayIT::payload)
                .containsExactly("FEE_RELEASE", "SYNCHRONIZE#2");
        assertThat(handler.delivered().get(1).requeues()).isPositive();
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
        var writers = Executors.newFixedThreadPool(4);
        var sent = new ArrayList<Future<List<UUID>>>();
        for (int w = 0; w < 4; w++) {
            var outbox = outbox(started.get(w % 3)::wake);
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
        writers.shutdown();

        await(Duration.ofSeconds(60), () -> handler.delivered().size() >= 5000);
        assertThat(started.get(0).drain()).isZero(); // everything is committed, nothing left

        assertThat(ids).hasSize(5000);
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
        var writers = Executors.newFixedThreadPool(4);
        var sent = new ArrayList<Future<List<UUID>>>();
        for (int w = 0; w < 4; w++) {
            var outbox = outbox(started.get(w % 3)::wake);
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
        writers.shutdown();

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
            var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), failing, maintenance(db));
            relays.add(relay);
            relay.init();

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
    void idleRelayConsumesFewTransactionIds() throws Exception {
        config.shards(32).workers(4).minTickInterval(Duration.ofMillis(50)).idlePollInterval(Duration.ofMillis(200));
        bootstrap(); // the defaults
        start(new RecordingHandler());
        await(Duration.ofSeconds(5), () -> ticksMade() >= 3);
        awaitParked();
        long xid = currentXid();
        long started = System.nanoTime();

        Thread.sleep(10_000); // the measured idle period

        double seconds = (System.nanoTime() - started) / 1e9;
        double perSecond = (currentXid() - xid) / seconds;
        System.out.printf("idle relay: %.1f xids/s%n", perSecond);
        // an idle tick per idlePollInterval plus a cursor append per shard every 5 s: about 11/s (162/s before)
        assertThat(perSecond).as("xids/s").isLessThan(20);
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
        relay.init();
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
        // the other shards step only to move their cursors along every 5 s (idle append), each such step tried by
        // up to every worker the same tick woke; before, every tick stepped every shard behind it
        assertThat(others).as("steps of the busiest other shard")
                .isLessThanOrEqualTo(config.workers() * ((int) (seconds / 5) + 2));
        assertThat(steps.get(5)).as("steps of shard 5").hasPositiveValue();
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).doesNotHaveDuplicates();
        assertIdOrderPerKey(handler.delivered());
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
    void idleShardsAreSteppedAgainWhenTickIdsGoBackwardsAfterAHaltIsLifted() throws Exception {
        bootstrap(); // 4 shards, 2 workers
        var names = schema.names();
        long dumped = ticksMade();
        try (var c = TestDb.connect()) {
            // the dump: ticks and cursors as of now (the tick id sequence with them)
            execute(c, "create table " + names.tick() + "_dump as select * from " + names.tick());
            execute(c, "create table " + names.cursor() + "_dump as select * from " + names.cursor());
            // then a while of idle relaying on the old database
            execute(c, "insert into " + names.tick() + " (snapshot, max_id)"
                    + " select pg_current_snapshot(), 0 from generate_series(1, 1000)");
        }
        var maintenance = maintenance(db);
        var handler = new RecordingHandler();
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance);
        relays.add(relay);
        relay.init();
        await(Duration.ofSeconds(10), () -> IntStream.range(0, config.shards())
                .allMatch(shard -> cursorTick(shard) > dumped + 900)); // then every shard steps IDLE
        awaitParked();

        // the restore: its xid regression halts the relay; the operator restores and lifts the halt
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.halt() + " (max_tick_xmax) values ('0')");
        }
        assertThat(maintenance.checkHalt()).isTrue();
        awaitParked();
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            for (var table : List.of(names.cursor(), names.tick())) {
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
    void drainOnAnIdleRunningRelayReturnsAtOnce() throws Exception {
        config.shards(8);
        bootstrap();
        var relay = start(new RecordingHandler());
        awaitParked(); // its idle steps leave the cursors on ticks up to 5 s old

        long started = System.nanoTime();
        assertThat(relay.drain()).isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(500));
    }

    @Test
    void drainOfAnIdleOutboxRightAfterAnotherDrainReturnsAtOnce() throws Exception {
        config.shards(8).relayEnabled(false);
        bootstrap();
        var relay = relay(new RecordingHandler());
        relay.init();
        assertThat(relay.drain()).isZero(); // the cursors are on its tick

        long started = System.nanoTime();
        assertThat(relay.drain()).as("its tick is younger than 5 s after the previous one").isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(500));
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
        first.release();
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
        second.release();
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
        relay.release();

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
        relay.release();

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
        relay.init();
        relay.release();
        relay.wake(); // after release
        assertThat(relay.running()).isFalse();
    }

    private void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
    }

    private OutboxRelay relay(OutboxHandler handler) {
        return relay(db, handler);
    }

    private OutboxRelay relay(JdbcConnectionFactory factory, OutboxHandler handler) {
        var relay = new OutboxRelay(factory, config, schema, handler, new OutboxMetrics(null), null, maintenance(db));
        relays.add(relay);
        return relay;
    }

    /** Real maintenance; with the 1 h {@code maintenanceInterval} it runs once, right after the relay starts. */
    private Maintenance maintenance(JdbcConnectionFactory factory) {
        return new Maintenance(factory, schema.names(), config, new OutboxMetrics(null));
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
                                            if (m.getName().equals("setInt") && (int) a[0] == 2) { // the lock key
                                                steps.computeIfAbsent((int) a[1] - TableNames.LOCK_SHARD_BASE,
                                                        shard -> new AtomicInteger()).incrementAndGet();
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
        relay.init();
        assertThat(relay.running()).isTrue();
        return relay;
    }

    private PgOutbox outbox(Runnable wake) {
        return new PgOutbox(db, schema, config.shards(), wake, new OutboxMetrics(null));
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

    private long cursorTick(int shard) {
        try {
            return cursor(shard).tickId();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Cursor cursor(int shard) throws Exception {
        try (var c = TestDb.connect()) {
            return new Cursors(schema.names()).latest(c, shard);
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
