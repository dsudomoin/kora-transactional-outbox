package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The relay step and relay failure hooks of a running relay: each step ends once, each failure counts once. */
class RelayTelemetryIT {

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final RecordingTelemetry telemetry = new RecordingTelemetry();
    @Nullable
    private OutboxRuntime runtime;

    @AfterEach
    void close() {
        if (runtime != null) {
            runtime.close();
        }
    }

    @Test
    void everyStepWithLockedShardsStartsAndEndsOnce() throws Exception {
        var handler = new RecordingHandler();
        var outbox = start(new TestOutboxConfig(), handler);
        var messages = new ArrayList<OutboxMessage>();
        for (int i = 0; i < 20; i++) {
            messages.add(OutboxMessage.of("orders", "k" + i % 5, bytes("m" + i)));
        }

        transactions.inTx(c -> outbox.sendAll(messages));
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 20);
        runtime.close(); // no step in flight from here

        assertThat(telemetry.steps).isNotEmpty().allSatisfy(step -> {
            assertThat(step.shards).isNotEmpty();
            assertThat(step.finished).hasValue(1);
            assertThat(step.failed).hasValue(0);
        });
        assertThat(telemetry.relayFailures).hasValue(0);
    }

    @Test
    void aFailingShardIsCountedOncePerSingleShardFailure() throws Exception {
        // 4 shards, the relay running; one worker steps both shards of the send together first
        var config = new TestOutboxConfig().workers(1);
        int goodShard = shard("good");
        String bad = "bad";
        for (int i = 0; shard(bad) == goodShard; i++) {
            bad = "bad" + i;
        }
        int badShard = shard(bad);
        var handler = new RecordingHandler();
        var outbox = start(config, handler);
        // the step's own SQL fails, not its handler call (a handler failure is an outcome): every cursor append of the
        // bad shard raises, failing each step that holds it
        var function = config.table() + "_fail";
        var cursor = config.table() + "_cursor";
        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "create function " + function + "() returns trigger language plpgsql as $$ begin"
                    + " if new.shard = " + badShard + " then raise exception 'shard down'; end if; return new; end $$");
            TestSql.execute(c, "create trigger fail before insert on " + cursor
                    + " for each row execute function " + function + "()");
        }

        var messages = new ArrayList<OutboxMessage>();
        for (int i = 0; i < 5; i++) {
            messages.add(OutboxMessage.of("orders", "good", bytes("g" + i)));
            messages.add(OutboxMessage.of("orders", bad, bytes("b" + i)));
        }
        var ids = transactions.inTx(c -> outbox.sendAll(messages));
        await(Duration.ofSeconds(30), () -> telemetry.failedAlone(badShard) >= 2);
        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "drop trigger fail on " + cursor);
        }
        await(Duration.ofSeconds(30), () -> delivered(handler).containsAll(ids));
        runtime.close(); // no step in flight from here

        var badKey = bytes(bad);
        assertThat(handler.delivered()).filteredOn(r -> Arrays.equals(r.key(), badKey)).isNotEmpty()
                .allSatisfy(r -> assertThat(r.shard()).isEqualTo(badShard));
        assertThat(telemetry.steps).allSatisfy(step -> assertThat(step.ends()).isOne());
        assertThat(telemetry.steps).as("a step of both shards failed, then was redone shard by shard")
                .anySatisfy(step -> {
                    assertThat(step.shards).containsExactlyInAnyOrder(goodShard, badShard);
                    assertThat(step.failed).hasValue(1);
                });
        assertThat(telemetry.relayFailures).hasValue(telemetry.failedAlone(badShard));
    }

    @Test
    void stoppingIsNotARelayFailure() throws Exception {
        var called = new CountDownLatch(1);
        OutboxHandler sleeping = record -> {
            called.countDown();
            Thread.sleep(Long.MAX_VALUE); // until interrupted
        };
        var outbox = start(new TestOutboxConfig().shutdownTimeout(Duration.ofMillis(200)), sleeping);

        transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "k", bytes("m"))));
        assertThat(called.await(10, TimeUnit.SECONDS)).isTrue();
        runtime.close(); // interrupts the step after shutdownTimeout

        await(Duration.ofSeconds(5), () -> telemetry.steps.stream().allMatch(s -> s.ends() > 0));
        assertThat(telemetry.steps).isNotEmpty().allSatisfy(step -> assertThat(step.ends()).isOne());
        assertThat(telemetry.relayFailures).hasValue(0);
    }

    private Outbox start(TestOutboxConfig config, OutboxHandler handler) {
        var r = OutboxRuntime.builder(config, transactions, handler).telemetry(telemetry).build();
        runtime = r;
        r.start();
        return r.outbox();
    }

    private static int shard(String key) {
        return Hashing.shard(Hashing.keyHash("orders", bytes(key)), 4);
    }

    private static List<UUID> delivered(RecordingHandler handler) {
        return handler.delivered().stream().map(OutboxRecord::messageId).toList();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(10);
        }
    }

    /** The steps started, in order, and the relay failures. */
    private static final class RecordingTelemetry implements OutboxTelemetry {

        final List<Step> steps = new CopyOnWriteArrayList<>();
        final AtomicInteger relayFailures = new AtomicInteger();

        @Override
        public RelayStep stepStarted(List<Integer> shards) {
            var step = new Step(List.copyOf(shards));
            steps.add(step);
            return step;
        }

        @Override
        public void relayFailed(Throwable error) {
            relayFailures.incrementAndGet();
        }

        /** The failed steps of {@code shard} alone. */
        int failedAlone(int shard) {
            return (int) steps.stream().filter(s -> s.failed.get() > 0 && s.shards.equals(List.of(shard))).count();
        }
    }

    /** Counts each way it ends. */
    private static final class Step implements OutboxTelemetry.RelayStep {

        final List<Integer> shards;
        final AtomicInteger finished = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();

        Step(List<Integer> shards) {
            this.shards = shards;
        }

        int ends() {
            return finished.get() + failed.get();
        }

        @Override
        public void finished() {
            finished.incrementAndGet();
        }

        @Override
        public void failed(Throwable error) {
            failed.incrementAndGet();
        }
    }
}
