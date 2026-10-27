package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The outbox and the inbox without any framework: public API only, a plain DataSource, the core's telemetry SPI. */
class FrameworkFreeIT {

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> runtimes = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (int i = runtimes.size() - 1; i >= 0; i--) {
            runtimes.get(i).close();
        }
    }

    @Test
    void sendAndSendAllReachTheHandlerWithoutWaitingForTheIdlePoll() throws Exception {
        var handler = new RecordingHandler();
        var config = new TestOutboxConfig().idlePollInterval(Duration.ofSeconds(10));
        var runtime = start(OutboxRuntime.builder(config, transactions, handler).build());
        Thread.sleep(500); // past the relay's first pass: from here only the wake-up after commit delivers in time

        var one = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("orders", "k1", bytes("a"))));
        var two = transactions.inTx(c -> runtime.outbox().sendAll(List.of(
                OutboxMessage.of("orders", "k2", bytes("b")), OutboxMessage.of("orders", "k3", bytes("c")))));

        await(Duration.ofSeconds(1), () -> handler.delivered().size() == 3);
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId)
                .containsExactlyInAnyOrder(one, two.get(0), two.get(1));
    }

    @Test
    void outcomesAsWithKora() throws Exception {
        var letters = new CopyOnWriteArrayList<DeadLetter>();
        OutboxDeadLetterHandler hook = batch -> {
            letters.addAll(batch);
            return batch.stream().map(l -> DeadLetterDecision.keep()).toList();
        };
        var telemetry = new RecordingTelemetry();
        var handler = new ScriptedHandler();
        // one shard and one delivery in its batch: each other key's outcome is handled per key, not a shard pause
        var config = new TestOutboxConfig().relayEnabled(false).shards(1);
        var runtime = start(OutboxRuntime.builder(config, transactions, handler).deadLetterHandler(hook)
                .telemetry(telemetry).build());
        var outbox = runtime.outbox();

        var ids = transactions.inTx(c -> outbox.sendAll(List.of(
                OutboxMessage.of("jobs", "r", bytes("retry-once")),
                OutboxMessage.of("jobs", "p", bytes("permanent")),
                OutboxMessage.of("jobs", "q", bytes("requeue-once")),
                OutboxMessage.of("jobs", "n", bytes("not-attempted-once")),
                OutboxMessage.of("jobs", "o", bytes("ok")))));
        runtime.relay().drain();
        transactions.inTx(c -> outbox.send(OutboxMessage.of("jobs", "q", bytes("after-requeue"))));
        Thread.sleep(config.relay().retry().initialDelay().toMillis() * 2); // the retry is due
        runtime.relay().drain();

        assertThat(handler.delivered).containsSubsequence("requeue-once", "after-requeue");
        assertThat(handler.delivered).contains("retry-once", "not-attempted-once").doesNotContain("permanent");
        assertThat(letters).extracting(DeadLetter::messageId).containsExactly(ids.get(1));
        assertThat(runtime.admin().findDeadLetters(DeadLetterFilter.any(), null, 10).letters())
                .extracting(DeadLetter::messageId).containsExactly(ids.get(1));
        assertThat(telemetry.events).contains("enqueued", "deliveryStarted", "delivered", "retried", "deadLettered",
                "requeued");
    }

    @Test
    void inboxAcceptsAnIdOnce() {
        var runtime = start(InboxRuntime.builder(new TestOutboxConfig(), transactions).build());
        var id = UuidV7.next(); // a UUIDv7, as the outbox gives its messages: dedup holds inside the window

        boolean first = transactions.inTx(c -> runtime.inbox().tryAccept(id));
        boolean again = transactions.inTx(c -> runtime.inbox().tryAccept(id));

        assertThat(first).isTrue();
        assertThat(again).isFalse();
    }

    @Test
    void maintenanceExtendsThePartitionsWithoutErrors() throws Exception {
        var handler = new RecordingHandler();
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(2)
                .maintenanceInterval(Duration.ofMillis(200)).cursorAnchorInterval(Duration.ofMillis(200));
        try (var err = new CapturedStderr()) {
            var runtime = start(OutboxRuntime.builder(config, transactions, handler).build());
            await(Duration.ofSeconds(10), () -> runtime.admin().status().maintenanceSinceSuccessSeconds() != null);

            burnXids(3000); // past the premade ranges: maintenance extends them, swapping the tail if rows overflowed
            var id = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("p"))));

            await(Duration.ofSeconds(30), () -> {
                var status = runtime.admin().status();
                return !status.tailOverflow() && status.premade() >= 2;
            });
            await(Duration.ofSeconds(10), () -> handler.delivered().stream().anyMatch(r -> r.messageId().equals(id)));
            assertThat(err.text()).doesNotContain("ERROR");
        }
    }

    private <T extends AutoCloseable> T start(T runtime) {
        runtimes.add(runtime);
        if (runtime instanceof OutboxRuntime outbox) {
            outbox.start();
        } else if (runtime instanceof InboxRuntime inbox) {
            inbox.start();
        }
        return runtime;
    }

    private void burnXids(int n) throws Exception {
        try (var c = transactions.newConnection(); var st = c.createStatement()) {
            st.execute("set synchronous_commit = off");
            st.execute("do $$ begin for i in 1.." + n + " loop perform pg_current_xact_id(); commit; end loop; end $$");
        }
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

    /** Answers by payload: each "-once" outcome on the first call, then a delivery. */
    private static final class ScriptedHandler implements OutboxHandler {

        final List<String> delivered = new CopyOnWriteArrayList<>();
        private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

        @Override
        public void handle(OutboxRecord record) {
            throw new UnsupportedOperationException("handleBatch answers");
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            var outcomes = new ArrayList<Outcome>(records.size());
            for (var record : records) {
                var payload = new String(record.payload(), UTF_8);
                boolean first = calls.computeIfAbsent(payload, p -> new AtomicInteger()).incrementAndGet() == 1;
                Outcome outcome = switch (payload) {
                    case "retry-once" -> first ? Outcome.retry(new IOException("down")) : null;
                    case "permanent" -> Outcome.permanent(new IllegalArgumentException("never"));
                    case "requeue-once" -> first ? Outcome.requeue() : null;
                    case "not-attempted-once" -> first ? Outcome.notAttempted() : null;
                    default -> null;
                };
                if (outcome == null) {
                    delivered.add(payload);
                    outcome = Outcome.delivered();
                }
                outcomes.add(outcome);
            }
            return outcomes;
        }
    }

    /** The names of the hooks called. */
    private static final class RecordingTelemetry implements OutboxTelemetry {

        final Set<String> events = ConcurrentHashMap.newKeySet();

        @Override
        public void enqueued(String topic) {
            events.add("enqueued");
        }

        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            events.add("deliveryStarted");
            return Delivery.NONE;
        }

        @Override
        public void delivered(String topic, Duration delay) {
            events.add("delivered");
        }

        @Override
        public void retried(String topic) {
            events.add("retried");
        }

        @Override
        public void deadLettered(String topic) {
            events.add("deadLettered");
        }

        @Override
        public void requeued(String topic) {
            events.add("requeued");
        }
    }
}
