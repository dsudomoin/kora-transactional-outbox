package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.internal.HeaderCodec;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class OutboxRuntimeIT {

    @OutboxContract(2)
    record V2(String id) {}

    private static final OutboxHeader H = new OutboxHeader("h", bytes("v"));
    private static final OutboxHeader CONTRACT = new OutboxHeader("outbox-contract", "2".getBytes(US_ASCII));
    private static final OutboxHeader TRACE = new OutboxHeader("trace", bytes("t"));
    private static final OutboxHeader LOCAL = new OutboxHeader(PgOutbox.LOCAL_HEADER, null);
    /** A message's own headers: two {@code outbox-local}, one of them of a value, which marks nothing. */
    private static final List<OutboxHeader> OWN =
            List.of(H, new OutboxHeader(PgOutbox.LOCAL_HEADER, bytes("x")), LOCAL);
    /** Adds the {@code trace} header to every message, as tracing adds {@code traceparent}. */
    private static final OutboxTelemetry TRACING = new OutboxTelemetry() {
        @Override
        public Enqueue enqueueStarted() {
            return new Enqueue() {
                @Override
                public List<OutboxHeader> headers(List<OutboxHeader> headers) {
                    var traced = new ArrayList<>(headers);
                    traced.add(TRACE);
                    return traced;
                }
            };
        }
    };

    @Test
    void startThenCloseInOrder() throws Exception {
        var threads = new CopyOnWriteArrayList<String>(); // the thread of each connection the outbox asked for
        var transactions = new Recording(new JdbcTransactions(TestDataSources.newDataSource()), threads);
        var config = new TestOutboxConfig().shutdownTimeout(Duration.ofSeconds(5));
        var runtime = OutboxRuntime.builder(config, transactions, new RecordingHandler()).build();

        runtime.start();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (threads.stream().noneMatch(t -> t.startsWith("outbox-relay-")) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertThat(threads.get(0)).as("the schema, on the starting thread, first")
                .isEqualTo(Thread.currentThread().getName());
        assertThat(threads).anyMatch(t -> t.startsWith("outbox-relay-"));
        long closing = System.nanoTime();
        runtime.close();
        assertThat(Duration.ofNanos(System.nanoTime() - closing)).isLessThan(config.relay().shutdownTimeout());
        assertThat(runtime.relay().running()).isFalse();
    }

    @Test
    void withoutShardsANewOutboxGetsTheDefaultAndDelivers() throws Exception {
        var handler = new RecordingHandler();
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        try (var runtime = OutboxRuntime.builder(new TestOutboxConfig().shards(null), transactions, handler).build()) {
            runtime.start();
            var id = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", "p".getBytes(UTF_8))));

            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (handler.delivered().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
            assertThat(runtime.admin().status().shards()).isEqualTo(OutboxConfig.DEFAULT_SHARDS);
        }
    }

    @Test
    void withoutAHandlerATopicNoRouteClaimsIsRefusedBeforeAnythingIsWritten() {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var config = new TestOutboxConfig().relayEnabled(false);
        try (var runtime = OutboxRuntime.builder(config, transactions, null)
                .route(new Claiming(Set.of("billing", "audit"))).build()) {
            runtime.start();
            var outbox = runtime.outbox();
            var typed = new TypedOutbox<>(outbox, (topic, value) -> value.getBytes(UTF_8), String.class);
            var refused = "outbox topic orders is claimed by no OutboxTopicHandler and there is no OutboxHandler:"
                    + " claimed topics are [audit, billing]";

            transactions.inTx(c -> {
                assertThatThrownBy(() -> outbox.send(OutboxMessage.of("orders", "k", bytes("p"))))
                        .isInstanceOf(IllegalArgumentException.class).hasMessage(refused);
                assertThatThrownBy(() -> outbox.send(c, OutboxMessage.of("orders", "k", bytes("p"))))
                        .isInstanceOf(IllegalArgumentException.class).hasMessage(refused);
                assertThatThrownBy(() -> outbox.sendAll(List.of(OutboxMessage.of("audit", "k", bytes("a")),
                        OutboxMessage.of("orders", "k", bytes("p")))))
                        .isInstanceOf(IllegalArgumentException.class).hasMessage(refused);
                assertThatThrownBy(() -> typed.send("orders", "k", "p"))
                        .isInstanceOf(IllegalArgumentException.class).hasMessage(refused);
                return null;
            });
            assertThat(rows(transactions, config)).as("the transaction committed, and nothing in it was written, not"
                    + " even sendAll's claimed message").isZero();

            transactions.inTx(c -> outbox.sendAll(List.of(OutboxMessage.of("audit", "k", bytes("a")),
                    typed.message("billing", "k", "b"))));
            assertThat(rows(transactions, config)).isEqualTo(2);
        }
    }

    @Test
    void withAHandlerBehindTheRoutesEveryTopicIsSent() {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var config = new TestOutboxConfig().relayEnabled(false);
        try (var runtime = OutboxRuntime.builder(config, transactions, new RecordingHandler())
                .route(new Claiming(Set.of("audit"))).build()) {
            runtime.start();
            var outbox = runtime.outbox();
            var typed = new TypedOutbox<>(outbox, (topic, value) -> value.getBytes(UTF_8), String.class);

            transactions.inTx(c -> {
                outbox.send(OutboxMessage.of("orders", "k", bytes("p")));
                outbox.sendAll(List.of(OutboxMessage.of("orders", "k", bytes("p"))));
                return typed.send("orders", "k", "p");
            });

            assertThat(rows(transactions, config)).isEqualTo(3);
        }
    }

    /**
     * A write to a topic a route claims, typed or plain, carries {@code outbox-local} with a null value, last: after
     * the telemetry's headers, in place of the message's own, next to its contract header. A write to any other topic
     * carries none, the message's own dropped.
     */
    @Test
    void aWriteToARoutedTopicIsMarkedLocalAndNoOtherIs() {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var config = new TestOutboxConfig().relayEnabled(false);
        try (var runtime = OutboxRuntime.builder(config, transactions, new RecordingHandler())
                .route(new Claiming(Set.of("audit"))).route(new TypedOutboxTopicHandler<>(Set.of("orders"),
                        (topic, payload) -> new V2(new String(payload, UTF_8)), V2.class) {
                    @Override
                    protected void handle(V2 value, OutboxRecord record) {}
                }).telemetry(TRACING).build()) {
            runtime.start();
            var outbox = runtime.outbox();
            var typed = new TypedOutbox<>(outbox, (topic, value) -> bytes(value.id()), V2.class);

            var ids = transactions.inTx(c -> {
                var all = new ArrayList<UUID>();
                all.add(typed.send("orders", "k", new V2("1"), OWN));
                all.add(outbox.send(new OutboxMessage("audit", null, bytes("p"), OWN)));
                all.add(outbox.send(c, OutboxMessage.of("audit", "k", bytes("p"))));
                all.addAll(outbox.sendAll(List.of(typed.message("orders", "k", new V2("2"), OWN),
                        OutboxMessage.of("audit", "k", bytes("p")),
                        new OutboxMessage("kafka", null, bytes("p"), OWN))));
                return all;
            });

            assertThat(ids).extracting(id -> headers(transactions, config, id)).containsExactly(
                    List.of(H, CONTRACT, TRACE, LOCAL),
                    List.of(H, TRACE, LOCAL),
                    List.of(TRACE, LOCAL),
                    List.of(H, CONTRACT, TRACE, LOCAL),
                    List.of(TRACE, LOCAL),
                    List.of(H, TRACE));
        }
    }

    /** Without routes no write is marked, and a message's own {@code outbox-local} is dropped. */
    @Test
    void withoutRoutesNoWriteIsMarkedLocal() {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var config = new TestOutboxConfig().relayEnabled(false);
        try (var runtime = OutboxRuntime.builder(config, transactions, new RecordingHandler()).build()) {
            runtime.start();
            var outbox = runtime.outbox();
            var typed = new TypedOutbox<>(outbox, (topic, value) -> bytes(value.id()), V2.class);

            var ids = transactions.inTx(c -> {
                var all = new ArrayList<UUID>();
                all.add(typed.send("orders", "k", new V2("1"), OWN));
                all.add(outbox.send(OutboxMessage.of("orders", "k", bytes("p"))));
                all.addAll(outbox.sendAll(List.of(typed.message("orders", "k", new V2("2"), OWN),
                        new OutboxMessage("orders", null, bytes("p"), OWN))));
                return all;
            });

            assertThat(ids).extracting(id -> headers(transactions, config, id))
                    .containsExactly(List.of(H, CONTRACT), List.of(), List.of(H, CONTRACT), List.of(H));
        }
    }

    /** Redrive does not go through {@link Outbox}: a letter of a topic no route claims any more goes back in. */
    @Test
    void aRedriveToATopicNoRouteClaimsStillGoesIn() {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var config = new TestOutboxConfig().relayEnabled(false);
        // the release before: a handler took every topic, and it failed an orders record for good
        try (var before = OutboxRuntime.builder(config, transactions,
                new RecordingHandler().failAlways(r -> true, () -> new OutboxPermanentException("bad"))).build()) {
            before.start();
            transactions.inTx(c -> before.outbox().send(OutboxMessage.of("orders", "k", bytes("p"))));
            before.relay().drain();
        }

        try (var routeOnly = OutboxRuntime.builder(config, transactions, null)
                .route(new Claiming(Set.of("audit"))).build()) {
            routeOnly.start();
            assertThatThrownBy(() -> transactions.inTx(c -> routeOnly.outbox().send(
                    OutboxMessage.of("orders", "k", bytes("p"))))).isInstanceOf(IllegalArgumentException.class);
            var letters = routeOnly.admin().findDeadLetters(DeadLetterFilter.any(), null, 10).letters();
            assertThat(letters).extracting(DeadLetter::topic).containsExactly("orders");

            long written = rows(transactions, config); // the dead record's row stays until its partition goes
            assertThat(routeOnly.admin().redriveDeadLetters(List.of(letters.get(0).id()))).isOne();
            assertThat(rows(transactions, config)).as("the letter is back in the outbox").isEqualTo(written + 1);
        }
    }

    /**
     * A start that fails after the relay began beating (here the halt check's database call) leaves no instance
     * behind: once the start deleted the bootstrap's row, no beat writes it back, so an emergency reshard counts none.
     */
    @Test
    void aFailedStartLeavesNoInstanceBeating() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(100)); // a beat each 100 ms
        var ds = TestDataSources.newDataSource();
        var runtime = OutboxRuntime.builder(config, new RefusingHaltCheck(new JdbcTransactions(ds)),
                new RecordingHandler()).build();

        assertThatThrownBy(runtime::start).hasStackTraceContaining("no connection for the halt check");

        Thread.sleep(500);
        try (var c = ds.getConnection(); var rs = c.createStatement().executeQuery(
                "select count(*) from " + config.table() + "_instance")) {
            rs.next();
            assertThat(rs.getInt(1)).as("instance rows five beats after the failed start").isZero();
        }
    }

    /** The headers stored for the message {@code id}. */
    private static List<OutboxHeader> headers(JdbcTransactions transactions, TestOutboxConfig config, UUID id) {
        return transactions.inTx(c -> {
            try (var ps = c.prepareStatement("select headers from " + config.table() + " where message_id = ?")) {
                ps.setObject(1, id);
                try (var rs = ps.executeQuery()) {
                    assertThat(rs.next()).as("row of %s", id).isTrue();
                    return HeaderCodec.decode(rs.getBytes(1));
                }
            }
        });
    }

    private static long rows(JdbcTransactions transactions, TestOutboxConfig config) {
        return transactions.inTx(c -> TestSql.queryLong(c, "select count(*) from " + config.table()));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private record Claiming(Set<String> topics) implements OutboxTopicHandler {
        @Override
        public void handle(OutboxRecord record) {}
    }

    /** Refuses the connection {@code Maintenance.checkHalt} asks for, as a database down at that moment would. */
    private record RefusingHaltCheck(OutboxTransactions delegate) implements OutboxTransactions {
        @Override
        public Connection newConnection() throws SQLException {
            if (StackWalker.getInstance().walk(frames -> frames.anyMatch(f -> f.getMethodName()
                    .equals("checkHalt")))) {
                throw new SQLException("no connection for the halt check", "08001");
            }
            return delegate.newConnection();
        }

        @Nullable
        @Override
        public Connection currentConnection() {
            return delegate.currentConnection();
        }

        @Override
        public void afterCommit(Runnable action) throws SQLException {
            delegate.afterCommit(action);
        }

        @Override
        public String transactionHint() {
            return delegate.transactionHint();
        }
    }

    private record Recording(OutboxTransactions delegate, List<String> threads) implements OutboxTransactions {
        @Override
        public Connection newConnection() throws SQLException {
            threads.add(Thread.currentThread().getName());
            return delegate.newConnection();
        }

        @Nullable
        @Override
        public Connection currentConnection() {
            return delegate.currentConnection();
        }

        @Override
        public void afterCommit(Runnable action) throws SQLException {
            delegate.afterCommit(action);
        }

        @Override
        public String transactionHint() {
            return delegate.transactionHint();
        }
    }
}
