package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class OutboxRuntimeIT {

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
