package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

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
