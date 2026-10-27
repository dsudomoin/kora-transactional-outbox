package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OutboxRuntimeTest {

    private final NoDatabase transactions = new NoDatabase();

    @Test
    void buildValidatesConfigWithoutDatabase() {
        var invalid = new TestOutboxConfig().shards(0);

        assertThatThrownBy(() -> OutboxRuntime.builder(invalid, transactions, new RecordingHandler()).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outbox.shards must be within 1..");
        assertThat(transactions.asked).hasValue(0);
    }

    @Test
    void closeWithoutStartIsNoOp() {
        var runtime = OutboxRuntime.builder(new TestOutboxConfig(), transactions, new RecordingHandler()).build();

        assertThatCode(runtime::close).doesNotThrowAnyException();
        assertThat(transactions.asked).hasValue(0);
    }

    @Test
    void componentsBeforeStartFail() {
        var runtime = OutboxRuntime.builder(new TestOutboxConfig(), transactions, new RecordingHandler()).build();

        assertThatThrownBy(runtime::outbox).isInstanceOf(IllegalStateException.class).hasMessageContaining("start()");
        assertThatThrownBy(runtime::relay).isInstanceOf(IllegalStateException.class).hasMessageContaining("start()");
        assertThatThrownBy(runtime::admin).isInstanceOf(IllegalStateException.class).hasMessageContaining("start()");
    }

    @Test
    void inboxBuildsWithoutDatabase() {
        var runtime = InboxRuntime.builder(new TestOutboxConfig(), transactions).build();

        assertThatThrownBy(runtime::inbox).isInstanceOf(IllegalStateException.class).hasMessageContaining("start()");
        assertThatCode(runtime::close).doesNotThrowAnyException();
        assertThat(transactions.asked).hasValue(0);
    }

    /** A binding that must not be asked for a connection. */
    private static final class NoDatabase implements OutboxTransactions {

        final AtomicInteger asked = new AtomicInteger();

        @Override
        public Connection newConnection() {
            asked.incrementAndGet();
            throw new AssertionError("no database access expected");
        }

        @Override
        public Connection currentConnection() {
            return null;
        }

        @Override
        public void afterCommit(Runnable action) {
            throw new AssertionError("no transaction expected");
        }

        @Override
        public String transactionHint() {
            return "a test transaction";
        }
    }
}
