package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JdbcTransactionsIT {

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private OutboxRuntime runtime;

    @AfterEach
    void close() {
        if (runtime != null) {
            runtime.close();
        }
    }

    @Test
    void sendOutsideInTxNamesTheHint() {
        var config = new TestOutboxConfig().relayEnabled(false);
        runtime = OutboxRuntime.builder(config, transactions, new RecordingHandler()).build();
        runtime.start();

        assertThatThrownBy(() -> runtime.outbox().send(OutboxMessage.of("t", "p".getBytes(UTF_8))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("JdbcTransactions.inTx");
    }

    @Test
    void rollbackRunsNoAfterCommitAction() {
        var runs = new AtomicInteger();

        assertThatThrownBy(() -> transactions.inTx(c -> {
            transactions.afterCommit(runs::incrementAndGet);
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class).hasMessage("rollback");

        assertThat(runs).hasValue(0);
    }

    @Test
    void throwingAfterCommitActionDoesNotReachCaller() {
        var table = "jt_" + Long.toHexString(System.nanoTime());
        var later = new AtomicInteger();

        var result = transactions.inTx(c -> {
            try (var st = c.createStatement()) {
                st.execute("create table " + table + " (id int)");
            }
            transactions.afterCommit(() -> {
                throw new IllegalStateException("after commit");
            });
            transactions.afterCommit(later::incrementAndGet);
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(later).as("the next action still runs").hasValue(1);
        int rows = transactions.inTx(c -> {
            try (var st = c.createStatement(); var rs = st.executeQuery("select count(*) from " + table)) {
                rs.next();
                return rs.getInt(1);
            }
        });
        assertThat(rows).as("the commit stands").isZero();
    }

    @Test
    void nestedInTxJoinsOuter() {
        var commits = new AtomicInteger();

        transactions.inTx(outer -> {
            transactions.afterCommit(commits::incrementAndGet);
            transactions.inTx(inner -> {
                assertThat(inner).isSameAs(outer);
                assertThat(transactions.currentConnection()).isSameAs(outer);
                return null;
            });
            assertThat(commits).as("the inner call does not commit").hasValue(0);
            return null;
        });

        assertThat(commits).hasValue(1);
    }

    @Test
    void afterCommitDeduplicates() {
        var runs = new AtomicInteger();
        Runnable action = runs::incrementAndGet;

        transactions.inTx(c -> {
            transactions.afterCommit(action);
            transactions.afterCommit(action);
            return null;
        });

        assertThat(runs).hasValue(1);
    }

    @Test
    void currentConnectionIsNullOutsideInTx() {
        Connection inside = transactions.inTx(c -> c);

        assertThat(inside).isNotNull();
        assertThat(transactions.currentConnection()).isNull();
    }
}
