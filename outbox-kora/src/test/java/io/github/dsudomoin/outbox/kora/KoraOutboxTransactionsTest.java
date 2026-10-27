package io.github.dsudomoin.outbox.kora;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.TestDb;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlConsumer;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlRunnable;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

class KoraOutboxTransactionsTest {

    private final JdbcDatabase db = TestDb.newDatabase(2);
    private final KoraOutboxTransactions transactions = new KoraOutboxTransactions(db);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void currentConnectionIsInTxConnection() {
        db.inTx((SqlConsumer<Connection>) c -> assertThat(transactions.currentConnection()).isSameAs(c));
    }

    @Test
    void nullOutsideInTx() {
        assertThat(transactions.currentConnection()).isNull();
    }

    @Test
    void afterCommitRunsOncePerTransaction() {
        var runs = new AtomicInteger();
        Runnable action = runs::incrementAndGet;

        db.inTx((SqlRunnable) () -> {
            transactions.afterCommit(action);
            transactions.afterCommit(action);
            transactions.afterCommit(action);
            assertThat(runs).as("not before the commit").hasValue(0);
        });

        assertThat(runs).hasValue(1);
    }

    @Test
    void afterCommitNeverAfterRollback() {
        var runs = new AtomicInteger();

        assertThatThrownBy(() -> db.inTx((SqlRunnable) () -> {
            transactions.afterCommit(runs::incrementAndGet);
            throw new IllegalStateException("rollback");
        })).hasMessageContaining("rollback");

        assertThat(runs).hasValue(0);
    }

    @Test
    void afterCommitOutsideTransactionFails() {
        assertThatThrownBy(() -> transactions.afterCommit(() -> {})).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void translateGivesRuntimeSqlException() {
        var error = new SQLException("x");
        assertThat(transactions.translate(error)).isInstanceOf(RuntimeSqlException.class).hasCause(error);
    }

    @Test
    void hintNamesInTx() {
        assertThat(transactions.transactionHint()).isEqualTo("JdbcConnectionFactory.inTx(...)");
    }
}
