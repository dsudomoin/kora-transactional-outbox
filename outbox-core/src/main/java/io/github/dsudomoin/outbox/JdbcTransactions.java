package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transactions over a plain {@link DataSource}, for code without a framework: {@link #inTx} runs a call in a
 * transaction bound to the calling thread, in which {@code Outbox.send} and the inbox join it.
 * <pre>{@code
 * var transactions = new JdbcTransactions(dataSource);
 * transactions.inTx(c -> {
 *     orders.insert(c, order);
 *     return outbox.send(OutboxMessage.of("orders", order.userId(), payload));
 * });
 * }</pre>
 */
public final class JdbcTransactions implements OutboxTransactions {

    private static final Logger log = LoggerFactory.getLogger(JdbcTransactions.class);

    private final DataSource dataSource;
    private final ThreadLocal<Transaction> current = new ThreadLocal<>();

    public JdbcTransactions(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Runs {@code call} in a transaction and commits it, then runs its after-commit actions; any throwable rolls it
     * back and reaches the caller, an {@link SQLException} as {@link OutboxSqlException}. Inside another {@code inTx}
     * on this thread, {@code call} joins that transaction.
     */
    public <T> T inTx(SqlCall<T> call) {
        var outer = current.get();
        if (outer != null) {
            try {
                return call.call(outer.connection);
            } catch (SQLException e) {
                throw translate(e);
            }
        }
        Transaction tx;
        T result;
        try (var c = dataSource.getConnection()) {
            tx = new Transaction(c);
            current.set(tx);
            try {
                c.setAutoCommit(false);
                result = call.call(c);
                c.commit();
            } catch (Throwable t) {
                rollback(c, t);
                throw t;
            } finally {
                current.remove();
                c.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw translate(e);
        }
        for (var action : tx.afterCommit) {
            try {
                action.run();
            } catch (RuntimeException e) { // committed: the caller must not take it for a failure and retry
                log.warn("outbox after-commit action failed; the transaction is committed", e);
            }
        }
        return result;
    }

    @Nullable
    @Override
    public Connection currentConnection() {
        var tx = current.get();
        return tx == null ? null : tx.connection;
    }

    @Override
    public void afterCommit(Runnable action) {
        var tx = current.get();
        if (tx == null) {
            throw new IllegalStateException("not inside JdbcTransactions.inTx(...)");
        }
        for (var registered : tx.afterCommit) {
            if (registered == action) {
                return;
            }
        }
        tx.afterCommit.add(action);
    }

    @Override
    public Connection newConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public String transactionHint() {
        return "JdbcTransactions.inTx(...)";
    }

    private static void rollback(Connection c, Throwable cause) {
        try {
            c.rollback();
        } catch (SQLException e) {
            cause.addSuppressed(e);
        }
    }

    /** A call in a transaction, on its connection. */
    @FunctionalInterface
    public interface SqlCall<T> {
        T call(Connection connection) throws SQLException;
    }

    private static final class Transaction {
        final Connection connection;
        final List<Runnable> afterCommit = new ArrayList<>(1);

        Transaction(Connection connection) {
            this.connection = connection;
        }
    }
}
