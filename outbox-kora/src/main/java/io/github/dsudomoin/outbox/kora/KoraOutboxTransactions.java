package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import ru.tinkoff.kora.database.jdbc.ConnectionContext;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

/**
 * The outbox in the transactions of Kora's {@link JdbcConnectionFactory#inTx}, its SQL errors as
 * {@link RuntimeSqlException}.
 */
public final class KoraOutboxTransactions implements OutboxTransactions {

    private final JdbcConnectionFactory db;

    public KoraOutboxTransactions(JdbcConnectionFactory db) {
        this.db = db;
    }

    @Override
    public Connection newConnection() {
        return db.newConnection();
    }

    @Nullable
    @Override
    public Connection currentConnection() {
        var ctx = db.currentConnectionContext();
        return ctx == null ? null : ctx.connection();
    }

    /** A Kora post-commit action of the transaction's context, added once per {@code action} instance. */
    @Override
    public void afterCommit(Runnable action) throws SQLException {
        var ctx = db.currentConnectionContext();
        if (ctx == null) {
            throw new IllegalStateException("not inside JdbcConnectionFactory.inTx(...)");
        }
        var hook = new AfterCommit(action);
        if (!ctx.postCommitActions().contains(hook)) {
            ctx.addPostCommitAction(hook);
        }
    }

    @Override
    public String transactionHint() {
        return "JdbcConnectionFactory.inTx(...)";
    }

    @Override
    public RuntimeException translate(SQLException error) {
        return new RuntimeSqlException(error);
    }

    /** Equal for the same {@code action} instance, so the context's list holds one per transaction. */
    private record AfterCommit(Runnable action) implements ConnectionContext.PostCommitAction {
        @Override
        public void run(Connection connection) {
            action.run();
        }
    }
}
