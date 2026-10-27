package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * How the outbox joins the application's transactions: {@code send} and the inbox write on the connection of the
 * caller's transaction and wake the relay after its commit. The adapter of the application's transaction manager
 * implements it; {@link ConnectionSource#newConnection()} also serves the outbox's own work.
 */
public interface OutboxTransactions extends ConnectionSource {

    /** The connection of the caller's transaction on this thread; null outside one. */
    @Nullable
    Connection currentConnection();

    /**
     * Runs {@code action} once after the commit of the caller's transaction, never after a rollback; the same instance
     * registered twice in one transaction runs once. Only inside a transaction.
     */
    void afterCommit(Runnable action) throws SQLException;

    /** How the application opens a transaction, for error messages: "Outbox.send must be called inside {hint}". */
    String transactionHint();

    /** The exception the outbox throws for {@code error}; the adapter's own unchecked SQL exception, if it has one. */
    default RuntimeException translate(SQLException error) {
        return new OutboxSqlException(error);
    }
}
