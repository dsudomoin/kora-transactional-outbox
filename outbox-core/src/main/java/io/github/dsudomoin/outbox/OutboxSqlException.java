package io.github.dsudomoin.outbox;

import java.sql.SQLException;

/** A database error of the outbox, when the application's {@link OutboxTransactions} has no exception of its own. */
public final class OutboxSqlException extends RuntimeException {

    public OutboxSqlException(SQLException cause) {
        super(cause);
    }
}
