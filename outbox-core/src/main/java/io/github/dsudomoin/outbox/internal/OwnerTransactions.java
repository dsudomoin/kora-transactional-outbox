package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The application's binding with its new connections from the tables' owner: what the components that run DDL and
 * hold session locks open (the bootstrap, maintenance, recoveries); the caller's transaction stays the application's.
 */
public record OwnerTransactions(OutboxTransactions application, ConnectionSource owner) implements OutboxTransactions {

    /** {@code application} itself without an owner source. */
    public static OutboxTransactions of(OutboxTransactions application, @Nullable ConnectionSource owner) {
        return owner == null ? application : new OwnerTransactions(application, owner);
    }

    @Override
    public Connection newConnection() throws SQLException {
        return owner.newConnection();
    }

    @Nullable
    @Override
    public Connection currentConnection() {
        return application.currentConnection();
    }

    @Override
    public void afterCommit(Runnable action) throws SQLException {
        application.afterCommit(action);
    }

    @Override
    public String transactionHint() {
        return application.transactionHint();
    }

    @Override
    public RuntimeException translate(SQLException error) {
        return application.translate(error);
    }
}
