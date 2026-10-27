package io.github.dsudomoin.outbox;

import java.sql.Connection;
import java.sql.SQLException;

/** Where the outbox's own work gets its connections: relay, maintenance, dead letters, schema, admin. */
@FunctionalInterface
public interface ConnectionSource {

    /** A new connection, in autocommit, that the outbox closes; the outbox runs its own transactions on it. */
    Connection newConnection() throws SQLException;
}
