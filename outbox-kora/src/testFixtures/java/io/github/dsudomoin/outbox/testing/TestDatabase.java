package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.kora.KoraOutboxTransactions;
import java.sql.SQLException;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetryFactory;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseConfig;

/** A Kora pool that is also the outbox's binding to its transactions, the one the Kora modules give the outbox. */
public final class TestDatabase extends JdbcDatabase implements OutboxTransactions {

    private final KoraOutboxTransactions binding = new KoraOutboxTransactions(this);

    TestDatabase(JdbcDatabaseConfig config) {
        super(config, DataBaseTelemetryFactory.EMPTY);
    }

    @Override
    public void afterCommit(Runnable action) throws SQLException {
        binding.afterCommit(action);
    }

    @Override
    public String transactionHint() {
        return binding.transactionHint();
    }

    @Override
    public RuntimeException translate(SQLException error) {
        return binding.translate(error);
    }
}
