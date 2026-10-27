package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;

/** Bootstraps behind a transaction-pooling proxy, stubbed: every statement reports another backend. */
class TransactionPoolingTest {

    private static final String DETECTED =
            "needs a direct or session-pooled connection (transaction-pooling proxy detected)";

    private final List<String> sql = new ArrayList<>();

    @Test
    void outboxBootstrapFailsBeforeAnySessionStatement() {
        var schema = new OutboxSchema(pooled(), new TestOutboxConfig());

        assertThatThrownBy(schema::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outbox bootstrap")
                .hasMessageContaining(DETECTED);
        assertThat(sql).as("no lock or setting left on some server session").containsOnly("select pg_backend_pid()");
    }

    @Test
    void inboxBootstrapFailsBeforeAnySessionStatement() {
        var inbox = new PgInbox(pooled(), new TestOutboxConfig(), new OutboxMetrics(null));

        assertThatThrownBy(inbox::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inbox bootstrap")
                .hasMessageContaining(DETECTED);
        assertThat(sql).containsOnly("select pg_backend_pid()");
    }

    /** Connections whose statements each run in another server session; any statement but the pid query fails. */
    private JdbcConnectionFactory pooled() {
        var pid = new AtomicInteger(1000);
        return new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Connection currentConnection() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Connection newConnection() {
                return stub(Connection.class, (name, args) -> switch (name) {
                    case "getAutoCommit" -> true;
                    case "setAutoCommit", "close" -> null;
                    case "createStatement" -> statement(pid);
                    default -> throw new AssertionError("Connection." + name);
                });
            }

            @Override
            public DataBaseTelemetry telemetry() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Statement statement(AtomicInteger pid) {
        return stub(Statement.class, (name, args) -> switch (name) {
            case "executeQuery", "execute" -> {
                sql.add((String) args[0]);
                if (!args[0].equals("select pg_backend_pid()")) {
                    throw new AssertionError("statement " + args[0]);
                }
                int value = pid.incrementAndGet();
                yield name.equals("execute") ? true : stub(ResultSet.class, (rsName, rsArgs) -> switch (rsName) {
                    case "next" -> true;
                    case "getInt" -> value;
                    case "getLong" -> (long) value;
                    case "close" -> null;
                    default -> throw new AssertionError("ResultSet." + rsName);
                });
            }
            case "close" -> null;
            default -> throw new AssertionError("Statement." + name);
        });
    }

    private interface Answer {
        Object answer(String method, Object[] args);
    }

    private static <T> T stub(Class<T> type, Answer answer) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> answer.answer(method.getName(), args)));
    }
}
