package io.github.dsudomoin.outbox.kafka.wiring;

import static io.github.dsudomoin.outbox.kafka.wiring.RoutedIT.await;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.kafka.wiring.RoutedAuditHandler.AuditEvent;
import io.github.dsudomoin.outbox.kafka.wiring.RoutedNoKafkaTestApp.AppCode;
import io.github.dsudomoin.outbox.testing.TestDb;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

@KoraAppTest(RoutedNoKafkaTestApp.class)
class RoutedNoKafkaIT implements KoraAppTestConfigModifier {

    private final String table = TestDb.uniqueTable();

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private AppCode app;

    @TestComponent
    private RoutedAuditHandler audit;

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "routed-no-kafka"
                }
                outbox {
                  table = %s
                  shards = 4
                  relay { workers = 2, minTickInterval = 5ms, idlePollInterval = 100ms, headFailuresBeforeDivert = 1 }
                  partitions { xidsPerPartition = 1000000, premake = 4 }
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), table));
    }

    @Test
    void componentsDeliverWithoutAnyOutboxHandler() throws Exception {
        var unclaimed = db.inTx((SqlFunction0<UUID>) () -> {
            app.audit().send(RoutedAuditHandler.TOPIC, "k", new AuditEvent("login"));
            return app.outbox().send(OutboxMessage.of("routed-unclaimed", "k", "u".getBytes(UTF_8)));
        });

        await(() -> !audit.events.isEmpty());
        assertThat(audit.events).containsExactly(new AuditEvent("login"));
        await(() -> lastError(unclaimed) != null); // no route claims it: it waits for an instance with one
        assertThat(lastError(unclaimed))
                .isEqualTo("waiting (no_route): no handler for topic routed-unclaimed on this instance");
    }

    private String lastError(UUID messageId) {
        try (var c = TestDb.connect()) {
            return queryString(c, "select last_error from " + table + "_retry where message_id = ?::uuid",
                    messageId.toString());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
