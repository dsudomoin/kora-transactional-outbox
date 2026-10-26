package io.github.dsudomoin.kora.outbox.kafka.wiring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxHeaders;
import io.github.dsudomoin.kora.outbox.kafka.TestKafka;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

@KoraAppTest(KafkaTestApp.class)
class KafkaOutboxModuleIT implements KoraAppTestConfigModifier {

    private final String topic = TestKafka.createTopic(1);

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private Outbox outbox;

    @Override
    public KoraConfigModification config() {
        return config("kafka-wiring");
    }

    /** Test DB and broker, a fresh outbox table. */
    static KoraConfigModification config(String poolName) {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "%s"
                }
                outbox {
                  table = %s
                  shards = 4
                  relay { workers = 2, minTickInterval = 5ms, idlePollInterval = 100ms }
                  partitions { xidsPerPartition = 1000000, premake = 4 }
                  kafka.driverProperties { "bootstrap.servers" = "%s" }
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), poolName, TestDb.uniqueTable(),
                TestKafka.bootstrapServers()));
    }

    @Test
    void graphBuildsAndDelivers() throws Exception {
        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of(topic, "k", "v".getBytes(UTF_8))));

        var records = TestKafka.consumeUntil(topic, Duration.ofSeconds(30), r -> !r.isEmpty());

        assertThat(records).singleElement().satisfies(r -> {
            assertThat(KafkaOutboxHeaders.messageId(r)).isEqualTo(id);
            assertThat(r.value()).isEqualTo("v".getBytes(UTF_8));
        });
    }
}
