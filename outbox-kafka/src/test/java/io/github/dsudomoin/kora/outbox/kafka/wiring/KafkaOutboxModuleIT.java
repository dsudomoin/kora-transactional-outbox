package io.github.dsudomoin.kora.outbox.kafka.wiring;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxHeaders;
import io.github.dsudomoin.kora.outbox.kafka.TestKafka;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
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
    private final String table = TestDb.uniqueTable();

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private Outbox outbox;

    @Override
    public KoraConfigModification config() {
        return config("kafka-wiring", table, "");
    }

    /** Test DB and broker, a fresh outbox table. */
    static KoraConfigModification config(String poolName) {
        return config(poolName, TestDb.uniqueTable(), "");
    }

    /** Test DB and broker, outbox table {@code table}; {@code kafka} adds keys to {@code outbox.kafka}. */
    static KoraConfigModification config(String poolName, String table, String kafka) {
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
                  kafka {
                    driverProperties { "bootstrap.servers" = "%s" }
                    %s
                  }
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), poolName, table,
                TestKafka.bootstrapServers(), kafka));
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

    /** No {@code outbox.kafka.deadLetterTopic}: a record Kafka refuses for good is kept in the dead table. */
    @Test
    void withoutDeadLetterTopicLettersAreKept() throws Exception {
        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(
                OutboxMessage.of(topic, "k", new byte[2 * 1024 * 1024]))); // over the default max.request.size

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        var status = status(id);
        while (!"kept".equals(status) && System.nanoTime() < deadline) {
            Thread.sleep(20);
            status = status(id);
        }
        assertThat(status).isEqualTo("kept");
    }

    @Nullable
    private String status(UUID messageId) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select max(status) from " + table + "_dead where message_id = ?::uuid",
                    messageId.toString());
        }
    }
}
