package io.github.dsudomoin.kora.outbox.kafka.wiring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxHeaders;
import io.github.dsudomoin.kora.outbox.kafka.TestKafka;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

@KoraAppTest(KafkaOverrideTestApp.class)
class KafkaOverrideIT implements KoraAppTestConfigModifier {

    private final String orders = TestKafka.createTopic(1);

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private Outbox outbox;

    @TestComponent
    @Tag(KafkaOverrideTestApp.Audit.class)
    private RecordingHandler audit;

    @Override
    public KoraConfigModification config() {
        return KafkaOutboxModuleIT.config("kafka-override");
    }

    @Test
    void routingHandlerReplacesTheDefault() throws Exception {
        var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(
                OutboxMessage.of(KafkaOverrideTestApp.AUDIT_TOPIC, "k", "a".getBytes(UTF_8)),
                OutboxMessage.of(orders, "k", "o".getBytes(UTF_8)))));

        var records = TestKafka.consumeUntil(orders, Duration.ofSeconds(30), r -> !r.isEmpty());

        assertThat(records).extracting(KafkaOutboxHeaders::messageId).containsExactly(ids.get(1));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (audit.delivered().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(audit.delivered()).extracting(OutboxRecord::messageId).containsExactly(ids.get(0));
    }
}
