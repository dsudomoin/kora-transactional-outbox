package io.github.dsudomoin.outbox.kafka.wiring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxHeaders;
import io.github.dsudomoin.outbox.kafka.TestKafka;
import io.github.dsudomoin.outbox.kafka.wiring.RoutedAuditHandler.AuditEvent;
import io.github.dsudomoin.outbox.kafka.wiring.RoutedTestApp.AppCode;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

@KoraAppTest(RoutedTestApp.class)
class RoutedIT implements KoraAppTestConfigModifier {

    private final String orders = TestKafka.createTopic(1);

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private AppCode app;

    @TestComponent
    private RoutedAuditHandler audit;

    @TestComponent
    private RoutedBillingHandler billing;

    @Override
    public KoraConfigModification config() {
        return KafkaOutboxModuleIT.config("routed");
    }

    @Test
    void componentsTakeTheirTopicsAndKafkaTheRest() throws Exception {
        var ids = db.inTx((SqlFunction0<List<UUID>>) () -> List.of(
                app.audit().send(RoutedAuditHandler.TOPIC, "k", new AuditEvent("login")),
                app.outbox().send(OutboxMessage.of(RoutedBillingHandler.BILLING, "k", "b".getBytes(UTF_8))),
                app.outbox().send(OutboxMessage.of(orders, "k", "o".getBytes(UTF_8))),
                app.outbox().send(OutboxMessage.of(RoutedBillingHandler.INVOICES, "k", "i".getBytes(UTF_8)))));

        var records = TestKafka.consumeUntil(orders, Duration.ofSeconds(30), r -> !r.isEmpty());

        assertThat(records).extracting(KafkaOutboxHeaders::messageId).containsExactly(ids.get(2));
        await(() -> !audit.events.isEmpty() && billing.handled.size() == 2);
        assertThat(audit.events).as("decoded by the JsonReader-derived deserializer")
                .containsExactly(new AuditEvent("login"));
        assertThat(billing.handled).extracting(OutboxRecord::messageId) // two topics: two shards, no order
                .containsExactlyInAnyOrder(ids.get(1), ids.get(3));
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).as("within 10 s").isTrue();
    }
}
