package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxContract;
import io.github.dsudomoin.outbox.OutboxIncompatibleContractException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.util.Set;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/** Postgres only: the outbox's writer guard names the publisher's declared value type. */
class KafkaOutboxPublisherGuardIT {

    @OutboxContract(2)
    record Created(String id) {}

    @Test
    void aRefusedSendNamesTheValueType() {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        try (var runtime = OutboxRuntime.builder(new TestOutboxConfig(), transactions, null)
                .route(new ReadsV1()).build()) {
            runtime.start();
            var publisher = new KafkaOutboxPublisher<>(runtime.outbox(), "orders", new StringSerializer(),
                    (topic, created) -> created.id().getBytes(UTF_8), Created.class);

            assertThatThrownBy(() -> transactions.inTx(c -> publisher.send("k", new Created("1"))))
                    .isInstanceOfSatisfying(OutboxIncompatibleContractException.class,
                            e -> assertThat(e.type()).isEqualTo(Created.class.getName()));
        }
    }

    /** This instance's relay reads v1 of {@code orders}. */
    private static final class ReadsV1 extends TypedOutboxTopicHandler<String> {

        ReadsV1() {
            super(Set.of("orders"), (topic, payload) -> new String(payload, UTF_8));
        }

        @Override
        protected void handle(String value, OutboxRecord record) {}
    }
}
