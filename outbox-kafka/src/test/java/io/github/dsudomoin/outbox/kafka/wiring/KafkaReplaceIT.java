package io.github.dsudomoin.outbox.kafka.wiring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDb;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

@KoraAppTest(KafkaReplaceTestApp.class)
class KafkaReplaceIT implements KoraAppTestConfigModifier {

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private Outbox outbox;

    @TestComponent
    private RecordingHandler handler;

    @Override
    public KoraConfigModification config() { // the module's dead-letter topic is on: the app's hook still replaces it
        return KafkaOutboxModuleIT.config("kafka-replace", TestDb.uniqueTable(), "deadLetterTopic = \"{topic}.dlq\"");
    }

    @Test
    void subtypeComponentReplacesTheDefault() throws Exception {
        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", "k", "v".getBytes(UTF_8))));

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (handler.delivered().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
    }

    @Test
    void appHookReplacesTheDefault(@TestComponent KafkaReplaceTestApp.RecordingHook hook) throws Exception {
        handler.failAlways(r -> "poison".equals(new String(r.key(), UTF_8)),
                () -> new OutboxPermanentException("bad payload"));

        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", "poison", "p".getBytes(UTF_8))));

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (hook.handed.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(hook.handed).extracting(DeadLetter::messageId).containsExactly(id);
    }
}
