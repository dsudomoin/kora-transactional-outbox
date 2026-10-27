package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.GuardedTelemetry;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/** A telemetry whose every hook throws, guarded as the outbox's wiring guards it, breaks neither send nor the relay. */
class TelemetryFailureIT {

    private final JdbcDatabase db = TestDb.newDatabase(10);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private OutboxRelay relay;

    @AfterEach
    void release() {
        if (relay != null) {
            relay.release();
        }
        db.release();
    }

    @Test
    void throwingTelemetryBreaksNeitherSendNorRelay() throws Exception {
        var telemetry = GuardedTelemetry.of(throwing(OutboxTelemetry.class));
        var schema = new OutboxSchema(db, config);
        schema.init();
        var maintenance = new Maintenance(db, schema.names(), config, telemetry);
        var deadLetters = new DeadLetterPass(db, schema.names(), config, OutboxDeadLetterHandler.NONE, telemetry);
        var handler = new RecordingHandler();
        relay = new OutboxRelay(db, config, schema, handler, telemetry, maintenance, deadLetters);
        relay.init();
        var outbox = new PgOutbox(db, schema, config.shards(), relay::wake, telemetry);
        var headers = List.of(new OutboxHeader("a", "1".getBytes(UTF_8)));
        var message = new OutboxMessage("t", "k".getBytes(UTF_8), "p".getBytes(UTF_8), headers);

        UUID id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(message));

        assertThat(id).isNotNull();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (handler.delivered().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
        assertThat(handler.delivered().get(0).headers()).as("stored and handed as sent").isEqualTo(headers);
        assertThat(relay.running()).isTrue();
    }

    /** Every method throws, except that the telemetry's hooks hand out an enqueue and a delivery that throw. */
    @SuppressWarnings("unchecked")
    private static <T> T throwing(Class<T> type) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getReturnType() == OutboxTelemetry.Enqueue.class) {
                return throwing(OutboxTelemetry.Enqueue.class);
            }
            if (method.getReturnType() == OutboxTelemetry.Delivery.class) {
                return throwing(OutboxTelemetry.Delivery.class);
            }
            throw new IllegalStateException("telemetry bug in " + method.getName());
        });
    }
}
