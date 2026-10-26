package io.github.dsudomoin.kora.outbox.wiring;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.kora.outbox.OutboxModule;
import io.github.dsudomoin.kora.outbox.OutboxSerializer;
import io.github.dsudomoin.kora.outbox.TypedOutbox;
import io.github.dsudomoin.kora.outbox.json.OutboxJsonModule;
import io.github.dsudomoin.kora.outbox.protobuf.OutboxProtobufModule;
import io.github.dsudomoin.kora.outbox.protobuf.ProtobufOutbox;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;
import ru.tinkoff.kora.json.common.annotation.Json;

/**
 * JSON template and an explicit serializer in one app: compiles only if Kora prefers the explicit
 * {@code OutboxSerializer<Custom>} over the template, although {@code Custom} is {@code @Json} too.
 */
@KoraApp
public interface TypedTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, OutboxJsonModule,
        OutboxProtobufModule {

    /** Sent as JSON: no serializer of its own. */
    @Json
    record OrderCreated(long id, String userId) {}

    @Json
    record Custom(String text) {}

    default OutboxSerializer<Custom> customSerializer() {
        return (topic, value) -> (topic + ":" + value.text()).getBytes(UTF_8);
    }

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(TypedOutbox<OrderCreated> orders, TypedOutbox<Custom> custom, ProtobufOutbox protobuf) {}

    @Root
    default AppCode appCode(TypedOutbox<OrderCreated> orders, TypedOutbox<Custom> custom, ProtobufOutbox protobuf) {
        return new AppCode(orders, custom, protobuf);
    }
}
