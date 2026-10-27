package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxSerializer;
import io.github.dsudomoin.outbox.TypedOutbox;
import ru.tinkoff.kora.json.common.JsonWriter;

/**
 * {@link OutboxSerializer} from Kora's {@link JsonWriter} for any {@code T} that has one ({@code @Json} classes), so
 * {@link TypedOutbox TypedOutbox&lt;T&gt;} needs no hand-written serializer. An explicit {@code OutboxSerializer<T>}
 * component of the app still wins for its {@code T}. Needs {@code ru.tinkoff.kora:json-common} on the classpath
 * (any Kora JSON module brings it).
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, JsonModule, OutboxModule, OutboxJsonModule {}
 *
 * @Json
 * record OrderCreated(long id, String userId) {}
 *
 * // in a component taking TypedOutbox<OrderCreated> events:
 * db.inTx(() -> {
 *     events.send("orders", userId, new OrderCreated(id, userId));
 * });
 * }</pre>
 * Kotlin:
 * <pre>{@code
 * @KoraApp
 * interface App : HoconConfigModule, JdbcDatabaseModule, JsonModule, OutboxModule, OutboxJsonModule
 *
 * @Json
 * data class OrderCreated(val id: Long, val userId: String)
 *
 * db.inTx<Unit> { _: Connection -> events.send("orders", userId, OrderCreated(id, userId)) }
 * }</pre>
 */
public interface OutboxJsonModule {

    default <T> OutboxSerializer<T> jsonOutboxSerializer(JsonWriter<T> writer) {
        return (topic, value) -> writer.toByteArray(value);
    }
}
