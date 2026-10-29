package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxSerializer;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import ru.tinkoff.kora.json.common.JsonReader;
import ru.tinkoff.kora.json.common.JsonWriter;

/**
 * {@link OutboxSerializer} from Kora's {@link JsonWriter} and {@link OutboxDeserializer} from its {@link JsonReader},
 * for any {@code T} that has them ({@code @Json} classes), so {@link TypedOutbox TypedOutbox&lt;T&gt;} and
 * {@link TypedOutboxTopicHandler TypedOutboxTopicHandler&lt;T&gt;} need no hand-written codec. An explicit
 * {@code OutboxSerializer<T>} or {@code OutboxDeserializer<T>} component of the app still wins for its {@code T}.
 * Needs {@code ru.tinkoff.kora:json-common} on the classpath (any Kora JSON module brings it).
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
 *
 * // and where they are delivered:
 * @Component
 * public final class OrdersHandler extends TypedOutboxTopicHandler<OrderCreated> {
 *     public OrdersHandler(OutboxDeserializer<OrderCreated> deserializer) {
 *         super(Set.of("orders"), deserializer);
 *     }
 *
 *     @Override
 *     protected void handle(OrderCreated event, OutboxRecord record) { ... }
 * }
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
 *
 * @Component
 * class OrdersHandler(deserializer: OutboxDeserializer<OrderCreated>) :
 *     TypedOutboxTopicHandler<OrderCreated>(setOf("orders"), deserializer) {
 *     override fun handle(value: OrderCreated, record: OutboxRecord) { ... }
 * }
 * }</pre>
 */
public interface OutboxJsonModule {

    default <T> OutboxSerializer<T> jsonOutboxSerializer(JsonWriter<T> writer) {
        return (topic, value) -> writer.toByteArray(value);
    }

    default <T> OutboxDeserializer<T> jsonOutboxDeserializer(JsonReader<T> reader) {
        return (topic, payload) -> reader.read(payload);
    }
}
