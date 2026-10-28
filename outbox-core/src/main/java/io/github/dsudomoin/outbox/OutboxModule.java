package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.internal.PgOutboxAdmin;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * Wires the outbox into a Kora app; the app provides one {@link OutboxHandler} component.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule {}
 *
 * // sending and deduplicating on consumption as well
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, InboxModule {}
 * }</pre>
 * Config at {@code outbox}; every component but the schema runs with {@link OutboxSchema#config()}, whose shard count
 * the bootstrap resolved. The relay is a root: with {@code outbox.relay.enabled = false} it only serves
 * {@link OutboxRelay#drain()}. Tracing is on when the app has a {@link Tracer}. Dead letters go to the app's
 * {@link OutboxDeadLetterHandler} component when it has one, and are kept for humans otherwise. An app that only
 * deduplicates uses {@link InboxModule} alone.
 */
public interface OutboxModule extends OutboxCommonModule {

    default OutboxSchema outboxSchema(JdbcConnectionFactory db, OutboxConfig config) {
        return new OutboxSchema(db, config);
    }

    default Maintenance outboxMaintenance(JdbcConnectionFactory db, OutboxSchema schema, OutboxMetrics metrics) {
        return new Maintenance(db, schema.names(), schema.config(), metrics);
    }

    /** The relay runs it and closes it; without a hook component every dead letter is kept. */
    default DeadLetterPass outboxDeadLetterPass(JdbcConnectionFactory db, OutboxSchema schema,
            @Nullable OutboxDeadLetterHandler hook, OutboxMetrics metrics) {
        return new DeadLetterPass(db, schema.names(), schema.config(),
                hook == null ? OutboxDeadLetterHandler.NONE : hook, metrics);
    }

    @Root
    default OutboxRelay outboxRelay(JdbcConnectionFactory db, OutboxSchema schema, OutboxHandler handler,
            OutboxMetrics metrics, @Nullable Tracer tracer, Maintenance maintenance, DeadLetterPass deadLetters) {
        return new OutboxRelay(db, schema.config(), schema, handler, metrics, tracer, maintenance, deadLetters);
    }

    default Outbox outbox(JdbcConnectionFactory db, OutboxSchema schema, OutboxRelay relay, OutboxMetrics metrics) {
        return new PgOutbox(db, schema, schema.config().shards(), relay::wake, metrics);
    }

    /** For the service's own code; nothing runs unless it is called. */
    default OutboxAdmin outboxAdmin(
            JdbcConnectionFactory db, OutboxSchema schema, Maintenance maintenance, OutboxRelay relay) {
        return new PgOutboxAdmin(db, schema.names(), schema.config(), maintenance, relay::wake);
    }

    /** {@link TypedOutbox} for every {@code T} the app has an {@link OutboxSerializer} for. */
    default <T> TypedOutbox<T> typedOutbox(Outbox outbox, OutboxSerializer<T> serializer) {
        return new TypedOutbox<>(outbox, serializer);
    }
}
