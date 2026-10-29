package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.InternalApi;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxSerializer;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;
import ru.tinkoff.kora.application.graph.All;
import ru.tinkoff.kora.application.graph.LifecycleWrapper;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * Wires the outbox into a Kora app. Records go to the app's {@link OutboxTopicHandler} components, each for the topics
 * it claims, and every other topic to its one untagged {@link OutboxHandler} ({@code KafkaOutboxModule}'s or the app's
 * own); either may be absent, not both.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule {}
 *
 * // sending and deduplicating on consumption as well
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, InboxModule {}
 * }</pre>
 * A topic handler is a component of its own, no factory or tag needed; two of them claiming one topic fail the start:
 * <pre>{@code
 * @Component
 * public final class AuditHandler implements OutboxTopicHandler {
 *     public Set<String> topics() { return Set.of("audit"); }
 *     public void handle(OutboxRecord record) throws Exception { client.post(record.payload()); }
 * }
 * }</pre>
 * Config at {@code outbox}. The outbox is a root: started with the app, tables first, then the relay; with
 * {@code outbox.relay.enabled = false} the relay only serves {@link OutboxRelay#drain()}. Tracing is on when the app
 * has a {@link Tracer}. Dead letters go to the app's {@link OutboxDeadLetterHandler} component when it has one, and
 * are kept for humans otherwise. With a {@link OutboxOwner}-tagged {@code JdbcConnectionFactory} the outbox's DDL,
 * session locks and recoveries run through that one ({@code outbox.schema.bootstrap = false}). An app that only
 * deduplicates uses {@link InboxModule} alone.
 */
public interface OutboxModule extends OutboxCommonModule {

    @Root
    default LifecycleWrapper<OutboxRuntime> outboxRuntime(OutboxConfig config, OutboxTransactions db,
            @Nullable OutboxHandler handler, All<OutboxTopicHandler> routes, @Nullable OutboxDeadLetterHandler hook,
            OutboxTelemetry telemetry, @Tag(OutboxOwner.class) @Nullable JdbcConnectionFactory owner) {
        var builder = OutboxRuntime.builder(config, db, handler).telemetry(telemetry);
        routes.forEach(builder::route);
        if (hook != null) {
            builder.deadLetterHandler(hook);
        }
        if (owner != null) {
            builder.ownerConnections(owner::newConnection);
        }
        return new LifecycleWrapper<>(builder.build(), OutboxRuntime::start, OutboxRuntime::close);
    }

    /**
     * A root, as it was before the runtime; so are the schema, maintenance and dead-letter pass it depended on. The
     * runtime starts and stops them: Kora would close a node nothing depends on as soon as the app starts stopping,
     * while the components that send are still at work.
     */
    @Root
    default LifecycleWrapper<OutboxRelay> outboxRelay(OutboxRuntime runtime) {
        return new LifecycleWrapper<>(runtime.relay(), r -> {}, r -> {});
    }

    default Outbox outbox(OutboxRuntime runtime) {
        return runtime.outbox();
    }

    /** For the service's own code; nothing runs unless it is called. */
    default OutboxAdmin outboxAdmin(OutboxRuntime runtime) {
        return runtime.admin();
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    @Root
    default LifecycleWrapper<OutboxSchema> outboxSchema(OutboxRuntime runtime) {
        return new LifecycleWrapper<>(runtime.schema(), s -> {}, s -> {});
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    @Root
    default Maintenance outboxMaintenance(OutboxRuntime runtime) {
        return runtime.maintenance();
    }

    /** Internal type, unstable: for the adapters of this repository. The relay runs it and closes it. */
    @InternalApi
    @Root
    default DeadLetterPass outboxDeadLetterPass(OutboxRuntime runtime) {
        return runtime.deadLetterPass();
    }

    /** {@link TypedOutbox} for every {@code T} the app has an {@link OutboxSerializer} for. */
    default <T> TypedOutbox<T> typedOutbox(Outbox outbox, OutboxSerializer<T> serializer) {
        return new TypedOutbox<>(outbox, serializer);
    }
}
