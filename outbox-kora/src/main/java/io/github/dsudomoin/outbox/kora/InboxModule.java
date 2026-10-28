package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.InboxRuntime;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import ru.tinkoff.kora.application.graph.LifecycleWrapper;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * Consumer-side dedup ({@link Inbox}) without the outbox: no {@link OutboxHandler}, no relay.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, InboxModule {}
 * }</pre>
 * Config at {@code outbox.inbox}. The inbox is built, and its table created, only when a component uses {@link Inbox}.
 * With a {@link OutboxOwner}-tagged {@code JdbcConnectionFactory} its startup and maintenance run through that one. An
 * app that also sends messages extends {@link OutboxModule} as well.
 */
public interface InboxModule extends OutboxCommonModule {

    default LifecycleWrapper<InboxRuntime> inboxRuntime(OutboxTransactions db, OutboxConfig config,
            OutboxTelemetry telemetry, @Tag(OutboxOwner.class) @Nullable JdbcConnectionFactory owner) {
        var builder = InboxRuntime.builder(config, db).telemetry(telemetry);
        if (owner != null) {
            builder.ownerConnections(owner::newConnection);
        }
        return new LifecycleWrapper<>(builder.build(), InboxRuntime::start, InboxRuntime::close);
    }

    default Inbox inbox(InboxRuntime runtime) {
        return runtime.inbox();
    }
}
