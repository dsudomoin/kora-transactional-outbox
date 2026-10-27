package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.internal.PgInbox;
import ru.tinkoff.kora.application.graph.LifecycleWrapper;

/**
 * Consumer-side dedup ({@link Inbox}) without the outbox: no {@link OutboxHandler}, no relay.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, InboxModule {}
 * }</pre>
 * Config at {@code outbox.inbox}. The inbox is built, and its table created, only when a component uses {@link Inbox}.
 * An app that also sends messages extends {@link OutboxModule} as well.
 */
public interface InboxModule extends OutboxCommonModule {

    default LifecycleWrapper<Inbox> inbox(OutboxTransactions db, OutboxConfig config, OutboxTelemetry telemetry) {
        var inbox = new PgInbox(db, config, telemetry);
        return new LifecycleWrapper<>(inbox, i -> inbox.start(), i -> inbox.close());
    }
}
