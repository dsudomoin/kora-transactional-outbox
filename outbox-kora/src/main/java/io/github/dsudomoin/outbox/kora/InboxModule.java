package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.InboxRuntime;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTransactions;
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

    default LifecycleWrapper<InboxRuntime> inboxRuntime(OutboxTransactions db, OutboxConfig config,
            OutboxTelemetry telemetry) {
        var runtime = InboxRuntime.builder(config, db).telemetry(telemetry).build();
        return new LifecycleWrapper<>(runtime, InboxRuntime::start, InboxRuntime::close);
    }

    default Inbox inbox(InboxRuntime runtime) {
        return runtime.inbox();
    }
}
