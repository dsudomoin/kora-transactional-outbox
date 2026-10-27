package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.outbox.internal.PgInbox;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

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

    default Inbox inbox(JdbcConnectionFactory db, OutboxConfig config, OutboxMetrics metrics) {
        return new PgInbox(db, config, metrics);
    }
}
