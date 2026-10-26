package io.github.dsudomoin.kora.outbox.wiring;

import io.github.dsudomoin.kora.outbox.Inbox;
import io.github.dsudomoin.kora.outbox.InboxModule;
import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxModule;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

@KoraApp
public interface CoreTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, InboxModule {

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox, Inbox inbox) {}

    @Root
    default AppCode appCode(Outbox outbox, Inbox inbox) {
        return new AppCode(outbox, inbox);
    }
}
