package io.github.dsudomoin.outbox.wiring;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.kora.InboxModule;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

/** A consumer-only app: no {@code OutboxHandler}, so it compiles only if the inbox wiring needs no relay. */
@KoraApp
public interface InboxTestApp extends HoconConfigModule, JdbcDatabaseModule, InboxModule {

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Inbox inbox) {}

    @Root
    default AppCode appCode(Inbox inbox) {
        return new AppCode(inbox);
    }
}
