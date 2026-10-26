package io.github.dsudomoin.kora.outbox.kafka.wiring;

import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxModule;
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxModule;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

@KoraApp
public interface KafkaTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, KafkaOutboxModule {

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox) {}

    @Root
    default AppCode appCode(Outbox outbox) {
        return new AppCode(outbox);
    }
}
