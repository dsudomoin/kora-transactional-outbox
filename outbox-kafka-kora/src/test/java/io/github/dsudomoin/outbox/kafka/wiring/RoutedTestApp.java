package io.github.dsudomoin.outbox.kafka.wiring;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.kafka.kora.KafkaOutboxModule;
import io.github.dsudomoin.outbox.kafka.wiring.RoutedAuditHandler.AuditEvent;
import io.github.dsudomoin.outbox.kora.OutboxModule;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

/** Topic handler components in front of the module's Kafka handler: no factory, no tag. */
@KoraApp
public interface RoutedTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, KafkaOutboxModule {

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox, TypedOutbox<AuditEvent> audit) {}

    @Root
    default AppCode appCode(Outbox outbox, TypedOutbox<AuditEvent> audit) {
        return new AppCode(outbox, audit);
    }
}
