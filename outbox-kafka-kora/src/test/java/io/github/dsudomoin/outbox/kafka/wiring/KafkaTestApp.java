package io.github.dsudomoin.outbox.kafka.wiring;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.kora.OutboxModule;
import io.github.dsudomoin.outbox.kafka.kora.KafkaOutboxModule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

@KoraApp
public interface KafkaTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, KafkaOutboxModule {

    /** Turns the outbox metrics on. */
    default MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox) {}

    @Root
    default AppCode appCode(Outbox outbox) {
        return new AppCode(outbox);
    }
}
