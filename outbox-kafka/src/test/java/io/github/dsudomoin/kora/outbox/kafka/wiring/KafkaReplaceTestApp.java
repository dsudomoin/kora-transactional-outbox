package io.github.dsudomoin.kora.outbox.kafka.wiring;

import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxModule;
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxModule;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

/**
 * The app's own handler is a single component of an {@code OutboxHandler} subtype, as a
 * {@code @Component class Router implements OutboxHandler} would be: it replaces the module's default.
 */
@KoraApp
public interface KafkaReplaceTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, KafkaOutboxModule {

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox) {}

    @Root
    default AppCode appCode(Outbox outbox) {
        return new AppCode(outbox);
    }
}
