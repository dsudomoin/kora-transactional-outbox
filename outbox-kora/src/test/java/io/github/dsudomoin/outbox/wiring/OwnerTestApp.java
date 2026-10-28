package io.github.dsudomoin.outbox.wiring;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.kora.OutboxModule;
import io.github.dsudomoin.outbox.kora.OutboxOwner;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.common.Config;
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetryFactory;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseConfig;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

/** The app's pool at {@code db} logs in as a DML-only role; a second pool, tagged, as the owner of the tables. */
@KoraApp
public interface OwnerTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule {

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox) {}

    @Root
    default AppCode appCode(Outbox outbox) {
        return new AppCode(outbox);
    }

    @Tag(OutboxOwner.class)
    default JdbcDatabase outboxOwnerDatabase(Config config, ConfigValueExtractor<JdbcDatabaseConfig> extractor,
            DataBaseTelemetryFactory telemetry) {
        return new JdbcDatabase(extractor.extract(config.get("ownerDb")), telemetry);
    }
}
