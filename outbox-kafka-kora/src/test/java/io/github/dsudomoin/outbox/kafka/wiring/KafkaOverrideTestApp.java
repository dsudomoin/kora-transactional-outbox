package io.github.dsudomoin.outbox.kafka.wiring;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.kora.OutboxModule;
import io.github.dsudomoin.outbox.kafka.kora.KafkaOutboxModule;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxSender;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import java.util.Map;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

/**
 * Mixed targets: a routing handler declared as exactly {@link OutboxHandler} sends {@value #AUDIT_TOPIC} to a
 * sub-handler and everything else to Kafka. The sub-handler is an {@code OutboxHandler} component too, so it needs a
 * tag: Kora 1.2.23 reports two resolved untagged {@code OutboxHandler} components as duplicates when the relay's
 * dependency is checked again after the routing handler resolved, whatever their declared types.
 */
@KoraApp
public interface KafkaOverrideTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, KafkaOutboxModule {

    String AUDIT_TOPIC = "audit";

    /** Tag of the audit sub-handler. */
    interface Audit {}

    @Tag(Audit.class)
    default RecordingHandler auditHandler() {
        return new RecordingHandler();
    }

    default OutboxHandler outboxHandler(KafkaOutboxSender kafka, @Tag(Audit.class) RecordingHandler audit) {
        return OutboxHandler.byTopic(Map.of(AUDIT_TOPIC, audit), kafka.handler());
    }

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox) {}

    @Root
    default AppCode appCode(Outbox outbox) {
        return new AppCode(outbox);
    }
}
