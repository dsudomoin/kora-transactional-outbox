package io.github.dsudomoin.outbox.kafka.wiring;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.DeadLetterDecision;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxModule;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxModule;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

/**
 * The app's own handler and dead-letter hook are single components of an {@code OutboxHandler} and an
 * {@code OutboxDeadLetterHandler} subtype, as a {@code @Component class Router implements OutboxHandler} would be: they
 * replace the module's defaults. (Factories, not {@code @Component} classes, which every app of this source set would
 * pick up.)
 */
@KoraApp
public interface KafkaReplaceTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, KafkaOutboxModule {

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    default RecordingHook outboxDeadLetterHandler() {
        return new RecordingHook();
    }

    /** Keeps every letter, recording it. */
    final class RecordingHook implements OutboxDeadLetterHandler {

        final List<DeadLetter> handed = new CopyOnWriteArrayList<>();

        @Override
        public List<DeadLetterDecision> handle(List<DeadLetter> letters) {
            handed.addAll(letters);
            return letters.stream().map(l -> DeadLetterDecision.keep()).toList();
        }
    }

    /** Application code: Kora leaves components that no root depends on out of the graph. */
    record AppCode(Outbox outbox) {}

    @Root
    default AppCode appCode(Outbox outbox) {
        return new AppCode(outbox);
    }
}
