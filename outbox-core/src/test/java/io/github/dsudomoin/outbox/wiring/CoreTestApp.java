package io.github.dsudomoin.outbox.wiring;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.DeadLetterDecision;
import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.InboxModule;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxModule;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

@KoraApp
public interface CoreTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, InboxModule {

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    /**
     * The app's own dead-letter hook component. A factory, not a {@code @Component} class: Kora puts every
     * {@code @Component} class of the module into every app of it, and {@link TypedTestApp} has none.
     */
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
    record AppCode(Outbox outbox, Inbox inbox, OutboxAdmin admin) {}

    @Root
    default AppCode appCode(Outbox outbox, Inbox inbox, OutboxAdmin admin) {
        return new AppCode(outbox, inbox, admin);
    }
}
