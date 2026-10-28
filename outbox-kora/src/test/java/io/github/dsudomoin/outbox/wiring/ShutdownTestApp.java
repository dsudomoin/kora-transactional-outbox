package io.github.dsudomoin.outbox.wiring;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.InboxRuntime;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.kora.InboxModule;
import io.github.dsudomoin.outbox.kora.OutboxModule;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import ru.tinkoff.kora.application.graph.Lifecycle;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.annotation.Root;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;

/** An app component that uses the outbox and the inbox, and looks at them while the app stops. */
@KoraApp
public interface ShutdownTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, InboxModule {

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    @Root
    default Probe probe(Outbox outbox, Inbox inbox, OutboxRuntime runtime, InboxRuntime inboxRuntime) {
        return new Probe(runtime);
    }

    /** At its release, what a component still serving requests would find. */
    final class Probe implements Lifecycle {

        static final List<String> SEEN = new CopyOnWriteArrayList<>();
        private final OutboxRuntime runtime;

        Probe(OutboxRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        public void init() {}

        @Override
        public void release() throws InterruptedException {
            Thread.sleep(1_000); // a graceful drain: Kora releases the nodes nothing depends on in parallel
            SEEN.add("relay running " + runtime.relay().running());
        }
    }
}
