package io.github.dsudomoin.outbox.wiring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.testing.TestDb;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

/**
 * The relay stops after the app's components that send, as Kora releases a component after the ones that depend on
 * it: the check runs once the test's graph is released.
 */
@KoraAppTest(value = ShutdownTestApp.class, components = {OutboxRelay.class, OutboxSchema.class}) // roots of the app, as at its start
class ShutdownOrderIT implements KoraAppTestConfigModifier {

    @TestComponent
    private ShutdownTestApp.Probe probe;

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "outbox-shutdown"
                }
                outbox {
                  table = %s
                  shards = 4
                  inbox.table = %s
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), TestDb.uniqueTable(),
                TestDb.uniqueTable()));
    }

    @Test
    void appStarts() {
        assertThat(probe).isNotNull();
    }

    @AfterAll
    static void relayStillRanWhenTheComponentsThatSendStopped() {
        assertThat(ShutdownTestApp.Probe.SEEN).containsExactly("relay running true");
    }
}
