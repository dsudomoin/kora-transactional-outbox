package io.github.dsudomoin.kora.outbox.wiring;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.Inbox;
import io.github.dsudomoin.kora.outbox.Outbox;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.OutboxRelay;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

/** One graph per test method, with fresh tables: each test instance makes its own config. */
@KoraAppTest(CoreTestApp.class)
class OutboxModuleIT implements KoraAppTestConfigModifier {

    private final String table = TestDb.uniqueTable();
    private final String inboxTable = TestDb.uniqueTable();

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private Outbox outbox;

    @TestComponent
    private RecordingHandler handler;

    @Override
    public KoraConfigModification config() {
        return config(true);
    }

    private KoraConfigModification config(boolean relayEnabled) {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "outbox-wiring"
                }
                outbox {
                  table = %s
                  shards = 4
                  relay { enabled = %s, workers = 2, minTickInterval = 5ms, idlePollInterval = 30s }
                  partitions { xidsPerPartition = 1000000, premake = 4 }
                  inbox.table = %s
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), table, relayEnabled, inboxTable));
    }

    @Test
    void sendInsideInTxIsDeliveredThroughGraph() throws Exception {
        var first = send();
        await(() -> handler.delivered().size() == 1); // maybe by the relay's start-up pass
        var second = send();

        await(() -> handler.delivered().size() == 2); // workers idle now: within 10 s only the wake-up delivers, not
        // the 30 s idle poll
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(first, second);
        assertThat(exists(inboxTable)).as("the outbox and its relay do not build the inbox").isFalse();
    }

    @Test
    void inboxBuiltLazilyAndWorks(@TestComponent Inbox inbox) throws Exception {
        assertThat(exists(inboxTable)).as("created by the graph's init of the inbox").isTrue();
        var id = send();

        assertThat(db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id))).isTrue();
        assertThat(db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id))).isFalse();
    }

    @Nested
    class RelayDisabled implements KoraAppTestConfigModifier {

        @TestComponent
        private OutboxRelay relay;

        @Override
        public KoraConfigModification config() {
            return OutboxModuleIT.this.config(false);
        }

        @Test
        void relayDisabledByConfigDeliversOnlyOnDrain() throws Exception {
            var id = send();
            Thread.sleep(300); // a running relay would deliver on the commit's wake-up

            assertThat(relay.running()).isFalse();
            assertThat(handler.delivered()).isEmpty();
            assertThat(relay.drain()).isEqualTo(1);
            assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
        }
    }

    private UUID send() throws SQLException {
        return db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("orders", "k", "p".getBytes(UTF_8))));
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select to_regclass(?)::text", table) != null;
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10 s");
            }
            Thread.sleep(5);
        }
    }
}
