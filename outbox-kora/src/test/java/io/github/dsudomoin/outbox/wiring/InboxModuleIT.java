package io.github.dsudomoin.outbox.wiring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.testing.TestDb;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

@KoraAppTest(InboxTestApp.class)
class InboxModuleIT implements KoraAppTestConfigModifier {

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private Inbox inbox;

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "inbox-wiring"
                }
                outbox.inbox.table = %s
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), TestDb.uniqueTable()));
    }

    @Test
    void inboxWorksWithoutOutboxHandler() throws Exception {
        var id = UuidV7.next();

        assertThat(db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id))).isTrue();
        assertThat(db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id))).isFalse();
    }
}
