package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import org.junit.jupiter.api.Test;

class OutboxSqlTest {

    private final TestOutboxConfig config = new TestOutboxConfig().table("app.orders_outbox").inboxTable("app.inbox");

    @Test
    void roleMustBeAnIdentifier() {
        for (var role : new String[] {"app; drop table t", "", "1app"}) {
            assertThatThrownBy(() -> OutboxSql.outbox(config, role)).as(role)
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> OutboxSql.inbox(config, role)).as(role)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(OutboxSql.outbox(config, "app_role")).contains("to app_role;");
    }

    @Test
    void noGrantsWithoutRole() {
        assertThat(OutboxSql.outbox(config, null)).doesNotContainIgnoringCase("grant");
        assertThat(OutboxSql.inbox(config, null)).doesNotContainIgnoringCase("grant");
    }

    @Test
    void grantsNameTheRole() {
        assertThat(OutboxSql.outbox(config, "app"))
                .contains("grant select, insert, update, delete on table app.orders_outbox, app.orders_outbox_tick,")
                .contains("grant select on sequence app.orders_outbox_id_seq to app;");
        assertThat(OutboxSql.inbox(config, "app"))
                .contains("grant select, insert, update, delete on table app.inbox to app;");
    }
}
