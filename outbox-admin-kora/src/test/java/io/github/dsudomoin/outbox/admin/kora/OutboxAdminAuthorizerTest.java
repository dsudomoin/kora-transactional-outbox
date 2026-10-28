package io.github.dsudomoin.outbox.admin.kora;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.admin.kora.OutboxAdminAuthorizer.Allow;
import io.github.dsudomoin.outbox.admin.kora.OutboxAdminAuthorizer.Deny;
import org.junit.jupiter.api.Test;

class OutboxAdminAuthorizerTest {

    @Test
    void denyIsA401OrA403WithAMessage() {
        assertThat(OutboxAdminAuthorizer.unauthorized("who?")).isEqualTo(new Deny(401, "who?"));
        assertThat(OutboxAdminAuthorizer.forbidden("no")).isEqualTo(new Deny(403, "no"));
        for (int status : new int[] {200, 400, 404, 500}) {
            assertThatThrownBy(() -> new Deny(status, "x")).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> OutboxAdminAuthorizer.forbidden(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void allowNeedsAnIdentity() {
        assertThat(OutboxAdminAuthorizer.allow("ops")).isEqualTo(new Allow("ops"));
        assertThatThrownBy(() -> OutboxAdminAuthorizer.allow(null)).isInstanceOf(NullPointerException.class);
    }
}
