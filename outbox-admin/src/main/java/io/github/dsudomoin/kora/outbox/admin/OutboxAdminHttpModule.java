package io.github.dsudomoin.kora.outbox.admin;

import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiControllerModule;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiDelegate;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;

/**
 * The admin HTTP API on the app's public HTTP server, routes under {@code /kora-outbox/admin/v1}; contract at
 * {@code openapi/kora-outbox-admin.yaml} on the classpath. Needs {@code OutboxModule}, an HTTP server module, a JSON
 * module and one {@link OutboxAdminAuthorizer} component.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, UndertowHttpServerModule, JsonModule,
 *         OutboxAdminHttpModule {}
 * }</pre>
 */
public interface OutboxAdminHttpModule extends OutboxAdminApiControllerModule {

    default OutboxAdminInterceptor outboxAdminInterceptor(OutboxAdminAuthorizer authorizer) {
        return new OutboxAdminInterceptor(authorizer);
    }

    default OutboxAdminApiDelegate outboxAdminDelegate(Maintenance maintenance) {
        return new OutboxAdminDelegate(maintenance);
    }
}
