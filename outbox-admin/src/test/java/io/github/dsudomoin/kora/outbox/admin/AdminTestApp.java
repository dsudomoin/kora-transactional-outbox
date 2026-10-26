package io.github.dsudomoin.kora.outbox.admin;

import io.github.dsudomoin.kora.outbox.OutboxModule;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;
import ru.tinkoff.kora.http.common.HttpMethod;
import ru.tinkoff.kora.http.common.annotation.HttpRoute;
import ru.tinkoff.kora.http.server.common.HttpServerInterceptor;
import ru.tinkoff.kora.http.server.common.HttpServerModule;
import ru.tinkoff.kora.http.server.common.annotation.HttpController;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule;
import ru.tinkoff.kora.json.module.JsonModule;
import ru.tinkoff.kora.openapi.management.OpenApiManagementModule;

/** An app with its own route and global interceptor that gets the admin API by extending one module. */
@KoraApp
public interface AdminTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, UndertowHttpServerModule,
        JsonModule, OpenApiManagementModule, OutboxAdminHttpModule {

    @Component
    @HttpController
    final class HelloController {

        @HttpRoute(method = HttpMethod.GET, path = "/hello")
        public String hello() {
            return "hello";
        }
    }

    @Tag(HttpServerModule.class)
    default HttpServerInterceptor globalHeader() {
        return (context, request, chain) -> chain.process(context, request).thenApply(response -> {
            response.headers().set("X-Test-Global", "1");
            return response;
        });
    }

    default RecordingHandler outboxHandler() {
        return new RecordingHandler();
    }

    default OutboxAdminAuthorizer outboxAdminAuthorizer() {
        return request -> {
            var key = request.headers().getFirst("X-Api-Key");
            if (key == null) {
                return OutboxAdminAuthorizer.unauthorized("X-Api-Key required");
            }
            return key.equals("secret") ? OutboxAdminAuthorizer.allow("tester")
                    : OutboxAdminAuthorizer.forbidden("unknown key");
        };
    }
}
