package io.github.dsudomoin.kora.outbox.admin;

import io.github.dsudomoin.kora.outbox.OutboxModule;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;
import ru.tinkoff.kora.http.common.HttpMethod;
import ru.tinkoff.kora.http.common.annotation.HttpRoute;
import ru.tinkoff.kora.http.server.common.HttpServerInterceptor;
import ru.tinkoff.kora.http.server.common.HttpServerModule;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
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

    default KeyAuthorizer outboxAdminAuthorizer() {
        return new KeyAuthorizer();
    }

    /** Allows the key {@code secret} as {@code tester}; records the threads it runs on. */
    final class KeyAuthorizer implements OutboxAdminAuthorizer {

        final List<String> threads = new CopyOnWriteArrayList<>();

        @Override
        public Decision authorize(HttpServerRequest request) {
            threads.add(Thread.currentThread().getName());
            var key = request.headers().getFirst("X-Api-Key");
            if (key == null) {
                return OutboxAdminAuthorizer.unauthorized("X-Api-Key required");
            }
            return key.equals("secret") ? OutboxAdminAuthorizer.allow("tester")
                    : OutboxAdminAuthorizer.forbidden("unknown key");
        }
    }
}
