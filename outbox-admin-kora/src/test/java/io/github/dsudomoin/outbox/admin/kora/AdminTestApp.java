package io.github.dsudomoin.outbox.admin.kora;

import io.github.dsudomoin.outbox.OutboxModule;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiDelegate;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import ru.tinkoff.kora.application.graph.GraphInterceptor;
import io.github.dsudomoin.outbox.admin.kora.testapp.api.ApiSecurity;
import io.github.dsudomoin.outbox.admin.kora.testapp.api.AppApiDelegate;
import io.github.dsudomoin.outbox.admin.kora.testapp.api.AppApiResponses.AppDeadLettersApiResponse;
import io.github.dsudomoin.outbox.admin.kora.testapp.api.AppApiResponses.AppStatusApiResponse;
import io.github.dsudomoin.outbox.admin.kora.testapp.api.AppApiResponses.HelloApiResponse;
import java.util.concurrent.CompletableFuture;
import ru.tinkoff.kora.common.KoraApp;
import ru.tinkoff.kora.common.Principal;
import ru.tinkoff.kora.common.Tag;
import ru.tinkoff.kora.config.hocon.HoconConfigModule;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule;
import ru.tinkoff.kora.http.server.common.HttpServerInterceptor;
import ru.tinkoff.kora.http.server.common.HttpServerModule;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponseException;
import ru.tinkoff.kora.http.server.common.auth.HttpServerPrincipalExtractor;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule;
import ru.tinkoff.kora.json.module.JsonModule;
import ru.tinkoff.kora.openapi.management.OpenApiManagementModule;

/**
 * A contract-first app, its own API generated from {@code openapi/test-app.yaml} with a security scheme, models and
 * mappers of its own, two of its routes at the admin API's names without its prefix, and a global interceptor, that
 * gets the admin API by extending one module. It records every call that reaches the admin delegate.
 */
@KoraApp
public interface AdminTestApp extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, UndertowHttpServerModule,
        JsonModule, OpenApiManagementModule, OutboxAdminHttpModule {

    String APP_KEY = "app-secret";

    /** The app's own API: {@code /hello} open, {@code /status} and {@code /dead-letters} behind its {@code appKey}. */
    default AppApiDelegate appApiDelegate() {
        return new AppApiDelegate() {
            @Override
            public HelloApiResponse hello() {
                return new HelloApiResponse("hello");
            }

            @Override
            public AppStatusApiResponse appStatus() {
                return new AppStatusApiResponse.AppStatus200ApiResponse("app status");
            }

            @Override
            public AppDeadLettersApiResponse appDeadLetters() {
                return new AppDeadLettersApiResponse.AppDeadLetters200ApiResponse("app dead letters");
            }
        };
    }

    /** The app's {@code X-App-Key}, which knows nothing of the admin API's {@code X-Api-Key}. */
    @Tag(ApiSecurity.AppKey.class)
    default HttpServerPrincipalExtractor<Principal> appKeyExtractor() {
        return (request, key) -> APP_KEY.equals(key) ? CompletableFuture.completedFuture(new AppUser("app-user"))
                : CompletableFuture.failedFuture(HttpServerResponseException.of(401, "X-App-Key required"));
    }

    record AppUser(String name) implements Principal {}

    default DelegateCalls delegateCalls() {
        return new DelegateCalls();
    }

    /** Wraps the admin delegate so a test sees which operations ran. */
    default GraphInterceptor<OutboxAdminApiDelegate> recordingDelegate(DelegateCalls calls) {
        return new GraphInterceptor<>() {
            @Override
            public OutboxAdminApiDelegate init(OutboxAdminApiDelegate delegate) {
                return (OutboxAdminApiDelegate) Proxy.newProxyInstance(OutboxAdminApiDelegate.class.getClassLoader(),
                        new Class<?>[] {OutboxAdminApiDelegate.class}, (proxy, method, args) -> {
                            calls.operations.add(method.getName());
                            try {
                                return method.invoke(delegate, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
            }

            @Override
            public OutboxAdminApiDelegate release(OutboxAdminApiDelegate delegate) {
                return delegate;
            }
        };
    }

    /** The admin delegate's operations called, in order. */
    final class DelegateCalls {
        final List<String> operations = new CopyOnWriteArrayList<>();
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

    /** Allows the key {@code secret} as {@code tester}, fails on {@code boom}; records the threads it runs on. */
    final class KeyAuthorizer implements OutboxAdminAuthorizer {

        final List<String> threads = new CopyOnWriteArrayList<>();

        @Override
        public Decision authorize(HttpServerRequest request) {
            threads.add(Thread.currentThread().getName());
            var key = request.headers().getFirst("X-Api-Key");
            if (key == null) {
                return OutboxAdminAuthorizer.unauthorized("X-Api-Key required");
            }
            if (key.equals("boom")) {
                throw new IllegalStateException("key store down");
            }
            return key.equals("secret") ? OutboxAdminAuthorizer.allow("tester")
                    : OutboxAdminAuthorizer.forbidden("unknown key");
        }
    }
}
