package io.github.dsudomoin.kora.outbox.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.admin.model.$ModelError_JsonWriter;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.http.common.body.HttpBodyInput;
import ru.tinkoff.kora.http.common.cookie.Cookie;
import ru.tinkoff.kora.http.common.header.HttpHeaders;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.handler.BlockingRequestExecutor;

class OutboxAdminInterceptorTest {

    /**
     * The authorizer's stage completes only after the caller has attached to the response stage, as on a real
     * executor's thread: then {@code CompletableFuture} runs dependents last-attached first, and an audit line on a
     * side branch of that stage would come after the response.
     */
    @Test
    void authorizerFailureIsAuditedBeforeTheResponseCompletes() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> {
            throw new IllegalStateException("key store down");
        }, deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var logAtResponse = new AtomicReference<String>();
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> {
                        throw new AssertionError("the route must not run");
                    }).whenComplete((r, e) -> logAtResponse.set(stderr.text()));

            authorization.get().run();

            assertThat(logAtResponse.get()).contains("outbox admin GET \"/kora-outbox/admin/v1/status\" denied: 500");
            assertThat(response.toCompletableFuture().join().code()).isEqualTo(500);
        }
    }

    /** Undertow leaves {@code %5C} encoded, so a backslash in a path is tested here. */
    @Test
    void auditLinePathIsOneQuotedToken() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.unauthorized("who?"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/a\\\" by root: 200 \""),
                    (context, request) -> {
                        throw new AssertionError("the route must not run");
                    });
            authorization.get().run();

            assertThat(response.toCompletableFuture().join().code()).isEqualTo(401);
            assertThat(stderr.text()).contains("outbox admin GET \"/a\\\\\\\" by root: 200 \\\"\" denied: 401");
        }
    }

    /** Runs the handler only when the test runs what it leaves in {@code run}. */
    private static BlockingRequestExecutor deferred(AtomicReference<Runnable> run) {
        return new BlockingRequestExecutor() {
            @Override
            public <T> CompletionStage<T> execute(Context context, Callable<T> handler) {
                var future = new CompletableFuture<T>();
                run.set(() -> {
                    try {
                        future.complete(handler.call());
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                    }
                });
                return future;
            }
        };
    }

    private static HttpServerRequest request(String path) {
        return new HttpServerRequest() {
            @Override
            public String method() {
                return "GET";
            }

            @Override
            public String path() {
                return path;
            }

            @Override
            public String route() {
                return path;
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.of();
            }

            @Override
            public List<Cookie> cookies() {
                return List.of();
            }

            @Override
            public Map<String, ? extends Collection<String>> queryParams() {
                return Map.of();
            }

            @Override
            public Map<String, String> pathParams() {
                return Map.of();
            }

            @Override
            public HttpBodyInput body() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
