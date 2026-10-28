package io.github.dsudomoin.outbox.admin.kora;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.admin.kora.model.$ModelError_JsonWriter;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.http.common.body.HttpBodyInput;
import ru.tinkoff.kora.http.common.cookie.Cookie;
import ru.tinkoff.kora.http.common.header.HttpHeaders;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponse;
import ru.tinkoff.kora.http.server.common.HttpServerResponseException;
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

    /** As the authorizer's javadoc promises: a thrown 401 is no refusal, it is a failure. */
    @Test
    void anAuthorizerThrowing401GetsTheFixed500() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> {
            throw HttpServerResponseException.of(401, "token expired");
        }, deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> {
                        throw new AssertionError("the route must not run");
                    });
            authorization.get().run();

            var answered = response.toCompletableFuture().join();
            assertThat(answered.code()).isEqualTo(500);
            assertThat(body(answered)).isEqualTo("{\"message\":\"authorization failed\"}");
            assertThat(stderr.text()).contains("WARN").contains("OutboxAdminAuthorizer failed")
                    .as("the exception, which prints its code, not its text")
                    .contains("HttpServerResponseException{code=401")
                    .contains("outbox admin GET \"/kora-outbox/admin/v1/status\" denied: 500");
        }
    }

    /** A failure without a message is named by its class; the WARN has the route and the identity, escaped. */
    @Test
    void aRouteFailureIsA500Error() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops\n"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> CompletableFuture.failedFuture(new IllegalMonitorStateException()));
            authorization.get().run();

            var answered = response.toCompletableFuture().join();
            assertThat(answered.code()).isEqualTo(500);
            assertThat(body(answered)).isEqualTo("{\"message\":\"java.lang.IllegalMonitorStateException\"}");
            var call = "outbox admin GET \"/kora-outbox/admin/v1/status\" by \"ops\\n\"";
            assertThat(stderr.text()).contains("WARN " + OutboxAdminInterceptor.class.getName() + " - " + call
                    + " failed, answered 500").contains("INFO " + OutboxAdminInterceptor.class.getName() + " - "
                    + call + ": 500");
        }
    }

    /** The server's own response exceptions keep their status, 5xx included; only other failures become 500. */
    @Test
    void aServerResponseExceptionKeepsItsStatus() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("ops"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> CompletableFuture.failedFuture(HttpServerResponseException.of(503,
                            "draining")));
            authorization.get().run();

            var answered = response.toCompletableFuture().join();
            assertThat(answered.code()).isEqualTo(503);
            assertThat(body(answered)).isEqualTo("{\"message\":\"draining\"}");
            var call = "outbox admin GET \"/kora-outbox/admin/v1/status\" by \"ops\"";
            assertThat(stderr.text()).contains("WARN " + OutboxAdminInterceptor.class.getName() + " - " + call
                    + " failed, answered 503").contains(call + ": 503");
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

    /** The identity may be caller text (a token's subject): quoted, it cannot pass for the status that follows. */
    @Test
    void auditLineQuotesTheIdentity() {
        var authorization = new AtomicReference<Runnable>();
        var interceptor = new OutboxAdminInterceptor(request -> OutboxAdminAuthorizer.allow("x\": 200 \\"),
                deferred(authorization), new $ModelError_JsonWriter());

        try (var stderr = new CapturedStderr()) {
            var response = interceptor.intercept(Context.current(), request("/kora-outbox/admin/v1/status"),
                    (context, request) -> CompletableFuture.completedFuture(HttpServerResponse.of(409)));
            authorization.get().run();

            assertThat(response.toCompletableFuture().join().code()).isEqualTo(409);
            assertThat(stderr.text().lines().filter(l -> l.contains("outbox admin ")))
                    .singleElement(InstanceOfAssertFactories.STRING)
                    .endsWith("outbox admin GET \"/kora-outbox/admin/v1/status\" by \"x\\\": 200 \\\\\": 409");
        }
    }

    private static String body(HttpServerResponse response) {
        var out = new ByteArrayOutputStream();
        try {
            response.body().write(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toString(UTF_8);
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
