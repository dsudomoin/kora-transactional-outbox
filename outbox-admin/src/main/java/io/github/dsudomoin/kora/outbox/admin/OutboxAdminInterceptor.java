package io.github.dsudomoin.kora.outbox.admin;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.http.common.body.HttpBody;
import ru.tinkoff.kora.http.server.common.HttpServerInterceptor;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponse;
import ru.tinkoff.kora.http.server.common.HttpServerResponseException;
import ru.tinkoff.kora.http.server.common.handler.BlockingRequestExecutor;

/**
 * Guards every admin route with the {@link OutboxAdminAuthorizer}, called on the server's blocking executor, never on
 * an I/O thread. A denied call gets a JSON {@code Error} with the decision's status and never reaches the route. Every
 * call leaves one INFO audit line once it completes: {@code outbox admin <method> <path> by <identity>: <status>} when
 * allowed, {@code outbox admin <method> <path> denied: <status>} when denied or when the authorizer fails.
 */
public final class OutboxAdminInterceptor implements HttpServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(OutboxAdminInterceptor.class);

    private final OutboxAdminAuthorizer authorizer;
    private final BlockingRequestExecutor executor;

    public OutboxAdminInterceptor(OutboxAdminAuthorizer authorizer, BlockingRequestExecutor executor) {
        this.authorizer = authorizer;
        this.executor = executor;
    }

    @Override
    public CompletionStage<HttpServerResponse> intercept(Context context, HttpServerRequest request, InterceptChain chain) {
        var decided = executor.execute(context,
                () -> Objects.requireNonNull(authorizer.authorize(request), "OutboxAdminAuthorizer returned null"));
        decided.whenComplete((decision, error) -> {
            if (error != null) {
                log.info("outbox admin {} {} denied: {}", request.method(), request.path(), status(error));
            }
        });
        return decided.thenCompose(decision -> {
            if (decision instanceof OutboxAdminAuthorizer.Allow allow) {
                return process(context, request, chain).whenComplete((response, error) -> log.info(
                        "outbox admin {} {} by {}: {}", request.method(), request.path(), allow.identity(),
                        response != null ? response.code() : status(error)));
            }
            var deny = (OutboxAdminAuthorizer.Deny) decision;
            log.info("outbox admin {} {} denied: {}", request.method(), request.path(), deny.status());
            var body = "{\"message\":\"" + new String(JsonStringEncoder.getInstance().quoteAsString(deny.message())) + "\"}";
            return CompletableFuture.completedFuture(HttpServerResponse.of(deny.status(), HttpBody.json(body)));
        });
    }

    private static CompletionStage<HttpServerResponse> process(Context context, HttpServerRequest request,
            InterceptChain chain) {
        try {
            return chain.process(context, request);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static int status(Throwable error) {
        var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        return cause instanceof HttpServerResponseException e ? e.code() : 500;
    }
}
