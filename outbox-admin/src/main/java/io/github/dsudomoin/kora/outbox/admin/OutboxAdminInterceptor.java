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

/**
 * Guards every admin route with the {@link OutboxAdminAuthorizer}: a denied call gets a JSON {@code Error} with the
 * decision's status and never reaches the route; an allowed one is logged at INFO once it completes, with the caller's
 * identity and the response status.
 */
public final class OutboxAdminInterceptor implements HttpServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(OutboxAdminInterceptor.class);

    private final OutboxAdminAuthorizer authorizer;

    public OutboxAdminInterceptor(OutboxAdminAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public CompletionStage<HttpServerResponse> intercept(Context context, HttpServerRequest request, InterceptChain chain)
            throws Exception {
        var decision = Objects.requireNonNull(authorizer.authorize(request), "OutboxAdminAuthorizer returned null");
        if (decision instanceof OutboxAdminAuthorizer.Deny deny) {
            var body = "{\"message\":\"" + new String(JsonStringEncoder.getInstance().quoteAsString(deny.message())) + "\"}";
            return CompletableFuture.completedFuture(HttpServerResponse.of(deny.status(), HttpBody.json(body)));
        }
        var identity = ((OutboxAdminAuthorizer.Allow) decision).identity();
        return chain.process(context, request).whenComplete((response, error) -> log.info("outbox admin {} {} by {}: {}",
                request.method(), request.path(), identity, response != null ? response.code() : status(error)));
    }

    private static int status(Throwable error) {
        var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        return cause instanceof HttpServerResponseException e ? e.code() : 500;
    }
}
