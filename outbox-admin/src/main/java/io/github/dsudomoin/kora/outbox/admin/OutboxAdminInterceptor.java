package io.github.dsudomoin.kora.outbox.admin;

import io.github.dsudomoin.kora.outbox.admin.model.ModelError;
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
import ru.tinkoff.kora.json.common.JsonWriter;

/**
 * Guards every admin route with the {@link OutboxAdminAuthorizer}, called on the server's blocking executor, never on
 * an I/O thread. A denied call gets a JSON {@code Error} with the decision's status and never reaches the route. Every
 * call leaves one INFO audit line once it completes: {@code outbox admin <method> <path> by <identity>: <status>} when
 * allowed, {@code outbox admin <method> <path> denied: <status>} when denied or when the authorizer fails. The path
 * (its {@code {table}} is caller text, logged before any authorization) and the identity have control characters
 * escaped, so no caller can forge a log line.
 */
public final class OutboxAdminInterceptor implements HttpServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(OutboxAdminInterceptor.class);

    private final OutboxAdminAuthorizer authorizer;
    private final BlockingRequestExecutor executor;
    private final JsonWriter<ModelError> errorWriter;

    public OutboxAdminInterceptor(OutboxAdminAuthorizer authorizer, BlockingRequestExecutor executor,
            JsonWriter<ModelError> errorWriter) {
        this.authorizer = authorizer;
        this.executor = executor;
        this.errorWriter = errorWriter;
    }

    @Override
    public CompletionStage<HttpServerResponse> intercept(Context context, HttpServerRequest request, InterceptChain chain) {
        var call = request.method() + " " + printable(request.path());
        // whenComplete in the chain, not beside it: the audit line is written before the failed response leaves
        var decided = executor.execute(context,
                () -> Objects.requireNonNull(authorizer.authorize(request), "OutboxAdminAuthorizer returned null"))
                .whenComplete((decision, error) -> {
                    if (error != null) {
                        log.info("outbox admin {} denied: {}", call, status(error));
                    }
                });
        return decided.thenCompose(decision -> {
            if (decision instanceof OutboxAdminAuthorizer.Allow allow) {
                return process(context, request, chain).whenComplete((response, error) -> log.info(
                        "outbox admin {} by {}: {}", call, printable(allow.identity()),
                        response != null ? response.code() : status(error)));
            }
            var deny = (OutboxAdminAuthorizer.Deny) decision;
            log.info("outbox admin {} denied: {}", call, deny.status());
            var body = errorWriter.toByteArrayUnchecked(new ModelError(deny.message()));
            return CompletableFuture.completedFuture(
                    HttpServerResponse.of(deny.status(), HttpBody.json(context, body)));
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

    /** {@code text} with each control character as its Java escape, so on one log line. */
    private static String printable(String text) {
        var out = new StringBuilder(text.length());
        text.chars().forEach(ch -> {
            switch (ch) {
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (Character.isISOControl(ch) || ch == '\u2028' || ch == '\u2029') {
                        out.append(String.format("\\u%04x", ch));
                    } else {
                        out.append((char) ch);
                    }
                }
            }
        });
        return out.toString();
    }

    private static int status(Throwable error) {
        var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        return cause instanceof HttpServerResponseException e ? e.code() : 500;
    }
}
