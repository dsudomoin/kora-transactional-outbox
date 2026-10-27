package io.github.dsudomoin.kora.outbox.admin;

import io.github.dsudomoin.kora.outbox.admin.model.ModelError;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.http.common.body.HttpBody;
import ru.tinkoff.kora.http.common.header.HttpHeaders;
import ru.tinkoff.kora.http.common.header.MutableHttpHeaders;
import ru.tinkoff.kora.http.server.common.HttpServerInterceptor;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;
import ru.tinkoff.kora.http.server.common.HttpServerResponse;
import ru.tinkoff.kora.http.server.common.HttpServerResponseException;
import ru.tinkoff.kora.http.server.common.handler.BlockingRequestExecutor;
import ru.tinkoff.kora.json.common.JsonWriter;

/**
 * Guards every admin route with the {@link OutboxAdminAuthorizer}, called on the server's blocking executor, never on
 * an I/O thread. A denied call gets a JSON {@code Error} with the decision's status and never reaches the route; an
 * authorizer that fails gets a WARN with its exception and the caller a 500 {@code Error} "authorization failed",
 * nothing of why. A 4xx the server raises for an allowed call before the route runs (a malformed parameter or body)
 * is answered as an {@code Error} too.
 *
 * <p>Every call leaves one INFO audit line, written before its response completes:
 * {@code outbox admin <method> "<path>" by "<identity>": <status>} when allowed, {@code outbox admin <method>
 * "<path>" denied: <status>} when denied or when the authorizer fails. The path (its {@code {table}} is caller text,
 * logged before any authorization) and the identity (perhaps a token's subject) are quoted, with their quotes,
 * backslashes and control characters escaped, so no caller can forge a line or a part of one.
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
        var call = request.method() + " \"" + printable(request.path()) + "\"";
        return executor.execute(context, () -> authorizer.authorize(request))
                .handle((decision, error) -> respond(context, request, chain, call, decision, error))
                .thenCompose(Function.identity());
    }

    /** Runs where the decision completes, so each audit line is written before the response it is about. */
    private CompletionStage<HttpServerResponse> respond(Context context, HttpServerRequest request,
            InterceptChain chain, String call, @Nullable OutboxAdminAuthorizer.Decision decision,
            @Nullable Throwable error) {
        if (error != null || decision == null) {
            log.warn("outbox admin {}: the OutboxAdminAuthorizer failed, answered 500", call,
                    error != null ? unwrap(error) : new NullPointerException("OutboxAdminAuthorizer returned null"));
            log.info("outbox admin {} denied: 500", call);
            return CompletableFuture.completedFuture(error(context, 500, null, "authorization failed"));
        }
        if (decision instanceof OutboxAdminAuthorizer.Allow allow) {
            return process(context, request, chain)
                    .exceptionallyCompose(e -> clientError(context, e))
                    .whenComplete((response, e) -> log.info("outbox admin {} by \"{}\": {}", call,
                            printable(allow.identity()), response != null ? response.code() : status(e)));
        }
        var deny = (OutboxAdminAuthorizer.Deny) decision;
        log.info("outbox admin {} denied: {}", call, deny.status());
        return CompletableFuture.completedFuture(error(context, deny.status(), null, deny.message()));
    }

    /** A 4xx the server raised (a malformed parameter or body) as an {@code Error}; anything else as it failed. */
    private CompletionStage<HttpServerResponse> clientError(Context context, Throwable failure) {
        if (unwrap(failure) instanceof HttpServerResponseException e && e.code() >= 400 && e.code() < 500) {
            var message = Objects.requireNonNullElse(e.getMessage(), "HTTP " + e.code());
            return CompletableFuture.completedFuture(error(context, e.code(), e.headers(), message));
        }
        return CompletableFuture.failedFuture(failure);
    }

    private HttpServerResponse error(Context context, int code, @Nullable MutableHttpHeaders headers,
            String message) {
        var body = HttpBody.json(context, errorWriter.toByteArrayUnchecked(new ModelError(message)));
        return HttpServerResponse.of(code, headers != null ? headers : HttpHeaders.of(), body);
    }

    private static CompletionStage<HttpServerResponse> process(Context context, HttpServerRequest request,
            InterceptChain chain) {
        try {
            return chain.process(context, request);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * {@code text} with quotes and backslashes escaped and each control character as its Java escape, so it stays one
     * quoted token on one log line.
     */
    private static String printable(String text) {
        var out = new StringBuilder(text.length());
        text.chars().forEach(ch -> {
            switch (ch) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
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

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    private static int status(Throwable error) {
        return unwrap(error) instanceof HttpServerResponseException e ? e.code() : 500;
    }
}
