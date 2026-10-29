package io.github.dsudomoin.outbox.admin.kora;

import io.github.dsudomoin.outbox.InternalApi;
import io.github.dsudomoin.outbox.admin.kora.model.ModelError;
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
 * nothing of why. For an allowed call, a 4xx the server raises before the route runs (a malformed parameter or body)
 * is answered as an {@code Error} of its status too, as is any response exception of the server (a 503), and any
 * other failure as a 500 {@code Error} with the failure's message (its class name when it has none), for the
 * authenticated operator; a 5xx after one WARN with the exception. A call other than a {@code GET} or {@code HEAD}
 * that a browser marks as sent by another site ({@code Sec-Fetch-Site} other than {@code same-origin} or {@code none})
 * gets a 403 {@code Error} "cross-site request refused" without the authorizer: a page elsewhere cannot change state
 * with what the operator's browser adds by itself.
 *
 * <p>Every call leaves one INFO audit line, written before its response completes:
 * {@code outbox admin <method> "<path>" by "<identity>": <status>} when allowed, {@code outbox admin <method>
 * "<path>" denied: <status>} when denied or when the authorizer fails. The path (its {@code {table}} is caller text,
 * logged before any authorization) and the identity (perhaps a token's subject) are quoted, with their quotes,
 * backslashes and control characters escaped, so no caller can forge a line or a part of one.
 *
 * <p>Internal type, unstable: for the adapters of this repository.
 */
@InternalApi
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
        if (!request.method().equals("GET") && !request.method().equals("HEAD") && sentByAnotherSite(request)) {
            log.info("outbox admin {} denied: 403", call);
            return CompletableFuture.completedFuture(error(context, 403, null, "cross-site request refused"));
        }
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
            var caller = call + " by \"" + printable(allow.identity()) + "\"";
            return process(context, request, chain)
                    .exceptionallyCompose(e -> CompletableFuture.completedFuture(failure(context, caller, e)))
                    .whenComplete((response, e) -> log.info("outbox admin {}: {}", caller,
                            response != null ? response.code() : status(e)));
        }
        var deny = (OutboxAdminAuthorizer.Deny) decision;
        log.info("outbox admin {} denied: {}", call, deny.status());
        return CompletableFuture.completedFuture(error(context, deny.status(), null, deny.message()));
    }

    /**
     * The server's own response exception (a malformed parameter or body, a 503) as an {@code Error} of its code;
     * anything else as a 500 {@code Error} with its message. A 5xx gets a WARN with the exception first.
     */
    private HttpServerResponse failure(Context context, String caller, Throwable failure) {
        var cause = unwrap(failure);
        if (cause instanceof HttpServerResponseException e) {
            if (e.code() >= 500) {
                log.warn("outbox admin {} failed, answered {}", caller, e.code(), e);
            }
            var message = Objects.requireNonNullElse(e.getMessage(), "HTTP " + e.code());
            return error(context, e.code(), e.headers(), message);
        }
        log.warn("outbox admin {} failed, answered 500", caller, cause);
        return error(context, 500, null, Objects.requireNonNullElse(cause.getMessage(), cause.getClass().getName()));
    }

    /**
     * Whether a browser says another site made it send {@code request} ({@code Sec-Fetch-Site}): such a request may
     * carry what the browser adds by itself (a cookie, an SSO session, the operator's network address), which an
     * authorizer cannot tell from the operator's own call. Clients other than browsers send no such header.
     */
    private static boolean sentByAnotherSite(HttpServerRequest request) {
        var site = request.headers().getFirst("sec-fetch-site");
        return site != null && !site.equals("same-origin") && !site.equals("none");
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
