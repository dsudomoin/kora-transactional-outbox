package io.github.dsudomoin.outbox.admin.kora;

import java.util.Objects;
import ru.tinkoff.kora.http.server.common.HttpServerRequest;

/**
 * Decides who may call the admin API; the app provides one as a component, an app without it does not build. Runs
 * before every admin route: {@link Deny} answers with its status and an {@code Error} body, the route never runs. It
 * runs on the HTTP server's blocking executor, not on an I/O thread, so it may block (a database or remote lookup).
 *
 * <p>Refuse with {@link Deny}, never by throwing: any exception {@link #authorize} throws, even an
 * {@code HttpServerResponseException} 401 or 403, answers 500 {@code "authorization failed"}, and the exception goes
 * to a WARN line, never to the caller.
 *
 * <p>Base the decision on a credential the client sends on purpose (a header such as an API key or a bearer token),
 * not on one a browser adds by itself (a cookie, Basic or SSO credentials it remembers, a client certificate, the
 * caller's address): a page on another site can make the operator's browser send those. The admin API refuses state
 * changes a browser marks as cross-site before this runs, but a browser too old to mark them gets through.
 */
public interface OutboxAdminAuthorizer {

    Decision authorize(HttpServerRequest request) throws Exception;

    sealed interface Decision permits Allow, Deny {}

    /** {@code identity} goes, quoted and escaped, into the audit log line of the call. */
    record Allow(String identity) implements Decision {

        public Allow {
            Objects.requireNonNull(identity, "identity");
        }
    }

    /** {@code status} 401 or 403; {@code message} goes into the {@code Error} body. */
    record Deny(int status, String message) implements Decision {

        public Deny {
            if (status != 401 && status != 403) {
                throw new IllegalArgumentException("a denial is a 401 or a 403, not " + status);
            }
            Objects.requireNonNull(message, "message");
        }
    }

    static Decision allow(String identity) {
        return new Allow(identity);
    }

    /** 401: no credentials or unknown ones. */
    static Decision unauthorized(String message) {
        return new Deny(401, message);
    }

    /** 403: known credentials without access. */
    static Decision forbidden(String message) {
        return new Deny(403, message);
    }
}
