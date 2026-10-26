package io.github.dsudomoin.kora.outbox.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServer;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;

/** The admin routes, the app's own route and both specs are served by one app server, behind the global interceptor. */
@KoraAppTest(value = AdminTestApp.class, components = UndertowHttpServer.class)
class AdminWiringIT implements KoraAppTestConfigModifier {

    private static final String STATUS = "/kora-outbox/admin/v1/status";

    private final HttpClient client = HttpClient.newHttpClient();
    private final int port = freePort();

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "admin-wiring"
                }
                outbox {
                  table = %s
                  shards = 2
                  partitions { xidsPerPartition = 1000000, premake = 2 }
                }
                httpServer { publicApiHttpPort = %d, privateApiHttpPort = %d }
                openapi.management {
                  enabled = true
                  file = ["openapi/test-app.yaml", "openapi/kora-outbox-admin.yaml"]
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), TestDb.uniqueTable(), port,
                freePort()));
    }

    @Test
    void ownRouteAndGlobalInterceptorWork() throws Exception {
        var response = get("/hello", null);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("hello");
        assertThat(response.headers().firstValue("X-Test-Global")).hasValue("1");
    }

    @Test
    void adminRouteRunsBehindTheAuthorizer() throws Exception {
        HttpResponse<String> noKey, wrongKey, allowed;
        String log;
        try (var stderr = new CapturedStderr()) {
            noKey = get(STATUS, null);
            wrongKey = get(STATUS, "wrong");
            allowed = get(STATUS, "secret");
            log = stderr.text();
        }

        assertThat(noKey.statusCode()).isEqualTo(401);
        assertThat(noKey.body()).isEqualTo("{\"message\":\"X-Api-Key required\"}");
        assertThat(wrongKey.statusCode()).isEqualTo(403);
        assertThat(wrongKey.body()).isEqualTo("{\"message\":\"unknown key\"}");
        assertThat(allowed.statusCode()).isEqualTo(200);
        assertThat(allowed.body()).isEqualTo("{\"halted\":false}");
        assertThat(allowed.headers().firstValue("Content-Type")).hasValueSatisfying(
                t -> assertThat(t).startsWith("application/json"));
        for (var response : new HttpResponse<?>[] {noKey, wrongKey, allowed}) {
            assertThat(response.headers().firstValue("X-Test-Global")).hasValue("1");
        }
        assertThat(log).as("one audit line, for the allowed call").containsOnlyOnce("outbox admin ")
                .contains("outbox admin GET " + STATUS + " by tester: 200");
    }

    /** With two files, {@code openapi.management} serves each at {@code <endpoint>/<file name without extension>}. */
    @Test
    void bothSpecsAreServed() throws Exception {
        var app = get("/openapi/test-app", null);
        var admin = get("/openapi/kora-outbox-admin", null);

        assertThat(app.statusCode()).isEqualTo(200);
        assertThat(app.body()).contains("title: admin wiring test app");
        assertThat(admin.statusCode()).isEqualTo(200);
        assertThat(admin.body()).contains("title: kora-outbox admin");
    }

    private HttpResponse<String> get(String path, String apiKey) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path));
        if (apiKey != null) {
            request.header("X-Api-Key", apiKey);
        }
        return client.send(request.build(), BodyHandlers.ofString());
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
