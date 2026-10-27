package io.github.dsudomoin.kora.outbox.admin;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestSql;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServer;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

/**
 * The admin API in an app with its own routes (two at the admin API's names without its prefix), its own global
 * interceptor and its own spec, behind the app's API-key authorizer. The relay is disabled, so only calls change the
 * outbox. Each test has its own outbox table and server.
 */
@KoraAppTest(value = AdminTestApp.class, components = UndertowHttpServer.class)
class AdminCoexistenceIT implements KoraAppTestConfigModifier {

    private static final String API = "/kora-outbox/admin/v1";
    private static final String STATUS = API + "/status";
    private static final String AUDIT = "INFO " + OutboxAdminInterceptor.class.getName() + " - ";

    /** An admin route: its operation, method, path ({@code {orphan}} is the test's orphan table) and body. */
    record Route(String operation, String method, String path, String body) {

        @Override
        public String toString() {
            return operation;
        }
    }

    /** All seven, in the contract's order, each acting on what {@link #seed} made when allowed. */
    static final List<Route> ROUTES = List.of(
            new Route("getStatus", "GET", API + "/status", null),
            new Route("findDeadLetters", "GET", API + "/dead-letters?topic=t", null),
            new Route("countDeadLetters", "GET", API + "/dead-letters/count?topic=t", null),
            new Route("redriveDeadLetters", "POST", API + "/dead-letters/redrive", "{\"ids\":[1]}"),
            new Route("discardDeadLetters", "POST", API + "/dead-letters/discard", "{\"ids\":[1]}"),
            new Route("restoreOrphan", "POST", API + "/orphans/{orphan}/restore?fromXid=0", null),
            new Route("recoverXidRegression", "POST", API + "/recovery/xid-regression", null));

    @TestComponent
    private AdminTestApp.KeyAuthorizer authorizer;
    @TestComponent
    private AdminTestApp.DelegateCalls delegate;

    private final HttpClient client = HttpClient.newHttpClient();
    private final String table = TestDb.uniqueTable();
    private final int port = freePort();

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "admin-coexistence"
                }
                outbox {
                  table = %s
                  shards = 2
                  relay.enabled = false
                  partitions { xidsPerPartition = 1000000, premake = 2 }
                }
                httpServer { publicApiHttpPort = %d, privateApiHttpPort = %d }
                openapi.management {
                  enabled = true
                  file = ["openapi/test-app.yaml", "openapi/kora-outbox-admin.yaml"]
                  swaggerui.enabled = true
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), table, port, freePort()));
    }

    static Stream<Route> routes() {
        return ROUTES.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("routes")
    void everyAdminRouteAnswers401And403(Route route) throws Exception {
        seed();
        var before = state();
        HttpResponse<String> noKey, wrongKey;
        String log;
        try (var stderr = new CapturedStderr()) {
            noKey = call(route, null);
            wrongKey = call(route, "wrong");
            log = stderr.text();
        }

        assertError(noKey, 401, "X-Api-Key required");
        assertError(wrongKey, 403, "unknown key");
        assertThat(delegate.operations).as("the delegate never ran").isEmpty();
        assertThat(state()).as("dead letters, outbox rows, the orphan and the ticks unchanged").isEqualTo(before);
        assertThat(audit(log)).as("one INFO line per call").containsExactly(
                "outbox admin " + route.method() + " \"" + path(route) + "\" denied: 401",
                "outbox admin " + route.method() + " \"" + path(route) + "\" denied: 403");
        if (route.method().equals("POST")) {
            assertThat(call(route, "secret").statusCode()).isEqualTo(200);
            assertThat(state()).as("what an allowed call changes, so the check above can fail").isNotEqualTo(before);
        }
    }

    @Test
    void globalInterceptorAppliesToAdminRoutes() throws Exception {
        seed();

        for (var route : ROUTES) {
            for (var key : new String[] {null, "wrong", "secret"}) {
                var response = call(route, key);

                assertThat(response.headers().firstValue("X-Test-Global")).as(route + " with key " + key)
                        .hasValue("1");
            }
        }
        assertThat(delegate.operations).as("the allowed calls ran")
                .containsExactlyElementsOf(ROUTES.stream().map(Route::operation).toList());
    }

    @Test
    void ownRoutesStillWork() throws Exception {
        var hello = get("/hello", null);
        var status = get("/status", null);
        var deadLetters = get("/dead-letters", null);

        for (var response : List.of(hello, status, deadLetters)) {
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("X-Test-Global")).hasValue("1");
        }
        assertThat(hello.body()).isEqualTo("hello");
        assertThat(status.body()).isEqualTo("app status");
        assertThat(deadLetters.body()).isEqualTo("app dead letters");
        assertThat(authorizer.threads).as("the authorizer guards the admin routes alone").isEmpty();
        assertThat(delegate.operations).isEmpty();

        var adminStatus = get(STATUS, "secret");
        var adminLetters = get(API + "/dead-letters", "secret");

        assertThat(adminStatus.statusCode()).isEqualTo(200);
        assertThat(adminStatus.body()).startsWith("{\"halted\":false,");
        assertThat(adminLetters.statusCode()).isEqualTo(200);
        assertThat(adminLetters.body()).isEqualTo("{\"items\":[]}");
        assertThat(delegate.operations).containsExactly("getStatus", "findDeadLetters");
    }

    /** With two files, {@code openapi.management} serves each at {@code <endpoint>/<file name without extension>}. */
    @Test
    void bothSpecsAreServedAndListed() throws Exception {
        var app = get("/openapi/test-app", null);
        var admin = get("/openapi/kora-outbox-admin", null);
        var swaggerUi = get("/swagger-ui", null);

        assertThat(app.statusCode()).isEqualTo(200);
        assertThat(app.body()).contains("title: admin wiring test app").contains("operationId: appStatus");
        assertThat(admin.statusCode()).isEqualTo(200);
        String contract;
        try (var in = Objects.requireNonNull(getClass().getResourceAsStream("/openapi/kora-outbox-admin.yaml"))) {
            contract = new String(in.readAllBytes(), UTF_8);
        }
        assertThat(admin.body()).as("the shipped contract").isEqualTo(contract);
        assertThat(Pattern.compile("operationId: (\\w+)").matcher(admin.body()).results().map(m -> m.group(1)))
                .containsExactlyElementsOf(ROUTES.stream().map(Route::operation).toList());
        assertThat(admin.body()).doesNotContain("securitySchemes").doesNotContain("security:");
        assertThat(swaggerUi.statusCode()).isEqualTo(200);
        assertThat(swaggerUi.body()).as("both in the Swagger UI selector")
                .contains("\"/openapi/test-app\"").contains("name: \"test-app\"")
                .contains("\"/openapi/kora-outbox-admin\"").contains("name: \"kora-outbox-admin\"");
    }

    @Test
    void auditLineNamesTheIdentity() throws Exception {
        seed();

        String log;
        try (var stderr = new CapturedStderr()) {
            for (var route : ROUTES) {
                call(route, null);
                call(route, "wrong");
                assertThat(call(route, "secret").statusCode()).as(route.toString()).isEqualTo(200);
            }
            log = stderr.text();
        }

        var expected = new ArrayList<String>();
        for (var route : ROUTES) {
            var call = "outbox admin " + route.method() + " \"" + path(route) + "\"";
            expected.addAll(List.of(call + " denied: 401", call + " denied: 403", call + " by \"tester\": 200"));
        }
        assertThat(audit(log)).as("one INFO line per call").containsExactlyElementsOf(expected);
        assertThat(delegate.operations).containsExactlyElementsOf(ROUTES.stream().map(Route::operation).toList());
    }

    /** The caller, not yet authenticated, learns nothing of why: the WARN line has the exception. */
    @Test
    void authorizerFailureIsAFixed500() throws Exception {
        HttpResponse<String> failed;
        String log;
        try (var stderr = new CapturedStderr()) {
            failed = get(STATUS, "boom");
            log = stderr.text();
        }

        assertError(failed, 500, "authorization failed");
        assertThat(log).contains("WARN").contains("OutboxAdminAuthorizer failed").contains("key store down")
                .contains("outbox admin GET \"" + STATUS + "\" denied: 500");
        assertThat(delegate.operations).isEmpty();
    }

    /** {@code {table}} puts caller text into the path, so into the audit line, before any authorization. */
    @Test
    void auditLineEscapesThePath() throws Exception {
        var log = postWithoutKey(API + "/orphans/x%0D%0AINFO%20forged%09%07/restore?fromXid=0");

        assertThat(log).contains("orphans/x\\r\\nINFO forged\\t\\u0007/restore\" denied: 401")
                .doesNotContain("\nINFO forged");
    }

    /** Quoted, its quotes and backslashes escaped, a path cannot end early and pass for the rest of a line. */
    @Test
    void auditLineQuotesThePath() throws Exception {
        assertThat(postWithoutKey(API + "/orphans/x%20by%20root:%20200/restore?fromXid=0"))
                .contains("outbox admin POST \"" + API + "/orphans/x by root: 200/restore\" denied: 401");
        assertThat(postWithoutKey(API + "/orphans/x%22%20by%20root:%20200%20%22/restore?fromXid=0"))
                .contains("orphans/x\\\" by root: 200 \\\"/restore\" denied: 401");
    }

    /** XNIO names its I/O threads {@code kora-undertow I/O-<n>}, the blocking pool {@code kora-undertow task-<n>}. */
    @Test
    void authorizerRunsOffTheIoThreads() throws Exception {
        get(STATUS, null);
        get(STATUS, "secret");

        assertThat(authorizer.threads).as("a blocking authorizer must not stall the server's I/O threads")
                .hasSize(2).allSatisfy(t -> assertThat(t).startsWith("kora-undertow task-"));
    }

    // ---------------------------------------------------------------------------------------------------- fixtures

    /**
     * Something for every mutating route to change: kept dead letter 1, the foreign orphan {@code <table>_x1000}
     * with one row, one outbox row, and a tick whose snapshot xmax lies 10⁹ xids past the counter, as after a
     * dump/restore.
     */
    private void seed() throws SQLException {
        execute("insert into " + table + "_dead (id, shard, message_id, topic, created_at, attempts)"
                + " values (1, 0, gen_random_uuid(), 't', now(), 3)");
        execute("create table " + orphan() + " (like " + table + " including defaults)");
        execute("insert into " + orphan() + " (id, shard, xid, message_id, topic)"
                + " values (1, 0, '10', gen_random_uuid(), 't')");
        execute("insert into " + table + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
        execute("insert into " + table + "_tick (snapshot, created_at) select (x || ':' || x || ':')::pg_snapshot,"
                + " now() - interval '1 minute' from (select pg_snapshot_xmax(pg_current_snapshot())::text::bigint"
                + " + 1000000000 x) s");
    }

    /** What the mutating routes change: dead letter ids, outbox message ids, the orphan's rows, the ticks. */
    private String state() throws SQLException {
        try (var c = TestDb.connect()) {
            var orphanRows = queryString(c, "select to_regclass(?)::text", orphan()) == null ? "dropped"
                    : queryString(c, "select count(*)::text from " + orphan());
            return String.join(" | ",
                    queryString(c, "select string_agg(id::text, ',' order by id) from " + table + "_dead"),
                    queryString(c, "select string_agg(message_id::text, ',' order by id) from " + table),
                    orphanRows,
                    queryString(c, "select string_agg(id::text, ',' order by id) from " + table + "_tick"));
        }
    }

    private String orphan() {
        return table + "_x1000";
    }

    private static void execute(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, sql);
        }
    }

    // ------------------------------------------------------------------------------------------------------- HTTP

    /** The interceptor's INFO lines, from {@code outbox admin}. */
    private static List<String> audit(String log) {
        return log.lines().filter(l -> l.contains(AUDIT)).map(l -> l.substring(l.indexOf(AUDIT) + AUDIT.length()))
                .toList();
    }

    /** {@code route}'s path without its query, as the audit line names it. */
    private String path(Route route) {
        var path = route.path().replace("{orphan}", orphan());
        return path.contains("?") ? path.substring(0, path.indexOf('?')) : path;
    }

    private HttpResponse<String> call(Route route, String apiKey) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port
                        + route.path().replace("{orphan}", orphan())))
                .method(route.method(), route.body() == null ? BodyPublishers.noBody()
                        : BodyPublishers.ofString(route.body()));
        if (route.body() != null) {
            request.header("Content-Type", "application/json");
        }
        if (apiKey != null) {
            request.header("X-Api-Key", apiKey);
        }
        return client.send(request.build(), BodyHandlers.ofString());
    }

    private static void assertError(HttpResponse<String> response, int status, String message) {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.body()).isEqualTo("{\"message\":\"" + message + "\"}");
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                t -> assertThat(t).startsWith("application/json"));
    }

    /** The log lines of a POST to {@code pathAndQuery} without a key; asserts it was refused. */
    private String postWithoutKey(String pathAndQuery) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + pathAndQuery))
                .POST(BodyPublishers.noBody()).build();
        try (var stderr = new CapturedStderr()) {
            assertThat(client.send(request, BodyHandlers.ofString()).statusCode()).isEqualTo(401);
            return stderr.text();
        }
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
