package io.github.dsudomoin.kora.outbox.admin;

import static io.github.dsudomoin.kora.outbox.DeadLetter.Status.KEPT;
import static io.github.dsudomoin.kora.outbox.DeadLetter.Status.PENDING;
import static io.github.dsudomoin.kora.outbox.DeadLetter.Status.SCHEDULED;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.internal.HeaderCodec;
import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.kora.outbox.internal.OutboxSchema;
import io.github.dsudomoin.kora.outbox.internal.PgOutbox;
import io.github.dsudomoin.kora.outbox.internal.TableNames;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestSql;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServer;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

/**
 * Every admin operation over HTTP against the real {@code OutboxAdmin}, the relay disabled so nothing moves by
 * itself. Each test has its own outbox table and server.
 */
@KoraAppTest(value = AdminTestApp.class, components = UndertowHttpServer.class)
class AdminHttpIT implements KoraAppTestConfigModifier {

    private static final String API = "/kora-outbox/admin/v1";
    private static final Pattern LETTER_ID = Pattern.compile("\\{\"id\":(\\d+),\"messageId\"");
    private static final Instant T0 = Instant.parse("2026-01-15T10:00:00Z");

    @TestComponent
    private OutboxSchema schema;
    @TestComponent
    private JdbcDatabase db;

    private final HttpClient client = HttpClient.newHttpClient();
    private final int port = freePort();

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "admin-http"
                }
                outbox {
                  table = %s
                  shards = 2
                  relay.enabled = false
                  partitions { xidsPerPartition = 1000000, premake = 2 }
                }
                httpServer { publicApiHttpPort = %d, privateApiHttpPort = %d }
                openapi.management.file = ["openapi/kora-outbox-admin.yaml"]
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), TestDb.uniqueTable(), port,
                freePort()));
    }

    @Test
    void statusMapsEveryField() throws Exception {
        execute("insert into " + names().halt() + " (max_tick_xmax) values ('1')");
        execute("insert into " + names().retry() + " (id, shard, message_id, topic, created_at) values"
                + " (1, 0, gen_random_uuid(), 't', now()), (2, 0, gen_random_uuid(), 't', now())");
        letter(1, "a", "1", "boom", T0, KEPT);
        letter(2, "a", "2", "boom", T0, KEPT);
        letter(3, "a", "3", "boom", T0, PENDING);
        letter(4, "b", "4", "boom", T0, SCHEDULED);
        var orphan = foreignOrphan();

        var response = call("GET", "/status", null);

        assertJson(response, 200);
        assertThat(response.body().replaceAll("\"maintenanceSinceSuccessSeconds\":[-+0-9.E]+",
                "\"maintenanceSinceSuccessSeconds\":S")).isEqualTo("{\"halted\":true,\"lagSeconds\":0.0,"
                + "\"retrySize\":2,\"parkedSize\":0,\"deadLetters\":[{\"topic\":\"a\",\"status\":\"kept\",\"count\":2},"
                + "{\"topic\":\"a\",\"status\":\"pending\",\"count\":1},"
                + "{\"topic\":\"b\",\"status\":\"scheduled\",\"count\":1}],"
                + "\"orphans\":[{\"table\":\"" + orphan + "\",\"kind\":\"foreign\"}],\"tailOverflow\":false,"
                + "\"premade\":2,\"maintenanceSinceSuccessSeconds\":S,\"shards\":2}");
    }

    @Test
    void findsByEachFilterFieldAndPages() throws Exception {
        long a = letter(1, "t1", "k1", "disk full", T0.minusSeconds(7200), KEPT);
        long b = letter(2, "t2", "ключ", "timeout", T0.minusSeconds(3600), PENDING);
        long c = letter(3, "t1", "k3", "timeout", T0, SCHEDULED);

        assertThat(find("topic=t1")).containsExactly(a, c);
        assertThat(find("key=" + encode("ключ"))).as("the key's UTF-8 bytes").containsExactly(b);
        assertThat(find("error=time")).containsExactly(b, c);
        assertThat(find("failedFrom=" + encode(T0.minusSeconds(3600).toString()))).as("from inclusive")
                .containsExactly(b, c);
        assertThat(find("failedTo=" + encode(T0.minusSeconds(3600).atOffset(ZoneOffset.ofHours(3)).toString())))
                .as("to exclusive, any offset").containsExactly(a);
        assertThat(find("status=scheduled")).containsExactly(c);
        assertThat(find("topic=t1&error=timeout")).as("all fields must match").containsExactly(c);
        assertThat(find("")).containsExactly(a, b, c);

        var first = call("GET", "/dead-letters?limit=2", null);
        assertJson(first, 200);
        assertThat(ids(first.body())).containsExactly(a, b);
        assertThat(first.body()).endsWith("],\"nextAfterId\":" + b + "}");
        var last = call("GET", "/dead-letters?limit=2&afterId=" + b, null);
        assertThat(ids(last.body())).containsExactly(c);
        assertThat(last.body()).as("a short page is the last").doesNotContain("nextAfterId");
    }

    @Test
    void letterJsonCarriesEveryField() throws Exception {
        long id = letter(7, "t", "k", "timeout", T0, PENDING);
        execute("update " + names().dead() + " set redrives = 2, handler_attempts = 1 where id = " + id);

        var response = call("GET", "/dead-letters?topic=t", null);

        assertJson(response, 200);
        assertThat(response.body()).isEqualTo("{\"items\":[{\"id\":7,\"messageId\":\"" + messageId(id) + "\","
                + "\"topic\":\"t\",\"key\":\"" + base64("k") + "\",\"payload\":\"" + base64("p") + "\","
                + "\"headers\":[{\"name\":\"h1\",\"value\":\"" + base64("v") + "\"},{\"name\":\"h2\"}],"
                + "\"createdAt\":\"2026-01-15T09:00:00.000Z\",\"attempts\":3,"
                + "\"failedAt\":\"2026-01-15T10:00:00.000Z\",\"error\":\"timeout\",\"status\":\"pending\","
                + "\"redrives\":2,\"handlerAttempts\":1,\"dueAt\":\"2026-01-15T11:00:00.000Z\"}]}");
    }

    @Test
    void countsByTheFilter() throws Exception {
        letter(1, "t1", "a", "boom", T0, KEPT);
        letter(2, "t1", "b", "boom", T0, PENDING);
        letter(3, "t2", "c", "boom", T0, KEPT);

        assertThat(call("GET", "/dead-letters/count", null).body()).isEqualTo("{\"count\":3}");
        var t1 = call("GET", "/dead-letters/count?topic=t1", null);
        assertJson(t1, 200);
        assertThat(t1.body()).isEqualTo("{\"count\":2}");
        assertThat(call("GET", "/dead-letters/count?topic=t1&status=kept", null).body()).isEqualTo("{\"count\":1}");
    }

    @Test
    void redrivesByIdsIntoTheOutbox() throws Exception {
        long a = letter(1, "t", "a", "boom", T0, KEPT);
        long b = letter(2, "t", "b", "boom", T0, PENDING);
        long other = letter(3, "t", "c", "boom", T0, KEPT);
        var sent = List.of(messageId(a), messageId(b));

        var response = call("POST", "/dead-letters/redrive", "{\"ids\":[" + a + "," + b + ",999999]}");

        assertJson(response, 200);
        assertThat(response.body()).isEqualTo("{\"count\":2}");
        assertThat(deadIds()).containsExactly(other);
        assertThat(outboxMessageIds()).containsExactlyElementsOf(sent);
    }

    @Test
    void redrivesByFilter() throws Exception {
        long a = letter(1, "t1", "a", "boom", T0, KEPT);
        long other = letter(2, "t2", "b", "boom", T0, KEPT);
        var sent = messageId(a);

        var response = call("POST", "/dead-letters/redrive", "{\"filter\":{\"topic\":\"t1\"}}");

        assertThat(response.body()).isEqualTo("{\"count\":1}");
        assertThat(deadIds()).containsExactly(other);
        assertThat(outboxMessageIds()).containsExactly(sent);
    }

    @Test
    void discardsByFilterAndByIds() throws Exception {
        long a = letter(1, "t1", "a", "disk full", T0, KEPT);
        letter(2, "t1", "b", "timeout", T0, KEPT);
        letter(3, "t2", "c", "timeout", T0, KEPT);
        long kept = letter(4, "t2", "d", "disk full", T0, KEPT);

        var byFilter = call("POST", "/dead-letters/discard", "{\"filter\":{\"error\":\"timeout\"}}");
        var byIds = call("POST", "/dead-letters/discard", "{\"ids\":[" + a + "]}");

        assertJson(byFilter, 200);
        assertThat(byFilter.body()).isEqualTo("{\"count\":2}");
        assertThat(byIds.body()).isEqualTo("{\"count\":1}");
        assertThat(deadIds()).containsExactly(kept);
        assertThat(outboxMessageIds()).isEmpty();
    }

    @Test
    void selectionNeedsExactlyOneOfIdsAndFilter() throws Exception {
        long id = letter(1, "t", "k", "boom", T0, KEPT);

        for (var operation : List.of("redrive", "discard")) {
            for (var body : List.of("{\"ids\":[" + id + "],\"filter\":{\"topic\":\"t\"}}", "{}", "{\"filter\":{}}",
                    "{\"filter\":{\"topic\":\" \"}}", "{\"ids\":[null]}", "null")) {
                var response = call("POST", "/dead-letters/" + operation, body);

                assertJson(response, 400);
                assertThat(response.body()).as(operation + " " + body).startsWith("{\"message\":\"");
            }
        }
        assertThat(deadIds()).containsExactly(id);
        assertThat(outboxMessageIds()).isEmpty();
    }

    /** A typo must never widen a call: {@code eror} ignored would discard every letter of the topic. */
    @Test
    void unknownFieldsAreRefused() throws Exception {
        long id = letter(1, "t", "k", "boom", T0, KEPT);

        for (var operation : List.of("redrive", "discard")) {
            for (var body : List.of("{\"filter\":{\"topic\":\"t\",\"eror\":\"x\"}}",
                    "{\"filter\":{\"topic\":\"t\"},\"limit\":1}", "{\"idz\":[1],\"filter\":{\"topic\":\"t\"}}")) {
                var response = call("POST", "/dead-letters/" + operation, body);

                assertJson(response, 400);
                assertThat(response.body()).as(operation + " " + body).startsWith("{\"message\":\"unknown field '");
            }
        }
        assertThat(deadIds()).containsExactly(id);
        assertThat(outboxMessageIds()).isEmpty();
    }

    @Test
    void idsAreCappedAt1000() throws Exception {
        long id = letter(1, "t", "k", "boom", T0, KEPT);
        var thousand = LongStream.rangeClosed(1, 1000).mapToObj(Long::toString).collect(Collectors.joining(","));

        var tooMany = call("POST", "/dead-letters/discard", "{\"ids\":[" + thousand + ",1001]}");

        assertJson(tooMany, 400);
        assertThat(tooMany.body()).startsWith("{\"message\":\"at most 1000 ids");
        assertThat(deadIds()).containsExactly(id);
        assertThat(call("POST", "/dead-letters/discard", "{\"ids\":[" + thousand + "]}").body())
                .isEqualTo("{\"count\":1}");
    }

    @Test
    void restoresAStandaloneRangeFromTheCut() throws Exception {
        var orphan = foreignOrphan();
        orphanRow(orphan, 1, 10);
        var at = orphanRow(orphan, 2, 20);
        var above = orphanRow(orphan, 3, 30);

        var response = call("POST", "/orphans/" + orphan + "/restore?fromXid=20", null);

        assertJson(response, 200);
        assertThat(response.body()).isEqualTo("{\"count\":2}");
        assertThat(exists(orphan)).as("dropped").isFalse();
        assertThat(outboxMessageIds()).containsExactly(at, above);
    }

    @Test
    void tableNameIsNeverInterpolated() throws Exception {
        var victim = TestDb.uniqueTable();
        execute("create table " + victim + " (id bigint)");
        var orphan = foreignOrphan();
        orphanRow(orphan, 1, 10);

        for (var table : List.of(orphan + ";drop table " + victim, names().base() + "_x1000' or '1'='1",
                victim, names().base(), "otherschema." + orphan)) {
            var response = call("POST", "/orphans/" + encode(table).replace("+", "%20") + "/restore?fromXid=0",
                    null);

            assertJson(response, 400);
            assertThat(response.body()).as("the decoded name reached the call")
                    .startsWith("{\"message\":\"" + table + " is not ");
        }
        assertThat(exists(victim)).isTrue();
        assertThat(exists(orphan)).isTrue();
        assertThat(outboxMessageIds()).isEmpty();
    }

    @Test
    void recoversAnXidRegressionOnlyWhenThereIsOne() throws Exception {
        var none = call("POST", "/recovery/xid-regression", null);

        assertJson(none, 409);
        assertThat(none.body()).startsWith("{\"message\":\"no xid regression to recover");

        var outbox = new PgOutbox(db, schema, 2, () -> {}, new OutboxMetrics(null));
        var sent = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(
                OutboxMessage.of("t", "a", bytes("1")), OutboxMessage.of("t", "b", bytes("2")),
                OutboxMessage.of("t", "c", bytes("3")))));
        forgeRegression();

        var recovered = call("POST", "/recovery/xid-regression", null);

        assertJson(recovered, 200);
        assertThat(recovered.body()).isEqualTo("{\"count\":3}");
        assertThat(outboxMessageIds()).as("re-sent with the same message ids").containsAll(sent);
        assertThat(call("GET", "/status", null).body()).startsWith("{\"halted\":false,");
    }

    /** What the generated server or the call refuses for its input answers 400 before anything runs. */
    @Test
    void badInputIs400() throws Exception {
        long id = letter(1, "t", "k", "boom", T0, KEPT);

        for (var query : List.of("limit=0", "limit=1001")) {
            var response = call("GET", "/dead-letters?" + query, null);

            assertJson(response, 400);
            assertThat(response.body()).startsWith("{\"message\":\"limit must be within 1..1000");
        }
        var badLimit = call("GET", "/dead-letters?limit=x", null);
        assertJson(badLimit, 400);
        assertThat(badLimit.body()).isEqualTo("{\"message\":\"Query parameter 'limit' has invalid value: x\"}");
        var malformed = call("POST", "/dead-letters/discard", "not json");
        assertJson(malformed, 400);
        assertThat(malformed.body()).startsWith("{\"message\":\"Unrecognized token 'not'");

        var rejected = new ArrayList<HttpResponse<String>>();
        rejected.add(call("GET", "/dead-letters?afterId=x", null));
        for (var query : List.of("failedFrom=yesterday", "failedTo=2026-10-27", "status=PENDING", "status=gone")) {
            rejected.add(call("GET", "/dead-letters?" + query, null));
            rejected.add(call("GET", "/dead-letters/count?" + query, null));
        }
        rejected.add(call("POST", "/orphans/" + names().base() + "_x1000/restore", null)); // fromXid required
        rejected.add(call("POST", "/orphans/" + names().base() + "_x1000/restore?fromXid=x", null));
        rejected.add(call("POST", "/dead-letters/redrive", "{\"ids\":[\"x\"]}"));
        rejected.add(call("POST", "/dead-letters/redrive", null));
        for (var response : rejected) {
            assertJson(response, 400);
            assertThat(response.body()).as(response.uri().toString()).startsWith("{\"message\":\"");
        }
        assertThat(deadIds()).containsExactly(id);
    }

    /**
     * Neither bad input nor a refusal of the outbox's state: a 500 whose {@code Error} has the failure's message, for
     * the authenticated operator; one WARN with the exception; the audit line says 500.
     */
    @Test
    void otherFailuresAreA500Error() throws Exception {
        execute("drop table " + names().dead() + " cascade");

        HttpResponse<String> response;
        String log;
        try (var stderr = new CapturedStderr()) {
            response = call("GET", "/dead-letters/count", null);
            log = stderr.text();
        }

        assertJson(response, 500);
        assertThat(response.body()).startsWith("{\"message\":\"").contains(names().dead() + "\\\" does not exist");
        assertThat(response.headers().firstValue("X-Test-Global")).as("the global interceptor saw a response")
                .hasValue("1");
        var call = "outbox admin GET \"" + API + "/dead-letters/count\" by \"tester\"";
        assertThat(log.lines().filter(l -> l.contains(" WARN ") && l.contains(call + " failed, answered 500")))
                .as("one WARN").hasSize(1);
        assertThat(log).contains("PSQLException").contains(call + ": 500");
    }

    // ---------------------------------------------------------------------------------------------------- fixtures

    private TableNames names() {
        return schema.names();
    }

    /**
     * A dead letter as the relay writes one: payload p, headers h1=v and h2 without a value, created an hour before
     * {@code failedAt}; a pending or scheduled one is due an hour after it.
     */
    private long letter(long id, String topic, String key, String error, Instant failedAt, DeadLetter.Status status)
            throws SQLException {
        var headers = HeaderCodec.encode(List.of(new OutboxHeader("h1", bytes("v")), new OutboxHeader("h2", null)));
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + names().dead()
                + " (id, shard, message_id, topic, key, payload, headers, created_at, attempts, failed_at, error,"
                + " status, due_at) values (?, 0, gen_random_uuid(), ?, ?, ?, ?, ?, 3, ?, ?, ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, topic);
            ps.setBytes(3, bytes(key));
            ps.setBytes(4, bytes("p"));
            ps.setBytes(5, headers);
            ps.setObject(6, failedAt.minusSeconds(3600).atOffset(ZoneOffset.UTC));
            ps.setObject(7, failedAt.atOffset(ZoneOffset.UTC));
            ps.setString(8, error);
            ps.setString(9, status.name().toLowerCase(Locale.ROOT));
            ps.setObject(10, status == KEPT ? null : failedAt.plusSeconds(3600).atOffset(ZoneOffset.UTC));
            ps.executeUpdate();
        }
        return id;
    }

    private UUID messageId(long letter) throws SQLException {
        try (var c = TestDb.connect()) {
            return UUID.fromString(queryString(c, "select message_id::text from " + names().dead() + " where id = "
                    + letter));
        }
    }

    /** A standalone {@code <base>_x1000}, below every range of the outbox, as someone else's detach leaves one. */
    private String foreignOrphan() throws SQLException {
        var orphan = names().partitionPrefix() + "1000";
        execute("create table " + orphan + " (like " + names().base() + " including defaults)");
        return orphan;
    }

    /** A row of a standalone range with that xid; returns its message id. */
    private UUID orphanRow(String table, long id, long xid) throws SQLException {
        var messageId = UUID.randomUUID();
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + table
                + " (id, shard, xid, message_id, topic, key, payload) values (?, 0, ?::text::xid8, ?, 't', ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, Long.toString(xid));
            ps.setObject(3, messageId);
            ps.setBytes(4, bytes("k" + id));
            ps.setBytes(5, bytes("row"));
            ps.executeUpdate();
        }
        return messageId;
    }

    /** As after a dump/restore: a tick whose snapshot xmax lies 10⁹ xids past the counter, made a minute ago. */
    private void forgeRegression() throws SQLException {
        try (var c = TestDb.connect()) {
            long xmax = queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint") + 1_000_000_000L;
            execute("insert into " + names().tick() + " (snapshot, created_at) values ('" + xmax + ":" + xmax
                    + ":'::pg_snapshot, now() - interval '1 minute')");
        }
    }

    private List<Long> deadIds() throws SQLException {
        var all = new ArrayList<Long>();
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select id from " + names().dead() + " order by id")) {
            while (rs.next()) {
                all.add(rs.getLong(1));
            }
        }
        return all;
    }

    private List<UUID> outboxMessageIds() throws SQLException {
        var all = new ArrayList<UUID>();
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select message_id from " + names().base() + " order by id")) {
            while (rs.next()) {
                all.add(rs.getObject(1, UUID.class));
            }
        }
        return all;
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select to_regclass(?)::text", table) != null;
        }
    }

    private static void execute(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, sql);
        }
    }

    // ------------------------------------------------------------------------------------------------------- HTTP

    private List<Long> find(String query) throws Exception {
        var response = call("GET", "/dead-letters?" + query, null);
        assertJson(response, 200);
        return ids(response.body());
    }

    private static List<Long> ids(String page) {
        return LETTER_ID.matcher(page).results().map(m -> Long.parseLong(m.group(1))).toList();
    }

    private HttpResponse<String> call(String method, String path, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + API + path))
                .header("X-Api-Key", "secret")
                .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body));
        if (body != null) {
            request.header("Content-Type", "application/json");
        }
        return client.send(request.build(), BodyHandlers.ofString());
    }

    private static void assertJson(HttpResponse<String> response, int status) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                t -> assertThat(t).startsWith("application/json"));
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, UTF_8);
    }

    private static String base64(String s) {
        return Base64.getEncoder().encodeToString(bytes(s));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
