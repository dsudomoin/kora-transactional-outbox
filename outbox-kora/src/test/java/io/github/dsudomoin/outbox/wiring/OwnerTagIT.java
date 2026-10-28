package io.github.dsudomoin.outbox.wiring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxSql;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

/** A tagged owner pool beside the app's DML-only one: the outbox's DDL runs as the owner, its sends as the app. */
@KoraAppTest(OwnerTestApp.class)
class OwnerTagIT implements KoraAppTestConfigModifier {

    private final String schema = "s" + TestSql.uniqueTable();
    private final String owner = "o" + TestSql.uniqueTable();
    private final String app = "a" + TestSql.uniqueTable();

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private Outbox outbox;

    @TestComponent
    private Maintenance maintenance;

    @TestComponent
    private RecordingHandler handler;

    @Override
    public KoraConfigModification config() {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, "create role " + owner + " login password 'pw'");
            TestSql.execute(c, "create role " + app + " login password 'pw'");
            TestSql.execute(c, "grant pg_read_all_stats to " + owner);
            TestSql.execute(c, "create schema " + schema + " authorization " + owner);
            TestSql.execute(c, "grant usage on schema " + schema + " to " + app);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        try (var c = as(owner)) {
            TestSql.execute(c, OutboxSql.outbox(new TestOutboxConfig().table(schema + ".outbox"), app));
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return KoraConfigModification.ofString("""
                db { jdbcUrl = "%1$s", username = "%2$s", password = "pw", poolName = "owner-tag-app" }
                ownerDb { jdbcUrl = "%1$s", username = "%3$s", password = "pw", poolName = "owner-tag-owner" }
                outbox {
                  table = %4$s.outbox
                  schema.bootstrap = false
                  relay { workers = 2, minTickInterval = 5ms }
                  partitions { xidsPerPartition = 1000, premake = 2 }
                }
                """.formatted(TestDb.jdbcUrl(), app, owner, schema));
    }

    @Test
    void ddlRunsAsTheOwnerAndSendsAsTheApplication() throws Exception {
        try (var c = TestDb.connect()) { // past the premade ranges: the next run makes new ones
            TestSql.execute(c, "set synchronous_commit = off");
            TestSql.execute(c, "do $$ begin for i in 1..3000 loop perform pg_current_xact_id(); commit; end loop;"
                    + " end $$");
        }
        long until = System.nanoTime() + 10_000_000_000L;
        while (!maintenance.runOnce()) { // the run the relay started may hold the maintenance lock
            assertThat(System.nanoTime()).as("a maintenance run of this instance").isLessThan(until);
            Thread.sleep(50);
        }
        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", "k", "p".getBytes(UTF_8))));

        long deadline = System.nanoTime() + 10_000_000_000L;
        while (handler.delivered().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
        try (var c = TestDb.connect()) {
            assertThat(TestSql.queryString(c, "select string_agg(distinct pg_get_userbyid(r.relowner), ',') from"
                    + " pg_class r join pg_namespace n on n.oid = r.relnamespace where n.nspname = ? and r.relname like"
                    + " 'outbox\\_x%'", schema)).as("the owner of every range").isEqualTo(owner);
            assertThat(TestSql.queryLong(c, "select count(*) from pg_class r join pg_namespace n on n.oid ="
                    + " r.relnamespace where n.nspname = '" + schema + "' and r.relname like 'outbox\\_x%'"))
                    .as("ranges made past the premade ones").isGreaterThan(2);
        }
    }

    private static Connection as(String role) throws SQLException {
        return DriverManager.getConnection(TestDb.jdbcUrl(), role, "pw");
    }
}
