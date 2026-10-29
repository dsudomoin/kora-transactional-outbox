package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.internal.SchemaObjects;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/** {@code outbox.schema.bootstrap = false}: the base objects come from the rendered script and are checked at start. */
class SchemaValidationIT {

    private final String schema = "s" + TestSql.uniqueTable();
    private final TestOutboxConfig config = new TestOutboxConfig().table(schema + ".outbox")
            .inboxTable(schema + ".inbox").schemaBootstrap(false);
    private final TableNames names = TableNames.of(config.table());
    private final List<AutoCloseable> runtimes = new ArrayList<>();

    SchemaValidationIT() throws SQLException {
        sql("create schema " + schema);
    }

    @AfterEach
    void close() throws Exception {
        for (int i = runtimes.size() - 1; i >= 0; i--) {
            runtimes.get(i).close();
        }
    }

    @Test
    void emptyDatabaseFailsNamingEveryObject() throws Exception {
        var runtime = runtime(config, TestDataSources.newDataSource());

        assertThatThrownBy(runtime::start).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("outbox schema check of " + config.table()
                        + " failed (outbox.schema.bootstrap = false): ")
                .satisfies(e -> {
                    for (var table : SchemaObjects.outbox(names).tables()) {
                        assertThat(e.getMessage()).contains("table " + table.name()
                                + " is missing (schema version 1: apply OutboxSql.outbox)");
                    }
                });
        assertThat(count("select count(*) from pg_class r join pg_namespace n on n.oid = r.relnamespace"
                + " where n.nspname = '" + schema + "'")).as("no table made").isZero();
    }

    @Test
    void renderedScriptStartsAndDelivers() throws Exception {
        sql(OutboxSql.outbox(config, null));
        var handler = new RecordingHandler();
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var runtime = OutboxRuntime.builder(config, transactions, handler).build();
        runtimes.add(runtime);
        long started = System.nanoTime();
        runtime.start();

        var id = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", "p".getBytes(UTF_8))));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (handler.delivered().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
        // the start's own run is over: while it holds the maintenance lock, another run skips and returns false
        while (!maintainedSince(runtime, started) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(runtime.maintenance().runOnce()).isTrue();
        assertThat(runtime.admin().status().premade()).isGreaterThanOrEqualTo(config.partitions().premake());
    }

    @Test
    void bootstrappedDatabaseValidates() throws Exception {
        var bootstrapped = new TestOutboxConfig().table(config.table()).schemaBootstrap(true);
        runtime(bootstrapped, TestDataSources.newDataSource()).start();
        close();
        runtimes.clear();

        runtime(config, TestDataSources.newDataSource()).start();
    }

    @Test
    void missingColumnNamesItsVersion() throws Exception {
        sql(OutboxSql.outbox(config, null));
        sql("alter table " + names.dead() + " drop column redrives");

        assertThatThrownBy(runtime(config, TestDataSources.newDataSource())::start)
                .hasMessageContaining("column " + names.dead() + ".redrives is missing (schema version 1: apply"
                        + " OutboxSql.outbox)");
    }

    @Test
    void olderLibraryOnNewerSchemaStarts() throws Exception {
        sql(OutboxSql.outbox(config, null));
        sql("alter table " + names.base() + " add column trace_flags int");
        sql("create index " + names.unqualified(names.tick()) + "_created on " + names.tick() + " (created_at)");
        sql("create table " + names.base() + "_future (id int primary key)");

        runtime(config, TestDataSources.newDataSource()).start();
    }

    @Test
    void startRunsNoBaseDdl() throws Exception {
        sql(OutboxSql.outbox(config, null));
        sql(OutboxSql.inbox(config, null));
        sql("create table " + schema + ".ddl_log (tag text, identity text, query text)");
        sql("create function " + schema + ".log_ddl() returns event_trigger language plpgsql as $$ declare r record;"
                + " begin for r in select * from pg_event_trigger_ddl_commands() loop insert into " + schema
                + ".ddl_log values (r.command_tag, r.object_identity, current_query()); end loop; end $$");
        sql("create event trigger " + schema + "_ddl on ddl_command_end execute function " + schema + ".log_ddl()");
        try {
            runtime(config, TestDataSources.newDataSource()).start();
            var inbox = InboxRuntime.builder(config, new JdbcTransactions(TestDataSources.newDataSource())).build();
            runtimes.add(inbox);
            inbox.start();
        } finally {
            sql("drop event trigger " + schema + "_ddl");
        }

        var logged = new ArrayList<String>();
        try (var c = TestDataSources.newDataSource().getConnection(); var st = c.createStatement();
                var rs = st.executeQuery("select tag, identity, query from " + schema + ".ddl_log where identity"
                        + " like '" + schema + ".%'")) {
            while (rs.next()) {
                logged.add(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
            }
        }
        var inboxNames = TableNames.of(config.inbox().table());
        assertThat(logged).isNotEmpty().allSatisfy(entry -> {
            var identity = entry.split(" \\| ")[1];
            boolean partition = identity.startsWith(names.partitionPrefix()) || identity.startsWith(names.tail())
                    || identity.startsWith(inboxNames.base() + "_t");
            boolean attach = (identity.equals(names.base()) || identity.equals(inboxNames.base()))
                    && entry.toLowerCase().matches("(?s).*(attach|detach) partition.*");
            assertThat(partition || attach).as(entry).isTrue();
        });
    }

    @Test
    void missingPrivilegeNamed() throws Exception {
        var role = "r" + TestSql.uniqueTable();
        sql("create role " + role + " login password 'pw'");
        sql("alter schema " + schema + " owner to " + role);
        var ds = as(role);
        try (var c = ds.getConnection()) {
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "revoke update on " + names.cursor() + " from " + role);
        }

        assertThatThrownBy(runtime(config, ds)::start)
                .hasMessageContaining("role " + role + " lacks UPDATE on " + names.cursor());
    }

    @Test
    void inboxValidatedToo() {
        var inbox = InboxRuntime.builder(config, new JdbcTransactions(TestDataSources.newDataSource())).build();
        runtimes.add(inbox);

        assertThatThrownBy(inbox::start).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table " + config.inbox().table()
                        + " is missing (schema version 1: apply OutboxSql.inbox)");
    }

    private OutboxRuntime runtime(TestOutboxConfig config, DataSource ds) {
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(ds), new RecordingHandler()).build();
        runtimes.add(runtime);
        return runtime;
    }

    /** A maintenance run completed after {@code since} ({@link System#nanoTime()}). */
    private static boolean maintainedSince(OutboxRuntime runtime, long since) {
        long before = System.nanoTime(); // the status reads the age after this: a success it shows is no older
        var age = runtime.admin().status().maintenanceSinceSuccessSeconds();
        return age != null && age < (before - since) / 1e9;
    }

    private static PGSimpleDataSource as(String role) {
        var ds = (PGSimpleDataSource) TestDataSources.newDataSource();
        ds.setUser(role);
        ds.setPassword("pw");
        return ds;
    }

    private static void sql(String sql) throws SQLException {
        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, sql);
        }
    }

    private static long count(String sql) throws SQLException {
        try (var c = TestDataSources.newDataSource().getConnection()) {
            return TestSql.queryLong(c, sql);
        }
    }
}
