package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.SchemaObjects;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * The rendered scripts against the bootstrap: in one schema the bootstrap makes the tables, in another the script
 * does, and their catalogs (partitions and rows aside) are the same; qualified table names, and unqualified ones
 * resolved through the {@code search_path}.
 */
class OutboxSqlIT {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void renderedOutboxMatchesBootstrap(boolean qualified) throws Exception {
        var a = newSchema();
        var b = newSchema();
        new OutboxSchema(new JdbcTransactions(dataSource(a)), config(a, qualified)).start();

        apply(b, OutboxSql.outbox(config(b, qualified), null));

        assertThat(catalog(b)).isEqualTo(catalog(a));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void renderedInboxMatchesBootstrap(boolean qualified) throws Exception {
        var a = newSchema();
        var b = newSchema();
        try (var runtime = InboxRuntime.builder(config(a, qualified), new JdbcTransactions(dataSource(a))).build()) {
            runtime.start();
        }

        apply(b, OutboxSql.inbox(config(b, qualified), null));

        assertThat(catalog(b)).isEqualTo(catalog(a));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void schemaObjectsCoverTheScript(boolean outbox) throws Exception {
        var b = newSchema();
        var config = config(b, true);
        apply(b, outbox ? OutboxSql.outbox(config, null) : OutboxSql.inbox(config, null));

        var declared = new TreeSet<String>();
        var objects = outbox ? SchemaObjects.outbox(TableNames.of(config.table()))
                : SchemaObjects.inbox(TableNames.of(config.inbox().table()));
        for (var table : objects.tables()) {
            declared.add("table " + table.name());
            table.columns().forEach(column -> declared.add("column " + table.name() + "." + column.name()));
            table.indexes().forEach(index -> declared.add("index " + index.name()));
        }
        var made = new TreeSet<String>();
        try (var c = TestDataSources.newDataSource().getConnection()) {
            made.addAll(strings(c, "select 'table ' || n.nspname || '.' || r.relname from pg_class r"
                    + " join pg_namespace n on n.oid = r.relnamespace where n.nspname = ? and r.relkind in ('r', 'p')",
                    b));
            made.addAll(strings(c, "select 'column ' || n.nspname || '.' || r.relname || '.' || a.attname"
                    + " from pg_class r join pg_namespace n on n.oid = r.relnamespace join pg_attribute a"
                    + " on a.attrelid = r.oid where n.nspname = ? and r.relkind in ('r', 'p') and a.attnum > 0"
                    + " and not a.attisdropped", b));
            made.addAll(strings(c, "select 'index ' || n.nspname || '.' || i.relname from pg_index x join pg_class i"
                    + " on i.oid = x.indexrelid join pg_namespace n on n.oid = i.relnamespace where n.nspname = ?"
                    + " and not exists (select 1 from pg_constraint k where k.conindid = x.indexrelid)", b));
        }

        assertThat(made).isEqualTo(declared);
    }

    private static TestOutboxConfig config(String schema, boolean qualified) {
        return new TestOutboxConfig().table(qualified ? schema + ".outbox" : "outbox")
                .inboxTable(qualified ? schema + ".inbox" : "inbox");
    }

    private static String newSchema() throws SQLException {
        var schema = "s" + TestSql.uniqueTable();
        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
        }
        return schema;
    }

    /** Connections whose {@code search_path} is {@code schema}. */
    private static PGSimpleDataSource dataSource(String schema) {
        var ds = (PGSimpleDataSource) TestDataSources.newDataSource();
        ds.setCurrentSchema(schema);
        return ds;
    }

    private static void apply(String schema, String script) throws SQLException {
        try (var c = dataSource(schema).getConnection()) {
            TestSql.execute(c, script);
        }
    }

    /**
     * Tables (partitions aside) with their partition keys, columns with types, nullability, defaults and identity,
     * identity sequences' options, constraints and indexes, the schema's name taken out.
     */
    private static List<String> catalog(String schema) throws SQLException {
        var tables = "select r.oid, r.relname from pg_class r join pg_namespace n on n.oid = r.relnamespace"
                + " where n.nspname = ? and r.relkind in ('r', 'p') and not r.relispartition";
        var lines = new ArrayList<String>();
        try (var c = TestDataSources.newDataSource().getConnection()) {
            lines.addAll(strings(c, "select 'table ' || t.relname || ' ' || coalesce(pg_get_partkeydef(t.oid), '-')"
                    + " from (" + tables + ") t", schema));
            lines.addAll(strings(c, "select 'column ' || t.relname || '.' || a.attname || ' '"
                    + " || format_type(a.atttypid, a.atttypmod) || ' notnull=' || a.attnotnull || ' default='"
                    + " || coalesce(pg_get_expr(d.adbin, d.adrelid), '-') || ' identity=' || a.attidentity::text"
                    + " || ' generated=' || a.attgenerated::text from (" + tables + ") t join pg_attribute a"
                    + " on a.attrelid = t.oid left join pg_attrdef d on d.adrelid = t.oid and d.adnum = a.attnum"
                    + " where a.attnum > 0 and not a.attisdropped", schema));
            lines.addAll(strings(c, "select 'sequence ' || t.relname || '.' || a.attname || ' '"
                    + " || format_type(s.seqtypid, null) || ' start=' || s.seqstart || ' increment=' || s.seqincrement"
                    + " || ' min=' || s.seqmin || ' max=' || s.seqmax || ' cache=' || s.seqcache || ' cycle='"
                    + " || s.seqcycle from (" + tables + ") t join pg_attribute a on a.attrelid = t.oid"
                    + " join pg_sequence s on s.seqrelid = pg_get_serial_sequence(t.oid::regclass::text, a.attname)"
                    + "::regclass where a.attnum > 0", schema));
            lines.addAll(strings(c, "select 'constraint ' || t.relname || '.' || k.conname || ' '"
                    + " || pg_get_constraintdef(k.oid) from (" + tables + ") t join pg_constraint k"
                    + " on k.conrelid = t.oid", schema));
            lines.addAll(strings(c, "select 'index ' || t.relname || '.' || i.relname || ' ' || pg_get_indexdef(i.oid)"
                    + " from (" + tables + ") t join pg_index x on x.indrelid = t.oid join pg_class i"
                    + " on i.oid = x.indexrelid", schema));
        }
        lines.replaceAll(line -> line.replace(schema + ".", ""));
        lines.sort(null);
        return lines;
    }

    private static List<String> strings(Connection c, String sql, String schema) throws SQLException {
        var out = new ArrayList<String>();
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, schema);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
            }
        }
        return out;
    }
}
