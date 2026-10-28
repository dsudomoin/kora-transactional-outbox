package io.github.dsudomoin.outbox.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

/**
 * The startup check of base objects the application's migrations own ({@code outbox.schema.bootstrap = false}): every
 * table, column (type, nullability, default, identity), index, primary key and partition key of
 * {@link SchemaObjects} as this version needs it; DML rights on every table (and {@code SELECT} on the outbox's id
 * sequence) for the role of {@code app}; and for the role of {@code ddl}, which makes and drops the partitions,
 * membership of the partitioned table's owner and {@code CREATE} on its schema. What it does not know, a newer
 * schema's columns, indexes and tables, it ignores. Fails with one {@link IllegalStateException} naming every problem.
 */
final class SchemaCheck {

    private static final List<String> DML = List.of("SELECT", "INSERT", "UPDATE", "DELETE");

    private SchemaCheck() {}

    /** {@code table}: the configured name, for the message; {@code render}: the method whose SQL fixes it. */
    static void run(Connection app, Connection ddl, SchemaObjects required, String table, String render)
            throws SQLException {
        var problems = new ArrayList<String>();
        for (var t : required.tables()) {
            Long oid = oid(app, t.name());
            if (oid == null) {
                problems.add("table " + t.name() + " is missing" + fix(t.since(), render));
                continue;
            }
            if (t.partitionKey() != null) {
                var key = string(app, "select pg_get_partkeydef(?::oid)", oid);
                if (key == null || !key.toLowerCase(Locale.ROOT).equals(t.partitionKey())) {
                    problems.add(t.name() + " is " + (key == null ? "not partitioned" : "partitioned by " + key)
                            + ", not by " + t.partitionKey());
                }
            }
            columns(app, oid, t, render, problems);
            for (var index : t.indexes()) {
                if (oid(app, index.name()) == null) {
                    problems.add("index " + index.name() + " is missing" + fix(index.since(), render));
                }
            }
            if (!t.constraints().isEmpty() && !bool(app, "select exists (select 1 from pg_index where indrelid ="
                    + " ?::oid and indisprimary)", oid)) {
                problems.add(t.name() + " has no primary key" + fix(t.since(), render));
            }
            var missing = new ArrayList<String>();
            for (var privilege : DML) {
                if (!bool(app, "select has_table_privilege(?::oid, '" + privilege + "')", oid)) {
                    missing.add(privilege);
                }
            }
            if (!missing.isEmpty()) {
                problems.add(role(app) + " lacks " + String.join(", ", missing) + " on " + t.name());
            }
            if (t.partitionKey() != null) {
                partitionRights(ddl, t.name(), problems);
            }
        }
        var sequence = sequenceOf(app, required);
        if (sequence != null && !bool(app, "select has_sequence_privilege(?::oid, 'SELECT')", sequence.oid())) {
            problems.add(role(app) + " lacks SELECT on sequence " + sequence.name());
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("outbox schema check of " + table + " failed (outbox.schema.bootstrap ="
                    + " false): " + String.join("; ", problems));
        }
    }

    /**
     * Fails unless {@code app} and {@code owner} reach one database of one server, where {@code table} names the same
     * table (or none on both): an owner source pointed elsewhere would make partitions where the application never
     * writes.
     */
    static void requireSameDatabase(Connection app, Connection owner, String table) throws SQLException {
        var a = where(app, table);
        var o = where(owner, table);
        if (!a.equals(o)) {
            throw new IllegalStateException("outbox owner connection source reaches another database than the"
                    + " application's: owner " + o + ", application " + a + "; point both at the database of " + table
                    + " (and, for an unqualified name, give both roles the same search_path)");
        }
    }

    private static String where(Connection c, String table) throws SQLException {
        try (var ps = c.prepareStatement("select current_database(), pg_postmaster_start_time(),"
                + " to_regclass(?)::oid")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                var oid = rs.getString(3);
                return "database " + rs.getString(1) + " of the server started " + rs.getString(2) + ", "
                        + (oid == null ? "no table " + table : "table " + table + " oid " + oid);
            }
        }
    }

    private static void columns(Connection c, long oid, SchemaObjects.Table t, String render, List<String> problems)
            throws SQLException {
        record Actual(String type, boolean notNull, boolean hasDefault, boolean identity) {}
        var actual = new HashMap<String, Actual>();
        try (var ps = c.prepareStatement("select attname, format_type(atttypid, atttypmod), attnotnull, atthasdef,"
                + " attidentity <> '' from pg_attribute where attrelid = ?::oid and attnum > 0 and not attisdropped")) {
            ps.setLong(1, oid);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    actual.put(rs.getString(1), new Actual(rs.getString(2), rs.getBoolean(3), rs.getBoolean(4),
                            rs.getBoolean(5)));
                }
            }
        }
        for (var column : t.columns()) {
            var name = t.name() + "." + column.name();
            var a = actual.get(column.name());
            if (a == null) {
                problems.add("column " + name + " is missing" + fix(column.since(), render));
                continue;
            }
            if (!a.type().equals(column.type())) {
                problems.add("column " + name + " is " + a.type() + ", not " + column.type());
            }
            if (a.notNull() && !column.notNull()) { // the library leaves it null; the reverse is a newer schema's
                problems.add("column " + name + " is not null, but this version leaves it null");
            }
            if (column.identity() && !a.identity()) {
                problems.add("column " + name + " is not an identity column" + fix(column.since(), render));
            } else if (column.defaultValue() != null && !a.hasDefault()) {
                problems.add("column " + name + " has no default (" + column.defaultValue() + ")"
                        + fix(column.since(), render));
            }
        }
    }

    /** Partition DDL needs the parent's owner role (or membership of it) and {@code CREATE} on its schema. */
    private static void partitionRights(Connection ddl, String table, List<String> problems) throws SQLException {
        try (var ps = ddl.prepareStatement("select pg_has_role(r.relowner, 'USAGE'), has_schema_privilege("
                + "r.relnamespace, 'CREATE'), pg_get_userbyid(r.relowner), n.nspname from pg_class r join"
                + " pg_namespace n on n.oid = r.relnamespace where r.oid = to_regclass(?)")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return; // missing, as the application connection already said
                }
                if (!rs.getBoolean(1)) {
                    problems.add(role(ddl) + " does not own " + table + ", which its partition DDL needs (owner "
                            + rs.getString(3) + ": make this role a member of it, or give the outbox a connection"
                            + " source of the owner)");
                }
                if (!rs.getBoolean(2)) {
                    problems.add(role(ddl) + " lacks CREATE on schema " + rs.getString(4) + ", where the partitions"
                            + " of " + table + " are made");
                }
            }
        }
    }

    private record Sequence(String name, long oid) {}

    /** The outbox's id sequence, which the relay's ticks read; null for the inbox or when its table is missing. */
    private static Sequence sequenceOf(Connection c, SchemaObjects required) throws SQLException {
        var base = required.tables().get(0);
        if (base.columns().stream().noneMatch(SchemaObjects.Column::identity)) {
            return null;
        }
        try (var ps = c.prepareStatement("select s, s::regclass::oid from pg_get_serial_sequence(?, 'id') s"
                + " where to_regclass(?) is not null")) {
            ps.setString(1, base.name());
            ps.setString(2, base.name());
            try (var rs = ps.executeQuery()) {
                return rs.next() && rs.getString(1) != null ? new Sequence(rs.getString(1), rs.getLong(2)) : null;
            }
        }
    }

    private static String fix(int since, String render) {
        return " (schema version " + since + ": apply " + render + ")";
    }

    private static String role(Connection c) throws SQLException {
        return "role " + string(c, "select current_user", null);
    }

    private static Long oid(Connection c, String name) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?)::oid")) {
            ps.setString(1, name);
            try (var rs = ps.executeQuery()) {
                rs.next();
                long oid = rs.getLong(1);
                return rs.wasNull() ? null : oid;
            }
        }
    }

    private static boolean bool(Connection c, String sql, long oid) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            ps.setLong(1, oid);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static String string(Connection c, String sql, Long oid) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            if (oid != null) {
                ps.setLong(1, oid);
            }
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}
