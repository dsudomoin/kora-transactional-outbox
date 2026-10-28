package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.SchemaObjects;
import io.github.dsudomoin.outbox.internal.TableNames;
import jakarta.annotation.Nullable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The base schema as SQL, for an application whose migrations own it ({@code outbox.schema.bootstrap = false}): every
 * table, column, constraint, index and identity of the outbox, or of the inbox, for the configured table name, at
 * {@link #SCHEMA_VERSION}, the same objects the bootstrap makes. Partitions are not in it: the library makes and drops
 * them at runtime. An unqualified name lands in the migration's {@code current_schema()}, as with the bootstrap. With
 * an application role the script ends with the grants that role needs to run the outbox with DML only.
 * <pre>{@code
 * Files.writeString(Path.of("V7__outbox.sql"), OutboxSql.outbox(config, "app"));
 * }</pre>
 */
public final class OutboxSql {

    /** The version of the base schema this library needs. */
    public static final int SCHEMA_VERSION = 1;

    private static final Pattern ROLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    private OutboxSql() {}

    /** Every base object of the outbox for {@code config.table()}; with a role, the grants it needs. */
    public static String outbox(OutboxConfig config, @Nullable String applicationRole) {
        var names = TableNames.of(config.table());
        var sql = new StringBuilder();
        render(sql, "outbox " + names.base(), "OutboxSql.outbox", SchemaObjects.outbox(names), role(applicationRole));
        if (applicationRole != null) { // the tick's max_id reads it
            sql.append("grant select on sequence ").append(names.base()).append("_id_seq to ").append(applicationRole)
                    .append(";\n");
        }
        return sql.toString();
    }

    /** The inbox table for {@code config.inbox().table()}; with a role, the grants it needs. */
    public static String inbox(OutboxConfig config, @Nullable String applicationRole) {
        var names = TableNames.of(config.inbox().table());
        var sql = new StringBuilder();
        render(sql, "outbox inbox " + names.base(), "OutboxSql.inbox", SchemaObjects.inbox(names),
                role(applicationRole));
        return sql.toString();
    }

    @Nullable
    private static String role(@Nullable String role) {
        if (role != null && !ROLE.matcher(role).matches()) {
            throw new IllegalArgumentException(
                    "application role must be an SQL identifier [A-Za-z_][A-Za-z0-9_]{0,62}: " + role);
        }
        return role;
    }

    private static void render(StringBuilder sql, String what, String by, SchemaObjects objects,
            @Nullable String role) {
        sql.append("-- ").append(what).append(", schema version ").append(SCHEMA_VERSION).append(", rendered by ")
                .append(by).append("; the library makes its partitions at runtime\n");
        for (var table : objects.tables()) {
            sql.append("\ncreate table ").append(table.name()).append(" (\n");
            var lines = table.columns().stream().map(OutboxSql::column).collect(Collectors.toList());
            lines.addAll(table.constraints());
            sql.append(lines.stream().map(line -> "    " + line).collect(Collectors.joining(",\n")));
            sql.append("\n)");
            if (table.partitionKey() != null) {
                sql.append(" partition by ").append(table.partitionKey());
            }
            sql.append(";\n");
            for (var check : table.checks()) { // as the bootstrap adds them: no row has been checked
                sql.append("alter table ").append(table.name()).append(" add constraint ").append(check.name())
                        .append(' ').append(check.definition()).append(" not valid;\n");
            }
            for (var index : table.indexes()) {
                sql.append("create index ").append(index.name().substring(index.name().indexOf('.') + 1))
                        .append(" on ").append(table.name()).append(' ').append(index.definition()).append(";\n");
            }
        }
        if (role != null) {
            sql.append("\ngrant select, insert, update, delete on table ")
                    .append(objects.tables().stream().map(SchemaObjects.Table::name).collect(Collectors.joining(", ")))
                    .append(" to ").append(role).append(";\n");
        }
    }

    private static String column(SchemaObjects.Column column) {
        var line = new StringBuilder(column.name()).append(' ').append(column.type());
        if (column.identity()) {
            line.append(" generated always as identity (cache 1)"); // cached ids break the order of a key's messages
        } else if (column.notNull()) {
            line.append(" not null");
        }
        if (column.defaultValue() != null) {
            line.append(" default ").append(column.defaultValue());
        }
        return line.toString();
    }
}
