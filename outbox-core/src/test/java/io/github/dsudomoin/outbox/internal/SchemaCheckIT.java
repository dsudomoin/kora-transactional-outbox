package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxSql;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class SchemaCheckIT {

    @Test
    void newerLibraryOnOlderSchemaFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());
        var tables = new ArrayList<>(SchemaObjects.outbox(names).tables());
        var tick = tables.get(1);
        var columns = new ArrayList<>(tick.columns());
        columns.add(new SchemaObjects.Column("lineage", "bigint", false, null, false, 2));
        tables.set(1, new SchemaObjects.Table(tick.name(), tick.since(), tick.partitionKey(), columns,
                tick.constraints(), tick.checks(), tick.indexes()));

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));

            assertThatThrownBy(() -> SchemaCheck.run(c, c, new SchemaObjects(tables), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("column " + names.tick() + ".lineage is missing (schema version 2: apply"
                            + " OutboxSql.outbox)");
        }
    }

    @Test
    void missingWaitingSinceFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "alter table " + names.retry() + " drop column waiting_since");

            assertThatThrownBy(() -> SchemaCheck.run(c, c, SchemaObjects.outbox(names), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("column " + names.retry() + ".waiting_since is missing (schema version 1:"
                            + " apply OutboxSql.outbox)");
        }
    }

    @Test
    void missingRoutesFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "alter table " + names.instance() + " drop column routes");

            assertThatThrownBy(() -> SchemaCheck.run(c, c, SchemaObjects.outbox(names), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("column " + names.instance() + ".routes is missing (schema version 1:"
                            + " apply OutboxSql.outbox)");
        }
    }

    @Test
    void missingWaitingIndexFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "drop index " + names.retry() + "_waiting");

            assertThatThrownBy(() -> SchemaCheck.run(c, c, SchemaObjects.outbox(names), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("index " + names.retry() + "_waiting is missing (schema version 1: apply"
                            + " OutboxSql.outbox)");
        }
    }
}
