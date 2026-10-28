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
}
