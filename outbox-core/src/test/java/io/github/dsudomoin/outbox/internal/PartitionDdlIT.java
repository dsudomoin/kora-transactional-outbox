package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import org.junit.jupiter.api.Test;

class PartitionDdlIT {

    /**
     * A snapshot taken before another session dropped a range still lists it in {@code pg_inherits}, while
     * {@code pg_get_expr} finds no relation for it any more: as {@code admin.status()} during maintenance's drops.
     */
    @Test
    void aRangeDroppedAfterTheSnapshotIsLeftOut() throws Exception {
        var parent = TestSql.uniqueTable();
        try (var reader = TestDataSources.newDataSource().getConnection();
                var other = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(other, "create table " + parent + " (xid xid8 not null) partition by range (xid)");
            TestSql.execute(other, "create table " + parent + "_a partition of " + parent
                    + " for values from ('0') to ('100')");
            TestSql.execute(other, "create table " + parent + "_b partition of " + parent
                    + " for values from ('100') to ('200')");
            reader.setAutoCommit(false);
            reader.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            TestSql.queryLong(reader, "select 1"); // the snapshot
            TestSql.execute(other, "drop table " + parent + "_a");

            assertThat(PartitionDdl.listRanges(reader, parent))
                    .containsExactly(new PartitionDdl.Bound(parent + "_b", "100", "200"));
            reader.rollback();
        }
    }

    /** The same for a standalone table: {@code ::regclass} prints one dropped since the snapshot as its bare oid. */
    @Test
    void aStandaloneTableDroppedAfterTheSnapshotIsLeftOut() throws Exception {
        var parent = TestSql.uniqueTable();
        try (var reader = TestDataSources.newDataSource().getConnection();
                var other = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(other, "create table " + parent + " (xid xid8 not null) partition by range (xid)");
            TestSql.execute(other, "create table " + parent + "_x1 (like " + parent + ")");
            TestSql.execute(other, "create table " + parent + "_x2 (like " + parent + ")");
            reader.setAutoCommit(false);
            reader.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            TestSql.queryLong(reader, "select 1"); // the snapshot
            TestSql.execute(other, "drop table " + parent + "_x1");

            assertThat(PartitionDdl.standalone(reader, parent, "_x[0-9]+")).containsExactly(parent + "_x2");
            reader.rollback();
        }
    }
}
