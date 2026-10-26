package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

/**
 * An overflowed tail turned into the range {@code [H, U)} by detach, rename and attach in one transaction, which holds
 * ACCESS EXCLUSIVE on the parent throughout: short only if the attach skips its validation scan. Postgres says which
 * it did at DEBUG1, delivered to pgjdbc as the attach statement's warnings.
 */
class AttachScanIT {

    private static final int ROWS = 200_000;

    private final JdbcDatabase db = TestDb.newDatabase(2);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void attachSkipsTheScanWithAValidatedBoundCheck() throws Exception {
        var names = tailWithRows();

        try (var c = TestDb.connect()) {
            long h = tail(c, names);
            long u = upper(c);
            addBoundCheck(c, names, h, u);
            execute(c, "alter table " + names.tail() + " validate constraint " + names.tail() + "_bounded");

            var warnings = swap(c, names, h, u);

            assertThat(warnings).contains(implied(names, h)).doesNotContain(verifying(names, h));
            assertThat(queryLong(c, "select count(*) from " + names.base())).isEqualTo(ROWS);
            PartitionDdl.createAndAttach(c, names.base(), names.tail(), Long.toString(u), null,
                    PartitionDdl.CREATE_LOCK_TIMEOUT);
            execute(c, "insert into " + names.base()
                    + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
        }
    }

    @Test
    void attachScansWithoutTheCheck() throws Exception {
        var names = tailWithRows();

        try (var c = TestDb.connect()) {
            long h = tail(c, names);
            long u = upper(c);

            var warnings = swap(c, names, h, u);

            assertThat(warnings).doesNotContain(implied(names, h)).contains(verifying(names, h));
        }
    }

    @Test
    void aNotValidCheckDoesNotCount() throws Exception {
        var names = tailWithRows();

        try (var c = TestDb.connect()) {
            long h = tail(c, names);
            long u = upper(c);
            addBoundCheck(c, names, h, u);

            var warnings = swap(c, names, h, u);

            assertThat(warnings).doesNotContain(implied(names, h)).contains(verifying(names, h));
        }
    }

    /** An outbox whose tail holds {@link #ROWS} rows in 20 transactions, as when maintenance falls behind. */
    private TableNames tailWithRows() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(2);
        var schema = new OutboxSchema(db, config);
        schema.init();
        var names = schema.names();
        try (var c = TestDb.connect()) {
            TestDb.burnXids((int) (tail(c, names) - currentXid(c)) + 10);
            for (int i = 0; i < 20; i++) {
                execute(c, "insert into " + names.base() + " (shard, message_id, topic)"
                        + " select s % 4, gen_random_uuid(), 't' from generate_series(1, 10000) s");
            }
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(ROWS);
        }
        return names;
    }

    private static void addBoundCheck(Connection c, TableNames names, long h, long u) throws SQLException {
        execute(c, "alter table " + names.tail() + " add constraint " + names.tail() + "_bounded"
                + " check (xid >= '" + h + "' and xid < '" + u + "') not valid");
    }

    /** Detaches the tail, renames it to the range name and attaches it as {@code [h, u)}; the attach's warnings. */
    private static List<String> swap(Connection c, TableNames names, long h, long u) throws SQLException {
        String range = names.partitionPrefix() + h;
        c.setAutoCommit(false);
        try {
            execute(c, "set local client_min_messages = debug1");
            execute(c, "alter table " + names.base() + " detach partition " + names.tail());
            execute(c, "alter table " + names.tail() + " rename to " + range);
            var warnings = new ArrayList<String>();
            try (var st = c.createStatement()) {
                st.execute("alter table " + names.base() + " attach partition " + range
                        + " for values from ('" + h + "') to ('" + u + "')");
                for (var w = st.getWarnings(); w != null; w = w.getNextWarning()) {
                    warnings.add(w.getMessage());
                }
            }
            c.commit();
            return warnings;
        } finally {
            c.setAutoCommit(true);
        }
    }

    private static String implied(TableNames names, long h) {
        return "partition constraint for table \"" + names.partitionPrefix() + h
                + "\" is implied by existing constraints";
    }

    /** What ATRewriteTable says when it scans the table for the partition constraint. */
    private static String verifying(TableNames names, long h) {
        return "verifying table \"" + names.partitionPrefix() + h + "\"";
    }

    /** Two ranges above the current xid's, so the rows and this test's own DDL stay below it. */
    private static long upper(Connection c) throws SQLException {
        return (currentXid(c) / 1000 + 2) * 1000;
    }

    private static long currentXid(Connection c) throws SQLException {
        return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
    }

    private static long tail(Connection c, TableNames names) throws SQLException {
        return PartitionDdl.listRanges(c, names.base()).stream()
                .filter(b -> b.to() == null)
                .mapToLong(b -> Long.parseLong(b.from()))
                .findFirst()
                .orElseThrow();
    }
}
