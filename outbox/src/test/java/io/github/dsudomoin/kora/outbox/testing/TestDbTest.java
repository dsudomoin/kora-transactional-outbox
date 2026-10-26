package io.github.dsudomoin.kora.outbox.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class TestDbTest {

    @Test
    void containerAnswersAndXidsBurn() throws SQLException {
        try (var c = TestDb.connect()) {
            assertThat(c.getAutoCommit()).isTrue();
            long before = currentXid(c);

            TestDb.burnXids(10);

            assertThat(currentXid(c) - before).isGreaterThanOrEqualTo(10);
        }
    }

    @Test
    void newDatabaseRunsKoraTransactions() {
        var db = TestDb.newDatabase(2);
        try {
            Integer one = db.inTx(c -> {
                try (var st = c.createStatement(); var rs = st.executeQuery("select 1")) {
                    rs.next();
                    return rs.getInt(1);
                }
            });
            assertThat(one).isEqualTo(1);
        } finally {
            db.release();
        }
    }

    @Test
    void uniqueTableIsShortLowercaseHex() {
        assertThat(TestDb.uniqueTable()).matches("ob_[0-9a-f]{10}").isNotEqualTo(TestDb.uniqueTable());
    }

    @Test
    void testConfigDefaultsToIsolatedTables() {
        var config = new TestOutboxConfig();

        assertThat(config.table()).matches("ob_[0-9a-f]{10}").isNotEqualTo(new TestOutboxConfig().table());
        assertThat(config.inbox().table()).matches("ob_[0-9a-f]{10}").isNotEqualTo(config.table());
    }

    private static long currentXid(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select pg_current_xact_id()::text::bigint")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
