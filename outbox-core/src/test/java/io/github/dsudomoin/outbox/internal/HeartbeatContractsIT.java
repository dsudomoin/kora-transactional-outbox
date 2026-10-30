package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxIncompatibleContractException;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HeartbeatContractsIT {

    /** A beat that gives up on the cursor table's lock, which a recovery holds, has read the reports before. */
    @Test
    void aBeatThatGivesUpOnItsLockStillReadsTheReports() throws Exception {
        var ds = TestDataSources.newDataSource();
        var db = new JdbcTransactions(ds);
        var config = new TestOutboxConfig().relayEnabled(false); // its own row, reporting nothing, does not count
        try (var schema = new OutboxSchema(db, db, config, Map.of())) {
            schema.start();
            var names = schema.names();
            var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, OutboxTelemetry.NONE);
            var v2 = OutboxMessage.of("orders", "k", "p".getBytes(UTF_8))
                    .withHeader(Contracts.HEADER, "2".getBytes(US_ASCII));
            var row = "select relay || ' ' || contracts::text from " + names.instance() + " where instance_id = ?";
            String bootstrapped = db.inTx(c -> TestSql.queryString(c, row, schema.instanceId()));
            new Heartbeat(db, schema, schema.config(), false).beat();
            String beaten = db.inTx(c -> TestSql.queryString(c, row, schema.instanceId()));
            assertThat(bootstrapped).as("the bootstrap's row: no typed route reports {}, never null")
                    .isEqualTo("false {}");
            assertThat(beaten).as("a beat's").isEqualTo("false {}");
            try (var other = ds.getConnection()) {
                TestSql.execute(other, "insert into " + names.instance() + " (instance_id, shards, relay, contracts,"
                        + " expires_at) values ('old/1/00000000', 4, true, '{\"orders\": 1}', now() + interval '1h')");
                db.inTx(c -> outbox.send(v2)); // the bootstrap's read is from before that row
                other.setAutoCommit(false);
                TestSql.execute(other, "lock table " + names.cursor() + " in access exclusive mode");
                try (var err = new CapturedStderr()) {
                    new Heartbeat(db, schema, schema.config(), false).beat();
                    assertThat(err.text()).contains("waited 100 ms for a lock");
                }
                other.rollback();
            }

            assertThatThrownBy(() -> db.inTx(c -> outbox.send(v2)))
                    .isInstanceOfSatisfying(OutboxIncompatibleContractException.class,
                            e -> assertThat(e.incompatibleInstances()).containsExactly("old/1/00000000"));
        }
    }

    /**
     * A recovery holds the cursor table for its run, which may outlast the row's expiry: a beat that gives up on that
     * lock has refreshed the row, so the instance stays live to every guard.
     */
    @Test
    void aBeatThatGivesUpOnTheCursorLockStillRefreshesItsRow() throws Exception {
        var ds = TestDataSources.newDataSource();
        var db = new JdbcTransactions(ds);
        try (var schema = new OutboxSchema(db, db, new TestOutboxConfig(), Map.of())) {
            schema.start();
            var names = schema.names();
            var live = "select (expires_at > now())::text from " + names.instance() + " where instance_id = ?";
            db.inTx(c -> {
                TestSql.execute(c, "update " + names.instance() + " set last_seen = now() - interval '2 minutes',"
                        + " expires_at = now() - interval '1 minute'");
                return null;
            });
            String expired = db.inTx(c -> TestSql.queryString(c, live, schema.instanceId()));
            assertThat(expired).isEqualTo("false");
            try (var recovering = ds.getConnection()) {
                recovering.setAutoCommit(false);
                TestSql.execute(recovering, "lock table " + names.cursor() + " in access exclusive mode");
                try (var err = new CapturedStderr()) {
                    new Heartbeat(db, schema, schema.config(), false).beat();
                    assertThat(err.text()).contains("waited 100 ms for a lock").doesNotContain("failed");
                }
                String refreshed = db.inTx(c -> TestSql.queryString(c, live, schema.instanceId()));
                assertThat(refreshed).as("refreshed while the cursor table is locked").isEqualTo("true");
                recovering.rollback();
            }
        }
    }
}
