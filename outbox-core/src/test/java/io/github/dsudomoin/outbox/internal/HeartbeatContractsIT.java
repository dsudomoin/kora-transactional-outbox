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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HeartbeatContractsIT {

    /** A beat that gives up on the cursor table's lock, which a recovery holds, has read the reports before. */
    @Test
    void aBeatThatGivesUpOnItsLockStillReadsTheReports() throws Exception {
        var ds = TestDataSources.newDataSource();
        var db = new JdbcTransactions(ds);
        var config = new TestOutboxConfig().relayEnabled(false); // its own row, reporting nothing, does not count
        try (var schema = new OutboxSchema(db, db, config, Map.of(), Set.of())) {
            schema.start();
            var names = schema.names();
            var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, OutboxTelemetry.NONE);
            var v2 = OutboxMessage.of("orders", "k", "p".getBytes(UTF_8))
                    .withHeader(Contracts.HEADER, "2".getBytes(US_ASCII));
            var row = "select relay || ' ' || contracts::text || ' ' || routes::text from " + names.instance()
                    + " where instance_id = ?";
            String bootstrapped = db.inTx(c -> TestSql.queryString(c, row, schema.instanceId()));
            new Heartbeat(db, schema, schema.config(), false).beat();
            String beaten = db.inTx(c -> TestSql.queryString(c, row, schema.instanceId()));
            assertThat(bootstrapped).as("the bootstrap's row: no route reports {} twice, never null")
                    .isEqualTo("false {} {}");
            assertThat(beaten).as("a beat's").isEqualTo("false {} {}");
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
     * The row reports every route's topic, typed or plain, sorted, from the bootstrap on and again on each beat; a read
     * takes every live relay instance's routes, flattened and without null whatever was written by hand, null when
     * unknown.
     */
    @Test
    void theRowReportsTheRoutesSortedAndAReadTakesEveryLiveRelays() throws Exception {
        var db = new JdbcTransactions(TestDataSources.newDataSource());
        try (var schema = new OutboxSchema(db, db, new TestOutboxConfig(), Map.of("billing", 2),
                Set.of("orders", "audit", "billing"))) {
            schema.start();
            var names = schema.names();
            var guard = schema.contractGuard();
            var row = "select routes::text from " + names.instance() + " where instance_id = ?";
            String bootstrapped = db.inTx(c -> TestSql.queryString(c, row, schema.instanceId()));
            var bootstrapRead = guard.snapshot();
            db.inTx(c -> {
                TestSql.execute(c, "update " + names.instance() + " set routes = '{stale}'");
                TestSql.execute(c, "insert into " + names.instance() + " (instance_id, shards, relay, routes,"
                        + " expires_at) values ('flat/1/0', 4, true, '{y,x}', now() + interval '1h'), ('nested/1/0',"
                        + " 4, true, '{{b,NULL},{a,x}}', now() + interval '1h'), ('unknown/1/0', 4, true, null, now()"
                        + " + interval '1h'), ('writer/1/0', 4, false, '{w}', now() + interval '1h'), ('expired/1/0',"
                        + " 4, true, '{e}', now() - interval '1s')");
                return null;
            });
            new Heartbeat(db, schema, schema.config(), false).beat();
            String beaten = db.inTx(c -> TestSql.queryString(c, row, schema.instanceId()));
            db.inTx(c -> {
                guard.refresh(c, names); // what the beat's read missed: its own upsert
                return null;
            });

            assertThat(bootstrapped).isEqualTo("{audit,billing,orders}");
            assertThat(beaten).as("rewritten by the beat").isEqualTo("{audit,billing,orders}");
            assertThat(guard.routes()).containsExactly("audit", "billing", "orders");
            assertThat(bootstrapRead.routes).as("the bootstrap's read: its own row")
                    .isEqualTo(Map.of(schema.instanceId(), Set.of("audit", "billing", "orders")));
            var expected = new HashMap<String, Set<String>>();
            expected.put(schema.instanceId(), Set.of("audit", "billing", "orders"));
            expected.put("flat/1/0", Set.of("x", "y"));
            expected.put("nested/1/0", Set.of("a", "b", "x"));
            expected.put("unknown/1/0", null);
            assertThat(guard.snapshot().routes).as("live relay instances only").isEqualTo(expected);
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
        try (var schema = new OutboxSchema(db, db, new TestOutboxConfig(), Map.of(), Set.of())) {
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
