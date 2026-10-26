package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.dsudomoin.kora.outbox.OutboxRelay;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

class OutboxSchemaIT {

    private final JdbcDatabase db = TestDb.newDatabase(10);
    private final TestOutboxConfig config = new TestOutboxConfig();

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void bootstrapCreatesAllObjects() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        var n = schema.names();

        try (var c = TestDb.connect()) {
            for (var table : List.of(n.base(), n.tick(), n.cursor(), n.retry(), n.dead(), n.halt(), n.tail(),
                    n.maintenance(), n.parked(), n.parked() + "_key", n.parked() + "_due", n.redriven())) {
                assertThat(queryString(c, "select to_regclass(?)::text", table)).as(table).isEqualTo(table);
            }
            assertThat(queryString(c, "select pg_get_indexdef(to_regclass(?))", n.parked() + "_key"))
                    .endsWith("(shard, topic, key, id)");
            assertThat(queryString(c, "select pg_get_indexdef(to_regclass(?))", n.parked() + "_due"))
                    .endsWith("(shard, due_at)");
            assertThat(OutboxSchema.columnExists(c, n.cursor(), "parked_due_at")).isTrue();
            assertThat(OutboxSchema.columnExists(c, n.retry(), "requeues")).isTrue();
            assertThat(queryString(c, "select pg_get_indexdef(to_regclass(?))", n.dead() + "_due"))
                    .endsWith(" (due_at) WHERE (due_at IS NOT NULL)");
            assertThat(queryString(c, "select pg_get_partkeydef(to_regclass(?))", n.base())).isEqualTo("RANGE (xid)");
            assertThat(new XidPartitions(n, config.partitions()).premadeAhead(c))
                    .isGreaterThanOrEqualTo(config.premake());
            assertThat(PartitionDdl.listRanges(c, n.base()))
                    .filteredOn(b -> b.to() == null)
                    .singleElement()
                    .satisfies(tail -> assertThat(tail.name()).isEqualTo(n.tail()));
            assertThat(queryLong(c, "select count(*) from " + n.tick())).isEqualTo(1);
            long tickId = queryLong(c, "select id from " + n.tick());
            assertThat(cursors(c, n)).containsExactly(
                    List.of(0L, tickId), List.of(1L, tickId), List.of(2L, tickId), List.of(3L, tickId));
        }
    }

    @Test
    void startupToleratesPendingRangeDetach() throws Exception {
        config.xidsPerPartition(1000).premake(10);
        var schema = new OutboxSchema(db, config);
        schema.init();
        var n = schema.names();
        var partitions = new XidPartitions(n, config.partitions());
        PartitionDdl.Bound lowest;
        try (var reader = TestDb.connect(); var c = TestDb.connect()) {
            lowest = PartitionDdl.listRanges(c, n.base()).stream()
                    .filter(b -> b.to() != null)
                    .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                    .orElseThrow();
            while (currentXid(c) < Long.parseLong(lowest.to())) {
                TestDb.burnXids(200);
            }
            // a drop interrupted by its lock timeout leaves the range detach-pending (one allowed per parent)
            execute(c, "insert into " + n.dropping() + " (table_name) values ('" + n.unqualified(lowest.name()) + "')");
            reader.setAutoCommit(false);
            queryLong(reader, "select count(*) from " + n.base());
            try (var st = c.createStatement()) {
                st.execute("set lock_timeout = '300ms'");
                assertThatThrownBy(() -> st.execute(
                                "alter table " + n.base() + " detach partition " + lowest.name() + " concurrently"))
                        .isInstanceOf(SQLException.class);
                st.execute("reset lock_timeout");
            }
            reader.commit();
            assertThat(queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)",
                            lowest.name()))
                    .isEqualTo("true");
            // into the extension window, so the same start also extends beside the pending range
            while (partitions.premadeAhead(c) > config.premake() / 2) {
                TestDb.burnXids(200);
            }
        }

        new OutboxSchema(db, config).init();

        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select count(*)::text from pg_inherits where inhrelid = to_regclass(?)",
                            lowest.name()))
                    .as("finalized: no longer in the way of the extension")
                    .isEqualTo("0");
            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(config.premake());
            assertThat(PartitionDdl.listRanges(c, n.base()))
                    .filteredOn(b -> b.to() == null)
                    .singleElement()
                    .satisfies(tail -> assertThat(tail.name()).isEqualTo(n.tail()));
        }
        assertThat(new Maintenance(db, n, config, new OutboxMetrics(null)).runOnce()).isTrue(); // it drops the range
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select (to_regclass(?) is null)::text", lowest.name())).isEqualTo("true");
        }
    }

    @Test
    void bootstrapIsIdempotentAndConcurrent() throws Exception {
        var barrier = new CyclicBarrier(8);
        var pool = Executors.newFixedThreadPool(8);
        try {
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < 8; i++) {
                futures.add(pool.submit(() -> {
                    var schema = new OutboxSchema(db, config);
                    barrier.await();
                    schema.init();
                    return null;
                }));
            }
            for (var f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        var schema = new OutboxSchema(db, config);
        schema.init();

        var n = schema.names();
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + n.tick())).isEqualTo(1);
            assertThat(cursors(c, n)).hasSize(config.shards());
        }
    }

    @Test
    void bootstrapAddsMaxIdToAnOlderTickTable() throws Exception {
        new OutboxSchema(db, config).init();
        var n = TableNames.of(config.table());
        try (var c = TestDb.connect()) {
            execute(c, "alter table " + n.tick() + " drop column max_id"); // a tick table of an older version

            new OutboxSchema(db, config).init();

            assertThat(queryString(c, "select count(*) || ':' || count(max_id) from " + n.tick())).isEqualTo("1:0");
        }
    }

    @Test
    void bootstrapAddsShardsToAnOlderTickTable() throws Exception {
        new OutboxSchema(db, config).init();
        var n = TableNames.of(config.table());
        try (var c = TestDb.connect()) {
            execute(c, "alter table " + n.tick() + " drop column shards"); // a tick table of an older version

            new OutboxSchema(db, config).init();

            assertThat(queryString(c, "select count(*) || ':' || count(shards) from " + n.tick())).isEqualTo("1:0");
            assertThat(new Ticks(n, config.shards()).make(c, Duration.ZERO)).isTrue();
            assertThat(queryString(c, "select shards::text from " + n.tick() + " order by id desc limit 1"))
                    .isEqualTo("{}");
        }
    }

    @Test
    void bootstrapAddsParkedDueAtAndRequeuesToOlderTables() throws Exception {
        new OutboxSchema(db, config).init();
        var n = TableNames.of(config.table());
        try (var c = TestDb.connect()) {
            // cursor and retry tables of the previous version, with a retry row
            execute(c, "alter table " + n.cursor() + " drop column parked_due_at");
            execute(c, "alter table " + n.retry() + " drop column requeues");
            execute(c, "insert into " + n.retry() + " (id, shard, message_id, topic, created_at, attempts)"
                    + " values (1, 0, gen_random_uuid(), 't', now(), 2)");

            new OutboxSchema(db, config).init();

            assertThat(queryString(c, "select count(*) || ':' || count(parked_due_at) from " + n.cursor()))
                    .isEqualTo(config.shards() + ":0");
            assertThat(queryString(c, "select id || ':' || attempts || ':' || requeues from " + n.retry()))
                    .isEqualTo("1:2:0");
            assertThat(new Cursors(n).latestAll(c, config.shards())).hasSize(config.shards());
        }
    }

    @Test
    void deadTableIsUpgradedInPlace() throws Exception {
        var n = TableNames.of(config.table());
        String letters = "select string_agg(concat_ws(':', id, status, coalesce(due_at::text, 'null'),"
                + " handler_attempts, coalesce(last_handler_error, 'null'), redrives), ',' order by id)"
                + " from " + n.dead();
        try (var c = TestDb.connect()) {
            execute(c, "create table " + n.dead() + """
                     (
                      id         bigint primary key,
                      shard      smallint not null,
                      message_id uuid not null,
                      topic      text not null,
                      key        bytea,
                      payload    bytea,
                      headers    bytea,
                      created_at timestamptz not null,
                      attempts   int not null,
                      failed_at  timestamptz not null default now(),
                      error      text
                    )"""); // a dead table of a version before the dead-letter hook
            execute(c, "insert into " + n.dead() + " (id, shard, message_id, topic, created_at, attempts, error)"
                    + " values (1, 0, gen_random_uuid(), 't', now(), 3, 'boom'),"
                    + " (2, 1, gen_random_uuid(), 't', now(), 5, null)");

            new OutboxSchema(db, config).init();

            assertThat(queryString(c, letters)).isEqualTo("1:kept:null:0:null:0,2:kept:null:0:null:0");
            assertThat(queryString(c, "select (to_regclass(?) is not null)::text", n.dead() + "_due"))
                    .isEqualTo("true");
            assertThatThrownBy(() -> execute(c, "update " + n.dead() + " set status = 'gone' where id = 1"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining(n.unqualified(n.dead()) + "_status");

            try (var writer = TestDb.connect()) {
                writer.setAutoCommit(false);
                execute(writer, "update " + n.dead() + " set error = 'again' where id = 1");
                // a no-op: no ALTER or CREATE INDEX queues behind the writer's lock on the dead table
                assertTimeoutPreemptively(Duration.ofSeconds(2), () -> new OutboxSchema(db, config).init());
                writer.rollback();
            }
            assertThat(queryString(c, letters)).isEqualTo("1:kept:null:0:null:0,2:kept:null:0:null:0");
        }
    }

    @Test
    void shardMismatchFailsFast() throws Exception {
        new OutboxSchema(db, config.shards(4)).init();

        var resharded = new OutboxSchema(db, config.shards(8));

        assertThatThrownBy(resharded::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shards = 8")
                .hasMessageContaining("4 shards");
    }

    @Test
    void xidRegressionFailsFast() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        try (var c = TestDb.connect()) {
            long ahead = queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint") + 1_000_000;
            try (var ps = c.prepareStatement("update " + schema.names().tick()
                    + " set snapshot = (? || ':' || ? || ':')::pg_snapshot where id = (select max(id) from "
                    + schema.names().tick() + ")")) {
                ps.setLong(1, ahead);
                ps.setLong(2, ahead);
                assertThat(ps.executeUpdate()).isEqualTo(1);
            }
        }

        assertThatThrownBy(new OutboxSchema(db, config)::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("xid counter went backwards");
    }

    @Test
    void insertLandsInXidPartition() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        var n = schema.names();

        try (var c = TestDb.connect()) {
            String partition = queryString(c, "insert into " + n.base()
                    + "(shard, message_id, topic) values (0, gen_random_uuid(), 't')"
                    + " returning tableoid::regclass::text");

            assertThat(partition).startsWith(n.partitionPrefix()).isNotEqualTo(n.tail());
        }
    }

    @Test
    void bootstrapOnExistingSchemaDoesNotBlockWriters() throws Exception {
        new OutboxSchema(db, config).init();
        var n = TableNames.of(config.table());

        try (var writer = TestDb.connect()) {
            writer.setAutoCommit(false);
            execute(writer,
                    "insert into " + n.base() + "(shard, message_id, topic) values (0, gen_random_uuid(), 't')");
            var second = CompletableFuture.runAsync(() -> {
                try {
                    new OutboxSchema(db, config).init();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });
            try {
                second.get(2, TimeUnit.SECONDS);
            } finally {
                writer.rollback();
                second.handle((r, e) -> null).join();
            }
        }
    }

    @Test
    void bootstrapToleratesExtensionLockTimeout() throws Exception {
        config.xidsPerPartition(1000).premake(4);
        new OutboxSchema(db, config).init();
        var n = TableNames.of(config.table());
        TestDb.burnXids(2500); // half of premake used: the next bootstrap wants to extend

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false);
            queryLong(holder, "select count(*) from " + n.base()); // a running reader the detach must wait for

            new OutboxSchema(db, config).init();

            holder.commit();
            assertThat(queryString(c, "insert into " + n.base()
                            + "(shard, message_id, topic) values (0, gen_random_uuid(), 't')"
                            + " returning tableoid::regclass::text"))
                    .startsWith(n.partitionPrefix())
                    .isNotEqualTo(n.tail());
            var partitions = new XidPartitions(n, config.partitions());
            partitions.ensurePremade(c); // maintenance finishes the extension
            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
        }
    }

    @Test
    void rejectsBadConfigBeforeAnyDdl() throws Exception {
        List<Map.Entry<String, Consumer<TestOutboxConfig>>> cases = List.of(
                Map.entry("outbox.shards", c -> c.shards(0)),
                Map.entry("outbox.shards", c -> c.shards(Short.MAX_VALUE + 1)),
                Map.entry("outbox.relay.workers", c -> c.workers(0)),
                Map.entry("outbox.relay.batchSize", c -> c.batchSize(0)),
                Map.entry("outbox.relay.maxTicksPerBatch", c -> c.maxTicksPerBatch(0)),
                Map.entry("outbox.relay.minTickInterval", c -> c.minTickInterval(Duration.ofMillis(-1))),
                Map.entry("outbox.relay.idlePollInterval", c -> c.idlePollInterval(Duration.ZERO)),
                Map.entry("outbox.relay.batchTimeout", c -> c.batchTimeout(Duration.ZERO)),
                Map.entry("outbox.relay.shutdownTimeout", c -> c.shutdownTimeout(Duration.ofMillis(-1))),
                Map.entry("outbox.relay.headFailuresBeforeDivert", c -> c.headFailuresBeforeDivert(0)),
                Map.entry("outbox.relay.retry.initialDelay", c -> c.retry(Duration.ZERO, Duration.ofSeconds(1), 0)),
                Map.entry("outbox.relay.retry.maxDelay", c -> c.retry(Duration.ofSeconds(2), Duration.ofSeconds(1), 0)),
                Map.entry("outbox.relay.retry.maxAttempts",
                        c -> c.retry(Duration.ofSeconds(1), Duration.ofSeconds(1), -1)),
                Map.entry("outbox.partitions.xidsPerPartition", c -> c.xidsPerPartition(999)),
                Map.entry("outbox.partitions.premake", c -> c.premake(1)),
                Map.entry("outbox.partitions.maintenanceInterval", c -> c.maintenanceInterval(Duration.ZERO)),
                Map.entry("outbox.partitions.swapLockTimeout", c -> c.swapLockTimeout(Duration.ZERO)),
                Map.entry("outbox.deadLetters.batchSize", c -> c.deadLettersBatchSize(0)),
                Map.entry("outbox.deadLetters.timeout", c -> c.deadLettersTimeout(Duration.ZERO)),
                Map.entry("outbox.deadLetters.retention", c -> c.deadLettersRetention(Duration.ofMillis(-1))),
                Map.entry("outbox.deadLetters.redriveHistory", c -> c.deadLettersRedriveHistory(Duration.ZERO)));

        try (var c = TestDb.connect()) {
            for (var e : cases) {
                var invalid = new TestOutboxConfig();
                e.getValue().accept(invalid);

                assertThatThrownBy(() -> new OutboxSchema(db, invalid))
                        .as(e.getKey())
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageStartingWith(e.getKey() + " ");
                assertThat(queryString(c, "select (to_regclass(?) is null)::text", invalid.table())).isEqualTo("true");
            }
        }
    }

    @Test
    void relayAndEnqueueRejectBadConfigWithTheSameMessages() {
        var schema = new OutboxSchema(db, config); // not initialized: their checks come first
        List<Map.Entry<String, Consumer<TestOutboxConfig>>> cases = List.of(
                Map.entry("outbox.relay.workers", c -> c.workers(0)),
                Map.entry("outbox.relay.minTickInterval", c -> c.minTickInterval(Duration.ofMillis(-1))),
                Map.entry("outbox.relay.idlePollInterval", c -> c.idlePollInterval(Duration.ZERO)),
                Map.entry("outbox.relay.shutdownTimeout", c -> c.shutdownTimeout(Duration.ofMillis(-1))),
                Map.entry("outbox.partitions.maintenanceInterval", c -> c.maintenanceInterval(Duration.ZERO)));
        for (var e : cases) {
            var invalid = new TestOutboxConfig();
            e.getValue().accept(invalid);

            assertThatThrownBy(() -> new OutboxRelay(db, invalid, schema, new RecordingHandler(),
                    new OutboxMetrics(null), null, null))
                    .as(e.getKey())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith(e.getKey() + " ");
        }
        for (int shards : new int[] {0, Short.MAX_VALUE + 1}) {
            assertThatThrownBy(() -> new PgOutbox(db, schema, shards, () -> {}, new OutboxMetrics(null)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("outbox.shards ");
        }
    }

    @Test
    void schemaLockWaitIsBoundedAndNamesTheLock() throws Exception {
        try (var holder = TestDb.connect()) {
            int ns = holdSchemaLock(holder); // an instance frozen in its bootstrap
            var schema = new OutboxSchema(db, config, 1);

            assertTimeoutPreemptively(Duration.ofSeconds(30), () -> assertThatThrownBy(schema::init)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("pg_advisory_lock(" + ns + ", " + TableNames.LOCK_SCHEMA + ")")
                    .hasMessageContaining("classid = " + Integer.toUnsignedLong(ns)));
            assertThat(queryString(holder, "select (to_regclass(?) is null)::text", config.table())).isEqualTo("true");
        }
    }

    @Test
    void schemaLockWaitRetriesUntilTheHolderLeaves() throws Exception {
        var schema = new OutboxSchema(db, config, 3);
        Future<?> init;
        var pool = Executors.newSingleThreadExecutor();
        try (var holder = TestDb.connect()) {
            holdSchemaLock(holder);
            init = pool.submit(() -> {
                schema.init();
                return null;
            });
            Thread.sleep(6000); // past the first attempt's lock_timeout
            assertThat(init.isDone()).isFalse();
        } // the holder's session ends, and its lock with it
        try {
            init.get(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertThat(schema.names().base()).isEqualTo(config.table());
    }

    @Test
    void shortSessionStatementTimeoutDoesNotCutBootstrapWaits() throws Exception {
        var props = new Properties();
        props.setProperty("options", "-c statement_timeout=1000"); // a role or URL default
        var shortDb = TestDb.newDatabase(2, props);
        try (var holder = TestDb.connect()) {
            int ns = holdSchemaLock(holder);

            assertTimeoutPreemptively(Duration.ofSeconds(30), () -> assertThatThrownBy(
                            new OutboxSchema(shortDb, config, 1)::init)
                    .isInstanceOf(IllegalStateException.class) // the lock_timeout, not the 1 s statement_timeout
                    .hasMessageContaining("pg_advisory_lock(" + ns + ", " + TableNames.LOCK_SCHEMA + ")"));
        } finally {
            shortDb.release();
        }
    }

    @Test
    void identitySequenceCacheAboveOneFailsStartup() throws Exception {
        new OutboxSchema(db, config).init();
        var n = TableNames.of(config.table());

        try (var c = TestDb.connect()) {
            for (var column : List.of(List.of(n.base(), "id"), List.of(n.tick(), "id"), List.of(n.cursor(), "seq"))) {
                execute(c, "alter table " + column.get(0) + " alter column " + column.get(1) + " set cache 10");

                assertThatThrownBy(new OutboxSchema(db, config)::init)
                        .as(column.get(0))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(column.get(0) + "_" + column.get(1) + "_seq")
                        .hasMessageContaining("cache 10");

                execute(c, "alter table " + column.get(0) + " alter column " + column.get(1) + " set cache 1");
            }
        }
        new OutboxSchema(db, config).init();
    }

    private int holdSchemaLock(Connection holder) throws SQLException {
        int ns = TableNames.of("public." + config.table()).internalNamespace(); // what every spelling resolves to
        try (var ps = holder.prepareStatement("select pg_advisory_lock(?, ?)")) {
            ps.setInt(1, ns);
            ps.setInt(2, TableNames.LOCK_SCHEMA);
            ps.execute();
        }
        return ns;
    }

    private static List<List<Long>> cursors(Connection c, TableNames n) throws SQLException {
        var rows = new ArrayList<List<Long>>();
        try (var st = c.createStatement();
                var rs = st.executeQuery("select shard, tick_id from " + n.cursor() + " order by shard")) {
            while (rs.next()) {
                rows.add(List.of(rs.getLong(1), rs.getLong(2)));
            }
        }
        return rows;
    }

    private static long currentXid(Connection c) throws SQLException {
        return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
    }
}
