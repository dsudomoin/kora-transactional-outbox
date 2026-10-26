package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxPermanentException;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.OutboxRelay;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Park;
import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

class MaintenanceIT {

    private final JdbcDatabase db = TestDb.newDatabase(10);
    // premake 10: 3000 burnt xids (plus a range's worth of start offset) leave more than premake / 2 ranges ahead, so
    // no run extends unless a test drives it there
    private final TestOutboxConfig config =
            new TestOutboxConfig().relayEnabled(false).xidsPerPartition(1000).premake(10);
    private final List<OutboxRelay> relays = new ArrayList<>();
    private final List<JdbcDatabase> pools = new ArrayList<>();
    private OutboxSchema schema;
    private TableNames names;

    @AfterEach
    void release() {
        relays.forEach(OutboxRelay::release);
        pools.forEach(JdbcDatabase::release);
        db.release();
    }

    @Test
    void dropsOnlyFullyProcessedPartitions() throws Exception {
        bootstrap();
        var handler = new RecordingHandler();
        var relay = relay(handler, maintenance());
        var ids = send(msg("t", "k", "a"), msg("t", null, "b"));
        TestDb.burnXids(3000);
        assertThat(relay.drain()).isEqualTo(2);
        long xmin = minCursorXmin();
        var before = ranges();
        var processed = before.stream().filter(b -> b.to() != null && Long.parseLong(b.to()) <= xmin).toList();
        assertThat(processed).as("ranges below the cursors' xmin").hasSizeGreaterThanOrEqualTo(2);

        assertThat(maintenance().runOnce()).isTrue();

        var after = ranges();
        assertThat(after).containsExactlyInAnyOrderElementsOf(
                before.stream().filter(b -> !processed.contains(b)).toList());
        for (var b : processed) {
            assertThat(exists(b.name())).as(b.name()).isFalse();
        }
        long current = currentXid();
        assertThat(after).as("the partition holding the current xid").anyMatch(b -> Long.parseLong(b.from()) <= current
                && (b.to() == null || current < Long.parseLong(b.to())));
        assertThat(after).anyMatch(b -> b.name().equals(names.tail()));
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void inFlightTransactionKeepsItsPartition() throws Exception {
        bootstrap();
        var handler = new RecordingHandler();
        var relay = relay(handler, maintenance());
        // a range below the holder's (when its xid opened a new range) waits for it to detach: give up quickly
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(null), Duration.ofSeconds(1));
        var id = UUID.randomUUID();
        String partition;
        try (var holder = TestDb.connect()) {
            holder.setAutoCommit(false);
            try (var ps = holder.prepareStatement("insert into " + names.base()
                    + " (shard, message_id, topic) values (0, ?, 't') returning tableoid::regclass::text")) {
                ps.setObject(1, id);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    partition = rs.getString(1);
                }
            }
            TestDb.burnXids(3000); // well past the holder's range
            assertThat(relay.drain()).isZero();
            assertThat(minCursorXmin()).isLessThan(upperBound(partition));

            assertThat(maintenance.runOnce()).isTrue();

            assertThat(exists(partition)).isTrue();
            holder.commit();
        }
        assertThat(relay.drain()).isEqualTo(1);
        assertThat(minCursorXmin()).isGreaterThanOrEqualTo(upperBound(partition));

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(exists(partition)).isFalse();
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
    }

    @Test
    void detachLockTimeoutIsRetriedByTheNextRun() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        send(msg("t", "k", "a"));
        TestDb.burnXids(3000);
        relay.drain();
        long xmin = minCursorXmin();
        var processed = ranges().stream().filter(b -> b.to() != null && Long.parseLong(b.to()) <= xmin).toList();
        assertThat(processed).isNotEmpty();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(null), Duration.ofMillis(300));

        try (var reader = TestDb.connect(); var err = new CapturedStderr()) {
            reader.setAutoCommit(false);
            queryLong(reader, "select count(*) from " + names.base()); // a detach waits for this transaction

            String succeeded = lastSuccess();
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(droppingMarkers()).as("the range whose detach timed out").isNotEmpty();
            assertThat(err.text()).contains("WARN").contains("lock timeout");
            assertThat(lastSuccess()).as("a run with a failed step is no success").isEqualTo(succeeded);
            assertThat(ranges()).as("still listed, attached or detach-pending").containsAll(processed);
            long processedTick = minCursorTickId();
            assertThat(tickIds()).as("the steps after the drop still ran").allMatch(id -> id >= processedTick);
            reader.commit();
        }

        String failed = lastSuccess();
        assertThat(maintenance.runOnce()).isTrue();

        assertThat(lastSuccess()).isGreaterThan(failed);
        assertThat(droppingMarkers()).isEmpty();
        assertThat(ranges()).doesNotContainAnyElementsOf(processed);
        for (var b : processed) {
            assertThat(exists(b.name())).as(b.name()).isFalse();
        }
    }

    @Test
    void secondRunnerSkipsWhileFirstHoldsLock() throws Exception {
        bootstrap();
        var poolA = pool();
        var poolB = pool();
        var a = new Maintenance(poolA, names, config, new OutboxMetrics(null));
        var b = new Maintenance(poolB, names, config, new OutboxMetrics(null));
        CompletableFuture<Boolean> runA;
        CompletableFuture<Boolean> runB;
        try (var blocker = TestDb.connect()) {
            blocker.setAutoCommit(false);
            // the lock holder waits at its cursor read (the xid check before the lock reads only ticks); the other
            // runner returns at once
            execute(blocker, "lock table " + names.cursor() + " in access exclusive mode");
            runA = CompletableFuture.supplyAsync(a::runOnce);
            runB = CompletableFuture.supplyAsync(b::runOnce);
            CompletableFuture.anyOf(runA, runB).get(10, TimeUnit.SECONDS);
            assertThat(runA.isDone() && runB.isDone()).as("the lock holder still waits").isFalse();
            blocker.commit();
        }

        assertThat(List.of(runA.get(10, TimeUnit.SECONDS), runB.get(10, TimeUnit.SECONDS)))
                .containsExactlyInAnyOrder(true, false);
        // both runners reset their session and the lock is free again
        for (var pool : List.of(poolA, poolB)) {
            try (var c = pool.newConnection()) {
                assertThat(queryString(c, "show idle_session_timeout")).isEqualTo("0");
            }
        }
        try (var c = TestDb.connect(); var ps = c.prepareStatement("select pg_try_advisory_lock(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_MAINTENANCE);
            try (var rs = ps.executeQuery()) {
                rs.next();
                assertThat(rs.getBoolean(1)).isTrue();
            }
        }
    }

    @Test
    void xidRegressionHalts() throws Exception {
        config.relayEnabled(true).idlePollInterval(Duration.ofSeconds(30)); // no idle ticks while the test forges one
        bootstrap();
        var handler = new RecordingHandler();
        var maintenance = maintenance();
        var relay = relay(handler, maintenance);
        relay.init();
        var outbox = new PgOutbox(db, schema, config.shards(), relay::wake, new OutboxMetrics(null));
        send(outbox, msg("t", "k", "before"));
        await(Duration.ofSeconds(5), () -> handler.delivered().size() == 1);

        var err = new CapturedStderr(); // before the forge: the relay's own run at start may already see it
        String original;
        long forged;
        try (var c = TestDb.connect()) {
            forged = new Ticks(names, config.shards()).latest(c).id();
            original = queryString(c, "select snapshot::text from " + names.tick() + " where id = " + forged);
            long ahead = currentXid() + 2000;
            execute(c, "update " + names.tick() + " set snapshot = '" + ahead + ":" + ahead + ":'::pg_snapshot"
                    + " where id = " + forged);
            // a tick made after the regression, as running workers would: it is consistent, the forged one is not
            assertThat(new Ticks(names, config.shards()).make(c, Duration.ZERO)).isTrue();
        }
        try (err) {
            try (var otherRunner = TestDb.connect()) {
                // another instance is mid-run: the check needs no maintenance lock, so every instance halts
                try (var ps = otherRunner.prepareStatement("select pg_advisory_lock(?, ?)")) {
                    ps.setInt(1, names.internalNamespace());
                    ps.setInt(2, TableNames.LOCK_MAINTENANCE);
                    ps.execute();
                }

                maintenance.runOnce();
                assertThat(maintenance.halted()).isTrue();
                try (var ps = otherRunner.prepareStatement("select pg_advisory_unlock(?, ?)")) { // close does not wait
                    ps.setInt(1, names.internalNamespace());
                    ps.setInt(2, TableNames.LOCK_MAINTENANCE);
                    ps.execute();
                }
            }
            long cursorRows;
            try (var c = TestDb.connect()) {
                var cursors = new Cursors(names);
                cursors.append(c, cursors.latest(c, 0)); // a superseded row, which cleanup would delete
                cursorRows = queryLong(c, "select count(*) from " + names.cursor());
            }
            TestDb.burnXids(3000); // the counter passes every tick again, but cursors still sit on pre-regression ticks

            assertThat(maintenance.runOnce()).isTrue(); // the lock is free now

            assertThat(maintenance.halted()).as("only repaired ticks end a halt").isTrue();
            try (var c = TestDb.connect()) {
                assertThat(queryLong(c, "select count(*) from " + names.cursor()))
                        .as("while halted only the check runs")
                        .isEqualTo(cursorRows);
            }
            assertThat(err.text().split("xid counter went backwards", -1)).as("ERROR at most once a minute").hasSize(2);
            assertThat(err.text()).contains("ERROR").contains("xid regression recovery");
        }
        send(outbox, msg("t", "k", "while halted"));
        Thread.sleep(500); // the wake-up would deliver within milliseconds

        assertThat(handler.delivered()).hasSize(1);
        assertThatThrownBy(relay::drain)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("outbox halted");

        try (var c = TestDb.connect()) { // the README procedure repairs the ticks
            execute(c, "update " + names.tick() + " set snapshot = '" + original + "'::pg_snapshot"
                    + " where id = " + forged);
        }
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maintenance.halted()).isFalse();
        try (var c = TestDb.connect()) { // a stale row would halt again once ticks pass its xmax
            assertThat(queryLong(c, "select count(*) from " + names.halt())).isZero();
        }
        relay.drain();
        assertThat(handler.delivered()).extracting(r -> new String(r.payload(), UTF_8))
                .containsExactly("before", "while halted");
    }

    @Test
    void xidCheckFindsTheHighestTickXmaxThroughAnIndex() throws Exception {
        bootstrap();
        var maintenance = maintenance();
        try (var c = TestDb.connect()) { // ticks pile up while a paused shard holds cleanup back
            execute(c, "insert into " + names.tick() + " (snapshot) select pg_current_snapshot()"
                    + " from generate_series(1, 50000)");
            execute(c, "analyze " + names.tick());
            var plan = new StringBuilder();
            try (var st = c.createStatement(); var rs = st.executeQuery("explain " + maintenance.xidCheck)) {
                while (rs.next()) {
                    plan.append(rs.getString(1)).append('\n');
                }
            }

            assertThat(plan.toString()).as(plan.toString())
                    .doesNotContain("Seq Scan on " + names.unqualified(names.tick()))
                    .contains("Index Scan Backward using " + names.unqualified(names.tick()) + "_xmax");
        }
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maintenance.halted()).isFalse();
    }

    @Test
    void haltSurvivesRestart() throws Exception {
        bootstrap();
        assertThat(maintenance().runOnce()).isTrue(); // the first instance: consistent
        forgeNewestTickAhead(2000);
        var first = maintenance();
        assertThat(first.runOnce()).isTrue();
        assertThat(first.halted()).isTrue();

        new OutboxSchema(db, config).init(); // a restart before the counter caught up: no failure, it comes up halted
        TestDb.burnXids(3000); // a restart after the counter caught up: the persisted halt still holds
        config.relayEnabled(true);
        var handler = new RecordingHandler();
        var restarted = maintenance();
        var relay = relay(handler, restarted);
        relay.init();

        assertThat(restarted.halted()).as("halted before its workers start").isTrue();
        var outbox = new PgOutbox(db, schema, config.shards(), relay::wake, new OutboxMetrics(null));
        send(outbox, msg("t", "k", "written while halted")); // business writes go on
        Thread.sleep(500); // the wake-up would deliver within milliseconds
        assertThat(handler.delivered()).isEmpty();
        assertThatThrownBy(relay::drain).isInstanceOf(IllegalStateException.class).hasMessageStartingWith("outbox");
        assertThat(restarted.runOnce()).isTrue();
        assertThat(restarted.halted()).isTrue();
    }

    @Test
    void haltCheckComesFirstAndOwnsTheStaleWarning() throws Exception {
        bootstrap();
        forgeNewestTickAhead(2000);
        try (var c = TestDb.connect()) { // no success for a day: the halt is why
            execute(c, "update " + names.maintenance() + " set succeeded_at = now() - interval '1 day'");
        }
        try (var err = new CapturedStderr()) {
            var maintenance = maintenance();
            assertThat(maintenance.runOnce()).isTrue();
            assertThat(maintenance.halted()).isTrue();
            assertThat(err.text()).contains("xid counter went backwards").doesNotContain("has not completed");
        }
        try (var c = TestDb.connect()) { // an operator drops the success marker
            execute(c, "drop table " + names.maintenance());
        }

        try (var err = new CapturedStderr()) {
            var restarted = maintenance();
            assertThat(restarted.runOnce()).isTrue();
            assertThat(restarted.halted()).as("the halt check still ran").isTrue();
            assertThat(err.text()).contains("WARN").contains(names.maintenance());
        }
    }

    @Test
    void operatorDeleteOfMarkerResumes() throws Exception {
        bootstrap();
        var maintenance = maintenance();
        forgeNewestTickAhead(2000);
        assertThat(maintenance.runOnce()).isTrue();
        TestDb.burnXids(3000);
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maintenance.halted()).as("sticky after the counter caught up").isTrue();
        var drainOnly = relay(new RecordingHandler(), maintenance()); // relay disabled: no maintenance runs there
        assertThatThrownBy(drainOnly::drain).isInstanceOf(IllegalStateException.class).hasMessageStartingWith("outbox");

        try (var c = TestDb.connect()) {
            execute(c, "delete from " + names.halt()); // the operator, per README "xid regression recovery"
        }
        assertThat(maintenance.runOnce()).isTrue();

        assertThat(maintenance.halted()).isFalse();
        var handler = new RecordingHandler();
        var relay = relay(handler, maintenance);
        send(msg("t", "k", "after resume"));
        assertThat(relay.drain()).isEqualTo(1);
    }

    @Test
    void cleansTicksAndCursors() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        var drained = new ArrayList<Long>();
        for (int i = 0; i < 3; i++) {
            if (i == 2) {
                TestDb.burnXids(2500); // ranges ending between the 2nd and the 3rd drain tick
            }
            send(msg("t", "k" + i, "m" + i));
            relay.drain();
            drained.add(latestCursors().get(0).tickId());
        }
        long slow = drained.get(1);
        long target = drained.get(2);
        long newer;
        List<Bound> between;
        try (var c = TestDb.connect()) {
            var ticks = new Ticks(names, config.shards());
            long slowXmin = ticks.get(c, slow).snapshot().xmin();
            long targetXmin = ticks.get(c, target).snapshot().xmin();
            between = ranges().stream()
                    .filter(b -> b.to() != null && Long.parseLong(b.to()) > slowXmin
                            && Long.parseLong(b.to()) <= targetXmin)
                    .toList();
            assertThat(between).isNotEmpty();
            // shard 0 lags behind the others, mid-batch towards the 3rd drain tick: it counts by its tick, not target
            var cursors = new Cursors(names);
            cursors.append(c, cursors.latest(c, 0).withProgress(slow, target, 1));
            assertThat(ticks.make(c, Duration.ZERO)).isTrue(); // a tick no shard has reached yet
            newer = ticks.latest(c).id();
            assertThat(queryLong(c, "select count(*) from " + names.cursor())).isGreaterThan(config.shards());
        }
        var cursorsBefore = latestCursors();
        assertThat(tickIds()).contains(drained.get(0));

        assertThat(maintenance().runOnce()).isTrue();

        assertThat(tickIds()).containsExactly(slow, target, newer); // from the slowest shard's tick on
        assertThat(ranges()).as("ranges the lagging shard has not passed").containsAll(between);
        assertThat(latestCursors()).isEqualTo(cursorsBefore);
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + names.cursor())).isEqualTo(config.shards());
        }
    }

    @Test
    void pendingRangeDetachDoesNotJamExtension() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        send(msg("t", "k", "a"));
        TestDb.burnXids(3000);
        relay.drain();
        long xmin = minCursorXmin();
        var pending = ranges().stream()
                .filter(b -> b.to() != null && Long.parseLong(b.to()) <= xmin)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry), Duration.ofMillis(300));
        try (var reader = TestDb.connect()) {
            reader.setAutoCommit(false);
            queryLong(reader, "select count(*) from " + names.base()); // the drop's detach times out waiting for it
            assertThat(maintenance.runOnce()).isTrue();
            reader.commit();
        }
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)",
                            pending.name()))
                    .isEqualTo("true");
            // into the extension window (at most premake / 2 ahead), whose tail detach a pending range would fail
            var partitions = new XidPartitions(names, config.partitions());
            while (partitions.premadeAhead(c) > config.premake() / 2) {
                TestDb.burnXids(200);
            }
        }

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(exists(pending.name())).isFalse();
        assertThat(gauge(registry, "outbox.partitions.premade")).isGreaterThanOrEqualTo(config.premake());
        assertThat(gauge(registry, "outbox.partitions.overflow")).isZero();
        assertThat(ranges()).filteredOn(b -> b.to() == null).singleElement()
                .satisfies(tail -> assertThat(tail.name()).isEqualTo(names.tail()));
    }

    @Test
    void tailNotAttachedReportsTheXidsLeft() throws Exception {
        bootstrap();
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry), Duration.ofMillis(300));
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(gauge(registry, "outbox.partitions.xids_left")).as("tail attached").isNaN();

        try (var holder = TestDb.connect(); var c = TestDb.connect(); var err = new CapturedStderr()) {
            holder.setAutoCommit(false); // a pg_dump
            execute(holder, "set transaction isolation level repeatable read");
            queryLong(holder, "select count(*) from " + names.base());
            // a tail detach that timed out waiting for it: the tail is pending, and FINALIZE waits for the snapshot
            execute(c, "set lock_timeout = '300ms'");
            assertThatThrownBy(() -> execute(c, "alter table " + names.base() + " detach partition " + names.tail()
                            + " concurrently"))
                    .isInstanceOf(SQLException.class);
            execute(c, "reset lock_timeout");
            long top = ranges().stream().filter(b -> b.to() != null).mapToLong(b -> Long.parseLong(b.to())).max()
                    .orElseThrow();
            long before = currentXid();

            assertThat(maintenance.runOnce()).isTrue();

            assertThat(gauge(registry, "outbox.partitions.xids_left")).isPositive().isLessThanOrEqualTo(top - before);
            assertThat(err.text()).contains("ERROR").contains("is not attached").contains("more xids");
            holder.commit();
        }

        assertThat(maintenance.runOnce()).isTrue(); // the snapshot is gone: FINALIZE, then a new tail

        assertThat(gauge(registry, "outbox.partitions.xids_left")).isNaN();
        assertThat(ranges()).filteredOn(b -> b.to() == null).singleElement()
                .satisfies(tail -> assertThat(tail.name()).isEqualTo(names.tail()));
    }

    @Test
    void rangeOrphanedBetweenDetachAndDropIsDroppedOnceRelayed() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        send(msg("t", "k", "a"), msg("t", "other", "b"));
        TestDb.burnXids(3000);
        relay.drain();
        long xmin = minCursorXmin();
        var orphan = ranges().stream()
                .filter(b -> b.to() != null && Long.parseLong(b.to()) <= xmin)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        try (var c = TestDb.connect()) { // a run killed after its DETACH went through, before its DROP
            markDropping(c, orphan.name());
            execute(c, "alter table " + names.base() + " detach partition " + orphan.name() + " concurrently");
        }
        var registry = new SimpleMeterRegistry();

        assertThat(new Maintenance(db, names, config, new OutboxMetrics(registry)).runOnce()).isTrue();

        assertThat(exists(orphan.name())).isFalse();
        assertThat(gauge(registry, "outbox.partitions.orphaned")).isZero();
        assertThat(droppingMarkers()).isEmpty();
    }

    @Test
    void rangeDetachedByHandIsKeptWithAnError() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        var orphan = ranges().stream() // a range with rows, detached before the relay got to them
                .filter(b -> b.to() != null)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + orphan.name() + " (id, shard, xid, message_id, topic) values (1000000, 0,"
                    + " '" + orphan.from() + "', gen_random_uuid(), 't')");
            while (currentXid() < Long.parseLong(orphan.to())) { // writers have moved on to the next range
                TestDb.burnXids(200);
            }
            execute(c, "alter table " + names.base() + " detach partition " + orphan.name() + " concurrently");
        }
        send(msg("t", "k", "a"));
        TestDb.burnXids(3000);
        relay.drain();
        assertThat(minCursorXmin()).as("the horizon passed it").isGreaterThanOrEqualTo(Long.parseLong(orphan.to()));
        var registry = new SimpleMeterRegistry();

        try (var err = new CapturedStderr()) {
            assertThat(new Maintenance(db, names, config, new OutboxMetrics(registry)).runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains(orphan.name()).contains("not by this library")
                    .contains("insert into " + names.base() + " (shard, message_id, topic, key, payload, headers)"
                            + " select shard, message_id, topic, key, payload, headers from " + orphan.name());
        }
        assertThat(exists(orphan.name())).isTrue();
        assertThat(gauge(registry, "outbox.partitions.orphaned")).isEqualTo(1);
    }

    @Test
    void operatorsPendingDetachIsNeverTakenOverAsRelayed() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        var range = ranges().stream()
                .filter(b -> b.to() != null)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(null), Duration.ofMillis(300));
        try (var reader = TestDb.connect(); var operator = TestDb.connect()) {
            execute(operator, "insert into " + range.name() + " (id, shard, xid, message_id, topic) values (1000000,"
                    + " 0, '" + range.from() + "', gen_random_uuid(), 't')");
            while (currentXid() < Long.parseLong(range.to())) {
                TestDb.burnXids(200);
            }
            reader.setAutoCommit(false); // a report: a snapshot and a lock on the outbox, no xid
            execute(reader, "set transaction isolation level repeatable read");
            queryLong(reader, "select count(*) from " + names.base());
            execute(operator, "set lock_timeout = '300ms'"); // an operator's detach, cancelled: the range stays pending
            assertThatThrownBy(() -> execute(operator, "alter table " + names.base() + " detach partition "
                            + range.name() + " concurrently"))
                    .isInstanceOf(SQLException.class);
            send(msg("t", "k", "a")); // the relay reads through the parent: it never sees the pending range's row
            TestDb.burnXids(3000);
            relay.drain();
            assertThat(minCursorXmin()).isGreaterThanOrEqualTo(Long.parseLong(range.to()));

            assertThat(maintenance.runOnce()).isTrue(); // FINALIZE waits for the report's snapshot: it times out

            assertThat(droppingMarkers()).as("not marked as relayed").doesNotContain(names.unqualified(range.name()));
            reader.commit();
        }
        var registry = new SimpleMeterRegistry();
        try (var err = new CapturedStderr()) {
            assertThat(new Maintenance(db, names, config, new OutboxMetrics(registry)).runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains(range.name()).contains("not by this library");
        }
        assertThat(exists(range.name())).as("the only copy of an undelivered row").isTrue();
        assertThat(gauge(registry, "outbox.partitions.orphaned")).isEqualTo(1);
    }

    @Test
    void keptOrphanIsNotLockedExclusivelyAndAFailedSweepIsUnknown() throws Exception {
        bootstrap();
        var range = ranges().stream()
                .filter(b -> b.to() != null)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic) values (1000000, 0,"
                    + " '" + range.from() + "', gen_random_uuid(), 't')");
            while (currentXid() < Long.parseLong(range.to())) {
                TestDb.burnXids(200);
            }
            execute(c, "alter table " + names.base() + " detach partition " + range.name() + " concurrently");
        }
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry), Duration.ofSeconds(5));
        try (var operator = TestDb.connect()) {
            operator.setAutoCommit(false); // compares its rows, as the ERROR asks, in an open transaction
            queryLong(operator, "select count(*) from " + range.name());
            long start = System.nanoTime();

            assertThat(maintenance.runOnce()).isTrue();

            assertThat(Duration.ofNanos(System.nanoTime() - start)).as("no wait for an exclusive lock")
                    .isLessThan(Duration.ofSeconds(4));
            assertThat(gauge(registry, "outbox.partitions.orphaned")).isEqualTo(1);
            operator.commit();
        }
        try (var c = TestDb.connect()) {
            execute(c, "drop table " + names.dropping()); // the sweep cannot run
        }

        maintenance.runOnce();

        assertThat(gauge(registry, "outbox.partitions.orphaned")).as("unknown, not the old count").isNaN();
    }

    @Test
    void emptyLeftoverRangeIsDroppedAndStaleMarkersGo() throws Exception {
        bootstrap();
        var leftover = names.partitionPrefix() + "999000000"; // a create whose attach never ran
        try (var c = TestDb.connect()) {
            execute(c, "create table " + leftover + " (like " + names.base() + " including defaults)");
            markDropping(c, names.partitionPrefix() + "1"); // its range was dropped, its marker not deleted
        }

        assertThat(maintenance().runOnce()).isTrue();

        assertThat(exists(leftover)).isFalse();
        assertThat(droppingMarkers()).isEmpty();
    }

    @Test
    void orphanedRangeWithRowsAtTheHorizonIsKeptWithAnError() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        send(msg("t", "k", "a"));
        TestDb.burnXids(3000);
        relay.drain();
        long xmin = minCursorXmin();
        var orphan = ranges().stream()
                .filter(b -> b.to() != null && Long.parseLong(b.to()) <= xmin)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        try (var c = TestDb.connect()) { // ours, detached without its range check, then a row the relay never saw
            markDropping(c, orphan.name());
            execute(c, "alter table " + names.base() + " detach partition " + orphan.name());
            execute(c, "insert into " + orphan.name() + " (id, shard, xid, message_id, topic) values (1000000, 0,"
                    + " '" + (Long.parseLong(orphan.from()) + 1) + "', gen_random_uuid(), 't'),"
                    + " (1000001, 3, '" + xmin + "', gen_random_uuid(), 't')");
        }
        var registry = new SimpleMeterRegistry();

        try (var err = new CapturedStderr()) {
            assertThat(new Maintenance(db, names, config, new OutboxMetrics(registry)).runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains(orphan.name()).contains("cannot deliver");
        }
        assertThat(exists(orphan.name())).isTrue();
        assertThat(gauge(registry, "outbox.partitions.orphaned")).isEqualTo(1);
    }

    @Test
    void dropDoesNotBlockConcurrentInserts() throws Exception {
        config.xidsPerPartition(10_000).premake(8);
        bootstrap();
        var relay = relay(noop(), maintenance());
        var maintenance = maintenance();
        var lowest = ranges().stream()
                .filter(b -> b.to() != null)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        var done = new AtomicBoolean();
        var maxLatency = new AtomicLong();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writers = new ArrayList<Thread>();
        for (int i = 0; i < 4; i++) {
            var writer = new Thread(() -> {
                try (var c = TestDb.connect()) {
                    c.setAutoCommit(false);
                    while (!done.get()) {
                        long start = System.nanoTime();
                        execute(c, "insert into " + names.base()
                                + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
                        c.commit();
                        maxLatency.accumulateAndGet(System.nanoTime() - start, Math::max);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            writer.start();
            writers.add(writer);
        }

        long cap = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        try {
            // until the lowest range is dropped, at least 3 runs (more drops, and extensions) while the writers insert
            for (int runs = 0; runs < 3 || exists(lowest.name()); runs++) {
                assertThat(System.nanoTime()).as("the lowest range dropped within 60 s").isLessThan(cap);
                TestDb.burnXids(3000);
                relay.drain();
                assertThat(maintenance.runOnce()).isTrue();
            }
        } finally {
            done.set(true);
            for (var w : writers) {
                w.join();
            }
        }

        assertThat(errors).isEmpty();
        assertThat(Duration.ofNanos(maxLatency.get())).isLessThan(Duration.ofSeconds(2));
        assertThat(ranges()).doesNotContain(lowest);
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + names.base())).isPositive();
        }
    }

    @Test
    void transactionPoolingSkipsTheRunAndRaisesTheGauge() throws Exception {
        bootstrap();
        var registry = new SimpleMeterRegistry();
        var sql = new ConcurrentLinkedQueue<String>();
        var pooled = new Maintenance(TestDb.transactionPooled(db, sql), names, config, new OutboxMetrics(registry));

        try (var err = new CapturedStderr()) {
            assertThat(pooled.runOnce()).as("skipped").isFalse();
            assertThat(pooled.runOnce()).isFalse();
            assertThat(err.text()).contains("ERROR");
            assertThat(err.text().split("transaction-pooling proxy detected", -1)).as("once a minute").hasSize(2);
        }

        assertThat(gauge(registry, "outbox.maintenance.transaction_pooling")).isEqualTo(1);
        assertThat(sql).as("no session lock or setting").isNotEmpty()
                .noneMatch(s -> s.contains("advisory") || s.contains("idle_session_timeout"));
        var directRegistry = new SimpleMeterRegistry();
        var direct = new Maintenance(db, names, config, new OutboxMetrics(directRegistry));
        assertThat(direct.runOnce()).isTrue();
        assertThat(gauge(directRegistry, "outbox.maintenance.transaction_pooling")).isZero();
    }

    @Test
    void timeSinceSuccessIsSharedByInstancesAndWarnsWhenStale() throws Exception {
        config.maintenanceInterval(Duration.ofMillis(200)); // stale after 5 × that
        bootstrap();
        var registry = new SimpleMeterRegistry();
        var other = new Maintenance(db, names, config, new OutboxMetrics(registry)); // never gets the lock below
        assertThat(maintenance().runOnce()).isTrue();

        try (var holder = TestDb.connect(); var err = new CapturedStderr()) {
            try (var ps = holder.prepareStatement("select pg_advisory_lock(?, ?)")) { // a stuck holder
                ps.setInt(1, names.internalNamespace());
                ps.setInt(2, TableNames.LOCK_MAINTENANCE);
                ps.execute();
            }
            assertThat(other.runOnce()).isFalse();
            assertThat(gauge(registry, "outbox.maintenance.since_success")).as("the other instance's run")
                    .isLessThan(1.0);
            Thread.sleep(1200);
            assertThat(gauge(registry, "outbox.maintenance.since_success")).as("grows between runs")
                    .isGreaterThan(1.0);
            assertThat(err.text()).doesNotContain("has not completed");

            assertThat(other.runOnce()).isFalse();

            assertThat(err.text()).contains("WARN").contains("has not completed");
            try (var ps = holder.prepareStatement("select pg_advisory_unlock(?, ?)")) { // close does not wait
                ps.setInt(1, names.internalNamespace());
                ps.setInt(2, TableNames.LOCK_MAINTENANCE);
                ps.execute();
            }
        }
        assertThat(other.runOnce()).isTrue();
        assertThat(gauge(registry, "outbox.maintenance.since_success")).isLessThan(1.0);
    }

    @Test
    void gaugesReflectState() throws Exception {
        config.shards(1).retry(Duration.ofHours(1), Duration.ofHours(1), 0); // a diverted head is due in an hour
        bootstrap();
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));
        assertThat(registry.getMeters()).extracting(m -> m.getId().getName()).containsExactlyInAnyOrder(
                "outbox.shard.lag", "outbox.retry.size", "outbox.dead.size", "outbox.parked.size",
                "outbox.partitions.premade", "outbox.partitions.overflow", "outbox.partitions.xids_left",
                "outbox.partitions.orphaned", "outbox.maintenance.transaction_pooling",
                "outbox.maintenance.since_success");
        var handler = new RecordingHandler()
                .failAlways(r -> "poison".equals(key(r)), () -> new OutboxPermanentException("bad payload"))
                .failAlways(r -> "slow".equals(key(r)), () -> new RuntimeException("target slow"));
        var relay = relay(handler, maintenance);
        send(msg("t", "ok", "1"), msg("t", "poison", "2"), msg("t", "slow", "3"), msg("t", "slow", "4"));
        relay.drain();
        try (var c = TestDb.connect()) {
            assertThat(new Ticks(names, config.shards()).make(c, Duration.ZERO)).isTrue(); // not processed: 10 s ahead of the cursor
            execute(c, "update " + names.tick() + " set created_at = created_at - interval '10 seconds'"
                    + " where id = " + minCursorTickId());
            var parked =
                    new OutboxRecord(1_000_000, UUID.randomUUID(), "t", null, null, List.of(), Instant.now(), 0, 0);
            new ParkedStore(names).apply(c, 0, List.of(new Park(parked, 1, Instant.now())), Set.of());
        }

        assertThat(maintenance.runOnce()).isTrue();

        try (var c = TestDb.connect()) {
            assertThat(gauge(registry, "outbox.retry.size"))
                    .isEqualTo(queryLong(c, "select count(*) from " + names.retry()))
                    .isEqualTo(2);
            assertThat(gauge(registry, "outbox.dead.size"))
                    .isEqualTo(queryLong(c, "select count(*) from " + names.dead()))
                    .isEqualTo(1);
            assertThat(gauge(registry, "outbox.parked.size"))
                    .isEqualTo(queryLong(c, "select count(*) from " + names.parked()))
                    .isEqualTo(1);
            assertThat(gauge(registry, "outbox.partitions.premade"))
                    .isEqualTo(new XidPartitions(names, config.partitions()).premadeAhead(c));
        }
        assertThat(gauge(registry, "outbox.partitions.overflow")).isZero();
        assertThat(registry.get("outbox.shard.lag").gauge().value()).isBetween(10.0, 20.0);
    }

    private void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
    }

    /** As after a dump/restore: the newest tick's xmax lies {@code ahead} xids past the counter. */
    private void forgeNewestTickAhead(long ahead) throws SQLException {
        try (var c = TestDb.connect()) {
            long xmax = currentXid() + ahead;
            execute(c, "update " + names.tick() + " set snapshot = '" + xmax + ":" + xmax + ":'::pg_snapshot"
                    + " where id = (select max(id) from " + names.tick() + ")");
        }
    }

    private Maintenance maintenance() {
        return new Maintenance(db, names, config, new OutboxMetrics(null));
    }

    private JdbcDatabase pool() {
        var pool = TestDb.newDatabase(1);
        pools.add(pool);
        return pool;
    }

    private OutboxRelay relay(OutboxHandler handler, Maintenance maintenance) {
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance);
        relays.add(relay);
        return relay;
    }

    private static OutboxHandler noop() {
        return record -> {};
    }

    private List<UUID> send(OutboxMessage... messages) {
        return send(new PgOutbox(db, schema, config.shards(), () -> {}, new OutboxMetrics(null)), messages);
    }

    private List<UUID> send(PgOutbox outbox, OutboxMessage... messages) {
        return db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(messages)));
    }

    private static OutboxMessage msg(String topic, String key, String payload) {
        return key == null
                ? OutboxMessage.of(topic, payload.getBytes(UTF_8))
                : OutboxMessage.of(topic, key, payload.getBytes(UTF_8));
    }

    private static String key(OutboxRecord r) {
        return r.key() == null ? null : new String(r.key(), UTF_8);
    }

    private List<Cursor> latestCursors() throws SQLException {
        try (var c = TestDb.connect()) {
            return new Cursors(names).latestAll(c, config.shards());
        }
    }

    private long minCursorTickId() throws SQLException {
        return latestCursors().stream().mapToLong(Cursor::tickId).min().orElseThrow();
    }

    /** {@code pg_snapshot_xmin} of the tick of the slowest shard. */
    private long minCursorXmin() throws SQLException {
        try (var c = TestDb.connect()) {
            return new Ticks(names, config.shards()).get(c, minCursorTickId()).snapshot().xmin();
        }
    }

    private List<Long> tickIds() throws SQLException {
        try (var c = TestDb.connect();
                var st = c.createStatement();
                var rs = st.executeQuery("select id from " + names.tick() + " order by id")) {
            var ids = new ArrayList<Long>();
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
            return ids;
        }
    }

    private List<Bound> ranges() throws SQLException {
        try (var c = TestDb.connect()) {
            return PartitionDdl.listRanges(c, names.base());
        }
    }

    private long upperBound(String partition) throws SQLException {
        return ranges().stream()
                .filter(b -> b.name().equals(partition))
                .mapToLong(b -> Long.parseLong(b.to()))
                .findFirst()
                .orElseThrow();
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select (to_regclass(?) is not null)::text", table).equals("true");
        }
    }

    /** As {@code XidPartitions.dropProcessed} records a range before its detach. */
    private void markDropping(java.sql.Connection c, String range) throws SQLException {
        execute(c, "insert into " + names.dropping() + " (table_name) values ('" + names.unqualified(range) + "')");
    }

    private List<String> droppingMarkers() throws SQLException {
        try (var c = TestDb.connect();
                var st = c.createStatement();
                var rs = st.executeQuery("select table_name from " + names.dropping())) {
            var markers = new ArrayList<String>();
            while (rs.next()) {
                markers.add(rs.getString(1));
            }
            return markers;
        }
    }

    /** {@code succeeded_at} of the maintenance marker, as sortable text. */
    private String lastSuccess() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select to_char(succeeded_at at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') from "
                    + names.maintenance());
        }
    }

    private static long currentXid() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
        }
    }

    private static double gauge(SimpleMeterRegistry registry, String name) {
        return registry.get(name).gauge().value();
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void await(Duration timeout, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(5);
        }
    }
}
