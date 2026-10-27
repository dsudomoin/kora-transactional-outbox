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
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

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
        long xmin = horizonXmin();
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
            assertThat(horizonXmin()).isLessThan(upperBound(partition));

            assertThat(maintenance.runOnce()).isTrue();

            assertThat(exists(partition)).isTrue();
            holder.commit();
        }
        assertThat(relay.drain()).isEqualTo(1);
        assertThat(horizonXmin()).isGreaterThanOrEqualTo(upperBound(partition));

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
        long xmin = horizonXmin();
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
            assertThat(queryLong(reader, "select count(*) from " + names.cursor()))
                    .as("the steps after the drop still ran: cursor cleanup").isEqualTo((long) config.shards());
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
        config.xidRegression("halt"); // the halt itself; XidRecoveryIT covers the recovery
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
            // a tick attempt after the regression, as running workers make: refused, the halt recorded
            assertThat(new Ticks(names, config.shards()).tick(c, Duration.ZERO)).isEqualTo(Ticks.Made.REGRESSED);
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
                var cursors = new Cursors(names, config.shards());
                cursors.append(c, cursors.latest(c, 0).cursor()); // a superseded row, which cleanup would delete
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
            assertThat(err.text().split("xid regression detected", -1)).as("ERROR at most once a minute").hasSize(2);
            assertThat(err.text()).contains("ERROR").contains("OutboxAdmin.recoverXidRegression()");
        }
        send(outbox, msg("t", "k", "while halted"));
        Thread.sleep(500); // the wake-up would deliver within milliseconds

        assertThat(handler.delivered()).hasSize(1);
        assertThatThrownBy(relay::drain)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("outbox halted");

        try (var c = TestDb.connect()) { // ticks repaired, as the recovery leaves them
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
            try (var st = c.createStatement(); var rs = st.executeQuery("explain " + maintenance.recovery.xidCheck)) {
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
        config.xidRegression("halt");
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
        config.xidRegression("halt");
        bootstrap();
        forgeNewestTickAhead(2000);
        try (var c = TestDb.connect()) { // no success for a day: the halt is why
            execute(c, "update " + names.maintenance() + " set succeeded_at = now() - interval '1 day'");
        }
        try (var err = new CapturedStderr()) {
            var maintenance = maintenance();
            assertThat(maintenance.runOnce()).isTrue();
            assertThat(maintenance.halted()).isTrue();
            assertThat(err.text()).contains("xid regression detected").doesNotContain("has not completed");
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
        config.xidRegression("halt");
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
            execute(c, "delete from " + names.halt()); // the operator
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
            drained.add(tickIds().get(tickIds().size() - 1)); // the drain's tick: no relay makes others
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
            var cursors = new Cursors(names, config.shards());
            cursors.append(c, cursors.latest(c, 0).cursor().withProgress(slow, target, 1));
            assertThat(ticks.make(c, Duration.ZERO)).isTrue(); // a tick no shard has reached yet
            newer = ticks.latest(c).id();
            assertThat(queryLong(c, "select count(*) from " + names.cursor())).isGreaterThan(config.shards());
        }
        var cursorsBefore = latestCursors();
        var ticksBefore = tickIds();
        assertThat(ticksBefore).contains(drained.get(0));
        // the shards no drain stepped are idle on the bootstrap tick, which no run has anchored yet
        long floor = Math.min(slow, cursorsBefore.stream().mapToLong(Cursor::tickId).min().orElseThrow());

        assertThat(maintenance().runOnce()).isTrue();

        // from the slowest shard's tick on, and from every latest cursor row's
        assertThat(tickIds()).containsExactlyElementsOf(ticksBefore.stream().filter(id -> id >= floor).toList())
                .contains(slow, target, newer);
        assertThat(ranges()).as("ranges the lagging shard has not passed").containsAll(between);
        assertThat(latestCursors()).isEqualTo(cursorsBefore);
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + names.cursor())).isEqualTo((long) config.shards());
        }
    }

    @Test
    void cleanupKeepsEveryLatestCursorTick() throws Exception {
        bootstrap(); // 4 shards
        var handler = new RecordingHandler();
        var relay = relay(handler, maintenance());
        long bootstrapTick = tickIds().get(0);
        send(msg("t", "k", "a"));
        TestDb.burnXids(3000);
        assertThat(relay.drain()).isOne();
        var cursors = latestCursors();
        assertThat(cursors).as("the drain stepped only the shard with a row")
                .filteredOn(cursor -> cursor.tickId() == bootstrapTick).hasSize(config.shards() - 1);
        long drained = tickIds().get(tickIds().size() - 1);
        long xmin;
        try (var c = TestDb.connect()) {
            xmin = new Ticks(names, config.shards()).get(c, drained).snapshot().xmin();
        }
        var processed = ranges().stream().filter(b -> b.to() != null && Long.parseLong(b.to()) <= xmin).toList();
        assertThat(processed).as("ranges below the drain tick's xmin").hasSizeGreaterThanOrEqualTo(2);

        assertThat(maintenance().runOnce()).isTrue();

        // the previous release steps from the cursor's tick: the idle shards' stays, the drops go by the checkpoint
        assertThat(tickIds()).as("from the lowest cursor tick on").startsWith(bootstrapTick).endsWith(drained);
        for (var b : processed) {
            assertThat(exists(b.name())).as(b.name()).isFalse();
        }
        assertThat(latestCursors()).as("no cursor row written").isEqualTo(cursors);
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select tick_id || ':' || busy::text from " + names.checkpoint()))
                    .isEqualTo(drained + ":0000");
        }

        // idle ticks later, the next run moves on again, and shards whose cursor ticks are long gone deliver
        long later;
        try (var c = TestDb.connect()) {
            var ticks = new Ticks(names, config.shards());
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            later = ticks.latest(c).id();
        }
        assertThat(maintenance().runOnce()).isTrue();
        assertThat(tickIds()).startsWith(bootstrapTick).endsWith(later);
        assertThat(tickIds()).containsAll(latestCursors().stream().map(Cursor::tickId).toList());
        var ids = send(IntStream.range(0, 20).mapToObj(i -> msg("t", "k" + i, "m" + i)).toArray(OutboxMessage[]::new));
        assertThat(relay.drain()).isEqualTo(ids.size());
        assertThat(latestCursors()).as("every shard stepped").noneMatch(cursor -> cursor.tickId() == bootstrapTick);
    }

    @Test
    void idleShardsGetAnAnchorEveryInterval() throws Exception {
        config.maintenanceInterval(Duration.ofSeconds(1)).cursorAnchorInterval(Duration.ofSeconds(2));
        bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var relay = relay(new RecordingHandler(), maintenance());
        var maintenance = maintenance();
        long started = System.nanoTime();
        send(msg("t", "k", "a"));
        assertThat(relay.drain()).isOne();
        makeTick();
        long seq = maxCursorSeq();

        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maintenance.runOnce()).isTrue();

        assertThat(maxCursorSeq()).as("within the interval: no cursor row").isEqualTo(seq);
        Thread.sleep(Math.max(0, 2500 - (System.nanoTime() - started) / 1_000_000));
        long latest = makeTick();

        assertThat(maintenance.runOnce()).isTrue();

        try (var c = TestDb.connect()) {
            var positions = new Cursors(names, config.shards()).positions(c, latest);
            assertThat(positions).hasSize(config.shards()).allSatisfy(p -> {
                assertThat(p.cursor().tickId()).as("shard " + p.cursor().shard()).isEqualTo(p.effectiveTickId());
                assertThat(p.cursor().targetTickId()).isNull();
            });
        }
        assertThat(tickIds()).as("the anchors free the ticks below them").containsExactly(latest);
        seq = maxCursorSeq();
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maxCursorSeq()).as("anchored: no cursor row until the next interval").isEqualTo(seq);
    }

    @Test
    void anAnchorUnderARepeatableReadDefaultNeverMovesACursorBack() throws Exception {
        config.maintenanceInterval(Duration.ofSeconds(1)).cursorAnchorInterval(Duration.ofSeconds(1));
        bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var props = new java.util.Properties();
        props.setProperty("options", "-c default_transaction_isolation=repeatable\\ read"); // a role or pool default
        var rr = TestDb.newDatabase(2, props);
        pools.add(rr);
        try (var c = rr.newConnection()) {
            assertThat(queryString(c, "show default_transaction_isolation")).isEqualTo("repeatable read");
        }
        Thread.sleep(1100);
        makeTick();
        var maintenance = new Maintenance(rr, names, config, new OutboxMetrics(null));
        var ahead = new AtomicLong();
        var once = new AtomicBoolean();
        // a step of shard 0 that commits after the anchor transaction's first statement, before its re-read (it took
        // the shard's lock just before the anchor's try)
        maintenance.anchorLocked = () -> {
            if (once.compareAndSet(false, true)) {
                try (var c = TestDb.connect()) {
                    ahead.set(makeTick());
                    var cursors = new Cursors(names, config.shards());
                    cursors.append(c, cursors.latest(c, 0).cursor().withProgress(ahead.get(), null, 0));
                } catch (SQLException e) {
                    throw new RuntimeSqlException(e);
                }
            }
        };

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(once).isTrue();
        assertThat(latestCursors()).extracting(Cursor::tickId).as("shard 0 kept the step's tick, the others anchored")
                .containsExactly(ahead.get(), ahead.get() - 1, ahead.get() - 1, ahead.get() - 1);
    }

    @Test
    void aShardHeldByAStepIsAnchoredByTheNextRun() throws Exception {
        config.maintenanceInterval(Duration.ofSeconds(1)).cursorAnchorInterval(Duration.ofSeconds(1));
        bootstrap(); // 4 shards, every cursor on the bootstrap tick
        long bootstrapTick = latestCursors().get(0).tickId();
        Thread.sleep(1100);
        long latest = makeTick();
        var maintenance = maintenance();
        try (var step = TestDb.connect()) {
            step.setAutoCommit(false);
            execute(step, "select pg_advisory_xact_lock(" + names.internalNamespace() + ", "
                    + (TableNames.LOCK_SHARD_BASE + 2) + ")");

            assertThat(maintenance.runOnce()).isTrue();

            assertThat(latestCursors()).extracting(Cursor::tickId).as("shard 2 held: not anchored")
                    .containsExactly(latest, latest, bootstrapTick, latest);
            step.commit();
        }

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(latestCursors()).extracting(Cursor::tickId).containsOnly(latest);
    }

    @Test
    void dueWorkIsRepairedOnIdleShards() throws Exception {
        bootstrap(); // 4 shards, every cursor on the bootstrap tick, no due in any
        var handler = new RecordingHandler();
        var relay = relay(handler, maintenance());
        var retried = UUID.randomUUID();
        var parkedId = UUID.randomUUID();
        try (var c = TestDb.connect()) { // as a pre-requeue instance or a hand edit leaves them: no due in the cursor
            execute(c, "insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at, attempts,"
                    + " next_attempt_at) values (900001, 2, '" + retried + "', 't', 'r'::bytea, now(), 1,"
                    + " now() - interval '1 second')");
            execute(c, "insert into " + names.parked() + " (id, shard, message_id, topic, key, created_at, due_at)"
                    + " values (900002, 2, '" + parkedId + "', 't', 'p'::bytea, now(), now() - interval '2 second')");
        }
        assertThat(relay.drain()).as("nothing steps an idle shard without a due").isZero();
        long rows = outboxRows();

        assertThat(maintenance().runOnce()).isTrue();

        try (var c = TestDb.connect()) {
            var cursor = new Cursors(names, config.shards()).latest(c, 2).cursor();
            assertThat(cursor.retryDueAt()).isEqualTo(new RetryStore(names, false).minDue(c, 2)).isNotNull();
            assertThat(cursor.parkedDueAt()).isEqualTo(new ParkedStore(names, new RetryStore(names, false))
                    .minDue(c, 2)).isNotNull();
        }
        assertThat(relay.drain()).isEqualTo(2);
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId)
                .containsExactlyInAnyOrder(retried, parkedId);
        assertThat(outboxRows()).as("no new row on the shard").isEqualTo(rows);
    }

    @Test
    void aCheckpointFromAnotherHistoryIsReplaced() throws Exception {
        bootstrap();
        var maintenance = maintenance();
        long ahead;
        long latest;
        try (var c = TestDb.connect()) { // as after restoring the tick table without the checkpoint
            var ticks = new Ticks(names, config.shards());
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            latest = ticks.latest(c).id();
            ahead = latest + 1000;
            execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + ahead
                    + ", B'0000', " + ahead + ")");
        }
        String succeeded = lastSuccess();

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("gone or was made after the checkpoint")
                    .contains("another history");
        }
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(tick_id::text, ',') from " + names.checkpoint()))
                    .as("replaced by the run's").isEqualTo(Long.toString(latest));
        }
        assertThat(tickIds()).as("cleanup went by the cursors, which keep their tick")
                .containsExactly(latestCursors().get(0).tickId(), latest);
        assertThat(lastSuccess()).isGreaterThan(succeeded);

        try (var c = TestDb.connect(); var err = new CapturedStderr()) { // forged again within a minute: no ERROR
            execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + ahead
                    + ", B'0000', " + ahead + ")");
            assertThat(maintenance.runOnce()).isTrue();
            assertThat(err.text()).doesNotContain("It counted for nothing").doesNotContain("no longer exist");
        }
    }

    @Test
    void anObsoleteCheckpointIsReplacedWithoutAnAlarm() throws Exception {
        bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var maintenance = maintenance();
        long checkpointed = makeTick();
        assertThat(maintenance.runOnce()).isTrue(); // the checkpoint on it
        long ahead = makeTick();
        try (var c = TestDb.connect()) { // every shard steps past it, then the previous release's cleanup in a mixed
            // fleet deletes below the lowest cursor tick, the checkpoint's among them
            var cursors = new Cursors(names, config.shards());
            for (int shard = 0; shard < config.shards(); shard++) {
                cursors.append(c, cursors.latest(c, shard).cursor().withProgress(ahead, null, 0));
            }
            execute(c, "delete from " + names.tick() + " where id < " + ahead);
        }
        long latest = makeTick();

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).doesNotContain("ERROR").doesNotContain("another history");
        }
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(tick_id::text, ',') from " + names.checkpoint()))
                    .as("replaced by the run's").isEqualTo(Long.toString(latest));
        }
        assertThat(checkpointed).isLessThan(ahead);
    }

    @Test
    void aClockSteppedBackAfterATickDoesNotVoidTheCheckpointOnIt() throws Exception {
        bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var handler = new RecordingHandler();
        var relay = relay(handler, maintenance());
        long latest;
        try (var c = TestDb.connect()) {
            var ticks = new Ticks(names, config.shards());
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            latest = ticks.latest(c).id();
            // the clock that dated the tick ran an hour ahead, and has stepped back since
            execute(c, "update " + names.tick() + " set created_at = clock_timestamp() + interval '1 hour'"
                    + " where id = " + latest);
        }
        var maintenance = maintenance();

        long bootstrapTick = latestCursors().get(0).tickId();
        assertThat(maintenance.runOnce()).isTrue(); // checkpoints on the tick
        assertThat(maintenance.runOnce()).isTrue(); // reads by that checkpoint

        try (var c = TestDb.connect()) {
            assertThat(new Cursors(names, config.shards()).ignoredCheckpointTick(c)).as("the checkpoint counts")
                    .isNull();
            assertThat(queryString(c, "select tick_id || ':' || busy::text from " + names.checkpoint()))
                    .isEqualTo(latest + ":0000");
        }
        assertThat(tickIds()).as("the idle shards' cursor ticks are kept").containsExactly(bootstrapTick, latest);
        var ids = send(msg("t", "k", "a"));
        assertThat(relay.drain()).isOne();
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyElementsOf(ids);
    }

    @Test
    void shardsStrandedByAnIgnoredCheckpointAreReportedNotAFailedRead() throws Exception {
        bootstrap(); // 4 shards, every cursor on the bootstrap tick
        long latest;
        try (var c = TestDb.connect()) {
            var ticks = new Ticks(names, config.shards());
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            latest = ticks.latest(c).id();
        }
        var maintenance = maintenance();
        assertThat(maintenance.runOnce()).isTrue(); // the checkpoint moves every shard on
        long bootstrapTick = latestCursors().get(0).tickId();
        try (var c = TestDb.connect()) { // as if the tick at its id came from another history, made after it
            execute(c, "update " + names.checkpoint() + " set created_at = created_at - interval '1 hour'");
            execute(c, "delete from " + names.tick() + " where id = " + bootstrapTick); // and the cursors' by hand
        }

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).as("the read does not fail").isTrue();

            assertThat(err.text()).doesNotContain("no tick in").contains("ERROR")
                    .contains("outbox shards [0, 1, 2, 3] of " + names.base() + " start from ticks [" + bootstrapTick
                            + "] that no longer exist")
                    .contains("the relay is stopped for them").contains("deletes no ticks");
        }
        assertThat(tickIds()).as("no cleanup while shards are stranded").containsExactly(latest);
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select tick_id || ':' || busy::text || ':' || busy_tick_id from "
                    + names.checkpoint())).as("replaced; the stranded shards stay at their ticks")
                    .isEqualTo(latest + ":1111:" + bootstrapTick);
            assertThat(maintenance.lagSeconds(c)).as("from the oldest tick left").isGreaterThanOrEqualTo(0.0);
        }
        try (var err = new CapturedStderr()) { // within a minute: no second ERROR
            assertThat(maintenance.runOnce()).isTrue();
            assertThat(err.text()).doesNotContain("no longer exist");
        }
    }

    @Test
    void lagIsZeroWhenIdle() throws Exception {
        bootstrap();
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));
        try (var c = TestDb.connect()) {
            // every cursor on the bootstrap tick, an hour older than the latest; no shard has rows
            execute(c, "update " + names.tick() + " set created_at = created_at - interval '1 hour'");
            assertThat(new Ticks(names, config.shards()).make(c, Duration.ZERO)).isTrue();
        }

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(gauge(registry, "outbox.shard.lag")).isZero();
        try (var c = TestDb.connect()) {
            assertThat(maintenance.lagSeconds(c)).as("OutboxAdmin.status()").isZero();
        }

        send(msg("t", "k", "a")); // not relayed: its shard is behind the checkpoint's tick, made 10 s before the latest
        try (var c = TestDb.connect()) {
            assertThat(new Ticks(names, config.shards()).make(c, Duration.ZERO)).isTrue();
            execute(c, "update " + names.tick() + " set created_at = created_at - interval '10 seconds' where id ="
                    + " (select tick_id from " + names.checkpoint() + ")");
        }
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(gauge(registry, "outbox.shard.lag")).isBetween(10.0, 20.0);
        try (var c = TestDb.connect()) {
            assertThat(maintenance.lagSeconds(c)).isBetween(10.0, 20.0);
        }
    }

    @Test
    void oneRunExtendsWhenHalfOfPremadeIsUsed() throws Exception {
        bootstrap();
        try (var c = TestDb.connect()) {
            var partitions = new XidPartitions(names, config.partitions());
            while (partitions.premadeAhead(c) > config.premake() / 2) {
                TestDb.burnXids(200);
            }
            long offset = queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint % 1000");
            if (offset > 900) { // the run's own xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
        }
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));

        String log;
        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();
            log = err.text();
        }

        assertThat(log).contains("swapped the partition tail of " + names.base());
        assertThat(gauge(registry, "outbox.partitions.premade")).isGreaterThanOrEqualTo(config.premake());
        assertThat(gauge(registry, "outbox.partitions.overflow")).isZero();
        long top = ranges().stream().filter(b -> b.to() != null).mapToLong(b -> Long.parseLong(b.to())).max()
                .orElseThrow();
        assertThat(ranges()).filteredOn(b -> b.to() == null).singleElement()
                .satisfies(tail -> assertThat(tail.name()).isEqualTo(names.tail()))
                .satisfies(tail -> assertThat(Long.parseLong(tail.from())).isEqualTo(top));
    }

    @Test
    void rangesMadeForASwapThatTimedOutAreKeptForTheNextRun() throws Exception {
        bootstrap();
        try (var c = TestDb.connect()) {
            var partitions = new XidPartitions(names, config.partitions());
            while (partitions.premadeAhead(c) > config.premake() / 2) {
                TestDb.burnXids(200);
            }
        }
        var maintenance = maintenance();
        List<String> made;
        try (var reader = TestDb.connect()) {
            reader.setAutoCommit(false); // a young reader: no blocker, so the swap waits for it and gives up
            queryLong(reader, "select count(*) from " + names.base());
            String log;
            try (var err = new CapturedStderr()) {
                assertThat(maintenance.runOnce()).isTrue();
                log = err.text();
            }
            reader.commit();
            try (var c = TestDb.connect()) {
                made = PartitionDdl.standalone(c, names.base(), "_x[0-9]+");
            }
            assertThat(log).contains("55P03").doesNotContain("detached earlier but never dropped");
            assertThat(made).as("left standalone by the same run's orphan step").isNotEmpty();
        }

        String log;
        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();
            log = err.text();
        }

        assertThat(log).contains("swapped the partition tail")
                .doesNotContain("created table " + names.partitionPrefix());
        assertThat(ranges()).extracting(Bound::name).containsAll(made);
    }

    @Test
    void pendingRangeDetachDoesNotJamExtension() throws Exception {
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        send(msg("t", "k", "a"));
        TestDb.burnXids(3000);
        relay.drain();
        long xmin = horizonXmin();
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
            // into the extension window (at most premake / 2 ahead): the same run also extends beside the range
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
        long xmin = horizonXmin();
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
    void foreignOrphanIsKeptAndNamesTheAdminCall() throws Exception {
        bootstrap(); // restoreOrphans on: it covers only the ranges maintenance detached
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
        assertThat(horizonXmin()).as("the horizon passed it").isGreaterThanOrEqualTo(Long.parseLong(orphan.to()));
        var registry = new SimpleMeterRegistry();

        try (var err = new CapturedStderr()) {
            assertThat(new Maintenance(db, names, config, new OutboxMetrics(registry)).runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains(orphan.name()).contains("not by this library")
                    .contains("OutboxAdmin.restoreOrphan(\"" + orphan.name() + "\", fromXid)")
                    .contains("POST /kora-outbox/admin/v1/orphans/" + orphan.name() + "/restore?fromXid=")
                    .doesNotContain("insert into");
        }
        assertThat(exists(orphan.name())).isTrue();
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + orphan.name())).as("nothing moved").isOne();
        }
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
            assertThat(horizonXmin()).isGreaterThanOrEqualTo(Long.parseLong(range.to()));

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
        // a create whose attach never ran, below the tail: above it, the extension's pre-created ranges wait for a swap
        var leftover = names.partitionPrefix() + "2";
        try (var c = TestDb.connect()) {
            execute(c, "create table " + leftover + " (like " + names.base() + " including defaults)");
            markDropping(c, names.partitionPrefix() + "1"); // its range was dropped, its marker not deleted
        }

        assertThat(maintenance().runOnce()).isTrue();

        assertThat(exists(leftover)).isFalse();
        assertThat(droppingMarkers()).isEmpty();
    }

    @Test
    void oursOrphanWithRowsAboveTheHorizonIsRestored() throws Exception {
        bootstrap();
        var handler = new RecordingHandler();
        var relay = relay(handler, maintenance());
        var orphan = oursOrphan(relay, 2500);
        var registry = new SimpleMeterRegistry();

        try (var err = new CapturedStderr()) {
            assertThat(new Maintenance(db, names, config, new OutboxMetrics(registry)).runOnce()).isTrue();

            assertThat(err.text()).contains("INFO").contains("outbox table " + orphan.table())
                    .contains("moved 2500 rows at or above xid " + orphan.horizon())
                    .contains("they are new messages, delivered after later messages of their keys")
                    .contains("rows below it went with the table").doesNotContain("cannot deliver");
        }
        assertThat(exists(orphan.table())).isFalse();
        assertThat(droppingMarkers()).isEmpty();
        assertThat(gauge(registry, "outbox.partitions.orphaned")).isZero();
        relay.drain(); // their shards recomputed: the stale one would be on no worker's list
        var delivered = new ArrayList<>(orphan.sent());
        delivered.addAll(orphan.above());
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).as("each once, none below the horizon")
                .containsExactlyInAnyOrderElementsOf(delivered);
    }

    @Test
    void restoreIsChunkedAndResumable() throws Exception {
        bootstrap();
        var orphan = oursOrphan(relay(new RecordingHandler(), maintenance()), 2500);
        var killed = maintenance();
        killed.orphanRestore.afterChunk = c -> { // the session dies right after its first chunk committed
            long pid = queryLong(c, "select pg_backend_pid()");
            try (var other = TestDb.connect()) {
                execute(other, "select pg_terminate_backend(" + pid + ", 10000)");
            }
        };

        assertThatThrownBy(killed::runOnce).as("its session is gone").isInstanceOf(RuntimeSqlException.class);

        assertThat(exists(orphan.table())).isTrue();
        assertThat(count(orphan.table(), orphan.above())).isEqualTo(2500 - OrphanRestore.CHUNK);
        assertThat(count(names.base(), orphan.above())).as("one chunk, committed").isEqualTo(OrphanRestore.CHUNK);

        assertThat(maintenance().runOnce()).isTrue();

        assertThat(exists(orphan.table())).isFalse();
        assertThat(droppingMarkers()).isEmpty();
        try (var c = TestDb.connect(); var ps = c.prepareStatement("select count(*), count(distinct message_id) from "
                + names.base() + " where message_id = any(?)")) {
            ps.setArray(1, c.createArrayOf("uuid", orphan.above().toArray()));
            try (var rs = ps.executeQuery()) {
                rs.next();
                assertThat(List.of(rs.getLong(1), rs.getLong(2))).as("each once").containsExactly(2500L, 2500L);
            }
        }
    }

    @Test
    void oneFailingRestoreDoesNotHideTheOtherOrphans() throws Exception {
        bootstrap();
        var ours = oursOrphan(relay(new RecordingHandler(), maintenance()), 2);
        // a range detached by someone else, listed after ours (by name), so the sweep reaches it after the failure
        long oursLower = Long.parseLong(ours.table().substring(names.partitionPrefix().length()));
        var foreign = names.partitionPrefix() + (oursLower + 1);
        try (var c = TestDb.connect()) {
            execute(c, "create table " + foreign + " (like " + names.base() + " including defaults)");
            execute(c, "insert into " + foreign + " (id, shard, xid, message_id, topic) values (1, 0, '1',"
                    + " gen_random_uuid(), 't')");
        }
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry), Duration.ofMillis(300));
        try (var dump = TestDb.connect()) {
            dump.setAutoCommit(false); // a pg_dump of the orphan: the chunk's ACCESS EXCLUSIVE times out
            queryLong(dump, "select count(*) from " + ours.table());
            String succeeded = lastSuccess();

            String log;
            try (var err = new CapturedStderr()) {
                assertThat(maintenance.runOnce()).isTrue();
                log = err.text();
            }

            assertThat(log).contains("WARN").contains("could not move the rows of " + ours.table()).contains("55P03")
                    .doesNotContain("step 'orphan drop'");
            assertThat(log).contains("ERROR").contains("outbox table " + foreign + " holds rows")
                    .contains("not by this library");
            assertThat(gauge(registry, "outbox.partitions.orphaned")).as("both kept").isEqualTo(2);
            assertThat(lastSuccess()).as("a run that left rows outside the outbox is no success").isEqualTo(succeeded);
            assertThat(count(ours.table(), ours.above())).isEqualTo(2);
            dump.commit();
        }

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(exists(ours.table())).isFalse();
        assertThat(gauge(registry, "outbox.partitions.orphaned")).as("the foreign one").isEqualTo(1);
    }

    /**
     * A key held by an open send: its rows stay, in every chunk, without a wait, so they go back behind that send; the
     * other keys' rows go on, then the held key is waited for once, at most swapLockTimeout, and the call fails with
     * 55P03 for the next one to finish.
     */
    @Test
    void restoreLeavesABusyKeysRowsWithoutWaitingForIt() throws Exception {
        bootstrap(); // swapLockTimeout 200 ms
        var orphan = oursOrphan(relay(new RecordingHandler(), maintenance()), 2500); // every key in each of 3 chunks
        var busy = "k7";
        long busyRows;
        try (var c = TestDb.connect()) {
            busyRows = queryLong(c, "select count(*) from " + orphan.table() + " where key = convert_to('" + busy
                    + "', 'UTF8')");
        }
        assertThat(busyRows).isEqualTo(50);
        var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, new OutboxMetrics(null));
        var restore = maintenance().orphanRestore;
        try (var business = TestDb.connect()) {
            business.setAutoCommit(false); // a business transaction that sent the key and goes on working
            outbox.send(business, msg("t", busy, "busy"));

            long start = System.nanoTime();
            try (var c = TestDb.connect()) {
                assertThatThrownBy(() -> restore.restore(c, orphan.table(), orphan.horizon(), Duration.ofSeconds(30)))
                        .isInstanceOfSatisfying(SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("55P03"))
                        .hasMessageContaining(busyRows + " rows of keys another transaction held");
            }
            var took = Duration.ofNanos(System.nanoTime() - start);
            assertThat(took).as("one wait for the held key").isLessThan(config.swapLockTimeout().plusSeconds(1));

            assertThat(count(orphan.table(), orphan.above())).as("the busy key's rows stay").isEqualTo(busyRows);
            assertThat(count(names.base(), orphan.above())).as("the others moved").isEqualTo(2500 - busyRows);
            System.out.printf("MaintenanceIT: restored %d rows around a busy key in %d ms%n", 2500 - busyRows,
                    took.toMillis());
            business.commit();
        }

        try (var c = TestDb.connect()) {
            assertThat(restore.restore(c, orphan.table(), orphan.horizon(), Duration.ofSeconds(30)))
                    .isEqualTo(busyRows);
            assertThat(exists(orphan.table())).isFalse();
            assertThat(queryLong(c, "select count(*) from " + names.base() + " where key = convert_to('" + busy
                    + "', 'UTF8') and id < (select id from " + names.base() + " where payload = convert_to('busy',"
                    + " 'UTF8'))")).as("the busy key's restored rows after its send").isZero();
        }
    }

    /**
     * A key sent all the time, by two writers whose transactions overlap, is never free for a try-lock; the restore's
     * bounded wait for it gets in between them, so the orphan is restored within a few calls, not never.
     */
    @Test
    void restoreGetsAKeyThatIsAlmostAlwaysHeld() throws Exception {
        bootstrap();
        var orphan = oursOrphan(relay(new RecordingHandler(), maintenance()), 2500);
        var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, new OutboxMetrics(null));
        var restore = maintenance().orphanRestore;
        var stop = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writers = new ArrayList<Thread>();
        for (int i = 0; i < 2; i++) {
            var writer = new Thread(() -> {
                while (!stop.get()) {
                    try {
                        db.inTx((SqlFunction0<UUID>) () -> {
                            var id = outbox.send(msg("t", "k7", "hot"));
                            try {
                                Thread.sleep(5); // the other one queues for the key meanwhile
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return id;
                        });
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }
            });
            writer.start();
            writers.add(writer);
        }
        int calls = 0;
        try (var c = TestDb.connect()) {
            Thread.sleep(200);
            while (exists(orphan.table())) {
                assertThat(++calls).as("calls to restore it").isLessThanOrEqualTo(5);
                try {
                    restore.restore(c, orphan.table(), orphan.horizon(), Duration.ofSeconds(30));
                } catch (SQLException e) {
                    assertThat(e.getSQLState()).isEqualTo("55P03");
                }
            }
        } finally {
            stop.set(true);
            for (var w : writers) {
                w.join();
            }
        }
        assertThat(errors).isEmpty();
        assertThat(count(names.base(), orphan.above())).isEqualTo(orphan.above().size());
    }

    @Test
    void restoreRefusesAnythingButAStandaloneRangeOfTheOutbox() throws Exception {
        bootstrap();
        var attached = ranges().stream().filter(b -> b.to() != null).findFirst().orElseThrow().name();
        var restore = new OrphanRestore(names, new Resender(names, config.shards()), config.swapLockTimeout());
        try (var c = TestDb.connect()) {
            execute(c, "create schema if not exists other");
            execute(c, "create table other." + names.unqualified(names.partitionPrefix()) + "5 (id bigint)");
            for (var table : List.of(names.base(), names.tail(), attached, names.partitionPrefix() + "5",
                    "other." + names.unqualified(names.partitionPrefix()) + "5", attached + "; drop table x",
                    attached + " (id)", names.partitionPrefix())) {
                assertThatThrownBy(() -> restore.restore(c, table, 0, Duration.ofSeconds(1))).as(table)
                        .isInstanceOf(IllegalArgumentException.class);
            }
            assertThat(queryString(c, "select (to_regclass(?) is null)::text", attached + "_id"))
                    .as("nothing run on the attached range").isEqualTo("true");
        }
        assertThat(ranges()).extracting(Bound::name).contains(attached);
    }

    @Test
    void restoreOrphansOffKeepsTheError() throws Exception {
        config.restoreOrphans(false);
        bootstrap();
        var orphan = oursOrphan(relay(new RecordingHandler(), maintenance()), 2);
        var registry = new SimpleMeterRegistry();

        try (var err = new CapturedStderr()) {
            assertThat(new Maintenance(db, names, config, new OutboxMetrics(registry)).runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains(orphan.table()).contains("cannot deliver")
                    .contains("move them back with insert into " + names.base());
        }
        assertThat(exists(orphan.table())).isTrue();
        assertThat(count(orphan.table(), orphan.above())).isEqualTo(2);
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
                "outbox.shard.lag", "outbox.retry.size", "outbox.dead.size", "outbox.dead.pending",
                "outbox.dead.pending.oldest", "outbox.parked.size", "outbox.partitions.premade",
                "outbox.partitions.overflow", "outbox.partitions.xids_left",
                "outbox.partitions.orphaned", "outbox.maintenance.transaction_pooling",
                "outbox.maintenance.since_success");
        var handler = new RecordingHandler()
                .failAlways(r -> "poison".equals(key(r)), () -> new OutboxPermanentException("bad payload"))
                .failAlways(r -> "slow".equals(key(r)), () -> new RuntimeException("target slow"));
        var relay = relay(handler, maintenance);
        send(msg("t", "ok", "1"), msg("t", "poison", "2"), msg("t", "slow", "3"), msg("t", "slow", "4"));
        relay.drain();
        send(msg("t", "later", "5")); // not relayed: the shard is behind the tick below, 10 s ahead of its cursor
        try (var c = TestDb.connect()) {
            assertThat(new Ticks(names, config.shards()).make(c, Duration.ZERO)).isTrue();
            execute(c, "update " + names.tick() + " set created_at = created_at - interval '10 seconds'"
                    + " where id = " + horizonTickId());
            var parked =
                    new OutboxRecord(1_000_000, UUID.randomUUID(), "t", null, null, List.of(), Instant.now(), 0, 0);
            new ParkedStore(names, new RetryStore(names, false))
                    .apply(c, 0, List.of(new Park(parked, 1, Instant.now())), Set.of());
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

    @Test
    void deadLetterGaugesCountThePendingOnes() throws Exception {
        bootstrap();
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));

        assertThat(maintenance.runOnce()).isTrue();
        assertThat(gauge(registry, "outbox.dead.pending")).isZero();
        assertThat(gauge(registry, "outbox.dead.pending.oldest")).as("none").isZero();

        deadLetter(1, "pending", "1 hour");
        deadLetter(2, "pending", "1 minute");
        deadLetter(3, "kept", "2 hours");
        deadLetter(4, "scheduled", "3 hours");
        assertThat(maintenance.runOnce()).isTrue();

        assertThat(gauge(registry, "outbox.dead.pending")).isEqualTo(2);
        assertThat(gauge(registry, "outbox.dead.pending.oldest")).as("seconds since its failed_at")
                .isBetween(3600.0, 3700.0);
        assertThat(gauge(registry, "outbox.dead.size")).isEqualTo(4);
    }

    @Test
    void retentionDeletesOnlyOldKeptLetters() throws Exception {
        bootstrap();
        deadLetter(1, "kept", "2 days");
        deadLetter(2, "kept", "1 hour");
        deadLetter(3, "pending", "2 days");
        deadLetter(4, "scheduled", "2 days");

        assertThat(maintenance().runOnce()).isTrue();
        assertThat(deadIds()).as("retention 0: kept forever").containsExactly(1L, 2L, 3L, 4L);

        config.deadLettersRetention(Duration.ofDays(1));
        assertThat(maintenance().runOnce()).isTrue();

        assertThat(deadIds()).as("a letter the hook has not decided on is never deleted").containsExactly(2L, 3L, 4L);
    }

    @Test
    void redriveHistoryIsCleaned() throws Exception {
        config.deadLettersRedriveHistory(Duration.ofDays(30));
        bootstrap();
        var old = UUID.randomUUID();
        var recent = UUID.randomUUID();
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.redriven() + " (message_id, redrives, at) values ('" + old
                    + "', 2, now() - interval '31 days'), ('" + recent + "', 1, now() - interval '29 days')");
        }

        assertThat(maintenance().runOnce()).isTrue();

        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(message_id::text, ',') from " + names.redriven()))
                    .isEqualTo(recent.toString());
        }
    }

    @Test
    void deadDueIndexIsBuiltConcurrently() throws Exception {
        bootstrap();
        var maintenance = maintenance();
        deadTableWithoutIndex(200000);
        try (var c = TestDb.connect(); var holder = openDeadInsert(300000); var writer = TestDb.connect()) {
            execute(c, "update " + names.dead() + " set status = 'pending', due_at = now() where id <= 1000003");

            assertThat(maintenance.runOnce()).as("the build runs on its own thread").isTrue();
            await(Duration.ofSeconds(10), () -> queryLong(c, BUILD_WAITING) == 1); // for the open insert

            // a plain CREATE INDEX waiting there would queue every new dead insert behind it
            long start = System.nanoTime();
            execute(writer, "set statement_timeout = '1s'");
            insertDead(writer, 300001);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));

            holder.commit();
            awaitBuildEnd(maintenance);
            assertThat(indexState()).endsWith(":true");
            assertThat(queryString(c, "select pg_get_indexdef(to_regclass(?))", deadDue()))
                    .endsWith(" (due_at) WHERE (due_at IS NOT NULL)");
            assertThat(queryLong(c, "select count(*) from " + names.dead() + " where due_at is not null"))
                    .isEqualTo(3);
        }
    }

    @Test
    void invalidDeadDueIndexIsRebuilt() throws Exception {
        bootstrap();
        String invalid = invalidDeadDue();

        var maintenance = maintenance();
        assertThat(maintenance.runOnce()).isTrue();
        awaitBuildEnd(maintenance);

        assertThat(indexState()).endsWith(":true").isNotEqualTo(invalid.replace(":false", ":true"));
    }

    @Test
    void aDeadDueIndexBuildThatTimesOutBacksOff() throws Exception {
        config.maintenanceInterval(Duration.ofSeconds(1));
        bootstrap();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(null), Duration.ofMillis(300));
        deadTableWithoutIndex(1);
        try (var c = TestDb.connect(); var holder = openDeadInsert(2)) { // the build's lock wait times out
            String failed;
            long failedAt;
            try (var err = new CapturedStderr()) {
                assertThat(maintenance.runOnce()).isTrue();
                awaitBuildEnd(maintenance);
                failedAt = System.nanoTime();
                failed = indexState();
                assertThat(failed).as("left invalid by the timed-out build").endsWith(":false");
                assertThat(err.text()).contains(deadDue()).contains("55P03").contains("next try in 1 s");
            }
            try (var err = new CapturedStderr()) {
                assertThat(maintenance.runOnce()).isTrue();
                assertThat(maintenance.deadIndex.running()).as("not tried again within the backoff").isFalse();
                assertThat(indexState()).isEqualTo(failed);
                assertThat(err.text()).doesNotContain(deadDue());
            }

            holder.rollback();
            await(Duration.ofSeconds(10), () -> {
                maintenance.runOnce();
                awaitBuildEnd(maintenance);
                return indexState().endsWith(":true");
            });
            assertThat(Duration.ofNanos(System.nanoTime() - failedAt)).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
        }
    }

    @Test
    void aCancelledDeadDueIndexBuildBacksOffToo() throws Exception {
        config.maintenanceInterval(Duration.ofMinutes(10));
        bootstrap();
        var maintenance = maintenance();
        deadTableWithoutIndex(1);
        try (var c = TestDb.connect(); var holder = openDeadInsert(2); var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();
            await(Duration.ofSeconds(10), () -> queryLong(c, BUILD_WAITING) == 1);
            execute(c, "select pg_cancel_backend(pid) from pg_stat_activity where query like 'create index"
                    + " concurrently%'");
            awaitBuildEnd(maintenance);
            assertThat(err.text()).contains(deadDue()).contains("57014").contains("next try in 600 s");
            String failed = indexState();
            holder.rollback();

            assertThat(maintenance.runOnce()).isTrue();
            assertThat(maintenance.deadIndex.running()).isFalse();
            assertThat(indexState()).isEqualTo(failed);
        }
    }

    /** The unlock on the dead session fails too: the WARN names the build's cause, not the closed connection. */
    @Test
    void aTerminatedDeadDueIndexBuildNamesItsCause() throws Exception {
        config.maintenanceInterval(Duration.ofMinutes(10));
        bootstrap();
        var maintenance = maintenance();
        deadTableWithoutIndex(1);
        try (var c = TestDb.connect(); var holder = openDeadInsert(2); var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();
            await(Duration.ofSeconds(10), () -> queryLong(c, BUILD_WAITING) == 1);
            execute(c, "select pg_terminate_backend(pid) from pg_stat_activity where query like 'create index"
                    + " concurrently%'");
            awaitBuildEnd(maintenance);
            assertThat(err.text()).contains("could not build " + deadDue()).contains("(57P01: ")
                    .doesNotContain("08003");
            holder.rollback();
        }
    }

    @Test
    void onlyOneInstanceBuildsTheDeadDueIndex() throws Exception {
        bootstrap();
        var first = maintenance();
        var second = maintenance();
        deadTableWithoutIndex(1);
        try (var c = TestDb.connect(); var holder = openDeadInsert(2)) {
            assertThat(first.runOnce()).isTrue();
            await(Duration.ofSeconds(10), () -> queryLong(c, BUILD_WAITING) == 1);

            assertThat(second.runOnce()).isTrue();
            awaitBuildEnd(second); // its thread did not get LOCK_DEAD_INDEX
            assertThat(first.deadIndex.running()).isTrue();
            assertThat(queryLong(c, "select count(*) from pg_stat_activity where query like 'create index"
                    + " concurrently%' or query like 'drop index concurrently%'")).isEqualTo(1);

            holder.commit();
            awaitBuildEnd(first);
            assertThat(indexState()).endsWith(":true");
        }
    }

    @Test
    void closeCancelsABuildInFlight() throws Exception {
        bootstrap();
        var maintenance = maintenance();
        deadTableWithoutIndex(1);
        try (var c = TestDb.connect(); var holder = openDeadInsert(2)) {
            assertThat(maintenance.runOnce()).isTrue();
            await(Duration.ofSeconds(10), () -> queryLong(c, BUILD_WAITING) == 1);

            long start = System.nanoTime();
            maintenance.close(Duration.ofSeconds(10));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
            assertThat(maintenance.deadIndex.running()).isFalse();
            assertThat(queryLong(c, "select count(*) from pg_stat_activity where query like 'create index"
                    + " concurrently%' and state = 'active'")).isZero();
            assertThat(indexState()).as("the leftover").endsWith(":false");
            assertThat(maintenance.runOnce()).isTrue();
            assertThat(maintenance.deadIndex.running()).as("closed: no new build").isFalse();
            holder.rollback();
        }
        var next = maintenance();
        assertThat(next.runOnce()).isTrue();
        awaitBuildEnd(next);
        assertThat(indexState()).endsWith(":true");
    }

    @Test
    void aStartupQueuedForTheMaintenanceLockDoesNotDeadlockWithTheBuild() throws Exception {
        bootstrap();
        var maintenance = maintenance();
        deadTableWithoutIndex(1000);
        try (var c = TestDb.connect(); var holder = openDeadInsert(2); var run = TestDb.connect();
                var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();
            await(Duration.ofSeconds(10), () -> queryLong(c, BUILD_WAITING) == 1);

            // another maintenance run in progress; a startup queues behind it, its statement's snapshot open
            String lock = "(" + names.internalNamespace() + ", " + TableNames.LOCK_MAINTENANCE + ")";
            execute(run, "select pg_advisory_lock" + lock);
            var startup = CompletableFuture.runAsync(() -> {
                try {
                    new OutboxSchema(db, config).init();
                } catch (SQLException e) {
                    throw new RuntimeSqlException(e);
                }
            });
            await(Duration.ofSeconds(4), () -> queryLong(c, "select count(*) from pg_stat_activity"
                    + " where wait_event = 'advisory' and query like '%pg_advisory_lock%'") == 1);
            holder.commit(); // the build goes on to wait for older snapshots, the queued startup's among them
            Thread.sleep(1500); // past deadlock_timeout (1 s), so a cycle would be found
            assertThat(maintenance.deadIndex.running()).as("the build waits for the queued startup's snapshot")
                    .isTrue();
            assertThat(startup).isNotDone();
            execute(run, "select pg_advisory_unlock" + lock);

            startup.get(10, TimeUnit.SECONDS);
            awaitBuildEnd(maintenance);
            assertThat(indexState()).endsWith(":true");
            assertThat(err.text()).doesNotContain("40P01").doesNotContain("deadlock");
        }
    }

    private static final String BUILD_WAITING = "select count(*) from pg_stat_activity"
            + " where query like 'create index concurrently%' and wait_event_type = 'Lock'";

    private String deadDue() {
        return names.dead() + "_due";
    }

    /** {@code <oid>:<indisvalid>} of {@code <dead>_due}, {@code missing} without it. */
    private String indexState() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select coalesce((select indexrelid || ':' || indisvalid from pg_index"
                    + " where indexrelid = to_regclass(?)), 'missing')", deadDue());
        }
    }

    /** The dead table as bootstrap leaves an upgraded one with rows: {@code rows} kept letters, no index. */
    private void deadTableWithoutIndex(int rows) throws SQLException {
        try (var c = TestDb.connect()) {
            execute(c, "drop index if exists " + deadDue());
            execute(c, "insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts) select"
                    + " 1000000 + s, 0, gen_random_uuid(), 't', now(), 1 from generate_series(1, " + rows + ") s");
        }
    }

    private void insertDead(java.sql.Connection c, long id) throws SQLException {
        execute(c, "insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
                + " values (" + id + ", 0, gen_random_uuid(), 't', now(), 1)");
    }

    /** A connection with an open transaction that inserted dead letter {@code id}: a concurrent build waits for it. */
    private java.sql.Connection openDeadInsert(long id) throws SQLException {
        var c = TestDb.connect();
        c.setAutoCommit(false);
        insertDead(c, id);
        return c;
    }

    /** As a concurrent build cancelled by its lock timeout leaves it: invalid; returns its {@link #indexState}. */
    private String invalidDeadDue() throws SQLException {
        deadTableWithoutIndex(1);
        try (var c = TestDb.connect(); var holder = openDeadInsert(2)) {
            execute(c, "set lock_timeout = '300ms'");
            assertThatThrownBy(() -> execute(c, "create index concurrently " + names.unqualified(deadDue()) + " on "
                    + names.dead() + " (due_at) where due_at is not null")).isInstanceOf(SQLException.class);
            holder.rollback();
        }
        var state = indexState();
        assertThat(state).as("a cancelled concurrent build leaves it invalid").endsWith(":false");
        return state;
    }

    private static void awaitBuildEnd(Maintenance maintenance) throws Exception {
        await(Duration.ofSeconds(30), () -> !maintenance.deadIndex.running());
    }

    /** A dead letter of {@code status} that failed {@code age} ago (an interval), due now unless kept. */
    private void deadLetter(long id, String status, String age) throws SQLException {
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts,"
                    + " failed_at, status, due_at) values (" + id + ", 0, gen_random_uuid(), 't', now(), 1, now() -"
                    + " interval '" + age + "', '" + status + "', " + (status.equals("kept") ? "null" : "now()") + ")");
        }
    }

    private List<Long> deadIds() throws SQLException {
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select id from " + names.dead() + " order by id")) {
            var ids = new ArrayList<Long>();
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
            return ids;
        }
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
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance,
                new DeadLetterPass(db, schema.names(), config, null, new OutboxMetrics(null)));
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
            return new Cursors(names, config.shards()).positions(c, Long.MAX_VALUE).stream()
                    .map(Cursors.Position::cursor).toList();
        }
    }

    /**
     * The tick a maintenance run now drops partitions and deletes ticks below: the lowest effective tick of the shards
     * busy at the latest tick, the latest when none, which its checkpoint makes the lowest of all.
     */
    private long horizonTickId() throws SQLException {
        try (var c = TestDb.connect()) {
            long latest = new Ticks(names, config.shards()).latest(c).id();
            return Cursors.busyTick(new Cursors(names, config.shards()).positions(c, latest), latest);
        }
    }

    /** {@code pg_snapshot_xmin} of the {@link #horizonTickId} tick. */
    private long horizonXmin() throws SQLException {
        try (var c = TestDb.connect()) {
            return new Ticks(names, config.shards()).get(c, horizonTickId()).snapshot().xmin();
        }
    }

    private long makeTick() throws SQLException {
        try (var c = TestDb.connect()) {
            var ticks = new Ticks(names, config.shards());
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            return ticks.latest(c).id();
        }
    }

    private long maxCursorSeq() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select max(seq) from " + names.cursor());
        }
    }

    private long outboxRows() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + names.base());
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

    /** An orphan {@link #oursOrphan} made: the slowest shard's horizon, and the message ids sent and kept apart. */
    private record Detached(String table, long horizon, List<UUID> sent, List<UUID> above) {}

    /**
     * The lowest range once relayed, marked and detached as {@code dropProcessed} does and left by a run killed before
     * its drop (detached without its range check), then given {@code rows} rows at or above the horizon, which the
     * relay never saw, on a stale shard, over 50 keys and some unkeyed, and one row below the horizon.
     */
    private Detached oursOrphan(OutboxRelay relay, int rows) throws Exception {
        var sent = send(msg("t", "k", "a"));
        TestDb.burnXids(3000);
        relay.drain();
        long horizon = horizonXmin();
        var range = ranges().stream()
                .filter(b -> b.to() != null && Long.parseLong(b.to()) <= horizon)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        var above = new ArrayList<UUID>();
        try (var c = TestDb.connect()) {
            markDropping(c, range.name());
            execute(c, "alter table " + names.base() + " detach partition " + range.name());
            execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic) values (999999, 0, '"
                    + (Long.parseLong(range.from()) + 1) + "', gen_random_uuid(), 't')");
            execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic, key, payload)"
                    + " select 1000000 + s, 999, (" + horizon + " + s % 7)::text::xid8, gen_random_uuid(), 't',"
                    + " case when s % 10 > 0 then convert_to('k' || s % 50, 'UTF8') end, convert_to('p' || s, 'UTF8')"
                    + " from generate_series(1, " + rows + ") s");
            try (var st = c.createStatement(); var rs = st.executeQuery("select message_id from " + range.name()
                    + " where xid >= '" + horizon + "'::xid8")) {
                while (rs.next()) {
                    above.add(rs.getObject(1, UUID.class));
                }
            }
        }
        assertThat(above).hasSize(rows);
        return new Detached(range.name(), horizon, sent, above);
    }

    private static long count(String table, List<UUID> messageIds) throws SQLException {
        try (var c = TestDb.connect();
                var ps = c.prepareStatement("select count(*) from " + table + " where message_id = any(?)")) {
            ps.setArray(1, c.createArrayOf("uuid", messageIds.toArray()));
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
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
