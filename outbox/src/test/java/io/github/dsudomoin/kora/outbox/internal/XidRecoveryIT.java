package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.OutboxRelay;
import io.github.dsudomoin.kora.outbox.Outcome;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

class XidRecoveryIT {

    private final JdbcDatabase db = TestDb.newDatabase(10);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private final AtomicInteger wakes = new AtomicInteger();
    private OutboxSchema schema;
    private TableNames names;

    @AfterEach
    void release() {
        relays.forEach(OutboxRelay::release);
        db.release();
    }

    @Test
    void maintenanceRecoversARegressionItself() throws Exception {
        bootstrap();
        var handler = new RecordingHandler();
        var maintenance = maintenance();
        var relay = relay(handler, maintenance);
        relay.init();
        var outbox = outbox(relay);
        var sent = new ArrayList<>(send(outbox, messages("before", 20)));
        await(Duration.ofSeconds(5), () -> handler.delivered().size() >= 20);
        long forged = forgeRegression();
        sent.addAll(send(outbox, messages("after", 20))); // xids the forged tick takes for relayed
        await(Duration.ofSeconds(5), () -> latestTickId() > forged); // workers go on ticking past it

        try (var other = TestDb.connect()) { // two instances detect it: only the maintenance lock holder recovers
            advisory(other, "pg_advisory_lock");
            assertThat(maintenance.runOnce()).isFalse();
            assertThat(maintenance.halted()).isTrue();
            assertThat(tickExists(forged)).isTrue();
            advisory(other, "pg_advisory_unlock");
        }
        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("xid regression detected").contains("recovered: ")
                    .contains(" rows re-sent");
        }
        assertThat(maintenance.halted()).isFalse();
        assertThat(haltRows()).isZero();
        assertThat(tickExists(forged)).isFalse();
        await(Duration.ofSeconds(10), () -> delivered(handler).containsAll(sent));
    }

    @Test
    void bootstrapRecoversBeforeTheRelayStarts() throws Exception {
        bootstrap();
        var sent = new ArrayList<>(send(outbox(null), messages("before", 10))); // never relayed
        forgeRegression();
        sent.addAll(send(outbox(null), messages("after", 10)));

        try (var err = new CapturedStderr()) {
            bootstrap(); // the restart

            assertThat(err.text()).contains("ERROR").contains("recovered: 20 rows re-sent");
        }
        assertThat(haltRows()).isZero();
        var handler = new RecordingHandler();
        var maintenance = maintenance();
        relay(handler, maintenance).init();
        assertThat(maintenance.halted()).isFalse();
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 20);
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(sent);
    }

    @Test
    void aboveMaxRowsTheRelayStaysHalted() throws Exception {
        config.maxRows(10);
        bootstrap();
        send(outbox(null), messages("m", 100));
        long forged = forgeRegression();
        var maintenance = maintenance();

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("100 rows to re-send exceed outbox.recovery.maxRows 10")
                    .contains("OutboxAdmin.recoverXidRegression()")
                    .contains("POST /kora-outbox/admin/v1/recovery/xid-regression");
        }
        assertThat(maintenance.halted()).isTrue();
        assertThat(haltRows()).isOne();
        assertThat(tickExists(forged)).isTrue();
        assertThat(count(names.base())).isEqualTo(100);
        try (var err = new CapturedStderr()) {
            bootstrap(); // a restart starts halted with the same ERROR

            assertThat(err.text()).contains("100 rows to re-send exceed outbox.recovery.maxRows 10");
        }
        assertThat(tickExists(forged)).isTrue();
    }

    /**
     * A recovery that fails once it locked the outbox (here: out of its timeout copying large rows) stalled writers
     * for its run, as each try would: no instance tries again by itself, a restart neither; the admin call does.
     */
    @Test
    void aRecoveryThatFailedHoldingTheOutboxLockIsNotTriedAgain() throws Exception {
        config.relayEnabled(false).recoveryTimeout(Duration.ofMillis(100));
        bootstrap();
        try (var c = TestDb.connect()) { // 10 000 rows of 8 kB out of line: the count is quick, their copy is not
            execute(c, "insert into " + names.base() + " (shard, message_id, topic, payload) select s % "
                    + config.shards() + ", gen_random_uuid(), 't', (select decode(string_agg(md5(random()::text || s"
                    + " || i), ''), 'hex') from generate_series(1, 500) i) from generate_series(1, 10000) s");
        }
        forgeRegression();
        var outbox = outbox(null);
        var stop = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var locked = new AtomicInteger(); // samples that found ACCESS EXCLUSIVE on the outbox held or awaited
        var pool = Executors.newFixedThreadPool(2);
        var tasks = new ArrayList<Future<?>>();
        var maintenance = maintenance();
        try {
            tasks.add(pool.submit(() -> {
                while (!stop.get()) {
                    try {
                        send(outbox, msg("t", "w", "w"));
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
            }));
            tasks.add(pool.submit(() -> {
                try (var c = TestDb.connect()) {
                    while (!stop.get()) {
                        if (queryLong(c, "select count(*) from pg_locks where relation = to_regclass('" + names.base()
                                + "') and mode = 'AccessExclusiveLock'") > 0) {
                            locked.incrementAndGet();
                        }
                        Thread.sleep(2);
                    }
                }
                return null;
            }));

            String first;
            try (var err = new CapturedStderr()) {
                assertThat(maintenance.runOnce()).isTrue();
                first = err.text();
            }
            assertThat(first).contains("ERROR").contains("the automatic recovery failed after it locked the outbox")
                    .contains("57014").contains("raise that first").contains("OutboxAdmin.recoverXidRegression()");
            assertThat(locked.get()).as("the first try held the outbox").isPositive();
            assertThat(maintenance.halted()).isTrue();
            try (var c = TestDb.connect()) {
                assertThat(queryString(c, "select recovery_failed from " + names.halt())).contains("57014");
            }

            locked.set(0);
            assertThat(maintenance.runOnce()).isTrue();
            String restart;
            try (var err = new CapturedStderr()) {
                bootstrap();
                restart = err.text();
            }
            assertThat(locked.get()).as("neither the next run nor a restart took the outbox lock").isZero();
            assertThat(restart).contains("it is not tried again by itself");
            assertThat(maintenance.halted()).isTrue();
        } finally {
            stop.set(true);
            for (var task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();
        }
        assertThat(errors).isEmpty();

        config.recoveryTimeout(Duration.ofMinutes(10)); // as the README says: raise it, then call
        assertThat(admin(maintenance()).recoverXidRegression()).isPositive();
        assertThat(haltRows()).isZero();
    }

    /** Above maxRows a halted outbox is not scanned on every run: the count stands for 5 min. */
    @Test
    void aboveMaxRowsTheCountStandsForFiveMinutes() throws Exception {
        config.maxRows(10);
        bootstrap();
        send(outbox(null), messages("m", 100));
        forgeRegression();
        var recovery = maintenance().recovery;

        try (var c = TestDb.connect()) {
            assertThat(recovery.autoRecover(c)).startsWith("100 rows to re-send");
            send(outbox(null), messages("n", 50));

            assertThat(recovery.autoRecover(c)).as("not counted again").startsWith("100 rows to re-send");
        }
    }

    /** The count gets the recovery's own statement timeout, whatever the session's. */
    @Test
    void theCountRunsUnderTheRecoveryTimeout() throws Exception {
        bootstrap();
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.base() + " (shard, message_id, topic) select s % " + config.shards()
                    + ", gen_random_uuid(), 't' from generate_series(1, 1000000) s");
            forgeRegression();
            execute(c, "set statement_timeout = '100ms'"); // as a short role default would: the count takes longer

            assertThat(new XidRecovery(names, config).rowsToSave(c)).isEqualTo(1_000_000);
            assertThat(queryString(c, "show statement_timeout")).as("the session's again").isEqualTo("100ms");
        }
    }

    /**
     * While a recovery holds the tick and cursor tables, another instance's halt check and heartbeat wait at most
     * 100 ms: a starting instance takes no shards (halted) instead of hanging in its relay's init.
     */
    @Test
    void otherInstancesWaitForARecoveryAtMostTheirLockTimeout() throws Exception {
        bootstrap();
        var checked = maintenance();
        assertThat(checked.checkHalt()).isFalse();
        try (var recovering = TestDb.connect(); var err = new CapturedStderr()) {
            recovering.setAutoCommit(false); // as a recovery locks them, for its run
            execute(recovering, "lock table " + names.cursor() + ", " + names.checkpoint() + ", " + names.tick()
                    + " in access exclusive mode");
            var release = Executors.newSingleThreadScheduledExecutor();
            release.schedule(() -> { // so a check without its timeout ends, and fails the test instead of hanging it
                recovering.rollback();
                return null;
            }, 3, TimeUnit.SECONDS);
            try {
                long start = System.nanoTime();
                assertThat(maintenance().checkHalt()).as("a new instance: halted until it can read").isTrue();
                assertThat(checked.checkHalt()).as("its last answer stands").isFalse();
                new Heartbeat(db, schema, schema.config(), false).beat();
                assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
                assertThat(err.text()).contains("outbox heartbeat").contains("waited 100 ms for a lock");
            } finally {
                release.shutdown();
            }
        }
    }

    @Test
    void haltModeNeverRecoversByItself() throws Exception {
        config.xidRegression("halt");
        bootstrap();
        send(outbox(null), messages("m", 5));
        long forged = forgeRegression();
        var maintenance = maintenance();

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue();
            assertThat(maintenance.runOnce()).isTrue();

            assertThat(err.text()).contains("ERROR").contains("outbox.recovery.xidRegression = halt")
                    .contains("OutboxAdmin.recoverXidRegression()");
        }
        assertThat(maintenance.halted()).isTrue();
        assertThat(tickExists(forged)).isTrue();
        assertThat(count(names.base())).isEqualTo(5);
    }

    @Test
    void bootstrapStartsHaltedInsteadOfFailing() throws Exception {
        config.xidRegression("halt");
        bootstrap();
        send(outbox(null), messages("m", 5));
        forgeRegression();

        try (var err = new CapturedStderr()) {
            bootstrap(); // no exception: the admin call has to be reachable

            assertThat(err.text()).contains("ERROR").contains("xid regression detected").contains("relay halted");
        }
        assertThat(haltRows()).isOne();
        var handler = new RecordingHandler();
        var maintenance = maintenance();
        var relay = relay(handler, maintenance);
        relay.init();
        assertThat(maintenance.halted()).isTrue();
        Thread.sleep(300); // the workers would deliver within milliseconds
        assertThat(handler.delivered()).isEmpty();
        assertThatThrownBy(relay::drain).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("outbox halted");
    }

    @Test
    void recoveryRefusesWhenTheShardCountChanged() throws Exception {
        bootstrap(); // 4 shards
        send(outbox(null), messages("m", 5));
        long forged = forgeRegression();
        var resharded = new TestOutboxConfig().table(config.table()).shards(8);
        var maintenance = new Maintenance(db, names, resharded, new OutboxMetrics(null));

        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).as("the shard guard: nothing maintained").isFalse();

            assertThat(err.text()).contains("ERROR").contains("outbox.shards = 8").contains("4 shards");
        }
        assertThat(maintenance.halted()).isTrue();
        assertThat(tickExists(forged)).isTrue();
        assertThat(count(names.base())).isEqualTo(5);
    }

    @Test
    void standaloneRangesAreSetAside() throws Exception {
        config.xidsPerPartition(1000).premake(10);
        bootstrap();
        var range = ranges().stream()
                .filter(b -> b.to() != null)
                .min(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        var orphanId = UUID.randomUUID();
        try (var c = TestDb.connect()) { // a range with a row, detached by someone else
            execute(c, "insert into " + range.name() + " (id, shard, xid, message_id, topic) values (1000000, 0, '"
                    + range.from() + "', '" + orphanId + "', 't')");
            while (currentXid() < Long.parseLong(range.to())) { // writers have moved on to the next range
                TestDb.burnXids(200);
            }
            execute(c, "alter table " + names.base() + " detach partition " + range.name() + " concurrently");
        }
        send(outbox(null), messages("m", 3));
        forgeRegression();
        var registry = new SimpleMeterRegistry();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(registry));

        assertThat(maintenance.runOnce()).isTrue(); // the recovery
        var setAside = names.base() + "_o" + range.from();
        assertThat(exists(range.name())).isFalse();
        assertThat(exists(setAside)).isTrue();
        assertThat(count(setAside)).isOne();
        try (var err = new CapturedStderr()) {
            assertThat(maintenance.runOnce()).isTrue(); // the next run's orphan sweep keeps it

            assertThat(err.text()).contains("ERROR").contains(setAside)
                    .contains("OutboxAdmin.restoreOrphan(\"" + setAside + "\", fromXid)");
        }
        assertThat(exists(setAside)).isTrue();
        assertThat(registry.get("outbox.partitions.orphaned").gauge().value()).isEqualTo(1);
        try (var c = TestDb.connect()) {
            assertThat(maintenance.orphanRestore.restore(c, setAside, 0, Duration.ofSeconds(5))).isOne();
        }
        assertThat(exists(setAside)).isFalse();
        assertThat(count(names.base(), List.of(orphanId))).isOne();
    }

    @Test
    void rowsOutsideTheOutboxStayWhereTheyAre() throws Exception {
        config.headFailuresBeforeDivert(1);
        bootstrap();
        var down = new AtomicBoolean(true);
        var recording = new RecordingHandler()
                .failAlways(r -> down.get() && "r".equals(key(r)), () -> new RuntimeException("target down"));
        OutboxHandler handler = new OutboxHandler() { // and requeues key p while down: it waits parked
            @Override
            public void handle(OutboxRecord record) throws Exception {
                recording.handle(record);
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                boolean parks = down.get();
                var others = new ArrayDeque<>(recording.handleBatch(
                        records.stream().filter(r -> !parks || !"p".equals(key(r))).toList()));
                return records.stream()
                        .map(r -> parks && "p".equals(key(r)) ? Outcome.requeue() : others.poll())
                        .toList();
            }
        };
        var maintenance = maintenance();
        var relay = relay(handler, maintenance);
        relay.init();
        var outbox = outbox(relay);
        var held = send(outbox, msg("t", "r", "head"), msg("t", "r", "follower"));
        var parked = send(outbox, msg("t", "p", "parked"));
        await(Duration.ofSeconds(5), // the head diverted with its follower, p requeued
                () -> count(names.retry()) == 2 && count(names.parked(), parked) == 1);
        try (var c = TestDb.connect()) { // a parked row whose outbox range is gone: its id above every outbox id
            execute(c, "insert into " + names.parked() + " (id, shard, message_id, topic, key, created_at, due_at)"
                    + " values (1000000000, 0, gen_random_uuid(), 't', convert_to('q', 'UTF8'), now(),"
                    + " now() + interval '1 hour')");
        }
        forgeRegression();
        var other = send(outbox, msg("t", "k", "other"));

        assertThat(maintenance.runOnce()).isTrue();

        assertThat(maintenance.halted()).isFalse();
        assertThat(count(names.retry(), held)).as("the retry rows survive").isEqualTo(2);
        assertThat(count(names.base(), held)).as("not copied into the outbox").isZero();
        assertThat(count(names.base(), parked)).as("nor the parked one").isZero();
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select min(id) from " + names.base())).as("re-sent above every parked id")
                    .isGreaterThan(1_000_000_000L);
        }
        down.set(false);
        await(Duration.ofSeconds(10), () -> delivered(recording).containsAll(held)
                && delivered(recording).containsAll(other) && delivered(recording).containsAll(parked));
        assertThat(recording.delivered()).extracting(r -> new String(r.payload(), UTF_8))
                .filteredOn(p -> p.equals("head") || p.equals("follower"))
                .containsExactly("head", "follower");
        assertThat(recording.delivered()).filteredOn(r -> parked.contains(r.messageId())).as("once").hasSize(1);
        await(Duration.ofSeconds(5), () -> count(names.retry()) == 0);
    }

    @Test
    void writersWaitButDoNotFail() throws Exception {
        config.relayEnabled(false).batchSize(1000);
        bootstrap();
        try (var c = TestDb.connect()) {
            execute(c, "insert into " + names.base() + " (shard, message_id, topic, payload) select s % "
                    + config.shards() + ", gen_random_uuid(), 't', convert_to('p' || s, 'UTF8')"
                    + " from generate_series(1, 50000) s");
        }
        forgeRegression();
        var expected = new HashSet<>(messageIds());
        var outbox = outbox(null);
        var stop = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var written = new ConcurrentLinkedQueue<UUID>();
        var pool = Executors.newFixedThreadPool(5);
        var tasks = new ArrayList<Future<?>>();
        try {
            for (int i = 0; i < 4; i++) {
                var key = "w" + i;
                tasks.add(pool.submit(() -> {
                    while (!stop.get()) {
                        try {
                            written.addAll(send(outbox, msg("t", key, "w")));
                        } catch (Throwable t) {
                            errors.add(t);
                        }
                    }
                }));
            }
            tasks.add(pool.submit(() -> { // other traffic takes xids while the recovery runs
                while (!stop.get()) {
                    try {
                        TestDb.burnXids(20);
                    } catch (SQLException e) {
                        errors.add(e);
                    }
                }
            }));
            await(Duration.ofSeconds(5), () -> written.size() >= 40);

            try (var err = new CapturedStderr()) {
                assertThat(maintenance().runOnce()).isTrue();

                assertThat(err.text()).contains("recovered: ");
            }
            long after = written.size();
            await(Duration.ofSeconds(5), () -> written.size() >= after + 40);
        } finally {
            stop.set(true);
            for (var task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();
        }

        assertThat(errors).isEmpty();
        expected.addAll(written);
        var handler = new RecordingHandler();
        relay(handler, maintenance()).drain();
        assertThat(delivered(handler)).containsAll(expected);
    }

    @Test
    void recoveryLeavesTheSessionSettingsAsTheyWere() throws Exception {
        bootstrap();
        send(outbox(null), messages("m", 3));
        forgeRegression();

        try (var c = TestDb.connect()) { // a pooled connection: its partition DDL ran inside the transaction
            assertThat(new XidRecovery(names, config).recover(c)).isEqualTo(3);

            assertThat(c.getAutoCommit()).isTrue();
            assertThat(queryString(c, "select current_setting('lock_timeout') || ' ' || current_setting("
                    + "'statement_timeout')")).isEqualTo("0 0");
        }
    }

    @Test
    void adminRecoversAboveMaxRows() throws Exception {
        config.maxRows(10);
        bootstrap();
        var sent = send(outbox(null), messages("m", 100));
        long forged = forgeRegression();
        var maintenance = maintenance();
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maintenance.halted()).as("above maxRows").isTrue();

        try (var err = new CapturedStderr()) {
            assertThat(admin(maintenance).recoverXidRegression()).isEqualTo(100);

            assertThat(err.text()).contains("INFO")
                    .contains("recoverXidRegression() on " + names.base() + " returned 100");
        }
        assertThat(maintenance.halted()).as("this instance resumes at once").isFalse();
        assertThat(wakes).hasValue(1);
        assertThat(haltRows()).isZero();
        assertThat(tickExists(forged)).isFalse();
        var handler = new RecordingHandler();
        relay(handler, maintenance()).drain();
        assertThat(delivered(handler)).containsAll(sent);
    }

    @Test
    void adminRecoversInHaltMode() throws Exception {
        config.xidRegression("halt");
        bootstrap();
        var sent = send(outbox(null), messages("m", 5));
        forgeRegression();
        var maintenance = maintenance();
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maintenance.halted()).isTrue();

        assertThat(admin(maintenance).recoverXidRegression()).isEqualTo(5);

        assertThat(maintenance.halted()).isFalse();
        assertThat(haltRows()).isZero();
        var handler = new RecordingHandler();
        relay(handler, maintenance()).drain();
        assertThat(delivered(handler)).containsAll(sent);
    }

    @Test
    void adminRefusesWithoutARegression() throws Exception {
        bootstrap();
        send(outbox(null), messages("m", 3));
        var admin = admin(maintenance());

        assertThatThrownBy(admin::recoverXidRegression).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("no xid regression to recover");

        forgeRegression();
        admin.maintenanceWait = Duration.ofMillis(200);
        try (var other = TestDb.connect()) {
            advisory(other, "pg_advisory_lock");
            assertThatThrownBy(admin::recoverXidRegression).isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith("maintenance is running, retry");
        }
        assertThat(count(names.base())).as("nothing re-sent").isEqualTo(3);
        assertThat(wakes).hasValue(0);
    }

    @Test
    void rowsTheCursorTicksDoNotSeeAreSavedWithoutAClockMargin() throws Exception {
        config.clockMargin(Duration.ZERO);
        bootstrap();
        var sent = send(outbox(null), messages("m", 20)); // never relayed: after every cursor's tick
        // made after them, so T, its created_at, saves none of them: only the cursors' ticks not seeing them can
        forgeRegression("now()");

        try (var err = new CapturedStderr()) {
            assertThat(maintenance().runOnce()).isTrue();

            assertThat(err.text()).contains("recovered: 20 rows re-sent");
        }
        var handler = new RecordingHandler();
        relay(handler, maintenance()).drain();
        assertThat(delivered(handler)).containsAll(sent);
    }

    @Test
    void recoveryCursorsAreDueWhenTheRetryHeadIs() throws Exception {
        bootstrap();
        try (var c = TestDb.connect()) { // a retry head of shard 1 due in an hour, with a follower
            execute(c, "insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at, attempts,"
                    + " next_attempt_at) values (1000000000, 1, gen_random_uuid(), 't', 'r', now(), 1,"
                    + " now() + interval '1 hour'), (1000000001, 1, gen_random_uuid(), 't', 'r', now(), 0, null)");
        }
        forgeRegression();

        assertThat(maintenance().runOnce()).isTrue();

        assertThat(haltRows()).isZero();
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(c.shard || ':' || (c.retry_due_at = r.due)::text, ','"
                    + " order by c.shard) from (select distinct on (shard) shard, retry_due_at from " + names.cursor()
                    + " order by shard, seq desc) c left join (select shard, min(next_attempt_at) due from "
                    + names.retry() + " group by shard) r using (shard) where c.retry_due_at is not null"))
                    .as("due when the head is, as steps store it, not at once").isEqualTo("1:true");
        }
    }

    @Test
    void recoveryGoesByEffectiveTicks() throws Exception {
        config.clockMargin(Duration.ZERO).relayEnabled(false);
        bootstrap();
        var relay = relay(new RecordingHandler(), maintenance());
        send(outbox(null), msg("t", "k", "relayed"));
        assertThat(relay.drain()).isOne();
        long drained = latestTickId();
        try (var c = TestDb.connect()) {
            assertThat(new Ticks(names, config.shards()).make(c, Duration.ZERO)).isTrue();
        }
        assertThat(maintenance().runOnce()).isTrue(); // a checkpoint past the drain tick
        try (var c = TestDb.connect()) { // cleanup keeps the cursors' ticks: gone by hand, as an earlier build left them
            execute(c, "delete from " + names.tick() + " where id < (select tick_id from " + names.checkpoint() + ")");
        }
        assertThat(tickExists(drained)).as("the relayed shard's cursor tick").isFalse();
        try (var c = TestDb.connect()) {
            assertThat(new XidRecovery(names, config).rowsToSave(c)).as("relayed up to its effective tick").isZero();
        }
        forgeRegression("now()");

        try (var err = new CapturedStderr()) {
            assertThat(maintenance().runOnce()).isTrue();

            assertThat(err.text()).contains("recovered: 0 rows re-sent");
        }
        assertThat(count(names.checkpoint())).as("checkpoints go with the cursors").isZero();
    }

    @Test
    void anXidTakenWhileTheCounterCrossesARangeBoundaryFindsItsRange() throws Exception {
        config.xidsPerPartition(1000).premake(4);
        bootstrap();
        send(outbox(null), messages("m", 3));
        forgeRegression();
        TestDb.burnXids((int) (1000 - currentXid() % 1000) + 100); // the recovery's xids early in a range: no bridge
        var recovery = new XidRecovery(names, config);
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            writer.setAutoCommit(false);
            recovery.betweenReads = rc -> {
                long xid = queryLong(writer, "select pg_current_xact_id()::text::bigint");
                while (currentXid() < xid / 1000 * 1000 + 1000) { // the next read sees the next range's xids
                    TestDb.burnXids(200);
                }
            };

            assertThat(recovery.recover(c)).isEqualTo(3);

            execute(writer, "insert into " + names.base() + " (shard, message_id, topic) values (0, gen_random_uuid(),"
                    + " 't')"); // no partition for its xid: SQLState 23514
            writer.commit();
        }
        assertThat(count(names.base())).isEqualTo(4);
    }

    @Test
    void aDetachPendingInsideTheTransactionRollsItBack() throws Exception {
        bootstrap();
        send(outbox(null), messages("m", 3));
        long forged = forgeRegression();
        var range = ranges().stream()
                .filter(b -> b.to() != null)
                .max(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        var recovery = new XidRecovery(names, config);
        recovery.beforeTransaction = rc -> detachPending(range.name()); // after the finish before the transaction

        try (var c = TestDb.connect()) {
            var probe = names.base() + "_recovery"; // the copy's table: a copy before the check would fail on it
            execute(c, "create table " + probe + " (x int)");
            assertThatThrownBy(() -> recovery.recover(c)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("became pending");
            assertThat(tickExists(forged)).as("rolled back").isTrue();
            assertThat(isDetachPending(c, range.name())).isEqualTo("true");
            execute(c, "drop table " + probe);

            recovery.beforeTransaction = null;
            assertThat(recovery.recover(c)).as("the next attempt finishes it first").isEqualTo(3);

            assertThat(PartitionDdl.pendingDetach(c, names.base())).isNull();
        }
        assertThat(haltRows()).isZero();
    }

    @Test
    void rangeNamesWhoseDigitsFitNoLongAreLeftAlone() throws Exception {
        bootstrap();
        send(outbox(null), messages("m", 3));
        forgeRegression();
        var empty = names.partitionPrefix() + "9".repeat(20);
        var withRow = names.partitionPrefix() + "9".repeat(21);
        try (var c = TestDb.connect()) {
            PartitionDdl.create(c, names.base(), empty, Duration.ofSeconds(5));
            PartitionDdl.create(c, names.base(), withRow, Duration.ofSeconds(5));
            execute(c, "insert into " + withRow + " (id, shard, xid, message_id, topic) values (1, 0, '1',"
                    + " gen_random_uuid(), 't')");

            assertThat(new XidRecovery(names, config).recover(c)).isEqualTo(3);

            assertThat(PartitionDdl.standalone(c, names.base(), "_o[0-9]+")).as("nothing set aside").isEmpty();
        }
        assertThat(exists(empty)).as("not dropped").isTrue();
        assertThat(count(withRow)).isOne();
    }

    @Test
    void adminReturnsTheRowsWhenTheSessionDiesAfterTheCommit() throws Exception {
        bootstrap();
        send(outbox(null), messages("m", 5));
        forgeRegression();
        var maintenance = maintenance();
        var admin = new PgOutboxAdmin(TestDb.killedAfterCommit(db), names, config, maintenance,
                wakes::incrementAndGet);

        try (var err = new CapturedStderr()) {
            assertThat(admin.recoverXidRegression()).isEqualTo(5);

            var text = err.text();
            assertThat(text).contains("WARN").contains("the recovery committed, then releasing the maintenance lock")
                    .doesNotContain("nothing re-sent");
            assertThat(text.indexOf("returned 5, the rows re-sent")).as("the INFO, then the WARN").isNotNegative()
                    .isLessThan(text.indexOf("the recovery committed, then"));
        }
        assertThat(wakes).hasValue(1);
        assertThat(maintenance.halted()).isFalse();
        assertThat(haltRows()).isZero();
        assertThat(maintenance.runOnce()).as("the maintenance lock went with the session").isTrue();
    }

    @Test
    void adminReturnsTheRowsWhenReadingTheHaltAgainFails() throws Exception {
        bootstrap();
        send(outbox(null), messages("m", 5));
        forgeRegression();
        var closed = TestDb.newDatabase(1);
        var maintenance = new Maintenance(closed, names, config, new OutboxMetrics(null));
        closed.release(); // its checkHalt cannot connect

        try (var err = new CapturedStderr()) {
            assertThat(admin(maintenance).recoverXidRegression()).isEqualTo(5);

            assertThat(err.text()).contains("WARN").contains("reading the halt again failed");
        }
        assertThat(wakes).hasValue(1);
        assertThat(haltRows()).isZero();
    }

    @Test
    void adminWarnsOnAFailureBeforeTheRecoveryBegan() throws Exception {
        bootstrap();
        try (var c = TestDb.connect()) {
            execute(c, "drop table " + names.halt()); // the check's read fails
        }

        try (var err = new CapturedStderr()) {
            assertThatThrownBy(admin(maintenance())::recoverXidRegression).isInstanceOf(RuntimeSqlException.class);

            assertThat(err.text()).contains("WARN")
                    .contains("recoverXidRegression() on " + names.base() + " failed, nothing re-sent");
        }
        assertThat(wakes).hasValue(0);
    }

    /** Leaves {@code range} detach-pending: its DETACH CONCURRENTLY times out waiting for an older snapshot. */
    private void detachPending(String range) throws SQLException {
        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false);
            queryLong(holder, "select count(*) from " + names.base());
            execute(c, "set lock_timeout = '300ms'");
            assertThatThrownBy(() -> execute(c, "alter table " + names.base() + " detach partition " + range
                    + " concurrently")).isInstanceOf(SQLException.class);
            holder.commit();
        }
    }

    private static String isDetachPending(Connection c, String table) throws SQLException {
        return queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)", table);
    }

    private PgOutboxAdmin admin(Maintenance maintenance) {
        return new PgOutboxAdmin(db, names, config, maintenance, wakes::incrementAndGet);
    }

    private void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
    }

    /**
     * As after a dump/restore: a tick whose snapshot xmax lies 10⁹ xids past the counter, made a minute ago. Returns
     * its id.
     */
    private long forgeRegression() throws SQLException {
        return forgeRegression("now() - interval '1 minute'");
    }

    /** {@link #forgeRegression()} with the tick made at {@code createdAt}. */
    private long forgeRegression(String createdAt) throws SQLException {
        try (var c = TestDb.connect()) {
            long xmax = currentXid() + 1_000_000_000L;
            return queryLong(c, "insert into " + names.tick() + " (snapshot, created_at) values ('" + xmax + ":" + xmax
                    + ":'::pg_snapshot, " + createdAt + ") returning id");
        }
    }

    private Maintenance maintenance() {
        return new Maintenance(db, names, config, new OutboxMetrics(null));
    }

    private OutboxRelay relay(OutboxHandler handler, Maintenance maintenance) {
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance,
                new DeadLetterPass(db, schema.names(), config, null, new OutboxMetrics(null)));
        relays.add(relay);
        return relay;
    }

    private PgOutbox outbox(OutboxRelay relay) {
        return new PgOutbox(db, schema, config.shards(), relay == null ? () -> {} : relay::wake,
                new OutboxMetrics(null));
    }

    private List<UUID> send(PgOutbox outbox, OutboxMessage... messages) {
        return db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(messages)));
    }

    private static OutboxMessage[] messages(String prefix, int n) {
        return IntStream.range(0, n).mapToObj(i -> msg("t", "k" + i % 5, prefix + i)).toArray(OutboxMessage[]::new);
    }

    private static OutboxMessage msg(String topic, String key, String payload) {
        return OutboxMessage.of(topic, key, payload.getBytes(UTF_8));
    }

    private static String key(OutboxRecord r) {
        return r.key() == null ? null : new String(r.key(), UTF_8);
    }

    private static Set<UUID> delivered(RecordingHandler handler) {
        var ids = new HashSet<UUID>();
        handler.delivered().forEach(r -> ids.add(r.messageId()));
        return ids;
    }

    private void advisory(Connection c, String function) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_MAINTENANCE);
            ps.execute();
        }
    }

    private long latestTickId() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select max(id) from " + names.tick());
        }
    }

    private boolean tickExists(long id) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + names.tick() + " where id = " + id) == 1;
        }
    }

    private long haltRows() throws SQLException {
        return count(names.halt());
    }

    private static long count(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + table);
        }
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

    private List<UUID> messageIds() throws SQLException {
        try (var c = TestDb.connect();
                var st = c.createStatement();
                var rs = st.executeQuery("select message_id from " + names.base())) {
            var ids = new ArrayList<UUID>();
            while (rs.next()) {
                ids.add(rs.getObject(1, UUID.class));
            }
            return ids;
        }
    }

    private List<Bound> ranges() throws SQLException {
        try (var c = TestDb.connect()) {
            return PartitionDdl.listRanges(c, names.base());
        }
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select (to_regclass(?) is not null)::text", table).equals("true");
        }
    }

    private static long currentXid() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
        }
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
