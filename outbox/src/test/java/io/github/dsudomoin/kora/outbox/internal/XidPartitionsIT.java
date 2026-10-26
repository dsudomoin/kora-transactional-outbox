package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

class XidPartitionsIT {

    private final JdbcDatabase db = TestDb.newDatabase(4);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void extendsWhenHalfOfPremadeConsumed() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(2500);

        try (var c = TestDb.connect()) {
            assertThat(partitions.premadeAhead(c)).isLessThanOrEqualTo(2);
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }

            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void extensionDoesNotBlockConcurrentInserts() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(10_000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
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
                        try (var st = c.createStatement()) {
                            st.execute("insert into " + names.base()
                                    + "(shard, message_id, topic) values (0, gen_random_uuid(), 't')");
                        }
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

        try (var c = TestDb.connect()) {
            long initialTail = tail(c, names);
            long tail = initialTail;
            int extensions = 0;
            long cap = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            try {
                // condition-based, not time-boxed: two extensions (tail detached, dropped, re-attached higher) while
                // the writers insert, however slow a loaded machine is
                while (extensions < 2) {
                    assertThat(System.nanoTime()).as("two extensions within 60 s").isLessThan(cap);
                    TestDb.burnXids(3000);
                    partitions.ensurePremade(c);
                    long moved = tail(c, names);
                    if (moved != tail) {
                        extensions++;
                        tail = moved;
                    }
                }
            } finally {
                done.set(true);
                for (var w : writers) {
                    w.join();
                }
            }

            assertThat(errors).isEmpty();
            // an insert queued behind partition DDL would wait for that DDL's lock_timeout (30 s for the detach); the
            // bound leaves room for a saturated machine
            assertThat(Duration.ofNanos(maxLatency.get())).isLessThan(Duration.ofSeconds(10));
            assertThat(tail(c, names)).isGreaterThan(initialTail);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isZero();
            assertThat(queryLong(c, "select count(*) from " + names.base())).isPositive();
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void shortWriterTransactionsNeverBlockTheTailDetach() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        var done = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writers = new ArrayList<Thread>();
        for (int i = 0; i < 4; i++) { // each commits every few ms: between pg_locks and pg_stat_activity it may be idle
            var writer = new Thread(() -> {
                try (var c = TestDb.connect()) {
                    c.setAutoCommit(false);
                    while (!done.get()) {
                        execute(c, "insert into " + names.base()
                                + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
                        c.commit();
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            writer.start();
            writers.add(writer);
        }

        var blockers = new ArrayList<String>();
        try (var c = TestDb.connect()) {
            for (int i = 0; i < 500; i++) {
                var blocker = partitions.detachBlocker(c);
                if (blocker != null) {
                    blockers.add(blocker);
                }
            }
        } finally {
            done.set(true);
            for (var w : writers) {
                w.join();
            }
        }

        assertThat(errors).isEmpty();
        assertThat(blockers).isEmpty();
    }

    @Test
    void anotherRolesShortTransactionsDoNotDeferTheTailDetach() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1)); // 20 s in production
        var done = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writers = new ArrayList<Thread>();
        for (int i = 0; i < 4; i++) { // the app's own writers under another login role: their age is hidden
            var writer = new Thread(() -> {
                try (var c = TestDb.connect()) {
                    c.setAutoCommit(false);
                    while (!done.get()) {
                        execute(c, "insert into " + names.base()
                                + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
                        c.commit();
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            writer.start();
            writers.add(writer);
        }

        var blockers = new ArrayList<String>();
        try (var c = TestDb.connect(login(TestDb.newLoginRole()))) {
            for (int i = 0; i < 4; i++) {
                var blocker = partitions.detachBlocker(c);
                if (blocker != null) {
                    blockers.add(blocker);
                }
            }
        } finally {
            done.set(true);
            for (var w : writers) {
                w.join();
            }
        }

        assertThat(errors).isEmpty();
        assertThat(blockers).as("each writer transaction ends before the second sample").isEmpty();
    }

    @Test
    void anotherRolesTransactionHeldAcrossBothSamplesDefers() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect(login(TestDb.newLoginRole()))) {
            holder.setAutoCommit(false); // just begun, but its age is hidden from the maintenance role
            queryLong(holder, "select count(*) from " + names.base());

            var blocker = partitions.detachBlocker(c);

            assertThat(blocker).contains("age hidden").contains("pg_read_all_stats");
            holder.commit();
        }
    }

    @Test
    void aPgDumpThatStartsDuringTheSecondSampleCounts() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(2));
        var dump = new Properties();
        dump.setProperty("ApplicationName", "pg_dump");

        try (var holder = TestDb.connect(); var pgDump = TestDb.connect(dump);
                var c = TestDb.connect(login(TestDb.newLoginRole()))) {
            holder.setAutoCommit(false); // hidden from the maintenance role, so it is sampled again after 2 s
            queryLong(holder, "select count(*) from " + names.base());
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var meanwhile = new Thread(() -> { // the holder ends, and a dump starts, while maintenance waits
                try {
                    Thread.sleep(500);
                    holder.commit();
                    pgDump.setAutoCommit(false);
                    execute(pgDump, "set transaction isolation level repeatable read");
                    queryLong(pgDump, "select count(*) from " + names.base());
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            meanwhile.start();

            var blocker = partitions.detachBlocker(c);

            meanwhile.join();
            assertThat(errors).isEmpty();
            assertThat(blocker).contains("pg_dump");
            pgDump.commit();
        }
    }

    @Test
    void orphanCheckSeesARowCommittedBeforeItsExclusiveLockUnderRepeatableRead() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var orphan = names.partitionPrefix() + "999000000";
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            execute(c, "create table " + orphan + " (like " + names.base() + " including defaults)");
            execute(c, "set default_transaction_isolation = 'repeatable read'"); // a role or pool default
            writer.setAutoCommit(false); // an operator's insert, committed while the check waits for its lock
            execute(writer, "insert into " + orphan + " (id, shard, xid, message_id, topic)"
                    + " values (1, 0, '1', gen_random_uuid(), 't')");
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var commitLater = new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    writer.commit();
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            commitLater.start();

            boolean dropped = PartitionDdl.dropUnlessFound(
                    c, orphan, "select 1 from {table} limit 1", null, Duration.ofSeconds(10));

            commitLater.join();
            assertThat(errors).isEmpty();
            assertThat(dropped).as("the re-check under the exclusive lock sees the committed row").isFalse();
            assertThat(queryLong(c, "select count(*) from " + orphan)).isOne();
        }
    }

    @Test
    void walSenderSnapshotsDeferAtAnyAge() throws Exception {
        try (var c = TestDb.connect(); var ps = c.prepareStatement("select a.kind, " + XidPartitions.SNAPSHOT_DEFERS
                + " from (values ('walsender', '', 'walsender', '1000'::xid, now()),"
                + " ('pg_dump', 'pg_dump', 'client backend', '1000'::xid, now()),"
                + " ('old client', '', 'client backend', '1000'::xid, now() - interval '1 hour'),"
                + " ('young client', '', 'client backend', '1000'::xid, now()),"
                + " ('old autovacuum', '', 'autovacuum worker', '1000'::xid, now() - interval '1 hour'),"
                + " ('walsender without snapshot', '', 'walsender', null::xid, null::timestamptz))"
                + " a(kind, application_name, backend_type, backend_xmin, xact_start)")) {
            ps.setDouble(1, 20);
            var defers = new java.util.TreeMap<String, Boolean>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    defers.put(rs.getString(1), rs.getBoolean(2));
                }
            }

            assertThat(defers).containsEntry("walsender", true) // a logical replication initial sync
                    .containsEntry("pg_dump", true)
                    .containsEntry("old client", true)
                    .containsEntry("young client", false)
                    .containsEntry("old autovacuum", false)
                    .containsEntry("walsender without snapshot", false);
        }
    }

    @Test
    void hotTailIsNeverDetached() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = tail(c, names);
            long current = queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
            TestDb.burnXids((int) (lower - current - 500)); // within one range of the tail, not yet in it

            partitions.ensurePremade(c);

            assertThat(partitions.overflow()).isTrue();
            assertThat(queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)",
                            names.tail()))
                    .isEqualTo("false");
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint")).isLessThan(lower);
        }
    }

    @Test
    void tailWithRowsIsNotDetachedAndFlagsOverflow() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(6000);

        try (var c = TestDb.connect()) {
            long tailBefore = tail(c, names);
            assertThat(queryString(c, "insert into " + names.base()
                            + "(shard, message_id, topic) values (0, gen_random_uuid(), 't')"
                            + " returning tableoid::regclass::text"))
                    .isEqualTo(names.tail());

            partitions.ensurePremade(c);

            assertThat(partitions.overflow()).isTrue();
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(1);
            assertThat(tail(c, names)).isEqualTo(tailBefore);
        }
    }

    @Test
    void pendingTailDetachIsFinalizedOnNextExtension() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false);
            queryLong(holder, "select count(*) from " + names.base()); // an old snapshot the detach must wait for
            try (var st = c.createStatement()) {
                st.execute("set lock_timeout = '300ms'");
                assertThatThrownBy(() -> st.execute(
                                "alter table " + names.base() + " detach partition " + names.tail() + " concurrently"))
                        .isInstanceOf(SQLException.class);
                st.execute("reset lock_timeout");
            }
            holder.commit();
            assertThat(queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)",
                            names.tail()))
                    .isEqualTo("true");
            TestDb.burnXids(2500);
            long offset = queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint % 1000");
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }

            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertThat(queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)",
                            names.tail()))
                    .isEqualTo("false");
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void longSnapshotDefersTheTailDetachSoInsertsSpillIntoTheTail() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1)); // 20 s in production

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false); // a pg_dump: a repeatable-read snapshot that has read the table
            execute(holder, "set transaction isolation level repeatable read");
            queryLong(holder, "select count(*) from " + names.base());
            Thread.sleep(1100);
            long lower = tail(c, names);
            TestDb.burnXids(2500); // into the extension window
            assertThat(partitions.premadeAhead(c)).isLessThanOrEqualTo(2);

            ensurePremadeAsMaintenance(partitions, c);

            assertThat(detachPending(c, names.tail())).as("no detach started").isEqualTo("false");
            assertThat(tail(c, names)).isEqualTo(lower);
            // the snapshot outlasts the margin: rows go to the attached tail, the overflow path
            TestDb.burnXids((int) (lower - currentXid(c)) + 10);
            insertIntoTail(c, names);
            ensurePremadeAsMaintenance(partitions, c);
            assertThat(partitions.overflow()).isTrue();
            holder.commit();
        }
    }

    @Test
    void oldLockOnTheParentDefersTheTailDetachUntilItEnds() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false); // read committed, idle in transaction: no snapshot, but its lock stays
            queryLong(holder, "select count(*) from " + names.base());
            Thread.sleep(1100);
            long lower = tail(c, names);
            TestDb.burnXids(2500);

            partitions.ensurePremade(c, Duration.ofMillis(300)); // no lock timeout: no detach was tried

            assertThat(detachPending(c, names.tail())).isEqualTo("false");
            assertThat(tail(c, names)).isEqualTo(lower);
            holder.commit();
        }
        try (var c = TestDb.connect()) {
            partitions.ensurePremade(c, Duration.ofMillis(300));

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void oldSnapshotWithoutALockOnTheParentDefersTheTailDetach() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false); // a report on other tables: a snapshot, no lock on the outbox
            execute(holder, "set transaction isolation level repeatable read");
            queryLong(holder, "select count(*) from pg_class");
            Thread.sleep(1100);
            long lower = tail(c, names);
            TestDb.burnXids(2500);

            String log;
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c, Duration.ofMillis(300));
                partitions.ensurePremade(c, Duration.ofMillis(300));
                log = err.text();
            }

            assertThat(log.split("deferred to the next run", -1)).as("one WARN a minute").hasSize(2);
            assertThat(log).contains("holds a snapshot");
            assertThat(detachPending(c, names.tail())).isEqualTo("false");
            assertThat(tail(c, names)).isEqualTo(lower);
            holder.commit();
        }
    }

    @Test
    void pgDumpDefersTheTailDetachAtAnyAge() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions()); // the production 20 s
        var dump = new Properties();
        dump.setProperty("ApplicationName", "pg_dump");

        try (var holder = TestDb.connect(dump); var c = TestDb.connect()) {
            holder.setAutoCommit(false); // a dump that started a moment ago
            execute(holder, "set transaction isolation level repeatable read");
            queryLong(holder, "select count(*) from " + names.base());
            long lower = tail(c, names);
            TestDb.burnXids(2500);

            ensurePremadeAsMaintenance(partitions, c);

            assertThat(detachPending(c, names.tail())).as("no detach started").isEqualTo("false");
            assertThat(tail(c, names)).isEqualTo(lower);
            holder.commit();
        }
    }

    @Test
    void anotherRolesTransactionCountsAsOldWhenItsAgeIsHidden() throws Exception {
        var role = TestDb.newLoginRole(); // the app role, without pg_read_all_stats
        var app = TestDb.newDatabase(2, role);
        try {
            var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
            TableNames names;
            String log;
            try (var err = new CapturedStderr()) {
                var schema = new OutboxSchema(app, config);
                schema.init();
                names = schema.names();
                log = err.text();
            }
            var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));
            var login = new Properties();
            login.setProperty("user", role);
            login.setProperty("password", role);

            try (var holder = TestDb.connect(); var c = TestDb.connect(login)) {
                holder.setAutoCommit(false); // a pg_dump by a backup role, under another application name
                execute(holder, "set transaction isolation level repeatable read");
                queryLong(holder, "select count(*) from " + names.base());
                long lower = tail(c, names);
                TestDb.burnXids(2500);

                ensurePremadeAsMaintenance(partitions, c); // the holder outlives the second sample

                assertThat(detachPending(c, names.tail())).as("no detach started").isEqualTo("false");
                assertThat(tail(c, names)).isEqualTo(lower);
                holder.commit();
            }
            assertThat(log).as("said at startup").contains("WARN").contains(role).contains("pg_read_all_stats");
        } finally {
            app.release();
        }
    }

    @Test
    void shortRoleStatementTimeoutDoesNotCutTheTailDetachShort() throws Exception {
        var role = TestDb.newLoginRole();
        try (var c = TestDb.connect()) {
            execute(c, "alter role " + role + " set statement_timeout = '1s'");
        }
        var app = TestDb.newDatabase(2, role);
        try {
            var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
            var schema = new OutboxSchema(app, config);
            schema.init();
            var names = schema.names();
            var partitions = new XidPartitions(names, config.partitions());
            var login = new Properties();
            login.setProperty("user", role);
            login.setProperty("password", role);

            try (var holder = TestDb.connect(login); var c = TestDb.connect(login)) {
                assertThat(queryString(c, "show statement_timeout")).isEqualTo("1s");
                execute(c, "set lock_timeout = '7s'"); // a session value, which the DDL must give back
                long lower = tail(c, names);
                TestDb.burnXids(2500);
                holder.setAutoCommit(false); // a young reader the detach waits about 2 s for
                queryLong(holder, "select count(*) from " + names.base());
                var errors = new ConcurrentLinkedQueue<Throwable>();
                var release = new Thread(() -> {
                    try {
                        Thread.sleep(2000);
                        holder.commit();
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                });
                release.start();

                partitions.ensurePremade(c);

                release.join();
                assertThat(errors).isEmpty();
                assertThat(tail(c, names)).isGreaterThan(lower);
                assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
                assertThat(queryString(c, "show statement_timeout")).as("the role default again").isEqualTo("1s");
                assertThat(queryString(c, "show lock_timeout")).as("the session value again").isEqualTo("7s");
            }
        } finally {
            app.release();
        }
    }

    @Test
    void tailRowRacingTheDetachSurvives() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        var id = UUID.randomUUID();

        try (var c = TestDb.connect(); var racer = TestDb.connect()) {
            long lower = tail(c, names);
            // the state right after the extension's DETACH CONCURRENTLY: a writer routed to the tail under an older
            // snapshot inserts into the now standalone table and commits only after the extension looked at it
            execute(c, "alter table " + names.base() + " detach partition " + names.tail());
            racer.setAutoCommit(false);
            try (var ps = racer.prepareStatement("insert into " + names.tail() // detached: no identity default
                    + "(id, shard, xid, message_id, topic) values (1000000, 0, ?::text::xid8, ?, 't')")) {
                ps.setLong(1, lower + 5);
                ps.setObject(2, id);
                ps.executeUpdate();
            }
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var commitLater = new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    racer.commit();
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            commitLater.start();

            partitions.ensurePremade(c);

            commitLater.join();
            assertThat(errors).isEmpty();
            assertThat(partitions.overflow()).isTrue();
            assertThat(countThroughParent(c, names, id)).isEqualTo(1);
            assertThat(tail(c, names)).isEqualTo(lower);
        }
    }

    @Test
    void pendingTailWithRowsIsFinalizedAndReattached() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(6000);

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = tail(c, names);
            var id = insertIntoTail(c, names);
            holder.setAutoCommit(false);
            queryLong(holder, "select count(*) from " + names.base());
            try (var st = c.createStatement()) {
                st.execute("set lock_timeout = '300ms'");
                assertThatThrownBy(() -> st.execute(
                                "alter table " + names.base() + " detach partition " + names.tail() + " concurrently"))
                        .isInstanceOf(SQLException.class);
                st.execute("reset lock_timeout");
            }
            holder.commit();
            assertThat(countThroughParent(c, names, id)).isZero(); // a pending tail hides its rows

            partitions.ensurePremade(c);

            assertThat(partitions.overflow()).isTrue();
            assertThat(countThroughParent(c, names, id)).isEqualTo(1);
            assertThat(queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)",
                            names.tail()))
                    .isEqualTo("false");
            assertThat(tail(c, names)).isEqualTo(lower);
        }
    }

    @Test
    void standaloneTailWithRowsIsReattached() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(6000);

        try (var c = TestDb.connect()) {
            long lower = tail(c, names);
            var id = insertIntoTail(c, names);
            execute(c, "alter table " + names.base() + " detach partition " + names.tail());

            partitions.ensurePremade(c);

            assertThat(partitions.overflow()).isTrue();
            assertThat(countThroughParent(c, names, id)).isEqualTo(1);
            assertTailAboveLastRange(c, names);
            assertThat(tail(c, names)).isEqualTo(lower);
        }
    }

    @Test
    void missingTailIsAttachedAboveLastRange() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            execute(c, "alter table " + names.base() + " detach partition " + names.tail());
            execute(c, "drop table " + names.tail());

            partitions.ensurePremade(c);

            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
        }
    }

    private static Properties login(String role) {
        var login = new Properties();
        login.setProperty("user", role);
        login.setProperty("password", role);
        return login;
    }

    /** As a maintenance run: a failed step is logged, and the next run retries it. */
    private static void ensurePremadeAsMaintenance(XidPartitions partitions, Connection c) {
        try {
            partitions.ensurePremade(c, Duration.ofMillis(300));
        } catch (SQLException e) {
            System.err.println("ensurePremade failed, as a maintenance step would: " + e);
        }
    }

    private static String detachPending(Connection c, String table) throws SQLException {
        return queryString(c, "select inhdetachpending::text from pg_inherits where inhrelid = to_regclass(?)", table);
    }

    private static long currentXid(Connection c) throws SQLException {
        return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
    }

    private static UUID insertIntoTail(Connection c, TableNames names) throws SQLException {
        var id = UUID.randomUUID();
        try (var ps = c.prepareStatement("insert into " + names.base()
                + "(shard, message_id, topic) values (0, ?, 't') returning tableoid::regclass::text")) {
            ps.setObject(1, id);
            try (var rs = ps.executeQuery()) {
                rs.next();
                assertThat(rs.getString(1)).isEqualTo(names.tail());
            }
        }
        return id;
    }

    private static long countThroughParent(Connection c, TableNames names, UUID id) throws SQLException {
        try (var ps = c.prepareStatement("select count(*) from " + names.base() + " where message_id = ?")) {
            ps.setObject(1, id);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private TableNames bootstrap(TestOutboxConfig config) throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        return schema.names();
    }

    private static long tail(Connection c, TableNames names) throws SQLException {
        return PartitionDdl.listRanges(c, names.base()).stream()
                .filter(b -> b.to() == null)
                .mapToLong(b -> Long.parseLong(b.from()))
                .findFirst()
                .orElseThrow();
    }

    private static void assertTailAboveLastRange(Connection c, TableNames names) throws SQLException {
        var ranges = PartitionDdl.listRanges(c, names.base());
        long lastUpper = ranges.stream()
                .map(Bound::to)
                .filter(to -> to != null)
                .mapToLong(Long::parseLong)
                .max()
                .orElseThrow();
        assertThat(tail(c, names)).isEqualTo(lastUpper);
        assertThat(ranges).filteredOn(b -> b.to() == null).singleElement()
                .satisfies(tail -> assertThat(tail.name()).isEqualTo(names.tail()));
    }
}
