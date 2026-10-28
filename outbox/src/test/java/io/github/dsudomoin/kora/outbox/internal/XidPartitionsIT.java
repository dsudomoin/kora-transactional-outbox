package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.OutboxRelay;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Attachment;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.testing.CapturedStderr;
import io.github.dsudomoin.kora.outbox.testing.RecordingHandler;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

class XidPartitionsIT {

    private static final Duration SWAP_LOCK_TIMEOUT = Duration.ZERO; // the default: no wait

    private final JdbcDatabase db = TestDb.newDatabase(4);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void extendsWhenHalfOfPremadeConsumed() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
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
        var config = new TestOutboxConfig().xidsPerPartition(10_000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        var done = new AtomicBoolean();
        var maxLatency = new AtomicLong();
        var sawPending = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writers = new ArrayList<Thread>();
        var sampler = new Thread(() -> { // a pending tail would take no rows: inserts above the last range fail
            try (var c = TestDb.connect(); var ps = c.prepareStatement("select coalesce(bool_or(inhdetachpending),"
                    + " false) from pg_inherits where inhparent = to_regclass(?)")) {
                ps.setString(1, names.base());
                while (!done.get()) {
                    try (var rs = ps.executeQuery()) {
                        rs.next();
                        if (rs.getBoolean(1)) {
                            sawPending.set(true);
                        }
                    }
                    Thread.sleep(5);
                }
            } catch (Throwable e) {
                errors.add(e);
            }
        });
        sampler.start();
        writers.add(sampler);
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
                // condition-based, not time-boxed: two extensions (tail swapped for ranges and a tail higher up)
                // while the writers insert, however slow a loaded machine is
                while (extensions < 2) {
                    assertThat(System.nanoTime()).as("two extensions within 60 s").isLessThan(cap);
                    TestDb.burnXids(3000);
                    ensurePremadeAsMaintenance(partitions, c); // flat-out writers may leave no moment: next run
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
            assertThat(sawPending).as("the tail or a range seen detach-pending").isFalse();
            // an insert waits at most for the swap's lock wait plus the milliseconds it holds the lock; the second
            // leaves room for a saturated machine
            assertThat(Duration.ofNanos(maxLatency.get())).isLessThan(SWAP_LOCK_TIMEOUT.plusSeconds(1));
            assertThat(tail(c, names)).isGreaterThan(initialTail);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isZero();
            assertThat(queryLong(c, "select count(*) from " + names.base())).isPositive();
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void shortWriterTransactionsNeverBlockTheSwap() throws Exception {
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
                var blocker = partitions.swapBlocker(c);
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
    void anotherRolesShortTransactionsDoNotDeferTheSwap() throws Exception {
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
                var blocker = partitions.swapBlocker(c);
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

            var blocker = partitions.swapBlocker(c);

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

            var blocker = partitions.swapBlocker(c);

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
    void bootstrapLeavesATailWithRowsAttachedAndFlagsOverflow() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var atStartup = XidPartitions.forBootstrap(names, config.partitions());
        TestDb.burnXids(6000);

        try (var c = TestDb.connect()) {
            long tailBefore = tail(c, names);
            insertIntoTail(c, names);

            String log;
            try (var err = new CapturedStderr()) {
                atStartup.ensurePremade(c, Duration.ofSeconds(5), Duration.ofSeconds(5));
                log = err.text();
            }

            assertThat(atStartup.overflow()).isTrue();
            assertThat(log).contains("WARN").contains("rows went to the partition tail " + names.tail())
                    .contains("the next extension turns it into a range");
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(1);
            assertThat(tail(c, names)).as("its scan is left to maintenance").isEqualTo(tailBefore);
            assertNoCheck(c, names);
        }
    }

    /**
     * A tail holding rows, within maxTailScan, becomes the range [H, U) in one swap, U two ranges above the current
     * xid's: Postgres scans it under the swap's lock, as no check on it may ever refuse a row, and keeps its indexes.
     */
    @Test
    void hotTailIsConvertedIntoARangeByTheAttachScan() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 20_000);
            long current = currentXid(c);
            var attach = new ArrayList<String>();
            partitions.attachWarnings = w -> {
                for (; w != null; w = w.getNextWarning()) {
                    attach.add(w.getMessage());
                }
            };
            execute(c, "set client_min_messages = debug1"); // as the statements' warnings: what the attach did

            String log;
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c);
                log = err.text();
            }

            execute(c, "reset client_min_messages");
            var former = formerTail(c, names, lower);
            assertThat(Long.parseLong(former.to())).as("U, two ranges above the current xid's")
                    .isEqualTo((current / 1000 + 2) * 1000);
            assertThat(queryLong(c, "select count(*) from " + former.name())).isEqualTo(20_000);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isZero();
            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(config.premake());
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
            assertNoCheck(c, names);
            // scanned under the lock, but no index built on the rows: the tail keeps its own
            assertThat(attach).contains("verifying table \"" + names.unqualified(former.name()) + "\"")
                    .noneMatch(m -> m.startsWith("building index"));
            assertThat(log).contains("swapped the partition tail of " + names.base() + ": attached it with its rows as "
                    + former.name() + " [" + lower + ", " + former.to() + ")");
        }
    }

    @Test
    void hotTailOfASchemaQualifiedOutboxIsConverted() throws Exception {
        var config = new TestOutboxConfig().table("public." + TestDb.uniqueTable()).xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);

            partitions.ensurePremade(c);

            var former = formerTail(c, names, lower);
            assertThat(queryLong(c, "select count(*) from " + former.name())).isEqualTo(10);
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
            assertNoCheck(c, names);
        }
    }

    /**
     * No path of the extension leaves a check on a partition, so none can refuse a business insert: an empty tail
     * extended, a hot empty one, a hot one with rows, one over maxTailScan, a swap that fails on a young lock holder
     * and one deferred by it once old. Then, with every swap failing, xids are burnt far past any bound a check could
     * have had, and every insert succeeds.
     */
    @Test
    void noPathLeavesACheckThatCouldRefuseAnInsert() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));
        var small = new XidPartitions(names, new TestOutboxConfig().xidsPerPartition(1000).premake(4).maxTailScan(0)
                .partitions());

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            TestDb.burnXids(2500); // cold: the empty tail swapped for ranges
            ensurePremadeAsMaintenance(partitions, c);
            assertNoCheck(c, names);
            TestDb.burnXids((int) (tail(c, names) - currentXid(c)) - 500); // hot, empty: within a range of the tail
            ensurePremadeAsMaintenance(partitions, c);
            assertNoCheck(c, names);
            overflowTail(c, names, 10); // hot with rows
            ensurePremadeAsMaintenance(partitions, c);
            assertNoCheck(c, names);
            long lower = overflowTail(c, names, 10); // over maxTailScan
            ensurePremadeAsMaintenance(small, c);
            assertThat(tail(c, names)).isEqualTo(lower);
            assertNoCheck(c, names);
            holder.setAutoCommit(false); // a young lock on the parent: the swap fails on it
            execute(holder, "lock table only " + names.base() + " in access share mode");
            ensurePremadeAsMaintenance(partitions, c);
            assertNoCheck(c, names);
            Thread.sleep(1100); // now old: the swap is deferred
            ensurePremadeAsMaintenance(partitions, c);
            assertNoCheck(c, names);

            var failed = new ArrayList<SQLException>();
            for (int i = 0; i < 20; i++) { // 10 ranges past the tail, no swap all along
                TestDb.burnXids(500);
                try {
                    send(c, names, 1);
                } catch (SQLException e) {
                    failed.add(e);
                }
                ensurePremadeAsMaintenance(partitions, c);
                assertNoCheck(c, names);
            }

            assertThat(failed).as("failed inserts").isEmpty();
            assertThat(tail(c, names)).as("never swapped").isEqualTo(lower);
            holder.commit();
        }
    }

    /**
     * A tail over maxTailScan stays hot: no lock is asked for (a writer routed to the tail stays open across the run,
     * which a lock on the tail would wait for), writes go on into it, and an ERROR at most once an hour names the size
     * and the admin call that converts it.
     */
    @Test
    void tailOverMaxTailScanStaysHotWithAnErrorAndWritesGoOn() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(Duration.ofSeconds(5));
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, new TestOutboxConfig().xidsPerPartition(1000).premake(4)
                .swapLockTimeout(Duration.ofSeconds(5)).maxTailScan(64 * 1024).partitions());

        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10_000);
            writer.setAutoCommit(false);
            insertIntoTail(writer, names);

            String log;
            long start = System.nanoTime();
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c);
                partitions.ensurePremade(c);
                log = err.text();
            }

            assertThat(Duration.ofNanos(System.nanoTime() - start)).as("no lock waited for")
                    .isLessThan(Duration.ofSeconds(2));
            writer.commit();
            assertThat(log.split("over outbox.partitions.maxTailScan", -1)).as("one ERROR an hour").hasSize(2);
            assertThat(log).contains("ERROR").contains(names.tail());
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(partitions.overflow()).isTrue();
            assertNoCheck(c, names);
            send(c, names, 10);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(10_011);
        }
    }

    /**
     * A tail over maxTailScan is converted only inside {@code tailConversion.window}, by the same swap, its scan cut at
     * maxStall: at noon it stays hot with the ERROR naming the window, at 04:00 it becomes a range.
     */
    @Test
    void oversizedHotTailIsConvertedOnlyInsideTheWindow() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).maxTailScan(0)
                .tailConversion("03:00-05:00", Duration.ofSeconds(5));
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 1000);
            partitions.clock = at(12, 0);

            String log;
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c);
                log = err.text();
            }

            assertThat(tail(c, names)).as("outside the window: still hot").isEqualTo(lower);
            assertThat(partitions.overflow()).isTrue();
            assertThat(log).contains("ERROR").contains("over outbox.partitions.maxTailScan")
                    .contains("outbox.partitions.tailConversion.window 03:00-05:00").contains("maxStall 5000 ms");
            partitions.clock = at(4, 0);

            partitions.ensurePremade(c);

            assertThat(queryLong(c, "select count(*) from " + formerTail(c, names, lower).name())).isEqualTo(1000);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isZero();
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
            assertNoCheck(c, names);
        }
    }

    /**
     * Inside the window, a scan longer than maxStall rolls the swap back: the tail stays attached and hot, nothing of
     * the attempt stays attached or pending, a WARN names the size and maxStall, and every insert meanwhile succeeds.
     */
    @Test
    void aScanLongerThanMaxStallRollsBackCleanly() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).maxTailScan(0)
                .tailConversion("23:00-01:00", Duration.ofMillis(1));
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        partitions.clock = at(0, 30);
        var done = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var inserts = new AtomicLong();
        var slowest = new AtomicLong();
        var writer = new Thread(() -> {
            try (var w = TestDb.connect()) {
                while (!done.get()) {
                    long start = System.nanoTime();
                    send(w, names, 1);
                    slowest.accumulateAndGet(System.nanoTime() - start, Math::max);
                    inserts.incrementAndGet();
                }
            } catch (Throwable e) {
                errors.add(e);
            }
        });

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 600_000);
            var before = PartitionDdl.listRanges(c, names.base());
            writer.start();
            while (inserts.get() == 0 && errors.isEmpty()) {
                Thread.sleep(5);
            }

            String log;
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c);
                log = err.text();
            } finally {
                done.set(true);
                writer.join();
            }

            assertThat(errors).as("failed inserts").isEmpty();
            assertThat(inserts.get()).isPositive();
            // the statement was cut at maxStall past its try; the margin covers a loaded machine
            assertThat(Duration.ofNanos(slowest.get())).as("the slowest insert, against maxStall + the hold")
                    .isLessThan(Duration.ofMillis(1).plusMillis(250));
            assertThat(log).contains("WARN").contains(names.tail()).contains(" MB").contains("maxStall 1 ms");
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(PartitionDdl.listRanges(c, names.base())).as("the same partitions attached")
                    .containsExactlyInAnyOrderElementsOf(before);
            assertThat(detachPending(c, names.tail())).as("still attached").isEqualTo("false");
            assertThat(partitions.overflow()).isTrue();
            assertThat(swapLocks(c, names)).as("the quiesce lock released").isZero();
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(600_000 + inserts.get());
        }
    }

    /**
     * A session left idle in transaction after its insert, for the whole test: every swap is deferred, the ranges
     * run out and rows go to the tail; no insert fails or waits on the library, and the ERROR names the holder. Once it
     * ends, the next run turns the tail into a range.
     */
    @Test
    void aLeakedHolderNeverFailsAnInsertAndTheTailHealsAfterIt() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false);
            send(holder, names, 1);
            Thread.sleep(1100);
            long lower = tail(c, names);
            long slowest = 0;
            var failed = new ArrayList<SQLException>();

            String log;
            try (var err = new CapturedStderr()) {
                for (int i = 0; i < 12; i++) {
                    TestDb.burnXids(1000);
                    long start = System.nanoTime();
                    try {
                        send(c, names, 1);
                    } catch (SQLException e) {
                        failed.add(e);
                    }
                    slowest = Math.max(slowest, System.nanoTime() - start);
                    ensurePremadeAsMaintenance(partitions, c);
                }
                log = err.text();
            }

            assertThat(failed).as("failed inserts").isEmpty();
            assertThat(Duration.ofNanos(slowest)).as("the slowest insert").isLessThan(Duration.ofMillis(200));
            assertThat(tail(c, names)).as("never swapped").isEqualTo(lower);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isPositive();
            assertThat(partitions.overflow()).isTrue();
            assertThat(log).contains("ERROR").contains("outbox partition tail swap of " + names.base())
                    .contains("holds RowExclusiveLock on " + names.base());
            holder.commit();

            partitions.ensurePremade(c);

            assertThat(queryLong(c, "select count(*) from " + formerTail(c, names, lower).name())).isPositive();
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isZero();
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void convertedTailRowsAreDelivered() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).xidsPerPartition(1000).premake(4);
        var schema = new OutboxSchema(db, config);
        schema.init();
        var names = schema.names();
        var partitions = new XidPartitions(names, config.partitions());
        var handler = new RecordingHandler();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(null));
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance,
                new DeadLetterPass(db, schema.names(), config, null, new OutboxMetrics(null)));

        try (var c = TestDb.connect()) {
            long lower = tail(c, names);
            TestDb.burnXids((int) (lower - currentXid(c)) + 10);
            var sent = new ArrayList<>(send(c, names, 100)); // to the tail, relayed before the swap
            relay.drain();
            sent.addAll(send(c, names, 100)); // to the tail, relayed after it

            partitions.ensurePremade(c);

            formerTail(c, names, lower);
            sent.addAll(send(c, names, 100)); // to the former tail, a range now
            relay.drain();
            assertThat(handler.delivered()).extracting(OutboxRecord::messageId)
                    .as("each once").containsExactlyInAnyOrderElementsOf(sent);
        } finally {
            relay.release();
        }
    }

    @Test
    void swapRollsBackWhenTheXidPassedTheBound() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);
            long passed = lower + 1000; // above every row, below the current xid: all the swap needs but that
            TestDb.burnXids((int) (passed - currentXid(c)) + 10);
            for (long from = passed; from < passed + 4000; from += 1000) {
                PartitionDdl.create(c, names.base(), names.partitionPrefix() + from, PartitionDdl.CREATE_LOCK_TIMEOUT);
            }
            PartitionDdl.create(c, names.base(), names.tailNext(), PartitionDdl.CREATE_LOCK_TIMEOUT);
            var before = PartitionDdl.listRanges(c, names.base());

            assertThat(partitions.swap(c, lower, passed, passed + 4000, Duration.ofSeconds(5), Long.MAX_VALUE))
                    .contains("the current xid passed");

            assertThat(c.getAutoCommit()).isTrue();
            assertThat(PartitionDdl.listRanges(c, names.base())).containsExactlyInAnyOrderElementsOf(before);
            assertThat(detachPending(c, names.tail())).as("still attached").isEqualTo("false");
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(10);
            assertThat(PartitionDdl.attachment(c, names.base(), names.tailNext())).isEqualTo(Attachment.STANDALONE);
        }
    }

    @Test
    void failedConversionLeavesNoCheckAndTheNextRunConverts() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);
            holder.setAutoCommit(false); // a young lock on the parent alone: the swap gives up on it
            execute(holder, "lock table only " + names.base() + " in access share mode");

            assertThatThrownBy(() -> partitions.ensurePremade(c))
                    .isInstanceOfSatisfying(SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("55P03"));

            holder.commit();
            assertNoCheck(c, names);
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(partitions.overflow()).isTrue();

            partitions.ensurePremade(c);

            assertThat(queryLong(c, "select count(*) from " + formerTail(c, names, lower).name())).isEqualTo(10);
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void tailThatOverflowedWhileItsSwapWasDeferredIsConverted() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = tail(c, names);
            TestDb.burnXids(2500);
            holder.setAutoCommit(false); // older than the threshold: the swap is deferred after its ranges were made
            queryLong(holder, "select count(*) from " + names.base());
            Thread.sleep(1100);
            partitions.ensurePremade(c, Duration.ofMillis(300), Duration.ofMillis(300));
            assertThat(PartitionDdl.attachment(c, names.base(), names.partitionPrefix() + lower))
                    .as("pre-created at the tail's lower bound, the name the tail takes as a range")
                    .isEqualTo(Attachment.STANDALONE);
            holder.commit();
            overflowTail(c, names, 10);

            partitions.ensurePremade(c);

            assertThat(queryLong(c, "select count(*) from " + formerTail(c, names, lower).name())).isEqualTo(10);
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void noSwapWhenNoRangeIsToBeAdded() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        // xidsPerPartition shrunk since: half of premake used up, yet the ranges reach past premake new ones
        var shrunk = new XidPartitions(names, new TestOutboxConfig().xidsPerPartition(100).premake(4).partitions());
        TestDb.burnXids(2500);

        try (var c = TestDb.connect()) {
            assertThat(shrunk.premadeAhead(c)).isLessThanOrEqualTo(2);
            long oid = queryLong(c, "select to_regclass('" + names.tail() + "')::oid::bigint");

            String log;
            try (var err = new CapturedStderr()) {
                shrunk.ensurePremade(c);
                log = err.text();
            }

            assertThat(log).doesNotContain("swapped the partition tail");
            assertThat(queryLong(c, "select to_regclass('" + names.tail() + "')::oid::bigint")).as("the same tail")
                    .isEqualTo(oid);
            assertThat(PartitionDdl.attachment(c, names.base(), names.tailNext())).isEqualTo(Attachment.MISSING);
        }
    }

    @Test
    void tailNextLeftBeforeAColumnWasAddedIsMadeAnew() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(2500);

        try (var c = TestDb.connect()) {
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            // a failed swap's, then a library version with one more outbox column
            PartitionDdl.create(c, names.base(), names.tailNext(), PartitionDdl.CREATE_LOCK_TIMEOUT);
            execute(c, "alter table " + names.base() + " add column added_later int");

            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
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
    void snapshotsDoNotDeferTheSwap() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1)); // 20 s in production

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false); // a report on other tables: a snapshot, no lock on the outbox
            execute(holder, "set transaction isolation level repeatable read");
            queryLong(holder, "select count(*) from pg_class");
            Thread.sleep(1100); // older than the threshold
            long lower = tail(c, names);
            TestDb.burnXids(2500);
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            assertThat(queryString(c, "select (backend_xmin is not null)::text from pg_stat_activity"
                            + " where pid = ?::int", Long.toString(queryLong(holder, "select pg_backend_pid()"))))
                    .as("the holder keeps its snapshot")
                    .isEqualTo("true");

            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertThat(tail(c, names)).isGreaterThan(lower);
            assertTailAboveLastRange(c, names);
            holder.commit();
        }
    }

    @Test
    void oldLockOnTheParentDefersTheSwapUntilItEnds() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            holder.setAutoCommit(false); // read committed, idle in transaction: no snapshot, but its lock stays
            queryLong(holder, "select count(*) from " + names.base());
            Thread.sleep(1100);
            long lower = tail(c, names);
            TestDb.burnXids(2500);

            String log;
            try (var err = new CapturedStderr()) { // no lock timeout: no swap was tried
                partitions.ensurePremade(c, Duration.ofMillis(300), Duration.ofMillis(300));
                partitions.ensurePremade(c, Duration.ofMillis(300), Duration.ofMillis(300));
                log = err.text();
            }

            assertThat(log.split("deferred to the next run", -1)).as("one WARN a minute").hasSize(2);
            assertThat(log).contains("holds AccessShareLock");
            assertThat(tail(c, names)).isEqualTo(lower);
            holder.commit();
        }
        try (var c = TestDb.connect()) {
            partitions.ensurePremade(c, Duration.ofMillis(300), Duration.ofMillis(300));

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void pgDumpDefersTheSwapAtAnyAge() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
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

            String log;
            try (var err = new CapturedStderr()) {
                ensurePremadeAsMaintenance(partitions, c);
                log = err.text();
            }

            assertThat(log).contains("deferred to the next run").contains("(pg_dump) holds AccessShareLock");
            assertThat(tail(c, names)).isEqualTo(lower);
            holder.commit();
        }
    }

    @Test
    void relayStepHoldersDoNotCountAsBlockers() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var step = TestDb.connect(); var c = TestDb.connect()) {
            step.setAutoCommit(false); // a relay step whose handler runs past the threshold
            queryLong(step, "select count(*) from " + names.base());
            Thread.sleep(1100);
            assertThat(partitions.swapBlocker(c)).as("without the swap lock, an old holder").isNotNull();
            try (var ps = step.prepareStatement("select pg_advisory_xact_lock_shared(?, ?)")) {
                ps.setInt(1, names.internalNamespace());
                ps.setInt(2, TableNames.LOCK_SWAP);
                ps.execute();
            }

            assertThat(partitions.swapBlocker(c)).as("the quiesce waits for it instead").isNull();
            step.commit();
        }
    }

    @Test
    void failedSwapLeavesTheTailAttached() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = tail(c, names);
            TestDb.burnXids(2500);
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            execute(c, "set lock_timeout = '7s'"); // a session value, which the swap must give back
            holder.setAutoCommit(false); // a young reader, so no blocker: the swap waits for it and gives up
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

            long start = System.nanoTime();
            assertThatThrownBy(() -> partitions.ensurePremade(c))
                    .isInstanceOfSatisfying(SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("55P03"));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(SWAP_LOCK_TIMEOUT.plusSeconds(1));
            assertThat(detachPending(c, names.tail())).as("still attached").isEqualTo("false");
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(swapLocks(c, names)).as("the quiesce lock released").isZero();
            assertThat(queryString(c, "show lock_timeout")).as("the session value again").isEqualTo("7s");

            release.join();
            assertThat(errors).isEmpty();
            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
        }
    }

    /**
     * A session holding a lock on a range the extension pre-created fails the swap before it takes the parent's lock:
     * every table the swap attaches is locked first, without waiting, so no writer waits behind a lock wait of the
     * swap's while it holds the parent.
     */
    @Test
    void aLockOnAPreCreatedRangeFailsTheSwapBeforeItHoldsTheParent() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(Duration.ofSeconds(1));
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        var done = new AtomicBoolean();
        var slowest = new AtomicLong();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writer = new Thread(() -> {
            try (var w = TestDb.connect()) {
                while (!done.get()) {
                    long start = System.nanoTime();
                    send(w, names, 1);
                    slowest.accumulateAndGet(System.nanoTime() - start, Math::max);
                    Thread.sleep(2);
                }
            } catch (Throwable e) {
                errors.add(e);
            }
        });

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = tail(c, names);
            TestDb.burnXids(2500);
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            String range = names.partitionPrefix() + lower;
            PartitionDdl.create(c, names.base(), range, PartitionDdl.CREATE_LOCK_TIMEOUT);
            holder.setAutoCommit(false); // a stray reader of the range, as pg_dump or a query by hand
            execute(holder, "lock table " + range + " in access share mode");
            writer.start();
            Thread.sleep(100);

            try {
                assertThatThrownBy(() -> partitions.ensurePremade(c)).isInstanceOfSatisfying(SQLException.class,
                        e -> assertThat(e.getSQLState()).isEqualTo("55P03"));
                Thread.sleep(100);
            } finally {
                done.set(true);
                writer.join();
            }

            assertThat(errors).isEmpty();
            assertThat(Duration.ofNanos(slowest.get())).as("the slowest insert").isLessThan(Duration.ofMillis(500));
            assertThat(tail(c, names)).isEqualTo(lower);
            holder.commit();
            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
        }
    }

    /**
     * The swap's hold is one statement on the server, bounded by its own {@code statement_timeout}: a stall inside it
     * (here a delay where the maintenance JVM could pause between statements) is cut there, the swap rolls back with
     * the tail attached, and a writer waits about that bound, not the stall.
     */
    @Test
    void aStallInsideTheHoldIsCutByTheServerAndWritersGoOn() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        var done = new AtomicBoolean();
        var slowest = new AtomicLong();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writer = new Thread(() -> {
            try (var w = TestDb.connect()) {
                while (!done.get()) {
                    long start = System.nanoTime();
                    send(w, names, 1);
                    slowest.accumulateAndGet(System.nanoTime() - start, Math::max);
                    Thread.sleep(2);
                }
            } catch (Throwable e) {
                errors.add(e);
            }
        });

        try (var c = TestDb.connect()) {
            long lower = tail(c, names);
            TestDb.burnXids(2500);
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            stallInsideTheHold(partitions);
            writer.start();
            Thread.sleep(100);

            try {
                assertThatThrownBy(() -> partitions.ensurePremade(c)).isInstanceOf(SQLException.class);
                Thread.sleep(100);
            } finally {
                done.set(true);
                writer.join();
            }

            assertThat(errors).isEmpty();
            assertThat(Duration.ofNanos(slowest.get())).as("the slowest insert, the hold cut at its bound")
                    .isLessThan(Duration.ofMillis(800));
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(detachPending(c, names.tail())).as("still attached").isEqualTo("false");
            assertThat(swapLocks(c, names)).as("the quiesce lock released").isZero();
            noStallInsideTheHold(partitions);
            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
        }
    }

    /**
     * A swap attaches at most four ranges and the new tail, each made ahead of it with its indexes, which the attach
     * takes over instead of building them under the lock; the extension goes on, a swap a run, until premake ranges
     * lie ahead, the new tail always right after the last range.
     */
    @Test
    void aSwapAttachesAtMostFourRangesWithTheIndexesMadeAheadOfIt() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(16);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = tail(c, names);
            TestDb.burnXids(9000);
            long offset = currentXid(c) % 1000;
            if (offset > 900) {
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            holder.setAutoCommit(false); // old: the first run makes the ranges ahead, then defers
            queryLong(holder, "select count(*) from " + names.base());
            Thread.sleep(1100);
            ensurePremadeAsMaintenance(partitions, c);
            holder.commit();
            String range = names.partitionPrefix() + lower;
            long index = queryLong(c, "select coalesce(min(indexrelid::bigint), 0) from pg_index"
                    + " where indrelid = to_regclass('" + range + "')");
            assertThat(index).as("an index made with the range, ahead of the swap").isPositive();

            String log;
            try (var err = new CapturedStderr()) {
                for (int run = 0; run < 10 && partitions.premadeAhead(c) < config.premake(); run++) {
                    partitions.ensurePremade(c);
                    assertTailAboveLastRange(c, names);
                }
                log = err.text();
            }

            assertThat(queryLong(c, "select min(indexrelid::bigint) from pg_index where indrelid = to_regclass('"
                    + range + "')")).as("the same index, attached").isEqualTo(index);
            assertThat(queryLong(c, "select count(*) from pg_index where indrelid = to_regclass('" + range + "')"))
                    .as("as many indexes as the parent").isEqualTo(queryLong(c, "select count(*) from pg_index"
                            + " where indrelid = to_regclass('" + names.base() + "')"));
            assertThat(queryLong(c, "select count(*) from pg_index i where i.indrelid = to_regclass('" + range
                    + "') and not exists (select 1 from pg_inherits h join pg_index p on p.indexrelid = h.inhparent"
                    + " where h.inhrelid = i.indexrelid and p.indrelid = to_regclass('" + names.base() + "'))"))
                    .as("each index under the parent's").isZero();
            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(config.premake());
            var attached = java.util.regex.Pattern.compile("attached (\\d+) ranges").matcher(log).results()
                    .map(m -> Integer.parseInt(m.group(1))).toList();
            assertThat(attached).as("ranges per swap").hasSizeGreaterThan(1).allSatisfy(n -> assertThat(n)
                    .isBetween(1, 4));
        }
    }

    /**
     * The size gate is read again under the swap's lock: a tail that grew past maxTailScan since is not scanned, the
     * swap is left with nothing changed, and the run counts as one without a swap.
     */
    @Test
    void aTailThatGrewPastTheCapBeforeTheLockIsNotScanned() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).maxTailScan(256 * 1024);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);
            partitions.afterGate = () -> { // after the gate read the tail, rows keep arriving
                try (var w = TestDb.connect()) {
                    execute(w, "insert into " + names.base() + " (shard, message_id, topic, payload) select s % 4,"
                            + " gen_random_uuid(), 't', convert_to(repeat('p', 300), 'UTF8')"
                            + " from generate_series(1, 2000) s");
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            };

            String log;
            try (var err = new CapturedStderr()) {
                ensurePremadeAsMaintenance(partitions, c);
                log = err.text();
            }

            assertThat(tail(c, names)).as("not converted").isEqualTo(lower);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(2010);
            assertThat(partitions.overflow()).isTrue();
            assertThat(log).contains("grew past outbox.partitions.maxTailScan under the swap's lock");
        }
    }

    /**
     * An oversized attempt cut by maxStall is not tried again in the same window on this instance; once the window
     * ends without a fit, one ERROR names the size, maxStall and the advice; the next night tries again.
     */
    @Test
    void anOversizedAttemptCutByMaxStallWaitsForTheNextWindow() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).maxTailScan(0)
                .tailConversion("23:00-01:00", Duration.ofMillis(1));
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            overflowTail(c, names, 600_000);
            partitions.clock = at(0, 30);
            ensurePremadeAsMaintenance(partitions, c);
            assertThat(partitions.oversizedAttempts).as("cut by maxStall").hasValue(1);
            partitions.clock = at(0, 40);
            ensurePremadeAsMaintenance(partitions, c);
            assertThat(partitions.oversizedAttempts).as("not again in this window").hasValue(1);

            String log;
            try (var err = new CapturedStderr()) {
                partitions.clock = at(1, 10);
                ensurePremadeAsMaintenance(partitions, c);
                partitions.clock = at(1, 20);
                ensurePremadeAsMaintenance(partitions, c);
                log = err.text();
            }

            assertThat(log.split("ended without a conversion", -1)).as("one ERROR").hasSize(2);
            assertThat(log).as("the run that says the window ended says nothing more")
                    .doesNotContain("over outbox.partitions.maxTailScan");
            assertThat(log).contains("ERROR").contains(" MB").contains("maxStall 1 ms").contains("raise");
            partitions.clock = at(23, 30);
            ensurePremadeAsMaintenance(partitions, c);
            assertThat(partitions.oversizedAttempts).as("the next night").hasValue(2);
        }
    }

    @Test
    void bootstrapSaysTheTailConversionWindowAndItsZone() throws Exception {
        var config = new TestOutboxConfig().tailConversion("02:00-04:30", Duration.ofMillis(800))
                .tailConversionZone("Asia/Tokyo");

        String log;
        try (var err = new CapturedStderr()) {
            bootstrap(config);
            log = err.text();
        }

        assertThat(log).contains("INFO").contains("tail conversion window 02:00-04:30 in zone Asia/Tokyo")
                .contains("maxStall 800 ms");
    }

    /** The swap's statement as Postgres 17 runs it: an empty tail swapped, then a hot one turned into a range. */
    @Test
    void theSwapRunsOnPostgres17() throws Exception {
        try (var pg17 = new PostgreSQLContainer("postgres:17-alpine")) {
            pg17.start();
            var db17 = TestDb.newDatabase(pg17.getJdbcUrl(), pg17.getUsername(), pg17.getPassword(), 4);
            try {
                var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
                var schema = new OutboxSchema(db17, config);
                schema.init();
                var names = schema.names();
                var partitions = new XidPartitions(names, config.partitions());
                try (var c = DriverManager.getConnection(pg17.getJdbcUrl(), pg17.getUsername(), pg17.getPassword())) {
                    burn(c, 2500);
                    if (currentXid(c) % 1000 > 600) { // its own xids must not carry the counter into the next range
                        burn(c, 500);
                    }
                    long empty = tail(c, names);
                    partitions.ensurePremade(c);
                    assertThat(tail(c, names)).as("the empty tail swapped").isGreaterThan(empty);
                    long hot = tail(c, names);
                    burn(c, (int) (hot - currentXid(c)) + 10);
                    send(c, names, 10);
                    assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(10);

                    partitions.ensurePremade(c);

                    assertThat(queryLong(c, "select count(*) from " + formerTail(c, names, hot).name())).isEqualTo(10);
                    assertThat(queryLong(c, "select count(*) from " + names.tail())).isZero();
                    assertTailAboveLastRange(c, names);
                    assertNoCheck(c, names);
                }
            } finally {
                db17.release();
            }
        }
    }

    private static void burn(Connection c, int xids) throws SQLException {
        for (int i = 0; i < xids; i++) {
            queryLong(c, "select pg_current_xact_id()::text::bigint");
        }
    }

    /**
     * The swap commits locally: Postgres stops the statement's timeout before the commit, and a wait for a
     * synchronous standby ignores cancels, so a standby that never answers would hold the parent's lock for ever.
     * Here the server waits for a standby that does not exist; the swap still ends and a writer still passes.
     */
    @Test
    void theSwapsCommitDoesNotWaitForASynchronousStandby() throws Exception {
        try (var pg = new PostgreSQLContainer("postgres:16-alpine")) {
            pg.start();
            var own = TestDb.newDatabase(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword(), 4);
            var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
            try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
                    var admin = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
                    var w = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
                var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
                var schema = new OutboxSchema(own, config);
                schema.init();
                var names = schema.names();
                var partitions = new XidPartitions(names, config.partitions());
                burn(c, 2500);
                if (currentXid(c) % 1000 > 600) {
                    burn(c, 500);
                }
                long lower = tail(c, names);
                execute(w, "set synchronous_commit = local"); // the writer's own commits: not what this is about
                partitions.beforeSwap = () -> {
                    try {
                        execute(admin, "alter system set synchronous_standby_names = 'ghost'");
                        execute(admin, "select pg_reload_conf()");
                        Thread.sleep(300);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                };
                try {
                    var run = pool.submit(() -> {
                        partitions.ensurePremade(c);
                        return null;
                    });
                    try {
                        run.get(10, TimeUnit.SECONDS);
                    } catch (java.util.concurrent.TimeoutException e) {
                        throw new AssertionError("the swap still waits for the standby after 10 s", e);
                    }
                    long start = System.nanoTime();
                    send(w, names, 1);
                    assertThat(Duration.ofNanos(System.nanoTime() - start)).as("a writer after the swap")
                            .isLessThan(Duration.ofSeconds(1));
                    assertThat(tail(c, names)).as("swapped").isGreaterThan(lower);
                } finally {
                    execute(admin, "alter system reset synchronous_standby_names");
                    execute(admin, "select pg_reload_conf()");
                }
            } finally {
                pool.shutdownNow();
                own.release();
            }
        }
    }

    /**
     * Ranges made ahead of a deferred swap are made again when the parent gained a column since, so the next swap
     * attaches them: nothing is lost and the new column is there on every range.
     */
    @Test
    void aColumnAddedToTheParentReachesTheRangesMadeAhead() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions(), Duration.ofSeconds(1));

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            TestDb.burnXids(2500);
            long offset = currentXid(c) % 1000;
            if (offset > 600) {
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            holder.setAutoCommit(false); // old: the run makes the ranges ahead, then defers
            queryLong(holder, "select count(*) from " + names.base());
            Thread.sleep(1100);
            ensurePremadeAsMaintenance(partitions, c);
            holder.commit();
            assertThat(PartitionDdl.standalone(c, names.base(), "_x[0-9]+")).as("made ahead").isNotEmpty();
            send(c, names, 5);
            execute(c, "alter table " + names.base() + " add column added_later int"); // a newer library version

            partitions.ensurePremade(c);

            send(c, names, 5);
            assertThat(queryLong(c, "select count(*) from " + names.base())).isEqualTo(10);
            assertThat(PartitionDdl.listRanges(c, names.base())).allSatisfy(b -> assertThat(queryLong(c,
                    "select count(*) from pg_attribute where attrelid = '" + b.name() + "'::regclass"
                            + " and attname = 'added_later' and not attisdropped")).as(b.name()).isOne());
            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(config.premake());
            assertTailAboveLastRange(c, names);
        }
    }

    /** Any failure of a due swap counts towards the hourly ERROR, which names its SQLState and message. */
    @Test
    void everySwapFailureCountsTowardsTheError() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            while (partitions.premadeAhead(c) >= config.premake() / 2) { // ranges running out: the ERROR is due
                TestDb.burnXids(200);
            }
            if (currentXid(c) % 1000 > 600) {
                TestDb.burnXids(500);
            }
            partitions.inHold = "raise exception 'boom';";

            String log;
            try (var err = new CapturedStderr()) {
                ensurePremadeAsMaintenance(partitions, c);
                log = err.text();
            }

            assertThat(log).contains("ERROR").contains("outbox partition tail swap of " + names.base())
                    .contains("has not run on 1 due runs in a row").contains("SQLState P0001").contains("boom");
        }
    }

    @Test
    void quiesceWaitsForARelayStepAndTimesOutWithoutALock() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(2500);

        try (var step = TestDb.connect(); var c = TestDb.connect()) {
            long lower = tail(c, names);
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            step.setAutoCommit(false); // a relay step in flight, past the quiesce timeout
            try (var ps = step.prepareStatement("select pg_advisory_xact_lock_shared(?, ?)")) {
                ps.setInt(1, names.internalNamespace());
                ps.setInt(2, TableNames.LOCK_SWAP);
                ps.execute();
            }

            long start = System.nanoTime();
            assertThatThrownBy(() -> partitions.ensurePremade(c, Duration.ofMillis(300), Duration.ofMillis(300)))
                    .isInstanceOfSatisfying(SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("55P03"));

            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(300));
            assertThat(queryLong(c, "select count(*) from pg_locks where locktype = 'advisory' and pid ="
                            + " pg_backend_pid()"))
                    .as("this session holds no swap lock").isZero();
            assertThat(swapLocks(c, names)).as("only the step's shared one").isOne();
            assertThat(tail(c, names)).isEqualTo(lower);
            step.commit();
            partitions.ensurePremade(c, Duration.ofMillis(300), Duration.ofMillis(300));

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void errorMidSwapRollsBackAndReleasesTheSwapLock() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(2500);

        try (var c = TestDb.connect()) {
            long offset = currentXid(c) % 1000;
            if (offset > 900) { // ensurePremade's own DDL xids must not carry the counter into the next range
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
            var before = PartitionDdl.listRanges(c, names.base());
            partitions.inHold = "raise exception 'mid-swap';"; // any failure inside the swap's statement

            assertThatThrownBy(() -> partitions.ensurePremade(c)).isInstanceOf(SQLException.class)
                    .hasMessageContaining("mid-swap");

            assertThat(c.getAutoCommit()).isTrue();
            assertThat(swapLocks(c, names)).as("the quiesce lock released, not left to the pool").isZero();
            assertThat(PartitionDdl.listRanges(c, names.base())).as("the detach rolled back, nothing committed")
                    .containsExactlyInAnyOrderElementsOf(before);
            assertThat(detachPending(c, names.tail())).as("still attached").isEqualTo("false");
            partitions.inHold = null;
            partitions.ensurePremade(c);

            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(4);
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void tailRowCommittedWhileTheSwapWaitsIsNotDropped() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(Duration.ofSeconds(5));
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        TestDb.burnXids(2500);

        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            long lower = tail(c, names);
            // a role or pool default: a snapshot taken before the swap's lock wait would miss the row
            execute(c, "set default_transaction_isolation = 'repeatable read'");
            writer.setAutoCommit(false); // a writer routed to the tail (its xid given here) that commits meanwhile
            var id = insertIntoTail(writer, names, lower + 5);
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

            partitions.ensurePremade(c);

            commitLater.join();
            assertThat(errors).isEmpty();
            assertThat(countThroughParent(c, names, id)).isEqualTo(1);
            assertThat(partitions.overflow()).isTrue();
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(swapLocks(c, names)).as("the quiesce lock released").isZero();
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

                String deferred;
                try (var err = new CapturedStderr()) {
                    ensurePremadeAsMaintenance(partitions, c); // the holder outlives the second sample
                    deferred = err.text();
                }

                assertThat(deferred).contains("deferred to the next run").contains("age hidden");
                assertThat(tail(c, names)).isEqualTo(lower);
                holder.commit();
            }
            assertThat(log).as("said at startup").contains("WARN").contains(role).contains("pg_read_all_stats");
        } finally {
            app.release();
        }
    }

    /**
     * On Postgres 17+ a role or database {@code transaction_timeout} applies to a DDL statement's implicit transaction
     * too: a concurrent build of {@code <dead>_due} outlasting it would be ended and leave the index INVALID, every
     * try. Postgres 16, which every other test runs, has no such setting, so they pin that none is issued there.
     */
    @Test
    void ddlOutlivesARoleTransactionTimeoutOnPostgres17() throws Exception {
        try (var pg17 = new PostgreSQLContainer("postgres:17-alpine")) {
            pg17.start();
            try (var c = DriverManager.getConnection(pg17.getJdbcUrl(), pg17.getUsername(), pg17.getPassword())) {
                execute(c, "set transaction_timeout = '200ms'"); // as a role or database default would

                PartitionDdl.execute(c, Duration.ofSeconds(5), Duration.ZERO, "select pg_sleep(0.5)");

                assertThat(queryString(c, "show transaction_timeout")).as("the session value again").isEqualTo("200ms");
            }
        }
    }

    @Test
    void shortRoleStatementTimeoutDoesNotCutTheSwapShort() throws Exception {
        var role = TestDb.newLoginRole();
        try (var c = TestDb.connect()) {
            execute(c, "alter role " + role + " set statement_timeout = '1s'");
        }
        var app = TestDb.newDatabase(2, role);
        try {
            var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4)
                    .swapLockTimeout(Duration.ofSeconds(3)); // a lock wait longer than the role's statement_timeout
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
                long offset = currentXid(c) % 1000;
                if (offset > 400) { // its DDL and tries (two xids each) must not carry the counter into the next range
                    TestDb.burnXids((int) (1000 - offset) + 10);
                }
                holder.setAutoCommit(false); // a young reader the swap waits over 1 s for
                queryLong(holder, "select count(*) from " + names.base());
                var errors = new ConcurrentLinkedQueue<Throwable>();
                var release = new Thread(() -> {
                    try {
                        Thread.sleep(1500);
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
            // a tail detached by hand: a writer routed to it before the detach inserts into the now standalone
            // table and commits only after the extension looked at it
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
            partitions.ensurePremade(c, Duration.ofMillis(300), Duration.ofMillis(300));
        } catch (SQLException e) {
            System.err.println("ensurePremade failed, as a maintenance step would: " + e);
        }
    }

    /** Sessions holding the swap's advisory lock: a session takes its own again at once, so only this shows a leak. */
    private static long swapLocks(Connection c, TableNames names) throws SQLException {
        return queryLong(c, "select count(*) from pg_locks where locktype = 'advisory' and objsubid = 2 and objid = "
                + TableNames.LOCK_SWAP + " and classid::bigint = " + Integer.toUnsignedLong(names.internalNamespace()));
    }

    /** Rows in the tail, as after maintenance fell behind: the current xid past the tail's lower bound, returned. */
    private static long overflowTail(Connection c, TableNames names, int rows) throws SQLException {
        long lower = tail(c, names);
        TestDb.burnXids((int) Math.max(0, lower - currentXid(c)) + 10);
        for (int done = 0; done < rows; done += 10_000) {
            execute(c, "insert into " + names.base() + " (shard, message_id, topic)"
                    + " select s % 4, gen_random_uuid(), 't' from generate_series(1, " + Math.min(10_000, rows - done)
                    + ") s");
        }
        assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(rows);
        return lower;
    }

    /** The former tail, attached as the range from {@code lower}. */
    private static Bound formerTail(Connection c, TableNames names, long lower) throws SQLException {
        return PartitionDdl.listRanges(c, names.base()).stream()
                .filter(b -> names.unqualified(b.name()).equals(names.unqualified(names.partitionPrefix()) + lower))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no range " + names.partitionPrefix() + lower));
    }

    /** Makes the hold stall for 3 s where the swap could once wait on its own client. */
    private static void stallInsideTheHold(XidPartitions partitions) {
        partitions.inHold = "perform pg_sleep(3);";
    }

    private static void noStallInsideTheHold(XidPartitions partitions) {
        partitions.inHold = null;
    }

    /** A clock at {@code hour:minute} today in the JVM's zone. */
    private static Clock at(int hour, int minute) {
        var zone = ZoneId.systemDefault();
        return Clock.fixed(LocalDate.now(zone).atTime(hour, minute).atZone(zone).toInstant(), zone);
    }

    /**
     * No check on the outbox, any of its partitions, or the tables made ahead to become one (ranges, the next tail):
     * none of the library's, so none that refuses a row.
     */
    private static void assertNoCheck(Connection c, TableNames names) throws SQLException {
        assertThat(queryString(c, "select string_agg(conrelid::regclass || ' ' || pg_get_constraintdef(oid), ', ')"
                + " from pg_constraint where contype = 'c' and (conrelid = to_regclass(?)"
                + " or conrelid in (select inhrelid from pg_inherits where inhparent = to_regclass(?))"
                + " or conrelid in (select oid from pg_class where relkind = 'r'"
                + " and relnamespace = (select relnamespace from pg_class where oid = to_regclass(?))"
                + " and relname ~ ('^' || ? || '_(x[0-9]+|tail|tail_next)$')))",
                names.base(), names.base(), names.base(), names.unqualified(names.base()))).as("checks").isNull();
    }

    /** {@code n} messages over the shards, each in its own autocommit transaction; their ids. */
    private static List<UUID> send(Connection c, TableNames names, int n) throws SQLException {
        var ids = new ArrayList<UUID>();
        try (var ps = c.prepareStatement(
                "insert into " + names.base() + " (shard, message_id, topic) values (?, ?, 't')")) {
            for (int i = 0; i < n; i++) {
                var id = UUID.randomUUID();
                ps.setInt(1, i % 4);
                ps.setObject(2, id);
                ps.executeUpdate();
                ids.add(id);
            }
        }
        return ids;
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

    /** A row routed to the tail by its {@code xid}, as a writer's whose xid reached the tail. */
    private static UUID insertIntoTail(Connection c, TableNames names, long xid) throws SQLException {
        var id = UUID.randomUUID();
        try (var ps = c.prepareStatement("insert into " + names.base() + "(shard, xid, message_id, topic)"
                + " values (0, ?::text::xid8, ?, 't') returning tableoid::regclass::text")) {
            ps.setLong(1, xid);
            ps.setObject(2, id);
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
                .satisfies(tail ->
                        assertThat(names.unqualified(tail.name())).isEqualTo(names.unqualified(names.tail())));
    }
}
