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
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
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
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

class XidPartitionsIT {

    private static final Duration SWAP_LOCK_TIMEOUT = Duration.ofMillis(200);

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
            assertThat(tail(c, names)).isEqualTo(tailBefore);
            assertThat(boundCheck(c, names, names.tail())).as("its validate scan is left to maintenance").isNull();
        }
    }

    @Test
    void hotTailIsConvertedIntoARange() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config); // premake 4: the tail is near
        int premake = new OutboxConfig.PartitionsConfig() {}.premake(); // the library default, for the conversion
        var partitions = new XidPartitions(names, new TestOutboxConfig().xidsPerPartition(1000).premake(premake)
                .swapLockTimeout(SWAP_LOCK_TIMEOUT).partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 200_000);
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
            assertThat(Long.parseLong(former.to())).as("U, premake / 2 ranges above the current xid's")
                    .isEqualTo((current / 1000 + premake / 2) * 1000);
            assertThat(queryLong(c, "select count(*) from " + former.name())).isEqualTo(200_000);
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isZero();
            assertThat(partitions.premadeAhead(c)).isGreaterThanOrEqualTo(premake);
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
            // under ACCESS EXCLUSIVE on the parent: no scan of the rows, no index built on them
            assertThat(attach).contains("partition constraint for table \"" + names.unqualified(former.name())
                            + "\" is implied by existing constraints")
                    .noneMatch(m -> m.startsWith("verifying table") || m.startsWith("building index"));
            assertThat(log).contains("swapped the partition tail of " + names.base() + ": attached it with its rows as "
                    + former.name() + " [" + lower + ", " + former.to() + ")");
        }
    }

    @Test
    void hotTailOfASchemaQualifiedOutboxIsConverted() throws Exception {
        var config = new TestOutboxConfig().table("public." + TestDb.uniqueTable()).xidsPerPartition(1000).premake(4)
                .swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);

            partitions.ensurePremade(c);

            var former = formerTail(c, names, lower);
            assertThat(queryLong(c, "select count(*) from " + former.name())).isEqualTo(10);
            // a constraint name takes no schema: "public.<tail>_bounded" would fail to parse
            assertThat(boundCheck(c, names, former.name())).as("the check, by its unqualified name").isNotNull();
            assertThat(partitions.overflow()).isFalse();
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void swapThatFindsTheXidAtTheBoundDropsTheCheckWithAnError() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(Duration.ofSeconds(10));
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);
            holder.setAutoCommit(false); // a young lock on the parent alone: the swap waits for it, the check goes on
            execute(holder, "lock table only " + names.base() + " in access share mode");
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var meanwhile = new Thread(() -> { // while the swap waits, the xid passes U, 2 ranges above the current one
                try (var w = TestDb.connect()) {
                    while (queryLong(w, "select count(*) from pg_locks where relation = to_regclass('" + names.base()
                            + "') and mode = 'AccessExclusiveLock' and not granted") == 0) {
                        Thread.sleep(5);
                    }
                    TestDb.burnXids(2000);
                    holder.commit();
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            meanwhile.start();

            String log;
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c);
                log = err.text();
            }

            meanwhile.join();
            assertThat(errors).isEmpty();
            assertThat(log).contains("ERROR").contains("was not swapped").contains("may have failed");
            assertThat(boundCheck(c, names, names.tail())).as("dropped").isNull();
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(partitions.overflow()).isTrue();
            assertThat(swapLocks(c, names)).as("the quiesce lock released").isZero();

            partitions.ensurePremade(c); // a check higher up, then the swap

            assertThat(queryLong(c, "select count(*) from " + formerTail(c, names, lower).name())).isEqualTo(10);
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
        var relay = new OutboxRelay(db, config, schema, handler, new OutboxMetrics(null), null, maintenance);

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
    void staleBoundIsReplaced() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());
        var done = new AtomicBoolean();
        var errors = new ConcurrentLinkedQueue<Throwable>();
        var writer = new Thread(() -> { // routed to the tail: one at or above a check's U would fail
            try (var c = TestDb.connect()) {
                while (!done.get()) {
                    execute(c, "insert into " + names.base()
                            + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
                    Thread.sleep(1);
                }
            } catch (Throwable e) {
                errors.add(e);
            }
        });

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);
            long stale = (currentXid(c) / 1000 + 1) * 1000; // one range above the current xid
            execute(c, "alter table " + names.tail() + " add constraint " + names.unqualified(names.tail())
                    + "_bounded check (xid >= '" + lower + "' and xid < '" + stale + "') not valid");
            TestDb.burnXids((int) Math.max(0, stale - 500 - currentXid(c)) + 1); // within half a range of it
            writer.start();

            String log;
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c);
                log = err.text();
            } finally {
                done.set(true);
                writer.join();
            }

            assertThat(errors).isEmpty();
            assertThat(log).contains("replaced check");
            assertThat(Long.parseLong(formerTail(c, names, lower).to())).isEqualTo(stale + 1000);
            assertTailAboveLastRange(c, names);
        }
    }

    @Test
    void swapRollsBackWhenTheXidPassedTheBound() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);
            long passed = lower + 1000; // above every row, below the current xid: all the swap needs but that
            TestDb.burnXids((int) (passed - currentXid(c)) + 10);
            String check = names.unqualified(names.tail()) + "_bounded";
            execute(c, "alter table " + names.tail() + " add constraint " + check
                    + " check (xid >= '" + lower + "' and xid < '" + passed + "') not valid");
            execute(c, "alter table " + names.tail() + " validate constraint " + check);
            for (long from = passed; from < passed + 4000; from += 1000) {
                PartitionDdl.create(c, names.base(), names.partitionPrefix() + from, PartitionDdl.CREATE_LOCK_TIMEOUT);
            }
            PartitionDdl.create(c, names.base(), names.tailNext(), PartitionDdl.CREATE_LOCK_TIMEOUT);
            var before = PartitionDdl.listRanges(c, names.base());

            assertThat(partitions.swap(c, lower, passed, passed + 4000)).isFalse();

            assertThat(c.getAutoCommit()).isTrue();
            assertThat(PartitionDdl.listRanges(c, names.base())).containsExactlyInAnyOrderElementsOf(before);
            assertThat(detachPending(c, names.tail())).as("still attached").isEqualTo("false");
            assertThat(queryLong(c, "select count(*) from " + names.tail())).isEqualTo(10);
            assertThat(boundCheck(c, names, names.tail())).isNotNull();
            assertThat(PartitionDdl.attachment(c, names.base(), names.tailNext())).isEqualTo(Attachment.STANDALONE);
        }
    }

    @Test
    void failedConversionKeepsItsCheckForTheNextRun() throws Exception {
        var config = new TestOutboxConfig().xidsPerPartition(1000).premake(4).swapLockTimeout(SWAP_LOCK_TIMEOUT);
        var names = bootstrap(config);
        var partitions = new XidPartitions(names, config.partitions());

        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            long lower = overflowTail(c, names, 10);
            holder.setAutoCommit(false); // a young lock on the parent alone: the check goes on, the swap gives up
            execute(holder, "lock table only " + names.base() + " in access share mode");

            assertThatThrownBy(() -> partitions.ensurePremade(c))
                    .isInstanceOfSatisfying(SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("55P03"));

            holder.commit();
            var check = boundCheck(c, names, names.tail());
            assertThat(check).isNotNull();
            assertThat(tail(c, names)).isEqualTo(lower);
            assertThat(partitions.overflow()).isTrue();
            String log;
            try (var err = new CapturedStderr()) {
                partitions.ensurePremade(c);
                log = err.text();
            }

            var former = formerTail(c, names, lower);
            assertThat(boundCheck(c, names, former.name())).as("the same check, kept").isEqualTo(check);
            assertThat(log).doesNotContain("added check").doesNotContain("replaced check");
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
            partitions.inSwap = () -> { // an OutOfMemoryError, say: neither an SQLException nor a RuntimeException
                throw new Error("mid-swap");
            };

            assertThatThrownBy(() -> partitions.ensurePremade(c)).isExactlyInstanceOf(Error.class);

            assertThat(c.getAutoCommit()).isTrue();
            assertThat(swapLocks(c, names)).as("the quiesce lock released, not left to the pool").isZero();
            assertThat(PartitionDdl.listRanges(c, names.base())).as("the detach rolled back, nothing committed")
                    .containsExactlyInAnyOrderElementsOf(before);
            assertThat(detachPending(c, names.tail())).as("still attached").isEqualTo("false");
            partitions.inSwap = null;
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

    /** The oid and definition of {@code table}'s bound check, or null. */
    @Nullable
    private static String boundCheck(Connection c, TableNames names, String table) throws SQLException {
        try (var ps = c.prepareStatement("select oid || ' ' || pg_get_constraintdef(oid) from pg_constraint"
                + " where conrelid = to_regclass(?) and conname = ?")) {
            ps.setString(1, table);
            ps.setString(2, names.unqualified(names.tail()) + "_bounded");
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
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
