package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.outbox.kora.KoraOutboxTransactions;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlRunnable;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

class PgInboxIT {

    private static final long HOUR = Duration.ofHours(1).toMillis();
    private static final String NOT_IN_TX = "inside JdbcConnectionFactory.inTx";
    private static final String OUTSIDE_WARN = "accepted without dedup";

    private final TestDatabase db = TestDb.newDatabase(20);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final TestOutboxConfig config = new TestOutboxConfig()
            .inboxPartitionInterval(Duration.ofHours(1))
            .inboxRetention(Duration.ofHours(2))
            .inboxPremake(Duration.ofHours(3))
            .maintenanceInterval(Duration.ofMinutes(10)); // never due within a test
    private final ExecutorService pool = Executors.newFixedThreadPool(8);
    private PgInbox inbox;

    @AfterEach
    void release() {
        if (inbox != null) {
            inbox.release();
        }
        pool.shutdownNow();
        db.release();
        Context.clear();
    }

    @Test
    void firstAcceptTrueDuplicateFalse() throws Exception {
        start();
        var id = UuidV7.next();

        assertThat(accept(id)).isTrue();
        assertThat(accept(id)).isFalse();
        try (var c = TestDb.connect()) { // the explicit-connection overload dedups against the same table
            c.setAutoCommit(false);
            assertThat(inbox.tryAccept(c, id)).isFalse();
            assertThat(inbox.tryAccept(c, UuidV7.next())).isTrue();
            c.commit();
        }
        var rolledBack = UuidV7.next();
        assertThatThrownBy(() -> db.inTx((SqlRunnable) () -> {
            assertThat(inbox.tryAccept(rolledBack)).isTrue();
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class).hasMessage("rollback");
        assertThat(accept(rolledBack)).as("a rolled-back accept is forgotten").isTrue();

        assertThat(rows()).isEqualTo(3);
        assertThat(outsideWindow()).isZero();
    }

    @Test
    void requiresTransaction() throws Exception {
        start();
        var id = UuidV7.next();

        assertThatThrownBy(() -> inbox.tryAccept(id))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NOT_IN_TX);
        assertThatThrownBy(() -> db.withConnection((SqlRunnable) () -> inbox.tryAccept(id)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NOT_IN_TX);
        try (var c = TestDb.connect()) {
            assertThatThrownBy(() -> inbox.tryAccept(c, id))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(NOT_IN_TX);
            // the check comes first, also for ids that would be accepted without dedup
            assertThatThrownBy(() -> inbox.tryAccept(c, UUID.randomUUID()))
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThat(rows()).isZero();
    }

    @Test
    void duplicateAcrossConcurrentConsumersOnlyOneWins() throws Exception {
        start();
        for (int round = 0; round < 5; round++) {
            var id = UuidV7.next();
            var barrier = new CyclicBarrier(8);
            var results = new ArrayList<Future<Boolean>>();
            for (int i = 0; i < 8; i++) {
                results.add(pool.submit(() -> db.inTx((SqlFunction0<Boolean>) () -> {
                    await(barrier); // all transactions open, then all insert at once
                    return inbox.tryAccept(id);
                })));
            }
            int accepted = 0;
            for (var r : results) {
                accepted += r.get(30, TimeUnit.SECONDS) ? 1 : 0;
            }
            assertThat(accepted).as("round %d", round).isEqualTo(1);
        }
        assertThat(rows()).isEqualTo(5);
    }

    @Test
    void oldIdOutsideWindowAcceptedWithoutInsert() throws Exception {
        start();
        long now = System.currentTimeMillis();
        var old = v7(now - Duration.ofDays(30).toMillis());
        var future = v7(now + Duration.ofDays(30).toMillis());
        // ids within one interval of the partitions' edges are outside too: the window is conservative
        var belowLowest = v7(lowest() + HOUR / 2);
        var nearTop = v7(highest() - HOUR / 2);

        for (var id : List.of(old, future, belowLowest, nearTop)) {
            assertThat(accept(id)).as("first %s", id).isTrue();
            assertThat(accept(id)).as("again %s", id).isTrue();
        }

        assertThat(rows()).isZero();
        assertThat(outsideWindow()).isEqualTo(8);
    }

    @Test
    void nonV7AcceptedWithoutInsertAndCounted() throws Exception {
        start();
        var v4 = UUID.randomUUID();

        String log;
        try (var stderr = new CapturedStderr()) {
            assertThat(accept(v4)).isTrue();
            assertThat(accept(v4)).isTrue();
            assertThat(accept(new UUID(0, 0))).isTrue();
            log = stderr.text();
        }

        assertThat(rows()).isZero();
        assertThat(outsideWindow()).isEqualTo(3);
        assertThat(log.split(OUTSIDE_WARN, -1)).as("one WARN per minute").hasSize(2);
    }

    @Test
    void acceptAllReturnsFirstTimeIdsOnce() throws Exception {
        start();
        var seen = UuidV7.next();
        assertThat(accept(seen)).isTrue();
        var a = UuidV7.next();
        var b = UuidV7.next();

        assertThat(acceptAll(List.of(seen, a, b, a))).as("new ids, each once").containsExactlyInAnyOrder(a, b);
        assertThat(acceptAll(List.of(a, b))).isEmpty();
        assertThat(accept(a)).as("the single form dedups against the same rows").isFalse();
        try (var c = TestDb.connect()) { // the explicit-connection overload
            c.setAutoCommit(false);
            var d = UuidV7.next();
            assertThat(inbox.tryAcceptAll(c, List.of(b, d, d))).containsExactly(d);
            c.commit();
        }
        var rolledBack = List.of(UuidV7.next(), UuidV7.next());
        assertThatThrownBy(() -> db.inTx((SqlRunnable) () -> {
            assertThat(inbox.tryAcceptAll(rolledBack)).containsExactlyInAnyOrderElementsOf(rolledBack);
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class).hasMessage("rollback");
        assertThat(acceptAll(rolledBack)).as("a rolled-back accept is forgotten")
                .containsExactlyInAnyOrderElementsOf(rolledBack);

        assertThat(rows()).isEqualTo(6);
        assertThat(outsideWindow()).isZero();
    }

    @Test
    void acceptAllTakesOutsideIdsWithoutInsert() throws Exception {
        start();
        long now = System.currentTimeMillis();
        var old = v7(now - Duration.ofDays(30).toMillis());
        var future = v7(now + Duration.ofDays(30).toMillis());
        var v4 = UUID.randomUUID();
        var inWindow = UuidV7.next();

        String log;
        try (var stderr = new CapturedStderr()) {
            assertThat(acceptAll(List.of(old, future, v4, inWindow, v4)))
                    .containsExactlyInAnyOrder(old, future, v4, inWindow);
            assertThat(acceptAll(List.of(old, future, v4, inWindow)))
                    .as("outside ids always, the in-window one deduplicated")
                    .containsExactlyInAnyOrder(old, future, v4);
            log = stderr.text();
        }

        assertThat(rows()).isEqualTo(1);
        assertThat(outsideWindow()).as("once per distinct id and call").isEqualTo(6);
        assertThat(log.split(OUTSIDE_WARN, -1)).as("one WARN per minute").hasSize(2);
    }

    @Test
    void acceptAllSendsNothingWithoutInWindowIds() throws Exception {
        start();
        assertThat(acceptAll(List.of())).isEmpty();
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            // an aborted transaction fails every statement, so a call that succeeds sent none
            assertThatThrownBy(() -> queryLong(c, "select 1 / 0")).isInstanceOf(SQLException.class);
            assertThat(inbox.tryAcceptAll(c, List.of())).isEmpty();
            var v4 = UUID.randomUUID();
            assertThat(inbox.tryAcceptAll(c, List.of(v4, v4))).containsExactly(v4);
            assertThatThrownBy(() -> inbox.tryAcceptAll(c, List.of(UuidV7.next())))
                    .isInstanceOf(RuntimeSqlException.class);
            c.rollback();
        }
        assertThat(rows()).isZero();
    }

    @Test
    void acceptAllRequiresTransaction() throws Exception {
        start();
        var ids = List.of(UuidV7.next());

        for (Collection<UUID> input : List.of(ids, List.<UUID>of())) { // empty input too
            assertThatThrownBy(() -> inbox.tryAcceptAll(input))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(NOT_IN_TX);
            assertThatThrownBy(() -> db.withConnection((SqlRunnable) () -> inbox.tryAcceptAll(input)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(NOT_IN_TX);
            try (var c = TestDb.connect()) {
                assertThatThrownBy(() -> inbox.tryAcceptAll(c, input))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(NOT_IN_TX);
            }
        }
        try (var c = TestDb.connect()) { // also for ids that would be accepted without dedup
            assertThatThrownBy(() -> inbox.tryAcceptAll(c, List.of(UUID.randomUUID())))
                    .isInstanceOf(IllegalStateException.class);
        }
        var withNull = Arrays.asList(UuidV7.next(), null);
        assertThatThrownBy(() -> db.inTx((SqlRunnable) () -> inbox.tryAcceptAll(withNull)))
                .isInstanceOf(NullPointerException.class);
        assertThat(rows()).isZero();
    }

    @Test
    void acceptAllOverlappingConcurrentSetsAcceptEachIdOnce() throws Exception {
        start();
        for (int round = 0; round < 5; round++) {
            var ids = Stream.generate(UuidV7::next).limit(300).toList();
            var first = new ArrayList<>(ids.subList(0, 200));
            var second = new ArrayList<>(ids.subList(100, 300));
            Collections.shuffle(first); // the overlap in different orders
            Collections.shuffle(second);
            var barrier = new CyclicBarrier(2);
            var results = new ArrayList<Future<Set<UUID>>>();
            for (var input : List.of(first, second)) {
                results.add(pool.submit(() -> db.inTx((SqlFunction0<Set<UUID>>) () -> {
                    await(barrier);
                    return inbox.tryAcceptAll(input);
                })));
            }
            var a = results.get(0).get(30, TimeUnit.SECONDS);
            var b = results.get(1).get(30, TimeUnit.SECONDS);
            assertThat(a).as("round %d", round).doesNotContainAnyElementsOf(b);
            var all = new HashSet<>(a);
            all.addAll(b);
            assertThat(all).as("round %d", round).containsExactlyInAnyOrderElementsOf(ids);
        }
        assertThat(rows()).isEqualTo(1500);
    }

    @Test
    void scopesDeduplicateSeparately() throws Exception {
        start();
        var projection = inbox.scope("orders-projection");
        var mailer = inbox.scope("mailer.v2");
        var id = UuidV7.next();

        assertThat(accept(projection, id)).isTrue();
        assertThat(accept(mailer, id)).as("another consumer of the same message").isTrue();
        assertThat(accept(inbox, id)).as("the unscoped consumer is one more").isTrue();
        assertThat(accept(projection, id)).isFalse();
        assertThat(accept(mailer, id)).isFalse();
        assertThat(accept(inbox.scope("mailer.v2"), id)).as("a scope is its name").isFalse();
        try (var c = TestDb.connect()) { // the explicit-connection overload
            c.setAutoCommit(false);
            assertThat(mailer.tryAccept(c, id)).isFalse();
            assertThat(inbox.scope("audit").tryAccept(c, id)).isTrue();
            c.commit();
        }
        assertThat(rows()).isEqualTo(4);
    }

    @Test
    void acceptAllIsScoped() throws Exception {
        start();
        var a = inbox.scope("a");
        var b = inbox.scope("b");
        var ids = List.of(UuidV7.next(), UuidV7.next(), UuidV7.next());

        assertThat(acceptAll(a, ids)).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(acceptAll(b, ids)).as("another consumer").containsExactlyInAnyOrderElementsOf(ids);
        assertThat(acceptAll(a, ids)).isEmpty();
        assertThat(accept(b, ids.get(0))).as("the single and the batch form share a scope").isFalse();
        var fresh = UuidV7.next();
        try (var c = TestDb.connect()) { // the explicit-connection overload
            c.setAutoCommit(false);
            assertThat(b.tryAcceptAll(c, List.of(ids.get(1), fresh))).containsExactly(fresh);
            c.commit();
        }
        assertThat(acceptAll(inbox, ids)).as("the unscoped consumer is one more")
                .containsExactlyInAnyOrderElementsOf(ids);
        assertThat(rows()).isEqualTo(10);
    }

    @Test
    void unscopedIsTheEmptyConsumer() throws Exception {
        start();
        var accepted = UuidV7.next();
        assertThat(accept(accepted)).isTrue();
        var recorded = UuidV7.next();
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select consumer from " + table() + " where message_id = ?::uuid",
                    accepted.toString())).isEmpty();
            try (var ps = c.prepareStatement("insert into " + table() + " (message_id, consumer) values (?, '')")) {
                ps.setObject(1, recorded);
                ps.executeUpdate();
            }
        }

        assertThat(accept(recorded)).isFalse();
        assertThat(acceptAll(inbox, List.of(recorded))).isEmpty();
    }

    @Test
    void rejectsInvalidScope() {
        inbox = new PgInbox(db, config, new OutboxMetrics(null));
        for (var name : List.of("", " ", "a b", "x".repeat(65), "tab\t", "na\u00efve", "a;b", "a'b")) {
            assertThatThrownBy(() -> inbox.scope(name)).as("[%s]", name)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("consumer");
        }
        assertThatThrownBy(() -> inbox.scope(null)).isInstanceOf(NullPointerException.class);
        assertThat(inbox.scope("x".repeat(64))).as("the limit itself").isNotNull();
        assertThat(inbox.scope("Orders_projection-1.v2:eu")).isNotNull();
    }

    @Test
    void tableWithoutConsumerColumnFailsInit() throws Exception {
        try (var c = TestDb.connect()) { // an inbox table made before consumer scopes
            execute(c, "create table " + table() + " (message_id uuid primary key,"
                    + " received_at timestamptz not null default now()) partition by range (message_id)");
        }
        var old = new PgInbox(db, config, new OutboxMetrics(null));

        assertThatThrownBy(old::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(table())
                .hasMessageContaining("consumer");
    }

    @Test
    void transactionPoolingSkipsMaintenanceAndRaisesTheGauge() throws Exception {
        start();
        var pooledRegistry = new SimpleMeterRegistry();
        var sql = new ConcurrentLinkedQueue<String>();
        var pooled = new PgInbox(new KoraOutboxTransactions(TestDb.transactionPooled(db, sql)), config,
                new OutboxMetrics(pooledRegistry));

        try (var err = new CapturedStderr()) {
            pooled.runMaintenance();
            pooled.runMaintenance();
            assertThat(err.text()).contains("ERROR");
            assertThat(err.text().split("transaction-pooling proxy detected", -1)).as("once a minute").hasSize(2);
        }

        assertThat(pooledRegistry.get("outbox.inbox.maintenance.transaction_pooling").gauge().value()).isEqualTo(1);
        assertThat(sql).as("no session lock or setting").isNotEmpty()
                .noneMatch(s -> s.contains("advisory") || s.contains("idle_session_timeout"));
        var id = UuidV7.next(); // the window refresh still ran: ids are deduplicated
        assertThat(accept(pooled, id)).isTrue();
        assertThat(accept(pooled, id)).isFalse();
        inbox.runMaintenance();
        assertThat(registry.get("outbox.inbox.maintenance.transaction_pooling").gauge().value()).isZero();
    }

    @Test
    void shortRoleStatementTimeoutDoesNotCutBootstrapLockWaitShort() throws Exception {
        var role = TestDb.newLoginRole();
        try (var c = TestDb.connect()) {
            execute(c, "alter role " + role + " set statement_timeout = '1s'");
        }
        var app = TestDb.newDatabase(2, role);
        try (var holder = TestDb.connect()) { // a maintenance run elsewhere holds the lock for about 2 s
            int ns = TableNames.of("public." + table()).internalNamespace();
            try (var ps = holder.prepareStatement("select pg_advisory_lock(?, ?)")) {
                ps.setInt(1, ns);
                ps.setInt(2, TableNames.LOCK_INBOX_MAINTENANCE);
                ps.execute();
            }
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var release = new Thread(() -> {
                try (var ps = holder.prepareStatement("select pg_advisory_unlock(?, ?)")) {
                    Thread.sleep(2000);
                    ps.setInt(1, ns);
                    ps.setInt(2, TableNames.LOCK_INBOX_MAINTENANCE);
                    ps.execute();
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            release.start();
            inbox = new PgInbox(app, config, new OutboxMetrics(null));

            inbox.init();

            release.join();
            assertThat(errors).isEmpty();
            var id = UuidV7.next();
            assertThat(app.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id))).isTrue();
            inbox.release();
            inbox = null;
        } finally {
            app.release();
        }
    }

    @Test
    void maintenanceDropsExpiredPartitions() throws Exception {
        long before = System.currentTimeMillis();
        start();
        long after = System.currentTimeMillis();
        long hour = lowest() + 2 * HOUR; // the hour init saw, wherever the clock is now
        var name = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC);
        var current = ranges();
        // fresh: hourly ranges from floor(now - retention) to past now + premake, named after their lower bound
        assertThat(hour).isIn(Math.floorDiv(before, HOUR) * HOUR, Math.floorDiv(after, HOUR) * HOUR);
        assertThat(current).hasSize(6);
        for (var b : current) {
            long from = millis(b.from());
            assertThat(millis(b.to())).isEqualTo(from + HOUR);
            assertThat(b.name()).isEqualTo(table() + "_t" + name.format(Instant.ofEpochMilli(from)));
        }
        assertThat(highest()).isGreaterThan(before + Duration.ofHours(3).toMillis());
        // time passes: partitions of an older range, expired by now - retention
        var expired = List.of(table() + "_old1", table() + "_old2");
        try (var c = TestDb.connect(); var reader = TestDb.connect()) {
            attach(c, expired.get(0), hour - 6 * HOUR);
            attach(c, expired.get(1), hour - 5 * HOUR);
            try (var ps = c.prepareStatement("insert into " + table() + " (message_id) values (?)")) {
                ps.setObject(1, v7(hour - 5 * HOUR + 1)); // expired rows go with their partition
                ps.executeUpdate();
            }
            // an interrupted drop left the upper one detach-pending: the lower one's detach fails (55000) until
            // the pending one is finished
            reader.setAutoCommit(false);
            queryLong(reader, "select count(*) from " + table());
            assertThatThrownBy(() -> PartitionDdl.detach(c, table(), expired.get(1), Duration.ofMillis(300)))
                    .isInstanceOf(SQLException.class);
            reader.commit();
            assertThat(PartitionDdl.pendingDetach(c, table())).isEqualTo(expired.get(1));
        }

        inbox.runMaintenance();

        // an hour boundary passed since init may add one range on top
        assertThat(ranges()).containsAll(current).noneMatch(b -> expired.contains(b.name()));
        try (var c = TestDb.connect()) {
            for (var t : expired) {
                assertThat(queryLong(c, "select count(*) from pg_class where relname = '" + t + "'"))
                        .as(t).isZero();
            }
        }
        var fresh = UuidV7.next();
        assertThat(accept(fresh)).isTrue();
        assertThat(accept(fresh)).isFalse();
    }

    @Test
    void rangeOrphanedBetweenDetachAndDropIsDroppedOnceExpired() throws Exception {
        start();
        long hour = lowest() + 2 * HOUR; // the hour init saw, wherever the clock is now
        var expired = table() + "_t" + DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC)
                .format(Instant.ofEpochMilli(hour - 5 * HOUR));
        var recent = table() + "_t" + DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC)
                .format(Instant.ofEpochMilli(hour + 240 * HOUR));
        try (var c = TestDb.connect()) {
            attach(c, expired, hour - 5 * HOUR);
            try (var ps = c.prepareStatement("insert into " + table() + " (message_id) values (?)")) {
                ps.setObject(1, v7(hour - 5 * HOUR + 1));
                ps.executeUpdate();
            }
            // a run killed after its DETACH went through, before its DROP
            execute(c, "alter table " + table() + " detach partition " + expired + " concurrently");
            // and a detached table whose ids are still in the window: its dedup records sit outside the inbox
            execute(c, "create table " + recent + " (like " + table() + " including defaults)");
            try (var ps = c.prepareStatement("insert into " + recent + " (message_id) values (?)")) {
                ps.setObject(1, UuidV7.next());
                ps.executeUpdate();
            }
        }
        var orphanRegistry = new SimpleMeterRegistry();
        var maintaining = new PgInbox(db, config, new OutboxMetrics(orphanRegistry));

        try (var err = new CapturedStderr()) {
            maintaining.runMaintenance();

            assertThat(err.text()).contains("ERROR").contains(recent);
        }
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from pg_class where relname = '" + expired + "'")).isZero();
            assertThat(queryLong(c, "select count(*) from pg_class where relname = '" + recent + "'")).isOne();
        }
        assertThat(orphanRegistry.get("outbox.inbox.partitions.orphaned").gauge().value()).isEqualTo(1);
    }

    @Test
    void pendingDetachInsideTheWindowIsNeverDropped() throws Exception {
        start();
        var range = ranges().stream() // the range now falls in
                .filter(b -> millis(b.from()) <= System.currentTimeMillis()
                        && System.currentTimeMillis() < millis(b.to()))
                .findFirst()
                .orElseThrow();
        var id = UuidV7.next();
        assertThat(accept(id)).isTrue();
        try (var reader = TestDb.connect(); var operator = TestDb.connect()) {
            reader.setAutoCommit(false);
            queryLong(reader, "select count(*) from " + table());
            execute(operator, "set lock_timeout = '300ms'"); // an operator's detach, cancelled: the range stays pending
            assertThatThrownBy(() -> execute(operator, "alter table " + table() + " detach partition " + range.name()
                            + " concurrently"))
                    .isInstanceOf(SQLException.class);
            reader.commit();
        }
        var orphanRegistry = new SimpleMeterRegistry();
        var maintaining = new PgInbox(db, config, new OutboxMetrics(orphanRegistry));

        try (var err = new CapturedStderr()) {
            maintaining.runMaintenance();

            assertThat(err.text()).doesNotContain("that is not attached");
        }
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + range.name())).as("its dedup records").isOne();
        }
        assertThat(ranges()).as("finished, then attached back").contains(range);
        assertThat(accept(id)).isFalse();
        assertThat(orphanRegistry.get("outbox.inbox.partitions.orphaned").gauge().value()).isZero();
    }

    @Test
    void emptyInWindowOrphanIsReportedAndEveryUnparsableNameSkipped() throws Exception {
        start();
        var hour = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC);
        var empty = table() + "_t" + hour.format(Instant.ofEpochMilli(highest() + 10 * HOUR));
        var feb30 = table() + "_t2026023012";
        var month13 = table() + "_t2026139912";
        try (var c = TestDb.connect()) {
            execute(c, "create table " + empty + " (like " + table() + " including defaults)");
            execute(c, "create table " + feb30 + " (like " + table() + " including defaults)");
            execute(c, "create table " + month13 + " (like " + table() + " including defaults)");
        }
        var orphanRegistry = new SimpleMeterRegistry();
        var maintaining = new PgInbox(db, config, new OutboxMetrics(orphanRegistry));

        try (var err = new CapturedStderr()) {
            maintaining.runMaintenance();

            assertThat(err.text()).contains("ERROR").contains(empty).contains("WARN").contains(feb30)
                    .as("not only the first by name").contains(month13)
                    .as("not attached back: the ranges below it are missing").contains("do not attach it by hand")
                    .as("no statement that would open the gap").doesNotContain("attach partition " + empty);
        }
        assertThat(orphanRegistry.get("outbox.inbox.partitions.orphaned").gauge().value())
                .as("the step completed: the empty one counted, the unparsable ones skipped").isEqualTo(1);
        var hour24 = table() + "_t2026010124";
        try (var c = TestDb.connect()) {
            execute(c, "create table " + hour24 + " (like " + table() + " including defaults)");
        }

        try (var err = new CapturedStderr()) {
            maintaining.runMaintenance();

            assertThat(err.text()).as("a new name at once, the others once a minute").contains(hour24)
                    .doesNotContain(feb30).doesNotContain(month13);
        }
    }

    @Test
    void aRangeLeftStandaloneAtTheTopIsAttachedNotReported() throws Exception {
        start();
        var top = ranges().stream().max(Comparator.comparingLong(b -> millis(b.to()))).orElseThrow();
        try (var c = TestDb.connect()) { // as when premake's attach timed out after its create, or an operator's detach
            execute(c, "alter table " + table() + " detach partition " + top.name());
        }
        var orphanRegistry = new SimpleMeterRegistry();
        var maintaining = new PgInbox(db, config, new OutboxMetrics(orphanRegistry));

        try (var err = new CapturedStderr()) {
            maintaining.runMaintenance();

            assertThat(err.text()).doesNotContain("that is not attached");
        }
        assertThat(ranges()).as("premake attached it back").contains(top);
        assertThat(orphanRegistry.get("outbox.inbox.partitions.orphaned").gauge().value()).isZero();
    }

    @Test
    void orphanInsideTheWindowIsAttachedBack() throws Exception {
        start();
        var id = UuidV7.next();
        assertThat(accept(id)).isTrue();
        var range = rangeOf(UuidV7.timestampMillis(id));
        try (var c = TestDb.connect()) { // an operator's detach
            execute(c, "alter table " + table() + " detach partition " + range.name());
        }
        assertThatThrownBy(() -> accept(id)).as("ranges on both sides of it: no partition for the id")
                .isInstanceOf(RuntimeSqlException.class);

        try (var err = new CapturedStderr()) {
            inbox.runMaintenance();

            assertThat(err.text()).contains(range.name() + ", a range of").doesNotContain("that is not attached");
        }
        assertThat(ranges()).contains(range);
        assertThat(accept(id)).as("its dedup record counts again").isFalse();
        assertThat(registry.get("outbox.inbox.partitions.orphaned").gauge().value()).isZero();
    }

    @Test
    void orphanAwayFromTheAttachedRangesStaysWithItsError() throws Exception {
        start();
        var low = ranges().stream().sorted(Comparator.comparingLong(b -> millis(b.from()))).toList();
        try (var c = TestDb.connect()) { // the second lowest detached, the ranges on both sides of it dropped
            execute(c, "alter table " + table() + " detach partition " + low.get(1).name());
            execute(c, "drop table " + low.get(0).name());
            execute(c, "drop table " + low.get(2).name());
        }

        try (var err = new CapturedStderr()) {
            inbox.runMaintenance();

            assertThat(err.text()).contains("ERROR").contains(low.get(1).name())
                    .contains("do not attach it by hand").doesNotContain("attach partition " + low.get(1).name());
        }
        assertThat(ranges()).doesNotContain(low.get(1));
        assertThat(registry.get("outbox.inbox.partitions.orphaned").gauge().value()).isEqualTo(1);
    }

    @Test
    void attachOrphansOffKeepsTheError() throws Exception {
        config.attachOrphans(false);
        start();
        var range = rangeOf(System.currentTimeMillis());
        var far = table() + "_t" + DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC)
                .format(Instant.ofEpochMilli(highest() + 10 * HOUR));
        try (var c = TestDb.connect()) {
            execute(c, "alter table " + table() + " detach partition " + range.name());
            execute(c, "create table " + far + " (like " + table() + " including defaults)");
        }

        try (var err = new CapturedStderr()) {
            inbox.runMaintenance();

            assertThat(err.text()).contains("ERROR").contains(range.name()).contains("23514")
                    .contains("alter table " + table() + " attach partition " + range.name() + " for values from ('"
                            + range.from() + "') to ('" + range.to() + "')")
                    .as("the statement only where attaching is safe").contains(far)
                    .doesNotContain("attach partition " + far);
        }
        assertThat(ranges()).doesNotContain(range);
        assertThat(registry.get("outbox.inbox.partitions.orphaned").gauge().value()).isEqualTo(2);
    }

    @Test
    void overlappingOrphanStaysWithItsError() throws Exception {
        start();
        var overlapping = rangeOf(System.currentTimeMillis());
        var next = rangeOf(millis(overlapping.to()));
        try (var c = TestDb.connect()) {
            execute(c, "alter table " + table() + " detach partition " + overlapping.name());
            execute(c, "alter table " + table() + " detach partition " + next.name());
            attach(c, table() + "_other", millis(overlapping.from())); // another table took its range meanwhile
        }

        try (var err = new CapturedStderr()) {
            inbox.runMaintenance();

            assertThat(err.text()).contains("ERROR").contains(overlapping.name()).contains("42P17")
                    .contains("would overlap").doesNotContain(next.name() + " is a range")
                    .contains("alter table " + table() + " attach partition " + overlapping.name() + " for values");
        }
        assertThat(ranges()).doesNotContain(overlapping).as("the failed attach does not stop the next").contains(next);
        assertThat(registry.get("outbox.inbox.partitions.orphaned").gauge().value()).isEqualTo(1);
    }

    @Test
    void inWindowOrphanIsReportedWithoutWaitingForItsLock() throws Exception {
        start();
        var hour = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC);
        var orphan = table() + "_t" + hour.format(Instant.ofEpochMilli(highest() + 10 * HOUR));
        var partitions = new InboxPartitions(table(), config);
        try (var holder = TestDb.connect(); var c = TestDb.connect()) {
            execute(c, "create table " + orphan + " (like " + table() + " including defaults)");
            holder.setAutoCommit(false); // e.g. an operator's ATTACH, or a VACUUM FULL, in an open transaction
            execute(holder, "lock table " + orphan + " in access exclusive mode");

            var kept = assertTimeoutPreemptively(Duration.ofSeconds(10),
                    () -> partitions.dropOrphans(c, System.currentTimeMillis(), Duration.ofSeconds(30)));

            assertThat(kept).extracting(InboxPartitions.Orphan::table).as("reported without a wait").contains(orphan);
            holder.rollback();
        }
    }

    @Test
    void idsOlderThanRetentionAreNotDeduplicatedWhilePartitionsRemain() throws Exception {
        start();
        long hour = lowest() + 2 * HOUR; // the hour init saw, wherever the clock is now
        try (var c = TestDb.connect()) { // expired ranges, contiguous with the fresh ones that start at hour - 2h
            attach(c, table() + "_old1", hour - 4 * HOUR);
            attach(c, table() + "_old2", hour - 3 * HOUR);
        }
        // a second instance starts before maintenance ran: its partitions span the expired ranges
        var second = new PgInbox(db, config, new OutboxMetrics(registry));
        second.init();
        var expiredId = v7(hour - 3 * HOUR + HOUR / 2);
        assertThat(accept(second, expiredId)).isTrue();
        assertThat(accept(second, expiredId)).as("not deduplicated: its range may go any moment").isTrue();
        assertThat(rows()).isZero();

        inbox.runMaintenance(); // the first instance drops them; the second one's window is stale now

        assertThat(accept(second, expiredId)).isTrue(); // no insert, so no missing partition
        second.release();
    }

    @Test
    void maintenanceExtendsPremadeAndRefreshesWindow() throws Exception {
        config.inboxPremake(Duration.ofHours(2));
        start();
        var nextTop = v7(highest() + HOUR / 2);
        assertThat(accept(nextTop)).isTrue();
        assertThat(rows()).isZero();
        // premake grows (a config change on restart takes the same path): maintenance extends, the window follows
        inbox.release();
        config.inboxPremake(Duration.ofHours(4));
        inbox = new PgInbox(db, config, new OutboxMetrics(registry));
        long before = System.currentTimeMillis();
        inbox.runMaintenance();

        assertThat(accept(nextTop)).isTrue();
        assertThat(accept(nextTop)).isFalse();
        assertThat(highest()).isGreaterThan(before + Duration.ofHours(4).toMillis());
    }

    @Test
    void initIsIdempotent() throws Exception {
        start();
        var before = ranges();
        var id = UuidV7.next();
        assertThat(accept(id)).isTrue();

        var second = new PgInbox(db, config, new OutboxMetrics(null));
        second.init();
        second.release();

        assertThat(ranges()).containsAll(before);
        assertThat(accept(id)).isFalse();
    }

    @Test
    void rejectsInvalidConfig() {
        var h = Duration.ofHours(1);
        var m = Duration.ofMinutes(1);
        rejected(config(Duration.ofSeconds(59), h.multipliedBy(2), h.multipliedBy(2), m),
                "outbox.inbox.partitionInterval must be at least 1m");
        // the guaranteed dedup span, retention - interval, is at least one interval
        rejected(config(h, Duration.ofMinutes(119), h.multipliedBy(3), m), "outbox.inbox.retention must be at least 2");
        // a whole range above the window's top margin
        rejected(config(h, h.multipliedBy(2), Duration.ofMinutes(119), m), "outbox.inbox.premake must be at least 2");
        // maintenance extends before the window's top reaches now
        rejected(config(h, h.multipliedBy(2), h.multipliedBy(2), h), "outbox.partitions.maintenanceInterval");
        // at most 1000 ranges over retention + premake
        rejected(config(m, Duration.ofMinutes(500), Duration.ofMinutes(501), Duration.ofMinutes(10)),
                "outbox.inbox.partitionInterval must be at least (outbox.inbox.retention + outbox.inbox.premake)"
                        + " / 1000");
        rejected(new TestOutboxConfig().inboxTable("bad-name"), "table name");

        var metrics = new OutboxMetrics(null); // the limits themselves are valid
        new PgInbox(db, config(h, h.multipliedBy(2), h.multipliedBy(2).plusMillis(1), h), metrics);
        new PgInbox(db, config(m, Duration.ofMinutes(500), Duration.ofMinutes(500), Duration.ofMinutes(10)), metrics);
    }

    @Test
    void subHourIntervalsNameMinutes() throws Exception {
        config.inboxPartitionInterval(Duration.ofMinutes(30))
                .inboxRetention(Duration.ofHours(1))
                .inboxPremake(Duration.ofHours(1));
        start();

        var name = DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);
        var current = ranges();
        assertThat(current).isNotEmpty().allSatisfy(b -> assertThat(b.name())
                .isEqualTo(table() + "_t" + name.format(Instant.ofEpochMilli(millis(b.from())))));
        var id = UuidV7.next();
        assertThat(accept(id)).isTrue();
        assertThat(accept(id)).isFalse();
    }

    private void rejected(TestOutboxConfig invalid, String message) {
        assertThatThrownBy(() -> new PgInbox(db, invalid, new OutboxMetrics(null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private static TestOutboxConfig config(
            Duration interval, Duration retention, Duration premake, Duration maintenanceInterval) {
        return new TestOutboxConfig()
                .inboxPartitionInterval(interval)
                .inboxRetention(retention)
                .inboxPremake(premake)
                .maintenanceInterval(maintenanceInterval);
    }

    private void start() throws Exception {
        inbox = new PgInbox(db, config, new OutboxMetrics(registry));
        inbox.init();
    }

    private boolean accept(UUID id) throws SQLException {
        return accept(inbox, id);
    }

    private boolean accept(Inbox inbox, UUID id) throws SQLException {
        return db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id));
    }

    private Set<UUID> acceptAll(Collection<UUID> ids) throws SQLException {
        return acceptAll(inbox, ids);
    }

    private Set<UUID> acceptAll(Inbox inbox, Collection<UUID> ids) throws SQLException {
        return db.inTx((SqlFunction0<Set<UUID>>) () -> inbox.tryAcceptAll(ids));
    }

    private void attach(Connection c, String partition, long from) throws SQLException {
        PartitionDdl.createAndAttach(c, table(), partition, bound(from), bound(from + HOUR),
                PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String table() {
        return TableNames.of(config.inbox().table()).base();
    }

    private long rows() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + table());
        }
    }

    private double outsideWindow() {
        var counter = registry.find("outbox.inbox.outside_window").counter();
        return counter == null ? 0 : counter.count();
    }

    private List<Bound> ranges() throws SQLException {
        try (var c = TestDb.connect()) {
            return PartitionDdl.listRanges(c, table());
        }
    }

    private Bound rangeOf(long millis) throws SQLException {
        return ranges().stream()
                .filter(b -> millis(b.from()) <= millis && millis < millis(b.to()))
                .findFirst()
                .orElseThrow();
    }

    private long lowest() throws SQLException {
        return ranges().stream().mapToLong(b -> millis(b.from())).min().orElseThrow();
    }

    private long highest() throws SQLException {
        return ranges().stream().mapToLong(b -> millis(b.to())).max().orElseThrow();
    }

    private static long millis(String bound) {
        return UuidV7.timestampMillis(UUID.fromString(bound));
    }

    private static String bound(long millis) {
        return UuidV7.lowerBound(millis).toString();
    }

    /** A v7 id of {@code millis}. */
    private static UUID v7(long millis) {
        var r = ThreadLocalRandom.current();
        return new UUID((millis << 16) | 0x7000L | (r.nextLong() & 0x0FFFL),
                (r.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L);
    }
}
