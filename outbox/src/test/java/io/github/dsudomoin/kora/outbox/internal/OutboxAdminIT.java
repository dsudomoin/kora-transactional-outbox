package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.DeadLetter.Status.KEPT;
import static io.github.dsudomoin.kora.outbox.DeadLetter.Status.PENDING;
import static io.github.dsudomoin.kora.outbox.DeadLetter.Status.SCHEDULED;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.DeadLetterDecision;
import io.github.dsudomoin.kora.outbox.DeadLetterFilter;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxStatus.DeadLetterCount;
import io.github.dsudomoin.kora.outbox.OutboxStatus.Orphan;
import io.github.dsudomoin.kora.outbox.OutboxStatus.OrphanKind;
import io.github.dsudomoin.kora.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.kora.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

class OutboxAdminIT {

    private final JdbcDatabase db = TestDb.newDatabase(8);
    private final TestOutboxConfig config = new TestOutboxConfig().relayEnabled(false);
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final AtomicInteger wakes = new AtomicInteger();
    private final AtomicLong ids = new AtomicLong();
    private final Map<Long, UUID> messageIds = new HashMap<>();
    private OutboxSchema schema;
    private TableNames names;
    private PgOutboxAdmin admin;

    @BeforeEach
    void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
        admin = new PgOutboxAdmin(db, names, config, new Maintenance(db, names, config, new OutboxMetrics(null)),
                wakes::incrementAndGet);
    }

    @AfterEach
    void release() {
        pool.shutdownNow();
        db.release();
    }

    @Test
    void findsByEachFilterField() throws Exception {
        var now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        long a = letter("t1", "k1", "disk full", now.minus(2, ChronoUnit.HOURS), KEPT);
        long b = letter("t2", "k2", "timeout", now.minus(1, ChronoUnit.HOURS), PENDING);
        long c = letter("t1", "k3", "timeout", now, SCHEDULED);

        assertThat(find(filter("t1", null, null, null, null, null))).containsExactly(a, c);
        assertThat(find(filter(null, "k2", null, null, null, null))).containsExactly(b);
        assertThat(find(filter(null, null, "time", null, null, null))).containsExactly(b, c);
        assertThat(find(filter(null, null, null, now.minus(1, ChronoUnit.HOURS), null, null)))
                .as("from inclusive").containsExactly(b, c);
        assertThat(find(filter(null, null, null, null, now.minus(1, ChronoUnit.HOURS), null)))
                .as("to exclusive").containsExactly(a);
        assertThat(find(filter(null, null, null, null, null, SCHEDULED))).containsExactly(c);
        assertThat(find(filter("t1", null, "timeout", null, null, null))).as("all fields must match")
                .containsExactly(c);
        assertThat(find(DeadLetterFilter.any())).containsExactly(a, b, c);

        var letter = admin.findDeadLetters(filter(null, "k2", null, null, null, null), null, 10).letters().get(0);
        assertThat(letter.id()).isEqualTo(b);
        assertThat(letter.topic()).isEqualTo("t2");
        assertThat(letter.key()).isEqualTo(bytes("k2"));
        assertThat(letter.payload()).isEqualTo(bytes("p"));
        assertThat(letter.attempts()).isEqualTo(3);
        assertThat(letter.failedAt()).isEqualTo(now.minus(1, ChronoUnit.HOURS));
        assertThat(letter.error()).isEqualTo("timeout");
        assertThat(letter.status()).isEqualTo(PENDING);
    }

    @Test
    void pagesByAfterId() throws Exception {
        var all = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            all.add(letter("t", "k" + i));
        }

        var first = admin.findDeadLetters(DeadLetterFilter.any(), null, 2);
        var second = admin.findDeadLetters(DeadLetterFilter.any(), first.nextAfterId(), 2);
        var third = admin.findDeadLetters(DeadLetterFilter.any(), second.nextAfterId(), 2);

        assertThat(first.letters()).extracting(DeadLetter::id).containsExactly(all.get(0), all.get(1));
        assertThat(first.nextAfterId()).isEqualTo(all.get(1));
        assertThat(second.letters()).extracting(DeadLetter::id).containsExactly(all.get(2), all.get(3));
        assertThat(third.letters()).extracting(DeadLetter::id).containsExactly(all.get(4));
        assertThat(third.nextAfterId()).as("a short page is the last").isNull();
        for (int limit : new int[] {0, 1001}) {
            assertThatThrownBy(() -> admin.findDeadLetters(DeadLetterFilter.any(), null, limit))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void countsByTheFilter() throws Exception {
        letter("t1", "a");
        letter("t1", "b");
        letter("t2", "c");

        assertThat(admin.countDeadLetters(DeadLetterFilter.any())).isEqualTo(3);
        assertThat(admin.countDeadLetters(filter("t1", null, null, null, null, null))).isEqualTo(2);
        assertThat(admin.countDeadLetters(filter(null, null, "nope", null, null, null))).isZero();
    }

    @Test
    void redrivesByIdsWhateverTheStatus() throws Exception {
        var now = Instant.now();
        long kept = letter("t", "a", "boom", now, KEPT);
        long pending = letter("t", "b", "boom", now, PENDING);
        long scheduled = letter("t", "c", "boom", now, SCHEDULED);
        long other = letter("t", "d");
        execute("update " + names.dead() + " set redrives = 2 where id = " + kept);

        assertThat(admin.redriveDeadLetters(List.of(scheduled, kept, pending, 999_999L))).isEqualTo(3);

        assertThat(deadIds()).containsExactly(other);
        assertThat(outboxMessageIds()).as("in id order").containsExactly(
                messageIds.get(kept), messageIds.get(pending), messageIds.get(scheduled));
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select redrives from " + names.redriven() + " where message_id = '"
                    + messageIds.get(kept) + "'")).isEqualTo(3);
            assertThat(queryLong(c, "select shard from " + names.base() + " where message_id = '"
                    + messageIds.get(kept) + "'")).as("the shard recomputed")
                    .isEqualTo(Hashing.shard(Hashing.keyHash("t", bytes("a")), config.shards()));
        }
        assertThat(wakes).hasValue(1);
        assertThat(admin.redriveDeadLetters(List.of(kept))).as("gone").isZero();
        assertThat(wakes).as("nothing put back, no wake-up").hasValue(1);
    }

    @Test
    void redrivesByFilter() throws Exception {
        long a = letter("t1", "a");
        long b = letter("t1", "b");
        long c = letter("t2", "c");

        assertThat(admin.redriveDeadLetters(filter("t1", null, null, null, null, null))).isEqualTo(2);

        assertThat(deadIds()).containsExactly(c);
        assertThat(outboxMessageIds()).containsExactly(messageIds.get(a), messageIds.get(b));
        assertThat(wakes).hasValue(1);
    }

    @Test
    void discardsByIdsAndByFilter() throws Exception {
        long a = letter("t1", "a");
        long b = letter("t1", "b");
        long c = letter("t2", "c");
        long d = letter("t2", "d");

        assertThat(admin.discardDeadLetters(List.of(a, 999_999L))).isOne();
        assertThat(admin.discardDeadLetters(filter("t2", null, null, null, null, null))).isEqualTo(2);

        assertThat(deadIds()).containsExactly(b);
        assertThat(outboxMessageIds()).isEmpty();
        assertThat(wakes).hasValue(0);
        assertThat(List.of(c, d)).doesNotContainAnyElementsOf(deadIds());
    }

    @Test
    void emptyFilterIsRefused() throws Exception {
        long id = letter("t", "k");

        assertThatThrownBy(() -> admin.redriveDeadLetters(DeadLetterFilter.any()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> admin.discardDeadLetters(DeadLetterFilter.any()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(deadIds()).containsExactly(id);
        assertThat(outboxMessageIds()).isEmpty();
    }

    @Test
    void lettersHeldByTheHookAreSkipped() throws Exception {
        long held = letter("t", "a", "boom", Instant.now(), PENDING);
        execute("update " + names.dead() + " set due_at = now() where id = " + held);
        long free = letter("t", "b");
        var inHook = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var pass = new DeadLetterPass(db, names, config, letters -> {
            inHook.countDown();
            release.await();
            return letters.stream().map(l -> DeadLetterDecision.keep()).toList();
        }, new OutboxMetrics(null));
        try {
            var run = pool.submit(pass::runOnce);
            assertThat(inHook.await(10, SECONDS)).isTrue();

            assertThat(admin.discardDeadLetters(List.of(held))).isZero();
            assertThat(admin.redriveDeadLetters(filter("t", null, null, null, null, null))).as("the free one only")
                    .isOne();
            assertThat(admin.countDeadLetters(DeadLetterFilter.any())).as("still there").isOne();

            release.countDown();
            run.get(10, SECONDS);
        } finally {
            pass.close();
        }
        assertThat(deadIds()).containsExactly(held);
        assertThat(outboxMessageIds()).containsExactly(messageIds.get(free));
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select status from " + names.dead())).as("the hook's decision")
                    .isEqualTo("kept");
        }
    }

    @Test
    void aLetterWhoseKeyIsHeldIsSkippedWithoutWaiting() throws Exception {
        long busy = letter("t", "busy");
        long free = letter("t", "free");
        var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, new OutboxMetrics(null));
        UUID live;
        try (var business = TestDb.connect()) {
            business.setAutoCommit(false); // a business transaction that sent the key and goes on working
            live = outbox.send(business, OutboxMessage.of("t", "busy", bytes("live")));

            // waiting for the key would time out (swapLockTimeout) and fail the call
            assertThat(admin.redriveDeadLetters(List.of(busy, free))).as("the free one only").isOne();
            assertThat(deadIds()).containsExactly(busy);

            business.commit();
        }
        assertThat(admin.redriveDeadLetters(List.of(busy))).as("once the send committed").isOne();
        assertThat(outboxMessageIds()).as("behind the send").containsExactly(live, messageIds.get(free),
                messageIds.get(busy));
    }

    @Test
    void bulkRedriveOverTheLockTable() throws Exception {
        // distinct keys, so key locks: one transaction can take about 56 000 on the test database (64
        // max_locks_per_transaction × 522 backends, plus the slack of shared memory), so not all of these
        int letters = 100_000;
        execute("insert into " + names.dead() + " (id, shard, message_id, topic, key, created_at, attempts, error)"
                + " select g, 0, gen_random_uuid(), 't', convert_to('k' || g, 'UTF8'), now(), 3, 'boom'"
                + " from generate_series(1, " + letters + ") g");

        assertThat(admin.redriveDeadLetters(filter("t", null, null, null, null, null))).isEqualTo(letters);

        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(distinct message_id) from " + names.base())).isEqualTo(letters);
            assertThat(queryLong(c, "select count(*) from " + names.dead())).isZero();
        }
    }

    @Test
    void errorSubstringIsNotAPattern() throws Exception {
        long percent = letter("t", "a", "a%b", Instant.now(), KEPT);
        letter("t", "b", "aXb", Instant.now(), KEPT);
        long underscore = letter("t", "c", "a_b", Instant.now(), KEPT);

        assertThat(find(filter(null, null, "a%b", null, null, null))).containsExactly(percent);
        assertThat(find(filter(null, null, "a_b", null, null, null))).containsExactly(underscore);
    }

    @Test
    void restoreOrphanRefusesWhatIsNotAStandaloneRange() throws Exception {
        var attached = ranges().stream().filter(b -> b.to() != null).findFirst().orElseThrow().name();
        var precreated = names.partitionPrefix() + tailLower(); // as the extension makes them before its swap
        var unrelated = TestDb.uniqueTable();
        var victim = TestDb.uniqueTable();
        try (var c = TestDb.connect()) {
            PartitionDdl.create(c, names.base(), precreated, Duration.ofSeconds(5));
            orphanRow(precreated, 1, 0, "pre");
            execute("create table " + unrelated + " (like " + names.base() + ")");
            execute("create table " + victim + " (id bigint)");
        }

        for (var table : List.of(names.partitionPrefix() + "1; drop table " + victim, attached, unrelated,
                precreated, names.tail(), names.base())) {
            assertThatThrownBy(() -> admin.restoreOrphan(table, 0)).as(table)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        assertThat(exists(victim)).isTrue();
        assertThat(exists(unrelated)).isTrue();
        assertThat(count(precreated)).as("nothing moved").isOne();
        assertThat(ranges()).extracting(Bound::name).contains(attached);
        assertThat(exists(attached + "_id")).as("nothing run on the attached range").isFalse();
        assertThat(exists(precreated + "_id")).isFalse();
        assertThat(outboxMessageIds()).isEmpty();
        assertThat(wakes).hasValue(0);
    }

    @Test
    void restoreOrphanMovesRowsFromTheCut() throws Exception {
        var orphan = foreignOrphan();
        orphanRow(orphan, 1, 10, "below");
        var at = orphanRow(orphan, 2, 20, "at");
        var above = orphanRow(orphan, 3, 30, "above");
        assertThat(admin.status().orphans()).containsExactly(new Orphan(orphan, OrphanKind.FOREIGN));

        assertThat(admin.restoreOrphan(orphan, 20)).isEqualTo(2);

        assertThat(exists(orphan)).as("dropped").isFalse();
        assertThat(outboxMessageIds()).containsExactly(at, above);
        assertThat(admin.status().orphans()).isEmpty();
        assertThat(wakes).hasValue(1);
    }

    @Test
    void restoreOrphanWaitsForMaintenanceAtMostItsWait() throws Exception {
        var orphan = foreignOrphan();
        orphanRow(orphan, 1, 10, "row");
        admin.maintenanceWait = Duration.ofMillis(200);

        try (var maintenance = TestDb.connect()) {
            TestSql.execute(maintenance, "select pg_advisory_lock(" + names.internalNamespace() + ", "
                    + TableNames.LOCK_MAINTENANCE + ")");
            assertThatThrownBy(() -> admin.restoreOrphan(orphan, 0)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("maintenance is running, retry");
            assertThat(count(orphan)).isOne();
        }

        assertThat(admin.restoreOrphan(orphan, 0)).as("once it ended").isOne();
    }

    @Test
    void refusesInsideInTx() throws Exception {
        long id = letter("t", "k");
        var filter = filter("t", null, null, null, null, null);
        List<ThrowingCallable> calls = List.of(
                admin::status,
                () -> admin.findDeadLetters(filter, null, 10),
                () -> admin.countDeadLetters(filter),
                () -> admin.redriveDeadLetters(List.of(id)),
                () -> admin.redriveDeadLetters(filter),
                () -> admin.discardDeadLetters(List.of(id)),
                () -> admin.discardDeadLetters(filter),
                () -> admin.restoreOrphan(names.partitionPrefix() + "1000", 0));

        for (var call : calls) {
            db.inTx((SqlFunction0<Void>) () -> {
                assertThatThrownBy(call).isInstanceOf(IllegalStateException.class).hasMessageContaining("inTx");
                return null;
            });
        }
        assertThat(deadIds()).containsExactly(id);
    }

    @Test
    void statusReportsTheState() throws Exception {
        execute("insert into " + names.halt() + " (max_tick_xmax) values ('1')");
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, created_at) values"
                + " (1, 0, gen_random_uuid(), 't', now()), (2, 0, gen_random_uuid(), 't', now())");
        var now = Instant.now();
        letter("a", "1", "boom", now, KEPT);
        letter("a", "2", "boom", now, KEPT);
        letter("a", "3", "boom", now, PENDING);
        letter("b", "4", "boom", now, SCHEDULED);
        var foreign = ranges().stream() // the highest range below the tail, far above the current xid
                .filter(b -> b.to() != null)
                .max(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .orElseThrow();
        orphanRow(foreign.name(), 1, Long.parseLong(foreign.from()), "row");
        execute("alter table " + names.base() + " detach partition " + foreign.name());
        var ours = foreignOrphan();
        execute("insert into " + names.dropping() + " (table_name) values ('" + names.unqualified(ours) + "')");
        var setAside = names.setAsidePrefix() + "7";
        var precreated = names.partitionPrefix() + tailLower();
        try (var c = TestDb.connect()) {
            PartitionDdl.create(c, names.base(), setAside, Duration.ofSeconds(5));
            PartitionDdl.create(c, names.base(), precreated, Duration.ofSeconds(5));
        }

        var status = admin.status();

        assertThat(status.halted()).isTrue();
        assertThat(status.lagSeconds()).isZero();
        assertThat(status.retrySize()).isEqualTo(2);
        assertThat(status.parkedSize()).isZero();
        assertThat(status.deadLetters()).containsExactlyInAnyOrder(new DeadLetterCount("a", KEPT, 2),
                new DeadLetterCount("a", PENDING, 1), new DeadLetterCount("b", SCHEDULED, 1));
        assertThat(status.orphans()).as("not the pre-created range").containsExactlyInAnyOrder(
                new Orphan(foreign.name(), OrphanKind.FOREIGN), new Orphan(ours, OrphanKind.DETACHED_BY_LIBRARY),
                new Orphan(setAside, OrphanKind.SET_ASIDE));
        assertThat(status.tailOverflow()).isFalse();
        assertThat(status.premade()).as("one detached").isEqualTo(config.premake() - 1);
        assertThat(status.xidsLeft()).as("the tail is attached").isNull();
        assertThat(status.maintenanceSinceSuccessSeconds()).isNotNull().isNotNegative();
        assertThat(status.shards()).isEqualTo(config.shards());
    }

    private long letter(String topic, String key) throws SQLException {
        return letter(topic, key, "boom", Instant.now(), KEPT);
    }

    /** A dead letter as the relay writes one, with payload p; a pending or scheduled one is due in an hour. */
    private long letter(String topic, String key, String error, Instant failedAt, DeadLetter.Status status)
            throws SQLException {
        long id = ids.incrementAndGet();
        var messageId = UUID.randomUUID();
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + names.dead()
                + " (id, shard, message_id, topic, key, payload, created_at, attempts, failed_at, error, status,"
                + " due_at) values (?, 0, ?, ?, ?, ?, now(), 3, ?, ?, ?, case when ? then null"
                + " else now() + interval '1 hour' end)")) {
            ps.setLong(1, id);
            ps.setObject(2, messageId);
            ps.setString(3, topic);
            ps.setBytes(4, bytes(key));
            ps.setBytes(5, bytes("p"));
            ps.setObject(6, failedAt.atOffset(ZoneOffset.UTC));
            ps.setString(7, error);
            ps.setString(8, status.name().toLowerCase(Locale.ROOT));
            ps.setBoolean(9, status == KEPT);
            ps.executeUpdate();
        }
        messageIds.put(id, messageId);
        return id;
    }

    private static DeadLetterFilter filter(@Nullable String topic, @Nullable String key,
            @Nullable String errorContains, @Nullable Instant from, @Nullable Instant to,
            @Nullable DeadLetter.Status status) {
        return new DeadLetterFilter(topic, key == null ? null : bytes(key), errorContains, from, to, status);
    }

    private List<Long> find(DeadLetterFilter filter) {
        return admin.findDeadLetters(filter, null, 1000).letters().stream().map(DeadLetter::id).toList();
    }

    /** A standalone {@code <base>_x1000}, below every range of the outbox, as someone else's detach leaves one. */
    private String foreignOrphan() throws SQLException {
        var orphan = names.partitionPrefix() + "1000";
        try (var c = TestDb.connect()) {
            PartitionDdl.create(c, names.base(), orphan, Duration.ofSeconds(5));
        }
        return orphan;
    }

    /** A row of a standalone range with that xid; returns its message id. */
    private UUID orphanRow(String table, long id, long xid, String payload) throws SQLException {
        var messageId = UUID.randomUUID();
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + table
                + " (id, shard, xid, message_id, topic, key, payload) values (?, 0, ?::text::xid8, ?, 't', ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, Long.toString(xid));
            ps.setObject(3, messageId);
            ps.setBytes(4, bytes("k" + id));
            ps.setBytes(5, bytes(payload));
            ps.executeUpdate();
        }
        return messageId;
    }

    private List<Bound> ranges() throws SQLException {
        try (var c = TestDb.connect()) {
            return PartitionDdl.listRanges(c, names.base());
        }
    }

    private long tailLower() throws SQLException {
        return Long.parseLong(ranges().stream().filter(b -> b.to() == null).findFirst().orElseThrow().from());
    }

    private List<Long> deadIds() throws SQLException {
        return longs("select id from " + names.dead() + " order by id");
    }

    private List<UUID> outboxMessageIds() throws SQLException {
        var all = new ArrayList<UUID>();
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select message_id from " + names.base() + " order by id")) {
            while (rs.next()) {
                all.add(rs.getObject(1, UUID.class));
            }
        }
        return all;
    }

    private List<Long> longs(String sql) throws SQLException {
        var all = new ArrayList<Long>();
        try (var c = TestDb.connect(); var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            while (rs.next()) {
                all.add(rs.getLong(1));
            }
        }
        return all;
    }

    private long count(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + table);
        }
    }

    private static boolean exists(String table) throws SQLException {
        try (var c = TestDb.connect()) {
            return queryString(c, "select to_regclass(?)::text", table) != null;
        }
    }

    private void execute(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, sql);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
