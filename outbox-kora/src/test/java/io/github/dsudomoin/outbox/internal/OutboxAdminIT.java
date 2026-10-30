package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.DeadLetter.Status.KEPT;
import static io.github.dsudomoin.outbox.DeadLetter.Status.PENDING;
import static io.github.dsudomoin.outbox.DeadLetter.Status.SCHEDULED;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.DeadLetterDecision;
import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.OutboxAdmin.Abandoned;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxStatus;
import io.github.dsudomoin.outbox.OutboxStatus.DeadLetterCount;
import io.github.dsudomoin.outbox.OutboxStatus.Orphan;
import io.github.dsudomoin.outbox.OutboxStatus.OrphanKind;
import io.github.dsudomoin.outbox.OutboxWaitException;
import io.github.dsudomoin.outbox.internal.PartitionDdl.Bound;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException;

class OutboxAdminIT {

    private final TestDatabase db = TestDb.newDatabase(8);
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
        schema.start();
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
    void blankFilterFieldsAreAbsent() throws Exception {
        long id = letter("t", "k");

        for (var blank : List.of("", "  ")) {
            assertThatThrownBy(() -> admin.discardDeadLetters(filter(null, null, blank, null, null, null)))
                    .as("errorContains '%s'", blank).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> admin.redriveDeadLetters(filter(blank, null, null, null, null, null)))
                    .as("topic '%s'", blank).isInstanceOf(IllegalArgumentException.class);
        }

        assertThat(deadIds()).containsExactly(id);
        assertThat(outboxMessageIds()).isEmpty();
    }

    /** With another field set, a blank one dropped would widen the call to every letter that field matches. */
    @Test
    void blankFilterStringsAreRefusedByBulkCalls() throws Exception {
        long id = letter("t", "k");
        long other = letter("u", "k");

        for (var blank : List.of("", "  ")) {
            assertThatThrownBy(() -> admin.discardDeadLetters(filter(blank, null, null, null, null, KEPT)))
                    .as("topic '%s'", blank).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("a blank topic");
            assertThatThrownBy(() -> admin.redriveDeadLetters(filter("t", null, blank, null, null, null)))
                    .as("errorContains '%s'", blank).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("a blank errorContains");
        }

        assertThat(deadIds()).containsExactly(id, other);
        assertThat(outboxMessageIds()).isEmpty();
        assertThat(find(filter(" ", null, null, null, null, KEPT))).as("reads take it as absent")
                .containsExactly(id, other);
    }

    @Test
    void aFilterCallLeavesLettersThatDieDuringIt() throws Exception {
        deadLetters(1000); // a full chunk and one more letter: the call claims a second chunk after the two below
        execute("insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
                + " values (2000, 0, gen_random_uuid(), 't', now(), 3)");
        var appeared = new AtomicBoolean();
        admin.afterChunk = c -> {
            if (appeared.compareAndSet(false, true)) {
                // an older message dying now: an id below the highest when the call began, only failed_at keeps it out
                execute("insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
                        + " values (1500, 0, gen_random_uuid(), 't', now(), 3)");
                // a redriven one dying again: a new, higher id; failed_at set back, so only the id keeps it out
                execute("insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts,"
                        + " failed_at) values (900000, 0, gen_random_uuid(), 't', now(), 3, now() - interval '1 day')");
            }
        };

        assertThat(admin.redriveDeadLetters(filter("t", null, null, null, null, null))).isEqualTo(1001);

        assertThat(deadIds()).containsExactly(1500L, 900_000L);
    }

    @Test
    void aCallFailingAfterAChunkStillWakesTheRelay() throws Exception {
        deadLetters(1001);
        admin.afterChunk = c -> {
            throw new IllegalStateException("killed after a chunk");
        };

        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> admin.redriveDeadLetters(filter("t", null, null, null, null, null)))
                    .hasMessage("killed after a chunk");

            assertThat(err.text()).contains("WARN").contains("failed after 1000 letters");
        }
        assertThat(outboxMessageIds()).hasSize(1000);
        assertThat(wakes).hasValue(1);
    }

    @Test
    void userTextIsLoggedWithoutLineBreaks() throws Exception {
        try (var err = new CapturedStderr()) {
            admin.discardDeadLetters(filter("t\r\nINFO forged", null, "a\nb\u0007", null, null, null));

            assertThat(err.text()).contains("topic=t\\r\\nINFO forged").contains("errorContains=a\\nb\\u0007")
                    .doesNotContain("\nINFO forged");
        }
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
        var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, TestTelemetry.kora());
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
        deadLetters(letters);

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

    /** A read stuck behind a lock (or a slow scan) gives its pool connection back after readTimeout. */
    @Test
    void readsGiveUpAfterTheirTimeout() throws Exception {
        admin.readTimeout = Duration.ofMillis(300);
        try (var holder = TestDb.connect()) {
            holder.setAutoCommit(false);
            TestSql.execute(holder, "lock table " + names.dead() + " in access exclusive mode");
            pool.submit(() -> { // so a read without its timeout ends, and fails the test instead of hanging it
                Thread.sleep(3000);
                holder.rollback();
                return null;
            });

            for (ThrowingCallable read : List.<ThrowingCallable>of(() -> admin.countDeadLetters(DeadLetterFilter.any()),
                    () -> admin.findDeadLetters(DeadLetterFilter.any(), null, 10), admin::status)) {
                long start = System.nanoTime();
                assertThatThrownBy(read).isInstanceOfSatisfying(RuntimeSqlException.class,
                        e -> assertThat(((SQLException) e.getCause()).getSQLState()).isEqualTo("57014"));
                assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
            }
        }
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
    void restoreOrphanHoldsTheMaintenanceLockAsMaintenanceDoes() throws Exception {
        var orphan = foreignOrphan();
        execute("insert into " + orphan + " (id, shard, xid, message_id, topic, key)"
                + " select g, 0, '10', gen_random_uuid(), 't', convert_to('k' || g, 'UTF8')"
                + " from generate_series(1, 1001) g");
        var one = TestDb.newDatabase(1); // one session: what the call leaves on it is seen by the next
        try {
            var single = new PgOutboxAdmin(one, names, config, new Maintenance(one, names, config,
                    new OutboxMetrics(null)), wakes::incrementAndGet);
            var idleTimeout = new AtomicReference<String>();
            single.orphanRestore.afterChunk = c -> {
                if (idleTimeout.compareAndExchange(null, queryString(c, "show idle_session_timeout")) == null) {
                    throw new RuntimeException("killed after a chunk");
                }
            };

            assertThatThrownBy(() -> single.restoreOrphan(orphan, 0)).hasMessage("killed after a chunk");

            assertThat(idleTimeout).as("a frozen holder loses the lock").hasValue("1min");
            assertThat(wakes).as("rows went back before it failed").hasValue(1);
            try (var c = one.newConnection()) {
                assertThat(queryString(c, "show idle_session_timeout")).as("reset").isEqualTo("0");
            }
            try (var c = TestDb.connect()) {
                assertThat(queryLong(c, "select count(*) from pg_locks where locktype = 'advisory' and classid::bigint"
                        + " = " + Integer.toUnsignedLong(names.internalNamespace()) + " and objid = "
                        + TableNames.LOCK_MAINTENANCE)).as("released").isZero();
            }
            assertThat(single.restoreOrphan(orphan, 0)).as("the rest").isOne();
            assertThat(outboxMessageIds()).hasSize(1001);
        } finally {
            one.release();
        }
    }

    @Test
    void restoreOrphanChunksEndAFrozenHoldersSession() throws Exception {
        var orphan = foreignOrphan();
        orphanRow(orphan, 1, 10, "row");
        var seen = TestDb.uniqueTable(); // what each chunk's delete saw
        execute("create table " + seen + " (setting text)");
        execute("create function " + seen + "_f() returns trigger language plpgsql as $$ begin insert into " + seen
                + " values (current_setting('idle_in_transaction_session_timeout')); return null; end $$");
        execute("create trigger " + seen + "_t after delete on " + orphan + " for each statement execute function "
                + seen + "_f()");

        assertThat(admin.restoreOrphan(orphan, 0)).isOne();

        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(setting, ',') from " + seen))
                    .as("the lock timeout, 30 s, + 30 s").isEqualTo("1min");
        }
    }

    @Test
    void namesWhoseDigitsFitNoLongAreLeftAlone() throws Exception {
        var digits = "99999999999999999999"; // 20 digits
        var strangers = List.of(names.partitionPrefix() + digits, names.setAsidePrefix() + digits);
        try (var c = TestDb.connect()) {
            for (var table : strangers) {
                PartitionDdl.create(c, names.base(), table, Duration.ofSeconds(5));
            }
        }

        try (var err = new CapturedStderr()) {
            assertThat(admin.status().orphans()).isEmpty();

            assertThat(err.text()).contains("WARN").contains(strangers.get(0)).contains(strangers.get(1))
                    .contains("fit no bigint");
        }
        for (var table : strangers) {
            assertThatThrownBy(() -> admin.restoreOrphan(table, 0)).as(table)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(new Maintenance(db, names, config, new OutboxMetrics(null)).runOnce()).isTrue();
        for (var table : strangers) {
            assertThat(exists(table)).as("empty, yet not dropped as a foreign orphan would be").isTrue();
        }
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
                () -> admin.restoreOrphan(names.partitionPrefix() + "1000", 0),
                admin::recoverXidRegression,
                () -> admin.abandonWaiting("t", null, "rollback", false));

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
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, created_at, waiting_since) values"
                + " (1, 0, gen_random_uuid(), 't', now(), now() - interval '1 hour'),"
                + " (2, 0, gen_random_uuid(), 't', now(), null)");
        execute("insert into " + names.parked() + " (id, shard, message_id, topic, created_at, due_at)"
                + " values (3, 0, gen_random_uuid(), 't', now(), now())");
        execute("delete from " + names.instance());
        execute("insert into " + names.instance() + " (instance_id, shards, last_seen, hook_active, relay, contracts)"
                + " values ('a', 4, '2026-01-15T10:00:00Z', true, true, '{\"t2\": 2, \"t1\": 1}'),"
                + " ('b', 4, '2026-01-15T10:00:00Z', false, true, null),"
                + " ('c', 4, '2026-01-15T10:00:00Z', false, false, '{}')");
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
        assertThat(status.parkedSize()).isOne();
        assertThat(status.waitingSize()).isOne();
        assertThat(status.waitingOldestSeconds()).isBetween(3600.0, 3660.0);
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
        var seen = Instant.parse("2026-01-15T10:00:00Z");
        assertThat(status.instances()).containsExactly(
                new OutboxStatus.Instance("a", 4, seen, true, true, Map.of("t1", 1, "t2", 2)),
                new OutboxStatus.Instance("b", 4, seen, false, true, null),
                new OutboxStatus.Instance("c", 4, seen, false, false, Map.of()));
    }

    @Test
    void abandonMovesEachWaitingKeyWholeToKeptDeadLetters() throws Exception {
        seedWaiting();

        assertThat(admin.abandonWaiting("t", bytes("k"), "rolled back to v1", false)).as("the head, its followers"
                + " and its key's parked rows").isEqualTo(new Abandoned(1, 5));
        assertThat(ids(names.retry())).containsExactly(20L, 30L, 31L, 40L);
        assertThat(ids(names.parked())).containsExactly(32L, 33L);
        assertThat(admin.abandonWaiting("t", null, "rolled back to v1", false)).as("the unkeyed head alone")
                .isEqualTo(new Abandoned(1, 1));

        assertThat(ids(names.retry())).as("a failing key and another topic's stay").containsExactly(30L, 31L, 40L);
        assertThat(ids(names.parked())).as("an unkeyed one is no follower").containsExactly(32L, 33L);
        try (var c = TestDb.connect()) {
            assertThat(queryString(c, "select string_agg(id || ':' || attempts || ':' || status || ':' || coalesce("
                    + "due_at::text, '-') || ':' || error, ', ' order by id) from " + names.dead())).isEqualTo(
                    "5:1:kept:-:abandoned: rolled back to v1, 10:2:kept:-:abandoned: rolled back to v1,"
                    + " 11:0:kept:-:abandoned: rolled back to v1, 12:0:kept:-:abandoned: rolled back to v1,"
                    + " 13:0:kept:-:abandoned: rolled back to v1, 20:0:kept:-:abandoned: rolled back to v1");
        }
        assertThat(wakes).as("nothing put back").hasValue(0);
    }

    @Test
    void aDryRunCountsAndMovesNothing() throws Exception {
        seedWaiting();

        assertThat(admin.abandonWaiting("t", null, "rollback", true)).isEqualTo(new Abandoned(2, 6));
        assertThat(admin.abandonWaiting("t", bytes("k"), "rollback", true)).isEqualTo(new Abandoned(1, 5));
        assertThat(admin.abandonWaiting("t", bytes("j"), "rollback", true)).as("j fails, it does not wait")
                .isEqualTo(new Abandoned(0, 0));

        assertThat(ids(names.retry())).containsExactly(10L, 11L, 12L, 20L, 30L, 31L, 40L);
        assertThat(ids(names.parked())).containsExactly(5L, 13L, 32L, 33L);
        assertThat(deadIds()).isEmpty();
    }

    @Test
    void abandonGoesThroughChunksOfAThousandKeys() throws Exception {
        // 1001 waiting heads with a follower each, so two chunks
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at, next_attempt_at,"
                + " waiting_since) select g, 0, gen_random_uuid(), 't', convert_to('k' || g, 'UTF8'), now(), now(),"
                + " now() from generate_series(1, 1001) g");
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at)"
                + " select 2000 + g, 0, gen_random_uuid(), 't', convert_to('k' || g, 'UTF8'), now()"
                + " from generate_series(1, 1001) g");

        assertThat(admin.abandonWaiting("t", null, "rollback", true)).isEqualTo(new Abandoned(1001, 2002));
        assertThat(admin.abandonWaiting("t", null, "rollback", false)).isEqualTo(new Abandoned(1001, 2002));

        assertThat(count(names.retry())).isZero();
        assertThat(count(names.dead())).isEqualTo(2002);
    }

    @Test
    void abandonNeedsATopicAndAReason() throws Exception {
        seedWaiting();

        for (var blank : List.of("", " ")) {
            assertThatThrownBy(() -> admin.abandonWaiting(blank, null, "rollback", false))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("topic");
            assertThatThrownBy(() -> admin.abandonWaiting("t", null, blank, false))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("reason");
        }
        assertThat(deadIds()).isEmpty();
    }

    /**
     * A relay step holds the shard's lock while its handler runs and adds a follower to the waiting key after it: the
     * abandon waits for the step's commit, then moves the follower too, or gives up with "retry" after its wait.
     */
    @Test
    void abandonWaitsForARelayStepOnItsShard() throws Exception {
        var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, TestTelemetry.kora());
        var ticks = new Ticks(names, config.shards());
        int shard = Hashing.shard(Hashing.keyHash("t", bytes("w")), config.shards());
        var inHandler = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var block = new AtomicBoolean();
        var processor = new ShardProcessor(names, config, record -> {
            if (block.compareAndSet(true, false)) {
                inHandler.countDown();
                release.await();
            }
            throw new OutboxWaitException("v2 not here");
        }, TestTelemetry.kora(), false);
        try {
            var now = Instant.now();
            db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", "w", bytes("w#1"))));
            step(processor, ticks, shard, now);
            assertThat(payloads(names.retry())).as("waiting").containsExactly("w#1");
            db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", "w", bytes("w#2"))));
            block.set(true);
            var step = pool.submit(() -> step(processor, ticks, shard, now.plusSeconds(1)));
            assertThat(inHandler.await(10, SECONDS)).isTrue();

            admin.shardWait = Duration.ofMillis(200);
            assertThatThrownBy(() -> admin.abandonWaiting("t", bytes("w"), "rollback", false))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("is busy, retry");
            assertThat(payloads(names.retry())).containsExactly("w#1");

            admin.shardWait = Duration.ofSeconds(10);
            var abandon = pool.submit(() -> admin.abandonWaiting("t", bytes("w"), "rollback", false));
            awaitShardLockWaiter(shard);
            assertThat(abandon.isDone()).as("waits for the step").isFalse();
            release.countDown();
            step.get(10, SECONDS);

            assertThat(abandon.get(10, SECONDS)).isEqualTo(new Abandoned(1, 2));
        } finally {
            release.countDown();
            processor.close();
        }
        assertThat(count(names.retry())).as("no follower left without its head").isZero();
        assertThat(payloads(names.dead())).containsExactly("w#1", "w#2");
    }
    /**
     * Topic t: waiting head 10 of key k with followers 11 and 12 and parked rows 5 and 13 of its key; unkeyed waiting
     * head 20 on shard 1; failing head 30 of key j with follower 31 and parked row 32; unkeyed parked row 33. Topic u:
     * waiting head 40 of key k.
     */
    private void seedWaiting() throws SQLException {
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at, attempts,"
                + " next_attempt_at, last_error, waiting_since) values"
                + " (10, 0, gen_random_uuid(), 't', 'k', now(), 2, now(), 'waiting (contract): v2', now()),"
                + " (11, 0, gen_random_uuid(), 't', 'k', now(), 0, null, null, null),"
                + " (12, 0, gen_random_uuid(), 't', 'k', now(), 0, null, null, null),"
                + " (20, 1, gen_random_uuid(), 't', null, now(), 0, now(), 'waiting (no_route): none', now()),"
                + " (30, 0, gen_random_uuid(), 't', 'j', now(), 1, now(), 'boom', null),"
                + " (31, 0, gen_random_uuid(), 't', 'j', now(), 0, null, null, null),"
                + " (40, 0, gen_random_uuid(), 'u', 'k', now(), 0, now(), 'waiting (contract): v2', now())");
        execute("insert into " + names.parked() + " (id, shard, message_id, topic, key, created_at, attempts, due_at)"
                + " values (5, 0, gen_random_uuid(), 't', 'k', now(), 1, now()),"
                + " (13, 0, gen_random_uuid(), 't', 'k', now(), 0, now()),"
                + " (32, 0, gen_random_uuid(), 't', 'j', now(), 0, now()),"
                + " (33, 1, gen_random_uuid(), 't', null, now(), 0, now())");
    }

    /** One relay step of {@code shard} at {@code now} after a new tick, committed. */
    private Void step(ShardProcessor processor, Ticks ticks, int shard, Instant now) throws Exception {
        long latest;
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            latest = ticks.latest(c).id();
        }
        try (var c = TestDb.connect()) {
            ShardProcessor.prepareTransaction(c, config.batchTimeout());
            processor.tryProcess(c, shard, latest, now);
            c.commit();
        }
        return null;
    }

    /** Until a session waits for the lock of {@code shard}. */
    private void awaitShardLockWaiter(int shard) throws Exception {
        long deadline = System.nanoTime() + SECONDS.toNanos(10);
        try (var c = TestDb.connect()) {
            while (queryLong(c, "select count(*) from pg_locks where locktype = 'advisory' and not granted and"
                    + " classid::bigint = " + Integer.toUnsignedLong(names.internalNamespace()) + " and objid = "
                    + (TableNames.LOCK_SHARD_BASE + shard)) == 0) {
                assertThat(System.nanoTime()).as("a waiter for the shard's lock").isLessThan(deadline);
                Thread.sleep(10);
            }
        }
    }

    private List<Long> ids(String table) throws SQLException {
        return longs("select id from " + table + " order by id");
    }

    private List<String> payloads(String table) throws SQLException {
        var all = new ArrayList<String>();
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select payload from " + table + " order by id")) {
            while (rs.next()) {
                all.add(new String(rs.getBytes(1), UTF_8));
            }
        }
        return all;
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

    /** Kept letters 1..n on topic t with distinct keys, dead before the call that follows. */
    private void deadLetters(int n) throws SQLException {
        execute("insert into " + names.dead() + " (id, shard, message_id, topic, key, created_at, attempts, error)"
                + " select g, 0, gen_random_uuid(), 't', convert_to('k' || g, 'UTF8'), now(), 3, 'boom'"
                + " from generate_series(1, " + n + ") g");
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
        // overriding: from Postgres 17 a partition has the outbox's identity column
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + table
                + " (id, shard, xid, message_id, topic, key, payload) overriding system value"
                + " values (?, 0, ?::text::xid8, ?, 't', ?, ?)")) {
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
