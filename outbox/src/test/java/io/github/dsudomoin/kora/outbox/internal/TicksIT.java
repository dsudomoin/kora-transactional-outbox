package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryString;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

class TicksIT {

    private static final int SHARDS = 8;

    private final JdbcDatabase db = TestDb.newDatabase(2);
    private TableNames names;
    private Ticks ticks;

    @BeforeEach
    void bootstrap() throws Exception {
        var schema = new OutboxSchema(db, new TestOutboxConfig().shards(SHARDS));
        schema.init();
        names = schema.names();
        ticks = new Ticks(names, SHARDS);
    }

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void makeRespectsMinInterval() throws Exception {
        try (var c = TestDb.connect()) {
            execute(c, "update " + names.tick() + " set created_at = created_at - interval '1 hour'");

            assertThat(ticks.make(c, Duration.ofSeconds(1))).isTrue();
            assertThat(ticks.make(c, Duration.ofSeconds(1))).isFalse();

            assertThat(queryLong(c, "select count(*) from " + names.tick())).isEqualTo(2);
            assertSessionRestored(c);
        }
    }

    @Test
    void futureCreatedAtIsStale() throws Exception {
        try (var c = TestDb.connect()) {
            execute(c, "update " + names.tick() + " set created_at = now() + interval '1 hour'");

            assertThat(ticks.make(c, Duration.ofSeconds(1))).isTrue();
        }
    }

    @Test
    void ticksAreMonotonicUnderConcurrency() throws Exception {
        var xids = new ConcurrentLinkedQueue<Long>();
        var stop = new AtomicBoolean();
        var pool = Executors.newFixedThreadPool(12);
        try {
            var writers = new ArrayList<Future<?>>();
            for (int i = 0; i < 4; i++) {
                writers.add(pool.submit(() -> {
                    try (var c = TestDb.connect()) {
                        c.setAutoCommit(false);
                        while (!stop.get()) {
                            xids.add(queryLong(c, "select pg_current_xact_id()::text::bigint"));
                            execute(c, "insert into " + names.base()
                                    + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
                            c.commit();
                        }
                    }
                    return null;
                }));
            }
            var makers = new ArrayList<Future<Integer>>();
            for (int i = 0; i < 8; i++) {
                makers.add(pool.submit(() -> {
                    int made = 0;
                    try (var c = TestDb.connect()) {
                        for (int j = 0; j < 50; j++) {
                            made += ticks.make(c, Duration.ZERO) ? 1 : 0;
                        }
                    }
                    return made;
                }));
            }
            int made = 0;
            for (var f : makers) {
                made += f.get(60, SECONDS);
            }
            stop.set(true);
            for (var f : writers) {
                f.get(60, SECONDS);
            }
            assertThat(made).isEqualTo(400);
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }

        try (var c = TestDb.connect()) {
            var xmaxes = new ArrayList<Long>();
            try (var st = c.createStatement();
                    var rs = st.executeQuery(
                            "select pg_snapshot_xmax(snapshot)::text::bigint from " + names.tick() + " order by id")) {
                while (rs.next()) {
                    xmaxes.add(rs.getLong(1));
                }
            }
            assertThat(xmaxes).hasSize(401).isSorted();

            // per consecutive tick pair (k, k+1): writer xids visible in k but not in k+1 (lost), and the reverse
            try (var ps = c.prepareStatement("select"
                    + " count(*) filter (where pg_visible_in_snapshot(x, t.snapshot)"
                    + " and not pg_visible_in_snapshot(x, t.next)),"
                    + " count(*) filter (where not pg_visible_in_snapshot(x, t.snapshot)"
                    + " and pg_visible_in_snapshot(x, t.next))"
                    + " from (select snapshot, lead(snapshot) over (order by id) next from " + names.tick() + ") t,"
                    + " lateral (select v::text::xid8 x from unnest(?::bigint[]) v) w"
                    + " where t.next is not null")) {
                ps.setArray(1, c.createArrayOf("bigint", xids.toArray()));
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    assertThat(rs.getLong(1)).as("xids visible in tick k but not in k+1").isZero();
                    assertThat(rs.getLong(2)).as("xids committed between ticks").isPositive();
                }
            }
        }
    }

    @Test
    void frozenHolderIsReleased() throws Exception {
        var frozen = TestDb.connect();
        try (var c = TestDb.connect()) {
            frozen.setAutoCommit(false);
            ticks.lockForTest(frozen); // then never touched again: idle in transaction
            long start = System.nanoTime();

            assertThat(ticks.make(c, Duration.ZERO)).as("tick lock held by the frozen session").isFalse();
            assertSessionRestored(c);
            boolean made = false;
            while (!made && System.nanoTime() - start < Duration.ofSeconds(8).toNanos()) {
                made = ticks.make(c, Duration.ZERO);
            }

            assertThat(made).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(8));
            assertThat(frozen.isValid(1)).as("frozen session terminated by Postgres").isFalse();
        } finally {
            try {
                frozen.close();
            } catch (Exception ignored) {
                // its session is gone
            }
        }
    }

    @Test
    void tickIsMonotonicEvenWhenSessionDefaultsToRepeatableRead() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        try (var holder = TestDb.connect();
                var maker = TestDb.connect();
                var throttled = TestDb.connect();
                var writer = TestDb.connect();
                var observer = TestDb.connect()) {
            execute(observer, "update " + names.tick() + " set created_at = created_at - interval '1 hour'");
            for (var c : List.of(maker, throttled)) {
                execute(c, "set session characteristics as transaction isolation level repeatable read");
            }
            long makerPid = queryLong(maker, "select pg_backend_pid()");
            long throttledPid = queryLong(throttled, "select pg_backend_pid()");
            holder.setAutoCommit(false);
            ticks.lockForTest(holder);

            // queued in this order behind the holder; both take their lock-statement snapshot now
            var made = pool.submit(() -> ticks.make(maker, Duration.ZERO));
            awaitLockWait(observer, makerPid);
            var madeThrottled = pool.submit(() -> ticks.make(throttled, Duration.ofSeconds(1)));
            awaitLockWait(observer, throttledPid);
            writer.setAutoCommit(false);
            long xid = queryLong(writer, "select pg_current_xact_id()::text::bigint");
            writer.commit();
            holder.commit();

            assertThat(made.get(10, SECONDS)).isTrue();
            try (var ps = observer.prepareStatement("select pg_visible_in_snapshot((?::bigint)::text::xid8, snapshot)"
                    + " from " + names.tick() + " order by id desc limit 1")) {
                ps.setLong(1, xid);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    assertThat(rs.getBoolean(1)).as("xid committed before the tick lock was granted").isTrue();
                }
            }
            assertThat(madeThrottled.get(10, SECONDS)).as("throttled by the tick made just before").isFalse();
            assertThat(queryLong(observer, "select count(*) from " + names.tick())).isEqualTo(2);
            assertSessionRestored(maker);
            assertThat(queryString(maker, "show transaction_isolation")).isEqualTo("repeatable read");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void readsAndDeletesTicks() throws Exception {
        try (var c = TestDb.connect()) {
            var first = ticks.latest(c);
            ticks.make(c, Duration.ZERO);
            var middle = ticks.latest(c);
            ticks.make(c, Duration.ZERO);
            var last = ticks.latest(c);

            assertThat(List.of(first.id(), middle.id(), last.id())).isSorted().doesNotHaveDuplicates();
            assertThat(last.snapshot().xmax()).isGreaterThanOrEqualTo(first.snapshot().xmax());
            assertThat(last.createdAt()).isAfterOrEqualTo(first.createdAt());
            assertThat(ticks.get(c, middle.id())).usingRecursiveComparison().isEqualTo(middle);
            assertThat(ticks.between(c, first.id(), last.id())).usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(middle, last);
            assertThat(ticks.between(c, last.id(), last.id() + 100)).isEmpty();

            assertThat(ticks.deleteBelow(c, middle.id())).isEqualTo(1);

            assertThatThrownBy(() -> ticks.get(c, first.id())).isInstanceOf(IllegalStateException.class);
            assertThat(ticks.between(c, 0, last.id())).extracting(Ticks.Tick::id)
                    .containsExactly(middle.id(), last.id());
        }
    }

    @Test
    void ticksRecordTheIdHighWaterMark() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(ticks.latest(c).maxId()).as("bootstrap tick, no id allocated yet").isZero();

            execute(c, "insert into " + names.base() + " (shard, message_id, topic)"
                    + " select 0, gen_random_uuid(), 't' from generate_series(1, 5)");
            c.setAutoCommit(false);
            execute(c, "insert into " + names.base()
                    + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
            c.rollback();
            c.setAutoCommit(true);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(ticks.latest(c).maxId()).as("rolled-back ids count too").isEqualTo(6);
        }
    }

    @Test
    void aTickRecordsTheShardsThatGainedRowsSinceThePreviousTick() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            insertRow(c, 3); // autocommit: three transactions
            insertRow(c, 7);
            insertRow(c, 3);

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).containsExactly(3, 7);

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).as("no row since the previous tick").isEmpty();
        }
    }

    @Test
    void aTransactionRunningAtThePreviousTickCountsInTheTickThatSeesItCommitted() throws Exception {
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            writer.setAutoCommit(false);
            insertRow(writer, 5);
            long xid = queryLong(writer, "select pg_current_xact_id()::text::bigint");
            queryLong(c, "select pg_current_xact_id()::text::bigint"); // a later xid completes: the writer is below xmax
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(ticks.latest(c).snapshot().xip()).as("in flight at P").contains(xid);
            assertThat(shards(c)).doesNotContain(5);

            writer.commit();
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(shards(c)).containsExactly(5); // P.xip minus C.xip
        }
    }

    @Test
    void aTransactionRunningAtATickCountsInTheTickWhereItBecomesVisible() throws Exception {
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            writer.setAutoCommit(false);
            insertRow(writer, 6); // xid at or above the next tick's xmax, or in its xip
            insertRow(c, 1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).containsExactly(1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).as("still running").isEmpty();

            writer.commit();
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(shards(c)).containsExactly(6);
        }
    }

    @Test
    void aRolledBackTransactionMarksAtMostItsOwnShard() throws Exception {
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            writer.setAutoCommit(false);
            insertRow(writer, 2);
            writer.rollback();

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).as("a superset is allowed, nothing beyond its shard").isSubsetOf(2);

            insertRow(c, 2);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).containsExactly(2);
        }
    }

    @Test
    void aTickWithoutAPreviousOneStoresNoShardSetMeaningAll() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(shards(c)).as("the bootstrap tick").isNull();
            insertRow(c, 4);
            execute(c, "delete from " + names.tick());

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(shards(c)).isNull();
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).isEmpty();
        }
    }

    @Test
    void aTickProbesEachShardOnceHoweverManyRowsItCovers() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            execute(c, "insert into " + names.base() + " (shard, message_id, topic, payload)"
                    + " select g % " + SHARDS + ", gen_random_uuid(), 't', repeat('x', 100)::bytea"
                    + " from generate_series(1, 20000) g");

            // the plan Ticks.make runs: generic (see Ticks.lock), where an exists per shard may become one hashed pass
            // over all 20 000 rows
            c.setAutoCommit(false);
            execute(c, "set local plan_cache_mode = force_generic_plan");
            execute(c, "prepare tick_insert(int, bigint) as " + ticks.insert(c).replaceFirst("\\?", "\\$1")
                    .replaceFirst("\\?", "\\$2"));
            long buffers = 0;
            try (var st = c.createStatement();
                    var rs = st.executeQuery("explain (analyze, buffers) execute tick_insert(" + SHARDS + ", 0)")) {
                while (rs.next() && buffers == 0) { // the first Buffers line: the whole statement's
                    var m = Pattern.compile("(hit|read)=(\\d+)").matcher(rs.getString(1));
                    while (m.find()) {
                        buffers += Long.parseLong(m.group(2));
                    }
                }
            }
            c.rollback();

            assertThat(buffers).as("buffers of a tick over 20 000 new rows on %d shards", SHARDS).isLessThan(200);
        }
    }

    /** One row of {@code shard} in the caller's transaction (its own one in autocommit). */
    private void insertRow(Connection c, int shard) throws Exception {
        execute(c, "insert into " + names.base() + " (shard, message_id, topic) values (" + shard
                + ", gen_random_uuid(), 't')");
    }

    /** The newest tick's {@code shards}, null when it has none. */
    @Nullable
    private List<Integer> shards(Connection c) throws Exception {
        try (var st = c.createStatement();
                var rs = st.executeQuery("select shards from " + names.tick() + " order by id desc limit 1")) {
            rs.next();
            var array = rs.getArray(1);
            return array == null ? null : Arrays.stream((Short[]) array.getArray()).map(Short::intValue).toList();
        }
    }

    private static void awaitLockWait(Connection observer, long pid) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (queryLong(observer, "select count(*) from pg_locks where not granted and pid = " + pid) == 0) {
            assertThat(System.nanoTime()).as("pid %d waits for a lock", pid).isLessThan(deadline);
            Thread.sleep(1);
        }
    }

    private static void assertSessionRestored(Connection c) throws Exception {
        assertThat(c.getAutoCommit()).isTrue();
        assertThat(queryString(c, "show lock_timeout")).isEqualTo("0");
        assertThat(queryString(c, "show idle_in_transaction_session_timeout")).isEqualTo("0");
        assertThat(queryString(c, "show plan_cache_mode")).isEqualTo("auto");
    }
}
