package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

class CursorsIT {

    private static final Instant T1 = Instant.parse("2026-10-25T10:00:00.123456Z");
    private static final Instant T2 = Instant.parse("2026-10-25T11:30:00.654321Z");

    private final JdbcDatabase db = TestDb.newDatabase(2);
    private final TestOutboxConfig config = new TestOutboxConfig();

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void positionsHoldTheNewestCursorPerShard() throws Exception {
        var cursors = new Cursors(bootstrap(), 4);
        try (var c = TestDb.connect()) {
            var initial = all(cursors, c);
            assertThat(initial).extracting(Cursor::shard).containsExactly(0, 1, 2, 3);
            assertThat(initial.get(0).targetTickId()).isNull();
            assertThat(initial.get(0).pausedUntil()).isNull();
            assertThat(initial.get(0).retryDueAt()).isNull();
            assertThat(initial.get(0).parkedDueAt()).isNull();

            var s0 = initial.get(0);
            cursors.append(c, s0.withProgress(5, 9L, 100).withPause(T1, 2));
            cursors.append(c, s0.withProgress(6, 9L, 200).withRetryDueAt(T2).withParkedDueAt(T1));
            var newest0 = s0.withProgress(9, null, 0).withPause(T2, 3).withRetryDueAt(T1).withParkedDueAt(T2).cleared();
            cursors.append(c, newest0);
            var newest2 = initial.get(2).withProgress(7, 8L, 42).withPause(T1, 1).withRetryDueAt(T2);
            cursors.append(c, newest2);

            assertThat(all(cursors, c)).containsExactly(newest0, initial.get(1), newest2, initial.get(3));
            assertThat(cursors.latest(c, 0)).as("no checkpoint: the cursor's tick, here without a row")
                    .isEqualTo(new Cursors.Current(newest0, 9, null));
            assertThat(cursors.latest(c, 2)).isEqualTo(new Cursors.Current(newest2, 7, null));
            var bootstrapped = cursors.latest(c, 1);
            assertThat(bootstrapped.tick()).as("the tick's row, read with the cursor").usingRecursiveComparison()
                    .isEqualTo(new Ticks(TableNames.of(config.table()), 4).get(c, bootstrapped.effectiveTickId()));
            assertThat(newest0).isEqualTo(new Cursor(0, 9, null, 0, null, 0, T1, T2));
            assertThat(newest2.parkedDueAt()).isNull();
        }
    }

    @Test
    void appendAllWritesOneRowPerShard() throws Exception {
        var names = bootstrap();
        var cursors = new Cursors(names, 4);
        try (var c = TestDb.connect()) {
            var initial = all(cursors, c);
            long rows = queryLong(c, "select count(*) from " + names.cursor());
            var s0 = initial.get(0).withProgress(9, null, 0).withPause(T2, 3).withRetryDueAt(T1).withParkedDueAt(T2);
            var s2 = initial.get(2).withProgress(7, 8L, 42);
            var s3 = initial.get(3).withProgress(5, 6L, 1).withPause(T1, 1).withParkedDueAt(T1);
            cursors.appendAll(c, List.of(s0, s2, s3));
            cursors.appendAll(c, List.of());

            assertThat(queryLong(c, "select count(*) from " + names.cursor())).isEqualTo(rows + 3);
            assertThat(all(cursors, c)).containsExactly(s0, initial.get(1), s2, s3);

            cursors.appendAll(c, List.of(s3.cleared(), s0.withProgress(10, null, 0)));
            assertThat(all(cursors, c)).as("each row the newest of its shard, as append writes it")
                    .containsExactly(s0.withProgress(10, null, 0), initial.get(1), s2, s3.cleared());
        }
    }

    @Test
    void latestOfManyShardsEqualsLatestOfEach() throws Exception {
        var names = bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var cursors = new Cursors(names, config.shards());
        var shards = List.of(3, 0, 1, 2);
        try (var c = TestDb.connect()) {
            assertSameLatest(cursors, c, shards); // no checkpoint
            long b = queryLong(c, "select max(id) from " + names.tick());
            tick(c, names, "B'0100'", 10);
            long t2 = tick(c, names, "B''", 20);
            cursors.checkpoint(c, t2, cursors.positions(c, t2)); // busy 0100 at t2
            cursors.append(c, cursors.latest(c, 3).cursor().withProgress(b, t2, 1)); // a batch in progress
            cursors.append(c, cursors.latest(c, 2).cursor().withProgress(t2 + 50, null, 0)); // its tick has no row
            assertSameLatest(cursors, c, shards);
            assertThat(cursors.latest(c, shards).get(0).effectiveTickId()).isEqualTo(t2);
            assertThat(cursors.latest(c, shards).get(1).effectiveTickId()).isEqualTo(b);
            assertThat(cursors.latest(c, shards).get(2).tick()).isNull();

            // from another history: counts for nothing in both
            execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + (t2 + 1)
                    + ", B'0000', " + (t2 + 1) + ")");
            assertSameLatest(cursors, c, shards);
            assertThat(cursors.latest(c, List.of(1, 2))).containsOnlyKeys(1, 2);
            assertThat(cursors.latest(c, List.of())).isEmpty();
            assertThatThrownBy(() -> cursors.latest(c, List.of(1, 7))).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("shard 7");
        }
    }

    private static void assertSameLatest(Cursors cursors, Connection c, List<Integer> shards) throws Exception {
        var many = cursors.latest(c, shards);
        assertThat(many).containsOnlyKeys(shards);
        for (int shard : shards) {
            assertThat(many.get(shard)).as("shard %s", shard).usingRecursiveComparison()
                    .isEqualTo(cursors.latest(c, shard));
        }
    }

    @Test
    void withersChangeOnlyTheirFields() {
        var c = new Cursor(1, 10, 20L, 30, T1, 4, T2, T1);

        assertThat(c.withProgress(11, null, 0)).isEqualTo(new Cursor(1, 11, null, 0, T1, 4, T2, T1));
        assertThat(c.withPause(T2, 5)).isEqualTo(new Cursor(1, 10, 20L, 30, T2, 5, T2, T1));
        assertThat(c.withRetryDueAt(null)).isEqualTo(new Cursor(1, 10, 20L, 30, T1, 4, null, T1));
        assertThat(c.withRetryDueAt(T1)).isEqualTo(new Cursor(1, 10, 20L, 30, T1, 4, T1, T1));
        assertThat(c.withParkedDueAt(null)).isEqualTo(new Cursor(1, 10, 20L, 30, T1, 4, T2, null));
        assertThat(c.withParkedDueAt(T2)).isEqualTo(new Cursor(1, 10, 20L, 30, T1, 4, T2, T2));
        assertThat(c.cleared()).isEqualTo(new Cursor(1, 10, 20L, 30, null, 0, T2, T1));
    }

    @Test
    void deleteSupersededKeepsLatest() throws Exception {
        var cursors = new Cursors(bootstrap(), 4);
        try (var c = TestDb.connect()) {
            var initial = all(cursors, c);
            for (int i = 1; i <= 3; i++) {
                cursors.append(c, initial.get(0).withProgress(i, null, 0));
                cursors.append(c, initial.get(3).withProgress(i, null, 0));
            }
            var before = all(cursors, c);

            assertThat(cursors.deleteSuperseded(c)).isEqualTo(6);

            assertThat(all(cursors, c)).isEqualTo(before);
            assertThat(queryLong(c, "select count(*) from " + TableNames.of(config.table()).cursor())).isEqualTo(4);
            assertThat(cursors.deleteSuperseded(c)).isZero();
        }
    }

    @Test
    void positionsAreFastWithLongHistory() throws Exception {
        config.shards(32);
        var names = bootstrap();
        var cursors = new Cursors(names, config.shards());
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            execute(c, "insert into " + names.cursor() + " (shard, tick_id, last_id)"
                    + " select 0, 1, g from generate_series(1, 50000) g");
            c.commit();
            c.setAutoCommit(true);

            for (int i = 0; i < 5; i++) {
                all(cursors, c);
            }
            long[] nanos = new long[20];
            for (int i = 0; i < nanos.length; i++) {
                long start = System.nanoTime();
                var all = all(cursors, c);
                nanos[i] = System.nanoTime() - start;
                assertThat(all).hasSize(32);
                assertThat(all.get(0).lastId()).isEqualTo(50000);
            }
            Arrays.sort(nanos);

            assertThat(nanos[nanos.length / 2] / 1_000_000.0).as("median positions ms").isLessThanOrEqualTo(20.0);
        }
    }

    @Test
    void aShardIsBehindWhenATickAfterItsCursorMayHoldItsRows() throws Exception {
        var names = bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var cursors = new Cursors(names, config.shards());
        try (var c = TestDb.connect()) {
            long first = queryLong(c, "select max(id) from " + names.tick());
            assertThat(behind(cursors.positions(c, first))).as("on the latest tick")
                    .containsExactly(false, false, false, false);

            long t1 = tick(c, names, "B'0100'", 10);
            long t2 = tick(c, names, "B''", 20); // no shard gained rows
            assertThat(behind(cursors.positions(c, t2))).containsExactly(false, true, false, false);
            long t3 = tick(c, names, "B'0011'", 30);
            assertThat(behind(cursors.positions(c, t3))).containsExactly(false, true, true, true);
            assertThat(behind(cursors.positions(c, t1))).as("ticks above the latest passed in do not count")
                    .containsExactly(false, true, false, false);

            var s1 = cursors.latest(c, 1).cursor().withProgress(t2, null, 0).withParkedDueAt(T1);
            cursors.append(c, s1); // past the tick that held its rows
            // a batch in progress: never behind, a candidate on its own
            var s2 = cursors.latest(c, 2).cursor().withProgress(t1, t3, 7).withParkedDueAt(T2);
            cursors.append(c, s2);
            var positions = cursors.positions(c, t3);
            assertThat(behind(positions)).containsExactly(false, false, false, true);
            assertThat(positions).extracting(Cursors.Position::cursor).isEqualTo(all(cursors, c));
            assertThat(positions).extracting(Cursors.Position::effectiveTickId).containsExactly(first, t2, t1, first);
            assertThat(positions.get(1).cursor()).isEqualTo(s1);
            assertThat(positions.get(2).cursor()).isEqualTo(s2);

            long t4 = tick(c, names, "null", 40); // an older instance's tick: any shard may have rows
            assertThat(behind(cursors.positions(c, t4))).containsExactly(true, true, false, true);
            for (int s = 0; s < 4; s++) {
                cursors.append(c, cursors.latest(c, s).cursor().withProgress(t4, null, 0));
            }
            long t5 = tick(c, names, "B'01'", 50); // drawn from another shard count: any shard may have rows
            assertThat(behind(cursors.positions(c, t5))).containsExactly(true, true, true, true);
        }
    }

    @Test
    void effectivePositionUsesTheCheckpoint() throws Exception {
        var names = bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var cursors = new Cursors(names, config.shards());
        try (var c = TestDb.connect()) {
            long b = queryLong(c, "select max(id) from " + names.tick());
            tick(c, names, "B'0100'", 10);
            long t2 = tick(c, names, "B''", 20);
            var read = cursors.positions(c, t2);
            assertThat(behind(read)).containsExactly(false, true, false, false);
            assertThat(Cursors.busyTick(read, t2)).as("the busy shard's").isEqualTo(b);

            cursors.checkpoint(c, t2, read); // busy 0100 at t2, busy_tick_id b

            assertThat(queryString(c, "select tick_id || ':' || busy::text || ':' || busy_tick_id from "
                    + names.checkpoint())).isEqualTo(t2 + ":0100:" + b);
            assertThat(cursors.latest(c, 0).effectiveTickId()).as("idle: the checkpoint's tick").isEqualTo(t2);
            assertThat(cursors.latest(c, 1).effectiveTickId()).as("busy: its own").isEqualTo(b);
            assertThat(cursors.latest(c, 0).cursor().tickId()).as("no cursor row written").isEqualTo(b);
            assertThat(cursors.positions(c, t2)).extracting(Cursors.Position::effectiveTickId)
                    .containsExactly(t2, b, t2, t2);
            long t3 = tick(c, names, "B'0010'", 30);
            assertThat(behind(cursors.positions(c, t3))).containsExactly(false, true, true, false);

            cursors.append(c, cursors.latest(c, 1).cursor().withProgress(t3, null, 0)); // a step of shard 1
            var positions = cursors.positions(c, t3);
            assertThat(behind(positions)).containsExactly(false, false, true, false);
            assertThat(positions).extracting(Cursors.Position::effectiveTickId).containsExactly(t2, t3, t2, t2);

            // shard 3 has a batch in progress up to t2 (from its effective tick, t2 itself): busy too
            cursors.append(c, cursors.latest(c, 3).cursor().withProgress(b, t2, 1));
            positions = cursors.positions(c, t3);
            assertThat(behind(positions)).containsExactly(false, false, true, false);

            cursors.checkpoint(c, t3, positions); // replaces the one before
            assertThat(queryString(c, "select tick_id || ':' || busy::text || ':' || busy_tick_id from "
                    + names.checkpoint())).isEqualTo(t3 + ":0011:" + t2);
            assertThat(cursors.positions(c, t3)).extracting(Cursors.Position::effectiveTickId)
                    .as("busy shards at the lowest busy shard's tick").containsExactly(t3, t3, t2, t2);

            // shard 3 ends its batch at t2, below the checkpoint's tick, which the read starts from: behind until it
            // steps, though no tick after t2 marks it
            cursors.append(c, cursors.latest(c, 3).cursor().withProgress(t2, null, 0));
            assertThat(behind(cursors.positions(c, t3))).containsExactly(false, false, true, true);

            // a checkpoint of another shard count counts for nothing
            execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + t3
                    + ", B'00', " + t3 + ")");
            assertThat(cursors.positions(c, t3)).extracting(Cursors.Position::effectiveTickId)
                    .containsExactly(b, t3, b, t2);
            assertThat(cursors.latest(c, 0).effectiveTickId()).isEqualTo(b);
        }
    }

    @Test
    void anchorsMoveIdleShardsToTheirEffectiveTickAndRepairMissingDues() throws Exception {
        var names = bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var cursors = new Cursors(names, config.shards());
        var all = List.of(0, 1, 2, 3);
        try (var c = TestDb.connect()) {
            long b = queryLong(c, "select max(id) from " + names.tick());
            long t2 = tick(c, names, "B'0100'", 10);
            cursors.checkpoint(c, t2, cursors.positions(c, t2)); // 0100: shard 1 busy at b
            cursors.append(c, cursors.latest(c, 2).cursor().withPause(T2, 2)); // paused, idle all the same
            cursors.append(c, cursors.latest(c, 3).cursor().withProgress(b, t2, 7)); // a batch in progress
            assertThat(cursors.lowestTick(c)).isEqualTo(b);
            assertThat(cursors.anchors(c, all, Duration.ofHours(1))).as("all younger than the interval").isEmpty();

            var anchors = cursors.anchors(c, all, Duration.ZERO);

            assertThat(anchors).as("idle shards without a batch in progress")
                    .containsExactly(new Cursor(0, t2, null, 0, null, 0, null, null),
                            new Cursor(2, t2, null, 0, T2, 2, null, null));

            execute(c, "insert into " + names.retry() + " (id, shard, message_id, topic, created_at, next_attempt_at)"
                    + " values (1, 3, gen_random_uuid(), 't', now(), '" + T1 + "')");
            execute(c, "insert into " + names.parked() + " (id, shard, message_id, topic, created_at, due_at)"
                    + " values (2, 1, gen_random_uuid(), 't', now(), '" + T2 + "')");
            assertThat(cursors.anchors(c, all, Duration.ofHours(1))).as("dues missing, whatever the age")
                    .containsExactly(new Cursor(1, b, null, 0, null, 0, null, T2),
                            new Cursor(3, b, t2, 7, null, 0, T1, null));
            assertThat(cursors.anchors(c, List.of(0, 1), Duration.ZERO)).as("only the shards asked")
                    .containsExactly(new Cursor(0, t2, null, 0, null, 0, null, null),
                            new Cursor(1, b, null, 0, null, 0, null, T2));
            cursors.appendAll(c, cursors.anchors(c, all, Duration.ZERO));
            assertThat(cursors.anchors(c, all, Duration.ZERO)).as("written: nothing more").isEmpty();
            assertThat(cursors.lowestTick(c)).isEqualTo(b);
        }
    }

    @Test
    void anchorsAlsoMoveAnIdleShardWhoseTickIsTooManyTicksBelowTheLatest() throws Exception {
        var names = bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var cursors = new Cursors(names, config.shards());
        var all = List.of(0, 1, 2, 3);
        try (var c = TestDb.connect()) {
            long b = queryLong(c, "select max(id) from " + names.tick());
            long edge = b + Cursors.ANCHOR_TICKS; // as under load: every tick young, the cursor far below
            idleAt(c, names, edge);
            assertThat(cursors.anchors(c, all, Duration.ofHours(1))).as("young, and not more ticks behind").isEmpty();

            idleAt(c, names, edge + 1);

            assertThat(cursors.anchors(c, all, Duration.ofHours(1))).extracting(Cursor::tickId)
                    .containsExactly(edge + 1, edge + 1, edge + 1, edge + 1);
        }
    }

    /** A tick at {@code id}, made now, and the checkpoint at it with every shard idle. */
    private static void idleAt(Connection c, TableNames names, long id) throws Exception {
        execute(c, "insert into " + names.tick() + " (id, snapshot, max_id) overriding system value values (" + id
                + ", pg_current_snapshot(), 0)");
        execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + id + ", B'0000', "
                + id + ")");
    }

    @Test
    void aCheckpointAboveTheNewestTickCountsForNothing() throws Exception {
        var names = bootstrap();
        var cursors = new Cursors(names, config.shards());
        try (var c = TestDb.connect()) {
            long b = queryLong(c, "select max(id) from " + names.tick());
            long latest = tick(c, names, "B'0010'", 10);
            // from another history: tick ids went backwards under it; its 0 bits would hide shard 2's rows
            execute(c, "insert into " + names.checkpoint() + " (tick_id, busy, busy_tick_id) values (" + (latest + 1)
                    + ", B'0000', " + (latest + 1) + ")");

            assertThat(cursors.positions(c, latest)).extracting(Cursors.Position::effectiveTickId).containsOnly(b);
            assertThat(behind(cursors.positions(c, latest))).containsExactly(false, false, true, false);
            assertThat(cursors.latest(c, 2).effectiveTickId()).isEqualTo(b);
            assertThat(cursors.ignoredCheckpointTick(c)).as("still there for maintenance to see")
                    .isEqualTo(latest + 1);
        }
    }

    @Test
    void aCheckpointWhoseTickIsGoneIsObsoleteOnlyWhenEveryCursorIsPastIt() throws Exception {
        var names = bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var cursors = new Cursors(names, config.shards());
        try (var c = TestDb.connect()) {
            long b = queryLong(c, "select max(id) from " + names.tick());
            long t1 = tick(c, names, "B''", 10);
            cursors.checkpoint(c, t1, cursors.positions(c, t1));
            long t2 = tick(c, names, "B'1111'", 20);
            for (int shard = 0; shard < 3; shard++) {
                cursors.append(c, cursors.latest(c, shard).cursor().withProgress(t2, null, 0));
            }
            // shard 3's latest row is still on b, below t1: the previous release cleans up to b only, so t1 is gone
            // only by hand or from another history
            execute(c, "delete from " + names.tick() + " where id = " + t1);
            assertThat(cursors.ignoredCheckpointTick(c)).as("a cursor below its tick: foreign").isEqualTo(t1);

            cursors.append(c, cursors.latest(c, 3).cursor().withProgress(t1, null, 0)); // at its tick: past it too
            assertThat(cursors.ignoredCheckpointTick(c)).as("every cursor at or above its tick: obsolete").isNull();
            assertThat(cursors.positions(c, t2)).as("still ignored by the reads: the cursors' own ticks")
                    .extracting(Cursors.Position::effectiveTickId).containsExactly(t2, t2, t2, t1);
            assertThat(b).isLessThan(t1);
        }
    }

    @Test
    void theTickAfterAGapInTickIdsCountsAsEveryShard() throws Exception {
        var names = bootstrap();
        var cursors = new Cursors(names, config.shards());
        try (var c = TestDb.connect()) {
            long gone = tick(c, names, "B'0100'", 10);
            long latest = tick(c, names, "B''", 20);
            execute(c, "delete from " + names.tick() + " where id = " + gone); // by hand: its bits are lost

            assertThat(behind(cursors.positions(c, latest))).containsOnly(true);
        }
    }

    @Test
    void aShardWhoseCursorTickIsGoneIsBehind() throws Exception {
        var names = bootstrap();
        var cursors = new Cursors(names, config.shards());
        try (var c = TestDb.connect()) {
            long latest = tick(c, names, "B'0000'", 10);
            execute(c, "delete from " + names.tick() + " where id < " + latest);

            assertThat(behind(cursors.positions(c, latest))).containsOnly(true);
        }
    }

    /**
     * A tick with {@code dirty} (SQL), made {@code millis} after the bootstrap tick by the database clock, but never
     * after now (a checkpoint counts only on a tick made before it); returns its id.
     */
    private static long tick(Connection c, TableNames names, String dirty, long millis) throws Exception {
        return queryLong(c, "insert into " + names.tick() + " (snapshot, max_id, dirty, created_at)"
                + " select pg_current_snapshot(), 0, " + dirty + "::varbit, least(min(created_at) + interval '"
                + millis + " milliseconds', clock_timestamp()) from " + names.tick() + " returning id");
    }

    /** Every shard's latest cursor, from {@link Cursors#positions}. */
    private static List<Cursor> all(Cursors cursors, Connection c) throws Exception {
        return cursors.positions(c, Long.MAX_VALUE).stream().map(Cursors.Position::cursor).toList();
    }

    private static List<Boolean> behind(List<Cursors.Position> positions) {
        return positions.stream().map(Cursors.Position::behind).toList();
    }

    private TableNames bootstrap() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        return schema.names();
    }
}
