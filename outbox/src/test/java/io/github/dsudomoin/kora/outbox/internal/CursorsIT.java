package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
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
    void latestAllReturnsNewestPerShard() throws Exception {
        var cursors = new Cursors(bootstrap());
        try (var c = TestDb.connect()) {
            var initial = cursors.latestAll(c, 4);
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

            assertThat(cursors.latestAll(c, 4)).containsExactly(newest0, initial.get(1), newest2, initial.get(3));
            assertThat(cursors.latest(c, 0)).isEqualTo(newest0);
            assertThat(cursors.latest(c, 2)).isEqualTo(newest2);
            assertThat(newest0).isEqualTo(new Cursor(0, 9, null, 0, null, 0, T1, T2));
            assertThat(newest2.parkedDueAt()).isNull();
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
        var cursors = new Cursors(bootstrap());
        try (var c = TestDb.connect()) {
            var initial = cursors.latestAll(c, 4);
            for (int i = 1; i <= 3; i++) {
                cursors.append(c, initial.get(0).withProgress(i, null, 0));
                cursors.append(c, initial.get(3).withProgress(i, null, 0));
            }
            var before = cursors.latestAll(c, 4);

            assertThat(cursors.deleteSuperseded(c)).isEqualTo(6);

            assertThat(cursors.latestAll(c, 4)).isEqualTo(before);
            assertThat(queryLong(c, "select count(*) from " + TableNames.of(config.table()).cursor())).isEqualTo(4);
            assertThat(cursors.deleteSuperseded(c)).isZero();
        }
    }

    @Test
    void latestAllIsFastWithLongHistory() throws Exception {
        config.shards(32);
        var names = bootstrap();
        var cursors = new Cursors(names);
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            execute(c, "insert into " + names.cursor() + " (shard, tick_id, last_id)"
                    + " select 0, 1, g from generate_series(1, 50000) g");
            c.commit();
            c.setAutoCommit(true);

            for (int i = 0; i < 5; i++) {
                cursors.latestAll(c, 32);
            }
            long[] nanos = new long[20];
            for (int i = 0; i < nanos.length; i++) {
                long start = System.nanoTime();
                var all = cursors.latestAll(c, 32);
                nanos[i] = System.nanoTime() - start;
                assertThat(all).hasSize(32);
                assertThat(all.get(0).lastId()).isEqualTo(50000);
            }
            Arrays.sort(nanos);

            assertThat(nanos[nanos.length / 2] / 1_000_000.0).as("median latestAll ms").isLessThanOrEqualTo(20.0);
        }
    }

    @Test
    void aShardIsBehindWhenATickAfterItsCursorMayHoldItsRows() throws Exception {
        var names = bootstrap(); // 4 shards, every cursor on the bootstrap tick
        var cursors = new Cursors(names);
        try (var c = TestDb.connect()) {
            long first = queryLong(c, "select max(id) from " + names.tick());
            assertThat(behind(cursors.positions(c, 4, first))).as("on the latest tick")
                    .containsExactly(false, false, false, false);

            long t1 = tick(c, names, "'{1}'", 10);
            long t2 = tick(c, names, "'{}'", 20);
            assertThat(behind(cursors.positions(c, 4, t2))).containsExactly(false, true, false, false);
            long t3 = tick(c, names, "'{2,3}'", 30);
            assertThat(behind(cursors.positions(c, 4, t3))).containsExactly(false, true, true, true);
            assertThat(behind(cursors.positions(c, 4, t1))).as("ticks above the latest passed in do not count")
                    .containsExactly(false, true, false, false);

            var s1 = cursors.latest(c, 1).withProgress(t2, null, 0).withParkedDueAt(T1);
            cursors.append(c, s1); // past the tick that held its rows
            // a batch in progress: candidate on its own
            var s2 = cursors.latest(c, 2).withProgress(t1, t3, 7).withParkedDueAt(T2);
            cursors.append(c, s2);
            var positions = cursors.positions(c, 4, t3);
            assertThat(behind(positions)).containsExactly(false, false, true, true);
            assertThat(positions).extracting(Cursors.Position::cursor).isEqualTo(cursors.latestAll(c, 4));
            assertThat(positions.get(1).cursor()).isEqualTo(s1);
            assertThat(positions.get(2).cursor()).isEqualTo(s2);

            long t4 = tick(c, names, "null", 40); // an older instance's tick: any shard may have rows
            assertThat(behind(cursors.positions(c, 4, t4))).containsExactly(true, true, true, true);
        }
    }

    @Test
    void anIdleShardIsBehindOnceItsTickIsFiveSecondsOlderThanTheLatest() throws Exception {
        var names = bootstrap();
        var cursors = new Cursors(names);
        try (var c = TestDb.connect()) {
            assertThat(behind(cursors.positions(c, 4, tick(c, names, "'{}'", 4_999)))).containsOnly(false);
            assertThat(behind(cursors.positions(c, 4, tick(c, names, "'{}'", 5_000)))).as("idle append due")
                    .containsOnly(true);
            assertThat(behind(cursors.positions(c, 4, tick(c, names, "'{}'", -1)))).as("the clock stepped back")
                    .containsOnly(true);
        }
    }

    @Test
    void aShardWhoseCursorTickIsGoneIsBehind() throws Exception {
        var names = bootstrap();
        var cursors = new Cursors(names);
        try (var c = TestDb.connect()) {
            long latest = tick(c, names, "'{}'", 10);
            execute(c, "delete from " + names.tick() + " where id < " + latest);

            assertThat(behind(cursors.positions(c, 4, latest))).containsOnly(true);
        }
    }

    /**
     * A tick with {@code shards} (SQL), made {@code millis} after the bootstrap tick by the database clock; returns its
     * id.
     */
    private static long tick(Connection c, TableNames names, String shards, long millis) throws Exception {
        return queryLong(c, "insert into " + names.tick() + " (snapshot, max_id, shards, created_at)"
                + " select pg_current_snapshot(), 0, " + shards + "::smallint[], min(created_at) + interval '"
                + millis + " milliseconds' from " + names.tick() + " returning id");
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
