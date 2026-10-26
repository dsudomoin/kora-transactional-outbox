package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.time.Instant;
import java.util.Arrays;
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

            var s0 = initial.get(0);
            cursors.append(c, s0.withProgress(5, 9L, 100).withPause(T1, 2));
            cursors.append(c, s0.withProgress(6, 9L, 200).withRetryDueAt(T2));
            var newest0 = s0.withProgress(9, null, 0).withPause(T2, 3).withRetryDueAt(T1).cleared();
            cursors.append(c, newest0);
            var newest2 = initial.get(2).withProgress(7, 8L, 42).withPause(T1, 1).withRetryDueAt(T2);
            cursors.append(c, newest2);

            assertThat(cursors.latestAll(c, 4)).containsExactly(newest0, initial.get(1), newest2, initial.get(3));
            assertThat(cursors.latest(c, 0)).isEqualTo(newest0);
            assertThat(cursors.latest(c, 2)).isEqualTo(newest2);
            assertThat(newest0).isEqualTo(new Cursor(0, 9, null, 0, null, 0, T1));
        }
    }

    @Test
    void withersChangeOnlyTheirFields() {
        var c = new Cursor(1, 10, 20L, 30, T1, 4, T2);

        assertThat(c.withProgress(11, null, 0)).isEqualTo(new Cursor(1, 11, null, 0, T1, 4, T2));
        assertThat(c.withPause(T2, 5)).isEqualTo(new Cursor(1, 10, 20L, 30, T2, 5, T2));
        assertThat(c.withRetryDueAt(null)).isEqualTo(new Cursor(1, 10, 20L, 30, T1, 4, null));
        assertThat(c.withRetryDueAt(T1)).isEqualTo(new Cursor(1, 10, 20L, 30, T1, 4, T1));
        assertThat(c.cleared()).isEqualTo(new Cursor(1, 10, 20L, 30, null, 0, T2));
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

    private TableNames bootstrap() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.init();
        return schema.names();
    }
}
