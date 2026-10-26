package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.OutboxConfig;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

/**
 * The duration of a tick against the shard count and the transactions that ended since the tick before it (README
 * «Заметки о дизайне»). Per shard count: a fresh outbox, partitions at the library defaults, seeded with 1 000 000
 * rows in 100 transactions over every shard. Per ended count E and round: E open transactions each take an xid (variant
 * {@code withRows}: and insert one row into a random shard), one later transaction ends (so P.xip lists the E), tick P
 * is made, the E commit, and the making of tick C is timed: its second probe looks up every xid of P.xip \ C.xip in
 * each shard where the first found nothing. Timed is the whole {@link Ticks#make}, round trips and commit included.
 * Prints p50 and p99 of tick C per (shards, E, variant), after 10 untimed rounds.
 *
 * <p>Not part of the build: {@code ./gradlew :outbox:test -Pbenchmark --tests '*TickCostBenchmark'}. Properties:
 * {@code bench.jdbcUrl}, {@code bench.user}, {@code bench.password} (default: the test container), {@code bench.shards}
 * (default 32,256,1024), {@code bench.ended} (default 0,100,400), {@code bench.rounds} (default 200). The E
 * transactions hold one connection each, opened once and kept: on a database with fewer free connection slots than the
 * largest E (the bench stand allows 300), every larger E is cut to the free slots less 10, and the line says so.
 */
@Tag("benchmark")
class TickCostBenchmark {

    private static final int SEED_TRANSACTIONS = 100;
    private static final int SEED_ROWS_PER_TRANSACTION = 10_000;
    private static final int WARMUP_ROUNDS = 10;

    @Test
    void tickDuration() throws Exception {
        String url = System.getProperty("bench.jdbcUrl");
        String user = url == null ? TestDb.user() : System.getProperty("bench.user");
        String password = url == null ? TestDb.password() : System.getProperty("bench.password");
        if (url == null) {
            url = TestDb.jdbcUrl();
        }
        int[] shardCounts = ints("bench.shards", "32,256,1024");
        int[] endedCounts = ints("bench.ended", "0,100,400");
        int rounds = Integer.getInteger("bench.rounds", 200);

        var db = TestDb.newDatabase(url, user, password, 2);
        var open = new ArrayList<Connection>();
        try (var tick = DriverManager.getConnection(url, user, password)) {
            long free = queryLong(tick, "select current_setting('max_connections')::int"
                    + " - current_setting('superuser_reserved_connections')::int"
                    + " - coalesce(current_setting('reserved_connections', true)::int, 0)"
                    + " - (select count(*) from pg_stat_activity where backend_type = 'client backend')");
            long wanted = Arrays.stream(endedCounts).max().orElse(0);
            for (long i = Math.min(wanted, free - 10); i > 0; i--) {
                var c = DriverManager.getConnection(url, user, password);
                open.add(c);
                c.setAutoCommit(false);
            }
            System.out.printf("TickCostBenchmark on %s: %d rounds per line, %d connections for the ended"
                    + " transactions (%d free slots)%n", url, rounds, open.size(), free);
            for (int shards : shardCounts) {
                run(db, tick, open, shards, endedCounts, rounds);
            }
        } finally {
            for (var c : open) {
                c.close();
            }
            db.release();
        }
    }

    private static void run(JdbcDatabase db, Connection tick, List<Connection> open, int shards, int[] endedCounts,
            int rounds) throws SQLException {
        var partitions = new OutboxConfig.PartitionsConfig() {};
        var schema = new OutboxSchema(db, new TestOutboxConfig().shards(shards)
                .xidsPerPartition(partitions.xidsPerPartition()).premake(partitions.premake()));
        schema.init();
        var names = schema.names();
        try {
            for (int t = 0; t < SEED_TRANSACTIONS; t++) {
                execute(tick, "insert into " + names.base() + " (shard, message_id, topic) select g % " + shards
                        + ", gen_random_uuid(), 't' from generate_series(1, " + SEED_ROWS_PER_TRANSACTION + ") g");
            }
            execute(tick, "vacuum analyze " + names.base()); // no autovacuum of the seed among the timed rounds
            var ticks = new Ticks(names, shards);
            String takeXid = "select pg_current_xact_id()";
            String withRow = takeXid + "; insert into " + names.base() + " (shard, message_id, topic)"
                    + " values (floor(random() * " + shards + ")::int, gen_random_uuid(), 't')";
            for (int ended : endedCounts) {
                var held = open.subList(0, Math.min(ended, open.size()));
                for (boolean withRows : new boolean[] {false, true}) {
                    long[] nanos = new long[rounds];
                    for (int r = -WARMUP_ROUNDS; r < rounds; r++) {
                        each(held, c -> execute(c, withRows ? withRow : takeXid));
                        // a later xid ends first, so P's xmax passes the E and P.xip lists them, as among concurrent
                        // writers: xids at or above xmax are in no xip, and the first probe alone would cover them
                        execute(tick, takeXid);
                        assertThat(ticks.make(tick, Duration.ZERO)).isTrue(); // P: the E running
                        each(held, Connection::commit);
                        long start = System.nanoTime();
                        boolean made = ticks.make(tick, Duration.ZERO); // C: the E ended since P
                        long took = System.nanoTime() - start;
                        assertThat(made).isTrue();
                        if (r >= 0) {
                            nanos[r] = took;
                        }
                    }
                    Arrays.sort(nanos);
                    System.out.printf(Locale.ROOT,
                            "tick C | shards %5d | ended %4d%-22s | %-8s | p50 %8.3f ms | p99 %8.3f ms%n", shards,
                            ended, held.size() < ended ? " (only " + held.size() + " opened)" : "",
                            withRows ? "withRows" : "noRows", ms(nanos, 0.50), ms(nanos, 0.99));
                }
            }
        } finally {
            for (var c : open) {
                c.rollback(); // after a failed round: their row locks would block the drop
            }
            execute(tick, "drop table if exists " + String.join(", ", names.base(), names.tick(), names.cursor(),
                    names.retry(), names.dead(), names.halt(), names.maintenance(), names.dropping()) + " cascade");
        }
    }

    /** Nearest-rank quantile of sorted nanoseconds, in ms. */
    private static double ms(long[] sorted, double q) {
        return sorted[Math.max(0, (int) Math.ceil(q * sorted.length) - 1)] / 1e6;
    }

    private static int[] ints(String property, String defaults) {
        return Arrays.stream(System.getProperty(property, defaults).split(","))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    private interface SqlAction {
        void run(Connection c) throws SQLException;
    }

    /** {@code action} on every connection, in parallel. */
    private static void each(List<Connection> connections, SqlAction action) {
        connections.parallelStream().forEach(c -> {
            try {
                action.run(c);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
