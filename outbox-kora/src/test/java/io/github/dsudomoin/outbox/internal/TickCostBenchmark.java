package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
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
 * is made, the E commit, and tick C is timed twice: its probe alone, then the whole {@link Ticks#make}. The probe's
 * second branch scans each cluster of P.xip \ C.xip in each shard where the first found nothing. In {@code noRows} and
 * {@code withRows} the E xids are consecutive, so they make one cluster and no committed row lies in its range: the
 * clustered probe's best case. Its bad case is variant {@code interleaved}: the E open one after another, and between
 * consecutive E xids a transaction of K rows (spread over every shard, the next rotated by K) commits before P, so
 * those rows sit inside the cluster's range; the E write nothing, so every shard scans its whole slice of it unless
 * the cluster splits (the rows stay, so the lines after it run on a larger table). The probe alone is the statement
 * the tick runs, bound as it binds it, under the tick lock with the tick's settings, then rolled back: read-only, so
 * its time (one round trip included) does not depend on the WAL flush, unlike the whole make
 * (about 7 round trips and a commit). Prints p50 and p99 of both per (shards, E, variant), after 10 untimed rounds,
 * and in how many rounds C marked every shard ({@code dirty} null: the probe's estimate went over its budget,
 * {@link Ticks#PROBE_BUDGET_MICROS}).
 *
 * <p>Not part of the build: {@code ./gradlew :outbox-kora:test -Pbenchmark --tests '*TickCostBenchmark'}. Properties:
 * {@code bench.jdbcUrl}, {@code bench.user}, {@code bench.password} (default: the test container), {@code bench.shards}
 * (default 32,256,1024), {@code bench.ended} (default 0,100,400), {@code bench.rounds} (default 200),
 * {@code bench.interleavedRows} (the K of variant {@code interleaved}, a line per K; default 1,1000). The E
 * transactions hold one connection each, opened once and kept: on a database with fewer free connection slots than the
 * largest E (the bench stand allows 300), every larger E is cut to the free slots less 10, and the line says so.
 */
@Tag("benchmark")
class TickCostBenchmark {

    private static final int SEED_TRANSACTIONS = 100;
    private static final int SEED_ROWS_PER_TRANSACTION = 10_000;
    private static final int WARMUP_ROUNDS = 10;

    /** {@code interleaved}: the K rows committed between consecutive E xids, 0 for none. */
    private record Variant(String name, boolean withRows, int interleaved) {}

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
        var variants = new ArrayList<>(List.of(new Variant("noRows", false, 0), new Variant("withRows", true, 0)));
        for (int k : ints("bench.interleavedRows", "1,1000")) {
            variants.add(new Variant("interleaved K " + k, false, k));
        }

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
                run(db, tick, open, shards, endedCounts, variants, rounds);
            }
        } finally {
            for (var c : open) {
                c.close();
            }
            db.release();
        }
    }

    private static void run(JdbcDatabase db, Connection tick, List<Connection> open, int shards, int[] endedCounts,
            List<Variant> variants, int rounds) throws SQLException {
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
            int spread = 0;
            for (int ended : endedCounts) {
                var held = open.subList(0, Math.min(ended, open.size()));
                for (var v : variants) {
                    if (v.interleaved() > 0 && held.size() < 2) {
                        continue; // no gap to commit into: noRows
                    }
                    long[] probe = new long[rounds];
                    long[] make = new long[rounds];
                    int everyShard = 0;
                    for (int r = -WARMUP_ROUNDS; r < rounds; r++) {
                        if (v.interleaved() > 0) {
                            for (int i = 0; i < held.size(); i++) { // one by one: the xids must alternate
                                execute(held.get(i), takeXid);
                                if (i < held.size() - 1) {
                                    execute(tick, "insert into " + names.base() + " (shard, message_id, topic)"
                                            + " select (" + spread + " + g) % " + shards + ", gen_random_uuid(), 't'"
                                            + " from generate_series(1, " + v.interleaved() + ") g");
                                    spread = (spread + v.interleaved()) % shards;
                                }
                            }
                        } else {
                            each(held, c -> execute(c, v.withRows() ? withRow : takeXid));
                        }
                        // a later xid ends first, so P's xmax passes the E and P.xip lists them, as among concurrent
                        // writers: xids at or above xmax are in no xip, and the first probe alone would cover them
                        execute(tick, takeXid);
                        assertThat(ticks.make(tick, Duration.ZERO)).isTrue(); // P: the E running
                        each(held, Connection::commit);
                        long probed = probeNanos(ticks, tick, shards); // C's probe alone, P still the newest tick
                        long start = System.nanoTime();
                        boolean made = ticks.make(tick, Duration.ZERO); // C: the E ended since P
                        long took = System.nanoTime() - start;
                        assertThat(made).isTrue();
                        if (r >= 0) {
                            probe[r] = probed;
                            make[r] = took;
                            everyShard += (int) queryLong(tick, "select count(*) from " + names.tick()
                                    + " where id = (select max(id) from " + names.tick() + ") and dirty is null");
                        }
                    }
                    Arrays.sort(probe);
                    Arrays.sort(make);
                    System.out.printf(Locale.ROOT, "tick C | shards %5d | ended %4d%-22s | %-18s"
                                    + " | probe p50 %8.3f ms, p99 %8.3f ms | make p50 %8.3f ms, p99 %8.3f ms"
                                    + " | every shard %d/%d%n",
                            shards, ended, held.size() < ended ? " (only " + held.size() + " opened)" : "",
                            v.name(), ms(probe, 0.50), ms(probe, 0.99), ms(make, 0.50), ms(make, 0.99), everyShard,
                            rounds);
                }
            }
        } finally {
            for (var c : open) {
                c.rollback(); // after a failed round: their row locks would block the drop
            }
            execute(tick, "drop table if exists " + String.join(", ", names.base(), names.tick(), names.cursor(),
                    names.retry(), names.dead(), names.halt(), names.maintenance(), names.dropping(),
                    names.redriven(), names.base() + "_parked", names.checkpoint()) + " cascade");
        }
    }

    /**
     * The nanoseconds of the probe of a tick made now, alone: {@link Ticks#make}'s settings and tick lock, then its
     * probe statement bound as {@code Ticks.insert} binds it (minimum interval 0), executed and its row read; rolled
     * back, so no tick is made.
     */
    private static long probeNanos(Ticks ticks, Connection c, int shards) throws SQLException {
        c.setAutoCommit(false);
        try {
            ticks.lockForTest(c);
            try (var ps = c.prepareStatement(ticks.probe(c))) {
                ps.setInt(1, shards);
                ps.setLong(2, 0);
                long start = System.nanoTime();
                try (var rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    rs.getString(3);
                }
                return System.nanoTime() - start;
            }
        } finally {
            c.rollback();
            c.setAutoCommit(true);
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
