package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

/** The shard sets of ticks made by several makers among concurrent writers, against the batches they stand for. */
class TickShardSetIT {

    private static final int SHARDS = 16;

    private final JdbcDatabase db = TestDb.newDatabase(2);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void everyTickShardSetCoversTheShardsOfItsBatch() throws Exception {
        var schema = new OutboxSchema(db, new TestOutboxConfig().shards(SHARDS));
        schema.init();
        var names = schema.names();
        var stop = new AtomicBoolean();
        var pool = Executors.newFixedThreadPool(15);
        var tasks = new ArrayList<Future<?>>();
        try {
            for (int w = 0; w < 12; w++) { // xids taken early, savepoints, 10 % rolled back
                tasks.add(pool.submit(() -> {
                    try (var c = TestDb.connect()) {
                        c.setAutoCommit(false);
                        var r = ThreadLocalRandom.current();
                        while (!stop.get()) {
                            if (r.nextInt(4) == 0) {
                                execute(c, "select pg_current_xact_id()");
                                Thread.sleep(r.nextInt(40));
                            }
                            for (int i = 1 + r.nextInt(3); i > 0; i--) {
                                var insert = "insert into " + names.base() + " (shard, message_id, topic) values ("
                                        + r.nextInt(SHARDS) + ", gen_random_uuid(), 't')";
                                if (r.nextInt(5) == 0) {
                                    execute(c, "savepoint s");
                                    execute(c, insert);
                                    execute(c, r.nextBoolean() ? "release savepoint s" : "rollback to savepoint s");
                                } else {
                                    execute(c, insert);
                                }
                                if (r.nextInt(3) == 0) {
                                    Thread.sleep(r.nextInt(30));
                                }
                            }
                            if (r.nextInt(10) == 0) {
                                c.rollback();
                            } else {
                                c.commit();
                            }
                        }
                    }
                    return null;
                }));
            }
            for (int m = 0; m < 3; m++) {
                tasks.add(pool.submit(() -> {
                    var ticks = new Ticks(names, SHARDS);
                    try (var c = TestDb.connect()) {
                        while (!stop.get()) {
                            ticks.make(c, Duration.ZERO);
                            Thread.sleep(ThreadLocalRandom.current().nextInt(8));
                        }
                    }
                    return null;
                }));
            }
            Thread.sleep(5000);
            stop.set(true);
            for (var task : tasks) {
                task.get(60, SECONDS);
            }
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }

        try (var c = TestDb.connect()) {
            // per tick C with a set and the tick P before it: the shards holding a row visible in C and not in P
            var batches = " from " + names.tick() + " t cross join lateral (select snapshot from " + names.tick()
                    + " p where p.id < t.id order by p.id desc limit 1) p cross join generate_series(0, " + (SHARDS - 1)
                    + ") s where t.dirty is not null and exists (select 1 from " + names.base() + " o"
                    + " where o.shard = s and o.xid >= pg_snapshot_xmin(p.snapshot)"
                    + " and o.xid < pg_snapshot_xmax(t.snapshot) and pg_visible_in_snapshot(o.xid, t.snapshot)"
                    + " and not pg_visible_in_snapshot(o.xid, p.snapshot))";
            long needed = queryLong(c, "select count(*)" + batches);
            long missing = queryLong(c, "select count(*)" + batches // no bits: no shard
                    + " and case when length(t.dirty) = 0 then true else get_bit(t.dirty, s) = 0 end");
            long recorded = queryLong(c, "select coalesce(sum(bit_count(dirty)), 0) from " + names.tick());
            long ticks = queryLong(c, "select count(*) from " + names.tick() + " where dirty is not null");
            long inFlight = queryLong(c, "select count(*) from " + names.tick()
                    + " where cardinality(array(select pg_snapshot_xip(snapshot))) > 0");
            System.out.printf("%d ticks with a shard set (%d taken with transactions in flight): (tick, shard) pairs"
                    + " needed %d, recorded %d, missing %d%n", ticks, inFlight, needed, recorded, missing);

            assertThat(missing).as("shards with rows in a tick's batch left out of its set").isZero();
            assertThat(needed).isPositive();
            assertThat(inFlight).as("ticks taken while writers were in flight").isPositive();
        }
    }
}
