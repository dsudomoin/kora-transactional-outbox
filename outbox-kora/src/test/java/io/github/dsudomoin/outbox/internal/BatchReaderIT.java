package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.internal.BatchReader.BatchRow;
import io.github.dsudomoin.outbox.internal.BatchReader.ShardRead;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

class BatchReaderIT {

    private static final byte[] PAYLOAD = bytes("payload");

    private final TestDatabase db = TestDb.newDatabase(4);
    private final TestOutboxConfig config = new TestOutboxConfig().shards(1);
    private final List<Connection> open = new ArrayList<>();
    private TableNames names;
    private Ticks ticks;
    private PgOutbox outbox;
    private BatchReader reader;

    @AfterEach
    void release() throws SQLException {
        for (var c : open) {
            c.close();
        }
        db.release();
    }

    @Test
    void batchContainsExactlyTransactionsCommittedBetweenTicks() throws Exception {
        bootstrap();
        var p = tick();
        var a = begin();
        var aId = outbox.send(a, OutboxMessage.of("t", "a", bytes("a")));
        var b = OutboxMessage.of("t", "b", PAYLOAD).withHeader("h", bytes("v")).withHeader("null", null);
        var bId = sendCommitted(b);
        var c = tick();
        a.commit();
        var d = tick();

        var pc = read(0, p, c, 0, 100, true);
        assertThat(messageIds(pc)).containsExactly(bId);
        var record = pc.get(0).record();
        assertThat(record.topic()).isEqualTo("t");
        assertThat(record.key()).isEqualTo(b.key());
        assertThat(record.payload()).isEqualTo(PAYLOAD);
        assertThat(record.headers()).isEqualTo(b.headers());
        assertThat(record.attempts()).isZero();
        assertThat(record.createdAt()).isCloseTo(Instant.now(), within(1, ChronoUnit.MINUTES));
        assertThat(pc.get(0).blocked()).isFalse();
        assertThat(messageIds(read(0, c, d, 0, 100, true))).containsExactly(aId);
        assertThat(read(1, p, d, 0, 100, true)).as("another shard").isEmpty();
    }

    @Test
    void inFlightAtPrevAppearsWhenCommitted() throws Exception {
        bootstrap();
        var o = tick();
        var a = begin();
        var aId = outbox.send(a, OutboxMessage.of("t", "a", bytes("a")));
        long aXid = queryLong(a, "select pg_current_xact_id()::text::bigint");
        var bId = sendCommitted(OutboxMessage.of("t", "b", PAYLOAD)); // xid above a's, committed before p
        var p = tick();
        assertThat(p.xip()).as("a is in flight at p, below p's xmax").contains(aXid);
        a.commit();
        var c = tick();

        assertThat(messageIds(read(0, o, p, 0, 100, true))).containsExactly(bId);
        assertThat(messageIds(read(0, p, c, 0, 100, true))).containsExactly(aId);
    }

    @Test
    void inFlightAtPrevStillRunningAtTargetIsExcluded() throws Exception {
        bootstrap();
        var a = begin();
        var aId = outbox.send(a, OutboxMessage.of("t", "a", bytes("a")));
        long aXid = queryLong(a, "select pg_current_xact_id()::text::bigint");
        sendCommitted(OutboxMessage.of("t", "b", PAYLOAD)); // a later xid completes, so p lists a in its xip
        var p = tick();
        var cId = sendCommitted(OutboxMessage.of("t", "c", PAYLOAD));
        var c = tick();
        assertThat(p.xip()).as("a is in flight at p").contains(aXid);
        assertThat(c.xip()).as("a is still in flight at c").contains(aXid);
        a.commit();
        var d = tick();

        assertThat(messageIds(read(0, p, c, 0, 100, true))).containsExactly(cId);
        assertThat(messageIds(read(0, c, d, 0, 100, true))).containsExactly(aId);
    }

    @Test
    void transactionInFlightAtBothTicksIsNotScanned() throws Exception {
        bootstrap();
        var xid = new CompletableFuture<Long>();
        var release = new CountDownLatch(1);
        var longTx = CompletableFuture.runAsync(() -> db.inTx((SqlFunction0<Void>) () -> {
            outbox.sendAll(IntStream.range(0, 50_000).mapToObj(i -> OutboxMessage.of("t", "x" + i % 10, PAYLOAD))
                    .toList());
            xid.complete(queryLong(db.currentConnection(), "select pg_current_xact_id()::text::bigint"));
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return null;
        }));
        PgSnapshot c;
        try {
            long x = xid.get(60, TimeUnit.SECONDS);
            sendCommitted(OutboxMessage.of("t", "a", PAYLOAD)); // a later xid completes, so p lists x in its xip
            var p = tick();
            var bId = sendCommitted(OutboxMessage.of("t", "b", PAYLOAD));
            c = tick();
            assertThat(p.xip()).as("x is in flight at p").contains(x);
            assertThat(c.xip()).as("x is still in flight at c").contains(x);

            assertThat(messageIds(read(0, p, c, 0, 100, true))).containsExactly(bId);
            assertThat(xipBranchBuffers(p, c)).as("buffers read for x's 50 000 invisible rows").isLessThan(10);
        } finally {
            release.countDown();
        }
        longTx.get(60, TimeUnit.SECONDS);
        var d = tick();
        assertThat(read(0, c, d, 0, 100, true)).hasSize(100)
                .allMatch(r -> new String(r.record().key(), UTF_8).startsWith("x"));
    }

    @Test
    void orderedByIdAndChunkedByLastId() throws Exception {
        bootstrap();
        var p = tick();
        var a = begin(); // lower xid than b, but rows on both sides of b's in id order
        var sent = new ArrayList<UUID>();
        sent.add(outbox.send(a, OutboxMessage.of("t", "a", bytes("1"))));
        sent.addAll(db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(
                OutboxMessage.of("t", "b", bytes("2")),
                OutboxMessage.of("t", "c", bytes("3")),
                OutboxMessage.of("u", (byte[]) null),
                OutboxMessage.of("t", "b", bytes("5")),
                OutboxMessage.of("t", "d", bytes("6"))))));
        for (int i = 7; i <= 10; i++) {
            sent.add(outbox.send(a, OutboxMessage.of("t", "a", bytes(Integer.toString(i)))));
        }
        a.commit();
        var c = tick();

        var chunks = new ArrayList<List<BatchRow>>();
        long lastId = 0;
        List<BatchRow> chunk;
        do {
            chunk = read(0, p, c, lastId, 4, true);
            chunks.add(chunk);
            if (!chunk.isEmpty()) {
                lastId = chunk.get(chunk.size() - 1).record().id();
            }
        } while (chunk.size() == 4);

        assertThat(chunks).extracting(List::size).containsExactly(4, 4, 2);
        var all = chunks.stream().flatMap(List::stream).toList();
        assertThat(all).extracting(r -> r.record().id()).isSorted().doesNotHaveDuplicates();
        assertThat(messageIds(all)).containsExactlyElementsOf(sent);
        assertThat(read(0, p, c, lastId, 4, true)).isEmpty();
    }

    @Test
    void blockedFlagReflectsRetryTable() throws Exception {
        bootstrap();
        var k = bytes("k");
        try (var c = TestDb.connect()) {
            insertRetry(c, 1, 0, "t", k);
            insertRetry(c, 2, 0, "t", null);
            insertRetry(c, 3, 1, "u", k); // another shard does not block shard 0
        }
        var p = tick();
        var blocked1 = sendCommitted(OutboxMessage.of("t", k, PAYLOAD));
        var otherKey = sendCommitted(OutboxMessage.of("t", "k2", PAYLOAD));
        var otherTopic = sendCommitted(OutboxMessage.of("u", k, PAYLOAD));
        var unkeyed = sendCommitted(OutboxMessage.of("t", PAYLOAD));
        var blocked2 = sendCommitted(OutboxMessage.of("t", k, PAYLOAD));
        var c = tick();

        var checked = read(0, p, c, 0, 100, true);
        assertThat(messageIds(checked)).containsExactly(blocked1, otherKey, otherTopic, unkeyed, blocked2);
        assertThat(checked).extracting(BatchRow::blocked).containsExactly(true, false, false, false, true);

        var unchecked = read(0, p, c, 0, 100, false);
        assertThat(messageIds(unchecked)).containsExactlyElementsOf(messageIds(checked));
        assertThat(unchecked).extracting(BatchRow::blocked).containsOnly(false);
    }

    @Test
    void prunesToCoveringPartitions() throws Exception {
        config.xidsPerPartition(1000).premake(4);
        try (var c = TestDb.connect()) { // keep the batch inside one range
            long offset = queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint % 1000");
            if (offset > 800) {
                TestDb.burnXids((int) (1000 - offset) + 10);
            }
        }
        bootstrap();
        var a = begin();
        var aId = outbox.send(a, OutboxMessage.of("t", "a", PAYLOAD));
        sendCommitted(OutboxMessage.of("t", "b", PAYLOAD));
        var p = tick();
        a.commit();
        var cId = sendCommitted(OutboxMessage.of("t", "c", PAYLOAD));
        var c = tick();
        assertThat(p.xip()).as("both union branches have candidates").isNotEmpty();
        assertThat(messageIds(read(0, p, c, 0, 100, true))).containsExactly(aId, cId);

        try (var conn = TestDb.connect()) {
            long ranges = queryLong(conn, "select count(*) from pg_inherits where inhparent = to_regclass('"
                    + names.base() + "') and inhrelid::regclass::text like '" + names.partitionPrefix() + "%'");
            assertThat(ranges).isGreaterThanOrEqualTo(4);
            for (boolean checkBlocked : List.of(true, false)) {
                var used = partitionsInPlan(conn, checkBlocked, p, c);
                assertThat(used).as("partitions in the plan (checkBlocked = %s)", checkBlocked).hasSizeBetween(1, 2);
            }
            var empty = new PgSnapshot(p.xmin(), p.xmax(), new long[0]);
            assertThat(partitionsInPlan(conn, true, empty, c)).as("empty xip prunes its branch").hasSize(1);
        }
        var q = tick();
        var noneInFlight = new PgSnapshot(q.xmin(), q.xmax(), new long[0]); // binds an empty xip array
        var dId = sendCommitted(OutboxMessage.of("t", "d", PAYLOAD));
        assertThat(messageIds(read(0, noneInFlight, tick(), 0, 100, true))).containsExactly(dId);
    }

    @Test
    void readManyEqualsPerShardReads() throws Exception {
        config.shards(16);
        bootstrap();
        var random = new Random(42);
        var o = tick();
        var a = begin(); // in flight at p, committed before c: the xip branch
        for (int i = 0; i < 40; i++) {
            outbox.send(a, OutboxMessage.of("t", "a" + random.nextInt(20), PAYLOAD)); // keys no other writer locks
        }
        sendRandom(random, 10); // a later xid ends, so p lists a in its xip
        var p = tick();
        a.commit();
        sendRandom(random, 10);
        var c = tick();
        try (var conn = TestDb.connect()) { // some keys blocked in the retry table
            execute(conn, "insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at)"
                    + " select 1000000 + row_number() over (), shard, gen_random_uuid(), topic, key, now() from"
                    + " (select distinct shard, topic, key from " + names.base() + " order by key limit 15) k");
        }
        assertThat(p.xip()).isNotEmpty();
        var spans = List.of(new PgSnapshot[] {o, p}, new PgSnapshot[] {p, c}, new PgSnapshot[] {o, c});

        for (int round = 0; round < 5; round++) {
            var reads = new ArrayList<ShardRead>();
            for (int shard : shuffled(random, 16)) {
                var span = spans.get(random.nextInt(spans.size()));
                var all = read(shard, span[0], span[1], 0, 1000, true);
                long lastId = all.isEmpty() || random.nextBoolean() ? 0
                        : all.get(random.nextInt(all.size())).record().id();
                reads.add(new ShardRead(shard, span[0], span[1], lastId, 1 + random.nextInt(30), random.nextBoolean()));
            }
            var batch = readMany(reads, 100_000);

            assertThat(batch.cut()).isFalse();
            assertThat(batch.rows().keySet()).as("every shard, in the order read")
                    .containsExactlyElementsOf(reads.stream().map(ShardRead::shard).toList());
            for (var r : reads) {
                assertThat(rowKeys(batch.rows().get(r.shard()))).as("shard %s", r.shard())
                        .isEqualTo(rowKeys(read(r.shard(), r.prev(), r.target(), r.lastId(), r.limit(),
                                r.checkBlocked())));
            }
            assertThat(batch.rows().values().stream().flatMap(List::stream).filter(BatchRow::blocked))
                    .as("the retry table is read").isNotEmpty();
        }
    }

    @Test
    void readManyOfOneShardIsItsRead() throws Exception {
        config.shards(4);
        bootstrap();
        var random = new Random(11);
        var p = tick();
        sendRandom(random, 30);
        var c = tick();
        int shard = 2;
        var all = read(shard, p, c, 0, 1000, true);
        assertThat(all).hasSizeGreaterThan(4);
        var one = List.of(new ShardRead(shard, p, c, 0, 1000, true));

        var prepared = new ArrayList<String>();
        BatchReader.Batch whole;
        try (var conn = recording(TestDb.connect(), prepared)) {
            conn.setAutoCommit(false);
            execute(conn, "set local plan_cache_mode = force_custom_plan");
            whole = reader.readMany(conn, one, 1000);
            conn.commit();
        }
        assertThat(prepared).as("read's own statement, not the group one").contains(reader.sql(true))
                .doesNotContain(reader.manySql());
        assertThat(rowKeys(whole.rows().get(shard))).isEqualTo(rowKeys(all));
        assertThat(whole.cut()).isFalse();
        assertThat(readMany(one, all.size()).cut()).as("exactly the limit counts as cut").isTrue();
        var cut = readMany(one, 3);
        assertThat(cut.cut()).isTrue();
        assertThat(rowKeys(cut.rows().get(shard))).isEqualTo(rowKeys(all.subList(0, 3)));
        var own = readMany(List.of(new ShardRead(shard, p, c, all.get(1).record().id(), 2, true)), 1000);
        assertThat(rowKeys(own.rows().get(shard))).as("its own limit, after lastId")
                .isEqualTo(rowKeys(all.subList(2, 4)));
        assertThat(own.cut()).as("its own limit is no cut").isFalse();
    }

    @Test
    void cutKeepsEachShardsRowsAPrefix() throws Exception {
        config.shards(16);
        bootstrap();
        var random = new Random(7);
        var p = tick();
        sendRandom(random, 20);
        var c = tick();
        var reads = IntStream.range(0, 16).mapToObj(shard -> new ShardRead(shard, p, c, 0, 1000, true)).toList();
        var full = readMany(reads, 100_000);
        int total = full.rows().values().stream().mapToInt(List::size).sum();
        assertThat(full.cut()).isFalse();

        for (int limit : List.of(1, total / 3, total - 1)) {
            var batch = readMany(reads, limit);

            assertThat(batch.cut()).as("limit %s", limit).isTrue();
            assertThat(batch.rows().values().stream().mapToInt(List::size).sum()).isEqualTo(limit);
            for (var r : reads) {
                var rows = rowKeys(batch.rows().get(r.shard()));
                var expected = rowKeys(full.rows().get(r.shard()));
                assertThat(expected.subList(0, rows.size())).as("shard %s, limit %s", r.shard(), limit)
                        .isEqualTo(rows);
            }
        }
        assertThat(readMany(reads, total).cut()).as("exactly the limit counts as cut").isTrue();
    }

    @Test
    void readManyPrunesPartitions() throws Exception {
        config.shards(16).xidsPerPartition(1000).premake(20);
        bootstrap();
        var random = new Random(3);
        var a = begin();
        outbox.send(a, OutboxMessage.of("t", "a", PAYLOAD));
        sendRandom(random, 3);
        var p = tick();
        a.commit();
        sendRandom(random, 3);
        var c = tick();
        assertThat(p.xip()).as("both union branches have candidates").isNotEmpty();
        var reads = IntStream.range(0, 16).mapToObj(shard -> new ShardRead(shard, p, c, 0, 100, shard % 2 == 0))
                .toList();
        assertThat(readMany(reads, 1000).rows().values().stream().mapToInt(List::size).sum()).isPositive();

        try (var conn = TestDb.connect()) {
            long ranges = queryLong(conn, "select count(*) from pg_inherits where inhparent = to_regclass('"
                    + names.base() + "') and inhrelid::regclass::text like '" + names.partitionPrefix() + "%'");
            assertThat(ranges).isGreaterThanOrEqualTo(20);
            var covering = new TreeSet<String>(); // the ranges of [p.xmax, c.xmax) and of p's xip
            for (long x : List.of(p.xmax(), c.xmax() - 1, p.xip()[0], p.xip()[p.xip().length - 1])) {
                covering.add(names.unqualified(names.partitionPrefix()) + x / 1000 * 1000);
            }
            assertThat(partitionsScanned(conn, reads)).as("partitions scanned").isNotEmpty().isSubsetOf(covering);
            // and at plan time: the statement does not even open the others (run-time pruning opens and locks them
            // all, past the fast-path locks with a few dozen partitions)
            assertThat(partitionsPlanned(conn, reads)).as("partitions planned").isNotEmpty().isSubsetOf(covering);
        }
    }

    @Test
    void readManyKeepsAnXipRowInAPartitionBelowTheRange() throws Exception {
        config.shards(4).xidsPerPartition(1000).premake(20);
        bootstrap();
        var a = begin();
        var aId = outbox.send(a, OutboxMessage.of("t", "a", PAYLOAD));
        long aXid = queryLong(a, "select pg_current_xact_id()::text::bigint");
        TestDb.burnXids(1500); // P's xmax lies ranges above a's xid
        var p = tick();
        a.commit();
        var c = tick();
        assertThat(p.xip()).contains(aXid);
        assertThat(p.xmax() / 1000).as("a's range is outside [p.xmax, c.xmax)").isGreaterThan(aXid / 1000);
        int shard = shardOf(aId);
        var reads = IntStream.range(0, 4).mapToObj(s -> new ShardRead(s, p, c, 0, 100, true)).toList();

        var batch = readMany(reads, 1000);

        assertThat(messageIds(batch.rows().get(shard))).containsExactly(aId);
        try (var conn = TestDb.connect()) {
            assertThat(partitionsScanned(conn, reads)).as("the lo/hi bounds keep a's range")
                    .contains(names.unqualified(names.partitionPrefix()) + aXid / 1000 * 1000);
        }
    }

    private int shardOf(UUID messageId) throws SQLException {
        try (var c = TestDb.connect()) {
            return (int) queryLong(c, "select shard from " + names.base() + " where message_id = '" + messageId + "'");
        }
    }

    /**
     * EXPLAIN of the statement {@code read} runs with the parameters bound exactly as {@code read} binds them, under a
     * custom plan. Inlined literals would hide a parameter cast the planner cannot fold (a varchar bound to
     * {@code ?::xid8[]} goes through the stable {@code array_in} and prunes nothing).
     */
    private TreeSet<String> partitionsInPlan(Connection c, boolean checkBlocked, PgSnapshot prev, PgSnapshot target)
            throws SQLException {
        var used = new TreeSet<String>();
        var partition = Pattern.compile(Pattern.quote(names.unqualified(names.partitionPrefix())) + "\\d+");
        execute(c, "set plan_cache_mode = force_custom_plan");
        try (var ps = c.prepareStatement("explain " + reader.sql(checkBlocked))) {
            reader.bind(ps, 0, prev, target, 0, 100);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var m = partition.matcher(rs.getString(1));
                    while (m.find()) {
                        used.add(m.group());
                    }
                }
            }
        }
        return used;
    }

    /**
     * {@code EXPLAIN (ANALYZE, BUFFERS)} of the statement {@code read} runs, bound as {@code read} binds it: shared
     * buffers (hit + read) of the xip branch's scans, 0 when the planner pruned the branch.
     */
    private long xipBranchBuffers(PgSnapshot prev, PgSnapshot target) throws SQLException {
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            execute(c, "set local plan_cache_mode = force_custom_plan");
            var plan = new ArrayList<String>();
            try (var ps = c.prepareStatement("explain (analyze, buffers) " + reader.sql(true))) {
                reader.bind(ps, 0, prev, target, 0, 100);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        plan.add(rs.getString(1));
                    }
                }
            }
            c.rollback();
            long buffers = 0;
            boolean xipScan = false;
            for (var line : plan) {
                if (line.contains("xid = ANY")) {
                    xipScan = true;
                } else if (xipScan && line.contains("Buffers:")) {
                    var m = Pattern.compile("(hit|read)=(\\d+)").matcher(line);
                    while (m.find()) {
                        buffers += Long.parseLong(m.group(2));
                    }
                    xipScan = false;
                }
            }
            return buffers;
        }
    }

    /**
     * {@code EXPLAIN (ANALYZE)} of the statement {@code readMany} runs, bound as it binds it: the partitions whose scans
     * ran (pruned ones are left out of the plan or marked never executed).
     */
    private TreeSet<String> partitionsScanned(Connection c, List<ShardRead> reads) throws SQLException {
        var used = new TreeSet<String>();
        var partition = Pattern.compile(Pattern.quote(names.unqualified(names.partitionPrefix())) + "\\d+");
        c.setAutoCommit(false);
        try (var ps = c.prepareStatement("explain (analyze) " + reader.manySql())) {
            reader.bindMany(ps, reads, 1000);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var line = rs.getString(1);
                    var m = partition.matcher(line);
                    while (m.find() && !line.contains("never executed")) {
                        used.add(m.group());
                    }
                }
            }
        } finally {
            c.rollback();
        }
        return used;
    }

    /** {@code EXPLAIN} (no analyze) of the statement {@code readMany} runs, bound as it binds it: the partitions planned. */
    private TreeSet<String> partitionsPlanned(Connection c, List<ShardRead> reads) throws SQLException {
        var planned = new TreeSet<String>();
        var partition = Pattern.compile(Pattern.quote(names.unqualified(names.partitionPrefix())) + "\\d+");
        c.setAutoCommit(false);
        try (var ps = c.prepareStatement("explain " + reader.manySql())) {
            execute(c, "set local plan_cache_mode = force_custom_plan");
            reader.bindMany(ps, reads, 1000);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var m = partition.matcher(rs.getString(1));
                    while (m.find()) {
                        planned.add(m.group());
                    }
                }
            }
        } finally {
            c.rollback();
        }
        return planned;
    }

    /** {@code c}, adding the SQL of each statement it prepares to {@code prepared}. */
    private static Connection recording(Connection c, List<String> prepared) {
        return (Connection) java.lang.reflect.Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("prepareStatement")) {
                        prepared.add((String) args[0]);
                    }
                    try {
                        return method.invoke(c, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private BatchReader.Batch readMany(List<ShardRead> reads, int totalLimit) throws SQLException {
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            execute(c, "set local plan_cache_mode = force_custom_plan");
            var batch = reader.readMany(c, reads, totalLimit);
            c.commit();
            return batch;
        }
    }

    /** {@code n} committed transactions of 1 to 10 rows each over 100 keys. */
    private void sendRandom(Random random, int n) {
        for (int tx = 0; tx < n; tx++) {
            int rows = 1 + random.nextInt(10);
            var messages = IntStream.range(0, rows)
                    .mapToObj(i -> OutboxMessage.of("t", "k" + random.nextInt(100), PAYLOAD)).toList();
            db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));
        }
    }

    private static List<Integer> shuffled(Random random, int n) {
        var list = new ArrayList<>(IntStream.range(0, n).boxed().toList());
        Collections.shuffle(list, random);
        return list;
    }

    /** What identifies a row and its flag (a record compares its byte arrays by reference). */
    private static List<String> rowKeys(List<BatchRow> rows) {
        return rows.stream().map(r -> r.record().id() + ":" + r.record().messageId() + ":" + r.blocked()).toList();
    }

    private void bootstrap() throws Exception {
        var schema = new OutboxSchema(db, config);
        schema.start();
        names = schema.names();
        ticks = new Ticks(names, config.shards());
        outbox = new PgOutbox(db, schema, config.shards(), () -> {}, TestTelemetry.kora());
        reader = new BatchReader(names);
    }

    private PgSnapshot tick() throws SQLException {
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            return ticks.latest(c).snapshot();
        }
    }

    /** A transaction left open until the test commits it; it holds an xid from its first send. */
    private Connection begin() throws SQLException {
        var c = TestDb.connect();
        open.add(c);
        c.setAutoCommit(false);
        return c;
    }

    private UUID sendCommitted(OutboxMessage m) throws SQLException {
        return db.inTx((SqlFunction0<UUID>) () -> outbox.send(m));
    }

    private List<BatchRow> read(int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit,
            boolean checkBlocked) throws SQLException {
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            execute(c, "set local plan_cache_mode = force_custom_plan");
            var rows = reader.read(c, shard, prev, target, lastId, limit, checkBlocked);
            c.commit();
            return rows;
        }
    }

    private void insertRetry(Connection c, long id, int shard, String topic, @Nullable byte[] key)
            throws SQLException {
        try (var ps = c.prepareStatement("insert into " + names.retry()
                + " (id, shard, message_id, topic, key, created_at) values (?, ?, gen_random_uuid(), ?, ?, now())")) {
            ps.setLong(1, id);
            ps.setShort(2, (short) shard);
            ps.setString(3, topic);
            ps.setBytes(4, key);
            ps.executeUpdate();
        }
    }

    private static List<UUID> messageIds(List<BatchRow> rows) {
        return rows.stream().map(BatchRow::record).map(OutboxRecord::messageId).toList();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
