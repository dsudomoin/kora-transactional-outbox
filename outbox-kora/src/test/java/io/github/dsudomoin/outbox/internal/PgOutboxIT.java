package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACEPARENT;
import static io.github.dsudomoin.outbox.testing.TestTracing.header;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import io.github.dsudomoin.outbox.testing.TestTracing;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlConsumer;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction1;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlRunnable;

class PgOutboxIT {

    private static final byte[] PAYLOAD = bytes("payload");
    private static final String NOT_IN_TX = "inside JdbcConnectionFactory.inTx";

    private final TestDatabase db = TestDb.newDatabase(20);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final AtomicInteger wakeUps = new AtomicInteger();
    private final ExecutorService pool = Executors.newFixedThreadPool(4);
    private OutboxSchema schema;
    private TableNames names;
    private PgOutbox outbox;

    record Row(long id, int shard, long xid, String topic, byte[] key, byte[] payload, byte[] headers) {}

    record Sent(UUID id, long returnedAt) {}

    @BeforeEach
    void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
        outbox = new PgOutbox(db, schema, config.shards(), wakeUps::incrementAndGet, TestTelemetry.kora());
    }

    @AfterEach
    void release() {
        pool.shutdownNow();
        db.release();
        Context.clear();
    }

    @Test
    void sendOutsideTransactionThrows() throws Exception {
        var m = OutboxMessage.of("t", "k", PAYLOAD);

        assertThatThrownBy(() -> outbox.send(m))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NOT_IN_TX);
        assertThatThrownBy(() -> outbox.sendAll(List.of(m)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NOT_IN_TX);
        assertThatThrownBy(() -> db.withConnection((SqlRunnable) () -> outbox.send(m)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NOT_IN_TX);
        assertThat(count()).isZero();
    }

    @Test
    void sendInAutocommitWithConnectionThrows() throws Exception {
        try (var c = TestDb.connect()) {
            assertThatThrownBy(() -> outbox.send(c, OutboxMessage.of("t", "k", PAYLOAD)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(NOT_IN_TX);
        }
        assertThat(count()).isZero();
    }

    @Test
    void sendInsideInTxPersistsRow() throws Exception {
        var m = OutboxMessage.of("orders", "k1", PAYLOAD).withHeader("h1", bytes("v1")).withHeader("h2", null);
        var xid = new AtomicLong();

        var id = db.inTx((SqlFunction1<Connection, UUID>) c -> {
            xid.set(queryLong(c, "select pg_current_xact_id()::text::bigint"));
            return outbox.send(m);
        });

        var row = row(id);
        assertThat(UuidV7.isV7(id)).isTrue();
        assertThat(row.shard()).isEqualTo(Hashing.shard(Hashing.keyHash("orders", m.key()), config.shards()));
        assertThat(row.xid()).isEqualTo(xid.get());
        assertThat(row.topic()).isEqualTo("orders");
        assertThat(row.key()).isEqualTo(m.key());
        assertThat(row.payload()).isEqualTo(PAYLOAD);
        assertThat(HeaderCodec.decode(row.headers())).isEqualTo(m.headers());
    }

    @Test
    void sendUnkeyedStoresNullsOnAnyShard() throws Exception {
        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", (byte[]) null)));

        var row = row(id);
        assertThat(row.shard()).isBetween(0, config.shards() - 1);
        assertThat(row.key()).isNull();
        assertThat(row.payload()).isNull();
        assertThat(row.headers()).isNull();
    }

    @Test
    void sendOnExplicitConnectionInTransaction() throws Exception {
        UUID id;
        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            id = outbox.send(c, OutboxMessage.of("t", "k", PAYLOAD));
            c.commit();
        }

        assertThat(row(id).payload()).isEqualTo(PAYLOAD);
        assertThat(wakeUps).hasValue(0); // no Kora transaction to hook
    }

    @Test
    void rollbackReleasesKeyLockAndIsNeverDelivered() throws Exception {
        var aLocked = new CountDownLatch(1);
        var aBeforeRollback = new AtomicLong();
        var a = pool.submit(() -> db.inTx((SqlRunnable) () -> {
            outbox.send(OutboxMessage.of("t", "k", bytes("a")));
            aLocked.countDown();
            sleep(300);
            aBeforeRollback.set(System.nanoTime());
            throw new SQLException("rollback A");
        }));
        assertThat(aLocked.await(10, SECONDS)).as("A holds the key lock").isTrue();
        var b = pool.submit(() -> db.inTx((SqlFunction0<Long>) () -> {
            outbox.send(OutboxMessage.of("t", "k", bytes("b")));
            return System.nanoTime();
        }));

        assertThatThrownBy(() -> a.get(10, SECONDS)).hasRootCauseMessage("rollback A");
        assertThat(b.get(10, SECONDS)).isGreaterThan(aBeforeRollback.get());
        assertThat(payloadsInIdOrder()).containsExactly("b");
    }

    @Test
    void sameKeyIdsFollowCommitOrder() throws Exception {
        var aLocked = new CountDownLatch(1);
        var aBeforeCommit = new AtomicLong();
        var a = pool.submit(() -> db.inTx((SqlFunction0<List<UUID>>) () -> {
            var first = outbox.send(OutboxMessage.of("t", "k", bytes("a1")));
            aLocked.countDown();
            sleep(300);
            // B is queued on the key lock by now: its row id must not be assigned before it holds the lock
            var second = outbox.send(OutboxMessage.of("t", "k", bytes("a2")));
            aBeforeCommit.set(System.nanoTime());
            return List.of(first, second);
        }));
        assertThat(aLocked.await(10, SECONDS)).as("A holds the key lock").isTrue();
        var b = pool.submit(() -> db.inTx((SqlFunction0<Sent>) () -> {
            var id = outbox.send(OutboxMessage.of("t", "k", bytes("b")));
            return new Sent(id, System.nanoTime());
        }));

        var aIds = a.get(10, SECONDS);
        var bSent = b.get(10, SECONDS);

        assertThat(bSent.returnedAt()).isGreaterThan(aBeforeCommit.get());
        assertThat(row(bSent.id()).id()).isGreaterThan(row(aIds.get(0)).id()).isGreaterThan(row(aIds.get(1)).id());
        assertThat(payloadsInIdOrder()).containsExactly("a1", "a2", "b");
    }

    @Test
    void differentTopicSameKeyDoesNotSerialize() throws Exception {
        assertThat(Hashing.keyHash("t1", bytes("k"))).isNotEqualTo(Hashing.keyHash("t2", bytes("k")));
        var aLocked = new CountDownLatch(1);
        var a = pool.submit(() -> db.inTx((SqlRunnable) () -> {
            outbox.send(OutboxMessage.of("t1", "k", PAYLOAD));
            aLocked.countDown();
            sleep(1000);
        }));
        assertThat(aLocked.await(10, SECONDS)).as("A holds the key lock").isTrue();

        long start = System.nanoTime();
        db.inTx((SqlRunnable) () -> outbox.send(OutboxMessage.of("t2", "k", PAYLOAD)));
        var bDuration = Duration.ofNanos(System.nanoTime() - start);

        assertThat(a.isDone()).as("A still holds its lock").isFalse();
        assertThat(bDuration).isLessThan(Duration.ofMillis(500));
        a.get(10, SECONDS);
        assertThat(count()).isEqualTo(2);
    }

    @Test
    void sendAllKeepsInputOrderInIds() throws Exception {
        var messages = List.of(
                OutboxMessage.of("t", "same", bytes("s1")),
                OutboxMessage.of("t", "x", bytes("x")),
                OutboxMessage.of("t", "same", bytes("s2")),
                OutboxMessage.of("t", (byte[]) null),
                OutboxMessage.of("u", "y", bytes("y")).withHeader("h", bytes("v")),
                OutboxMessage.of("t", "same", bytes("s3")),
                OutboxMessage.of("t", "z", (byte[]) null));

        var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));

        assertThat(ids).hasSize(messages.size()).doesNotHaveDuplicates();
        assertThat(messageIdsInIdOrder()).containsExactlyElementsOf(ids);
        for (int i = 0; i < messages.size(); i++) {
            var m = messages.get(i);
            var row = row(ids.get(i));
            assertThat(UuidV7.isV7(ids.get(i))).isTrue();
            assertThat(row.topic()).isEqualTo(m.topic());
            assertThat(row.key()).isEqualTo(m.key());
            assertThat(row.payload()).isEqualTo(m.payload());
            assertThat(HeaderCodec.decode(row.headers())).isEqualTo(m.headers());
            if (m.key() == null) {
                assertThat(row.shard()).isBetween(0, config.shards() - 1);
            } else {
                assertThat(row.shard()).isEqualTo(Hashing.shard(Hashing.keyHash(m.topic(), m.key()), config.shards()));
            }
        }
    }

    @Test
    void sendAllEmptyListIsNoop() throws Exception {
        var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of()));

        assertThat(ids).isEmpty();
        assertThat(count()).isZero();
        assertThat(wakeUps).hasValue(0);
    }

    @Test
    void sendAllTenThousandIsFast() throws Exception {
        var payload = new byte[100];
        var messages = IntStream.range(0, 10_000)
                .mapToObj(i -> OutboxMessage.of("bulk", "key-" + i, payload))
                .toList();

        long start = System.nanoTime();
        var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));
        var elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(ids).hasSize(10_000);
        assertThat(count()).isEqualTo(10_000);
        assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
    }

    @Test
    void concurrentSendAllWithOverlappingKeysDoesNotDeadlock() throws Exception {
        var futures = new ArrayList<Future<?>>();
        for (int t = 0; t < 4; t++) {
            futures.add(pool.submit(() -> {
                var random = ThreadLocalRandom.current();
                for (int tx = 0; tx < 20; tx++) {
                    var batch = IntStream.range(0, 500)
                            .mapToObj(i -> OutboxMessage.of("t", "key-" + random.nextInt(1000), PAYLOAD))
                            .toList();
                    db.inTx((SqlRunnable) () -> outbox.sendAll(batch));
                }
                return null;
            }));
        }
        for (var f : futures) {
            f.get(60, SECONDS);
        }

        assertThat(count()).isEqualTo(4 * 20 * 500);
    }

    @Test
    void sendAllLocksEveryKeyWhateverTheDriverFetchSize() throws Exception {
        var props = new Properties();
        props.setProperty("defaultRowFetchSize", "2"); // pgjdbc then reads statements in portals of 2 rows
        var fetching = TestDb.newDatabase(2, props);
        try {
            var bulk = new PgOutbox(fetching, schema, config.shards(), () -> {}, TestTelemetry.kora());
            var keys = List.of("a", "b", "c", "d");
            var lastHash =
                    keys.stream().max(Comparator.comparingInt(k -> Hashing.keyHash("t", bytes(k)))).orElseThrow();
            var locked = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var aBeforeCommit = new AtomicLong();
            var a = pool.submit(() -> fetching.inTx((SqlFunction1<Connection, Long>) c -> {
                bulk.sendAll(keys.stream().map(k -> OutboxMessage.of("t", k, PAYLOAD)).toList());
                long held = queryLong(c, "select count(*) from pg_locks where locktype = 'advisory'"
                        + " and pid = pg_backend_pid() and objsubid = 2 and classid = "
                        + Integer.toUnsignedLong(names.keyNamespace()));
                locked.countDown();
                await(release);
                aBeforeCommit.set(System.nanoTime());
                return held;
            }));
            assertThat(locked.await(10, SECONDS)).as("A holds its key locks").isTrue();
            var b = pool.submit(() -> db.inTx((SqlFunction0<Long>) () -> {
                outbox.send(OutboxMessage.of("t", lastHash, PAYLOAD));
                return System.nanoTime();
            }));
            sleep(300);
            var bBlocked = !b.isDone();
            release.countDown();

            assertThat(a.get(10, SECONDS)).as("advisory locks in the key namespace").isEqualTo(keys.size());
            assertThat(bBlocked).as("a send of the last-hash key waits for the bulk transaction").isTrue();
            assertThat(b.get(10, SECONDS)).isGreaterThan(aBeforeCommit.get());
        } finally {
            fetching.release();
        }
    }

    @Test
    void wakeUpRunsOncePerCommittedTransaction() throws Exception {
        db.inTx((SqlRunnable) () -> {
            outbox.send(OutboxMessage.of("t", "a", PAYLOAD));
            outbox.send(OutboxMessage.of("t", "b", PAYLOAD));
            outbox.sendAll(List.of(OutboxMessage.of("t", "c", PAYLOAD)));
            assertThat(wakeUps).hasValue(0);
        });
        assertThat(wakeUps).hasValue(1);

        wakeUps.set(0);
        assertThatThrownBy(() -> db.inTx((SqlRunnable) () -> {
                    outbox.send(OutboxMessage.of("t", "a", PAYLOAD));
                    throw new SQLException("rollback");
                }))
                .hasRootCauseMessage("rollback");
        assertThat(wakeUps).hasValue(0);

        // two transactions on one Kora connection share a ConnectionContext
        db.withConnection((SqlRunnable) () -> {
            db.inTx((SqlRunnable) () -> outbox.send(OutboxMessage.of("t", "a", PAYLOAD)));
            db.inTx((SqlRunnable) () -> outbox.send(OutboxMessage.of("t", "b", PAYLOAD)));
        });
        assertThat(wakeUps).hasValue(2);

        wakeUps.set(0);
        db.inTx((SqlConsumer<Connection>) c -> outbox.send(c, OutboxMessage.of("t", "a", PAYLOAD)));
        assertThat(wakeUps).hasValue(1);
    }

    @Test
    void failingWakeUpDoesNotFailCommittedTransaction() throws Exception {
        Runnable failing = () -> {
            throw new IllegalStateException("relay is down");
        };
        var failingOutbox = new PgOutbox(db, schema, config.shards(), failing, TestTelemetry.kora());

        var id = db.inTx((SqlFunction0<UUID>) () -> failingOutbox.send(OutboxMessage.of("t", "k", PAYLOAD)));

        assertThat(row(id).payload()).isEqualTo(PAYLOAD);
    }

    @Test
    void sendAllWithOnlyUnkeyedMessages() throws Exception {
        var messages = List.of(
                OutboxMessage.of("t", bytes("1")),
                OutboxMessage.of("t", bytes("2")),
                OutboxMessage.of("u", (byte[]) null));
        var advisoryLocks = new AtomicLong(-1);

        var ids = db.inTx((SqlFunction1<Connection, List<UUID>>) c -> {
            var sent = outbox.sendAll(messages);
            advisoryLocks.set(queryLong(
                    c, "select count(*) from pg_locks where locktype = 'advisory' and pid = pg_backend_pid()"));
            return sent;
        });

        assertThat(advisoryLocks).as("no lock statement for unkeyed rows").hasValue(0);
        assertThat(messageIdsInIdOrder()).containsExactlyElementsOf(ids);
        for (int i = 0; i < messages.size(); i++) {
            var row = row(ids.get(i));
            assertThat(row.shard()).isBetween(0, config.shards() - 1);
            assertThat(row.key()).isNull();
            assertThat(row.payload()).isEqualTo(messages.get(i).payload());
        }
    }

    @Test
    void sendCapturesCurrentTraceUnlessMessageCarriesOne() throws Exception {
        TestTracing.activateSpan();
        var traceparent = header("traceparent", TRACEPARENT);
        var plain = OutboxMessage.of("t", "k", PAYLOAD).withHeader("h", bytes("v"));
        var own = OutboxMessage.of("t", "k", PAYLOAD)
                .withHeader("traceparent", bytes("00-11111111111111111111111111111111-2222222222222222-01"));

        var ids = db.inTx((SqlFunction0<List<UUID>>) () -> {
            var all = new ArrayList<UUID>();
            all.add(outbox.send(plain));
            all.add(outbox.send(own));
            all.addAll(outbox.sendAll(List.of(plain, own)));
            return all;
        });

        assertThat(HeaderCodec.decode(row(ids.get(0)).headers())).containsExactly(header("h", "v"), traceparent);
        assertThat(HeaderCodec.decode(row(ids.get(1)).headers())).isEqualTo(own.headers());
        assertThat(HeaderCodec.decode(row(ids.get(2)).headers())).containsExactly(header("h", "v"), traceparent);
        assertThat(HeaderCodec.decode(row(ids.get(3)).headers())).isEqualTo(own.headers());
    }

    private Row row(UUID messageId) throws SQLException {
        try (var c = TestDb.connect();
                var ps = c.prepareStatement("select id, shard, xid::text::bigint, topic, key, payload, headers from "
                        + names.base() + " where message_id = ?")) {
            ps.setObject(1, messageId);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).as("row of %s", messageId).isTrue();
                return new Row(
                        rs.getLong(1),
                        rs.getInt(2),
                        rs.getLong(3),
                        rs.getString(4),
                        rs.getBytes(5),
                        rs.getBytes(6),
                        rs.getBytes(7));
            }
        }
    }

    private long count() throws SQLException {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + names.base());
        }
    }

    private List<String> payloadsInIdOrder() throws SQLException {
        var payloads = new ArrayList<String>();
        try (var c = TestDb.connect();
                var st = c.createStatement();
                var rs = st.executeQuery("select payload from " + names.base() + " order by id")) {
            while (rs.next()) {
                payloads.add(new String(rs.getBytes(1), UTF_8));
            }
        }
        return payloads;
    }

    private List<UUID> messageIdsInIdOrder() throws SQLException {
        var ids = new ArrayList<UUID>();
        try (var c = TestDb.connect();
                var st = c.createStatement();
                var rs = st.executeQuery("select message_id from " + names.base() + " order by id")) {
            while (rs.next()) {
                ids.add(rs.getObject(1, UUID.class));
            }
        }
        return ids;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
