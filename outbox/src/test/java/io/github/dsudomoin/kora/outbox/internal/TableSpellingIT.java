package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.NOT_LOCKED;
import static io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardStatus.PROGRESS;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxHandler;
import io.github.dsudomoin.kora.outbox.OutboxMessage;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.internal.ShardProcessor.ShardResult;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/** One table configured as {@code t} by one instance and {@code public.t} by another shares every lock. */
class TableSpellingIT {

    private final JdbcDatabase db = TestDb.newDatabase(8);
    private final String table = TestDb.uniqueTable();
    private final TestOutboxConfig plainConfig = new TestOutboxConfig().table(table).shards(1);
    private final TestOutboxConfig qualifiedConfig = new TestOutboxConfig().table("public." + table).shards(1);
    private final ExecutorService pool = Executors.newFixedThreadPool(2);
    private OutboxSchema plain;
    private OutboxSchema qualified;

    @AfterEach
    void release() {
        pool.shutdownNow();
        db.release();
    }

    @Test
    void bothSpellingsGetTheLockNamespacesOfTheResolvedTable() throws Exception {
        bootstrap();

        assertThat(qualified.names().keyNamespace()).isEqualTo(plain.names().keyNamespace());
        assertThat(qualified.names().internalNamespace()).isEqualTo(plain.names().internalNamespace());
        assertThat(plain.names().keyNamespace()).isNotEqualTo(plain.names().internalNamespace());
    }

    @Test
    void anUnqualifiedNameResolvesThroughTheSearchPath() throws Exception {
        var schemaName = TestDb.uniqueTable();
        try (var c = TestDb.connect()) {
            execute(c, "create schema " + schemaName);
        }
        var props = new Properties();
        props.setProperty("currentSchema", schemaName + ",public"); // like Kora's db.schema
        var appDb = TestDb.newDatabase(2, props);
        try {
            var inApp = new OutboxSchema(appDb, new TestOutboxConfig().table(table).shards(1));
            inApp.init();
            var explicit = new OutboxSchema(db, new TestOutboxConfig().table(schemaName + "." + table).shards(1));
            explicit.init();
            var inPublic = new OutboxSchema(db, plainConfig);
            inPublic.init();

            assertThat(inApp.names().keyNamespace()).isEqualTo(explicit.names().keyNamespace());
            assertThat(inApp.names().internalNamespace()).isEqualTo(explicit.names().internalNamespace());
            assertThat(inApp.names().internalNamespace()).isNotEqualTo(inPublic.names().internalNamespace());
        } finally {
            appDb.release();
        }
    }

    @Test
    void processorsOfBothSpellingsCannotHoldOneShardTogether() throws Exception {
        bootstrap();
        var outbox = new PgOutbox(db, plain, 1, () -> {}, new OutboxMetrics(null));
        db.inTx((SqlFunction0<UUID>) () -> outbox.send(OutboxMessage.of("t", "k", bytes("m"))));
        var ticks = new Ticks(plain.names(), plainConfig.shards());
        long latest;
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            latest = ticks.latest(c).id();
        }
        var inHandler = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        OutboxHandler blocking = r -> {
            inHandler.countDown();
            assertThat(release.await(10, SECONDS)).isTrue();
        };
        OutboxHandler idle = r -> {};
        try (var a = new ShardProcessor(plain.names(), plainConfig, blocking, new OutboxMetrics(null), null, false);
                var b = new ShardProcessor(qualified.names(), qualifiedConfig, idle, new OutboxMetrics(null), null,
                        false)) {
            var first = pool.submit(() -> step(a, latest));
            assertThat(inHandler.await(10, SECONDS)).as("A holds shard 0").isTrue();

            var second = step(b, latest);
            release.countDown();

            assertThat(second.status()).isEqualTo(NOT_LOCKED);
            assertThat(first.get(10, SECONDS).status()).isEqualTo(PROGRESS);
        }
    }

    @Test
    void writersOfBothSpellingsSerialiseOnAKey() throws Exception {
        bootstrap();
        var viaPlain = new PgOutbox(db, plain, 1, () -> {}, new OutboxMetrics(null));
        var viaQualified = new PgOutbox(db, qualified, 1, () -> {}, new OutboxMetrics(null));
        var aLocked = new CountDownLatch(1);
        var aBeforeCommit = new AtomicLong();
        var a = pool.submit(() -> db.inTx((SqlFunction0<UUID>) () -> {
            var id = viaPlain.send(OutboxMessage.of("t", "k", bytes("a")));
            aLocked.countDown();
            sleep(300);
            aBeforeCommit.set(System.nanoTime());
            return id;
        }));
        assertThat(aLocked.await(10, SECONDS)).as("A holds the key lock").isTrue();

        var bReturnedAt = db.inTx((SqlFunction0<Long>) () -> {
            viaQualified.send(OutboxMessage.of("t", "k", bytes("b")));
            return System.nanoTime();
        });

        a.get(10, SECONDS);
        assertThat(bReturnedAt).isGreaterThan(aBeforeCommit.get());
    }

    @Test
    void inboxOfEitherSpellingWaitsForTheOtherOnesLock() throws Exception {
        var inbox = TestDb.uniqueTable();
        var holderNamespace = TableNames.of("public." + inbox).internalNamespace(); // what the qualified one locks
        try (var holder = TestDb.connect()) {
            try (var ps = holder.prepareStatement("select pg_advisory_lock(?, ?)")) {
                ps.setInt(1, holderNamespace);
                ps.setInt(2, TableNames.LOCK_INBOX_MAINTENANCE);
                ps.execute();
            }

            var plainInbox = new PgInbox(db, new TestOutboxConfig().inboxTable(inbox), new OutboxMetrics(null));

            assertThatThrownBy(plainInbox::init)
                    .isInstanceOfSatisfying(SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("55P03"));
        }
    }

    private void bootstrap() throws Exception {
        plain = new OutboxSchema(db, plainConfig);
        plain.init();
        qualified = new OutboxSchema(db, qualifiedConfig);
        qualified.init();
    }

    private static ShardResult step(ShardProcessor processor, long latestTickId) throws Exception {
        try (var c = TestDb.connect()) {
            ShardProcessor.prepareTransaction(c, Duration.ofSeconds(10));
            try {
                var result = processor.tryProcess(c, 0, latestTickId, Instant.now(), false);
                c.commit();
                return result;
            } catch (Throwable t) {
                c.rollback();
                throw t;
            }
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

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
