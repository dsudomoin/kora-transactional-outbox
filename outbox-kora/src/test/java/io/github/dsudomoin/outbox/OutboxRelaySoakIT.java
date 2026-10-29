package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.kora.KoraOutboxTransactions;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The relay's long runs, a minute of load and 30 s of idling: a class of their own, so that the build runs them in
 * another test JVM beside {@link OutboxRelayIT} rather than after it.
 */
class OutboxRelaySoakIT {

    private final TestDatabase db = TestDb.newDatabase(30);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final List<OutboxRelay> relays = new ArrayList<>();
    private OutboxSchema schema;

    @AfterEach
    void release() {
        for (var relay : relays) {
            relay.close();
        }
        db.release();
    }

    @Test
    void idleRelayWritesOnlyTicks() throws Exception {
        config.shards(1024).workers(4).minTickInterval(Duration.ofMillis(50)).idlePollInterval(Duration.ofMillis(200))
                .maintenanceInterval(Duration.ofSeconds(30));
        bootstrap(); // the defaults, 1024 shards
        start(new RecordingHandler());
        await(Duration.ofSeconds(5), () -> ticksMade() >= 3);
        awaitParked();
        long seq = maxCursorSeq();
        long ticks = ticksMade();
        long used;
        double seconds;
        // xids are the cluster's: autovacuum analysing the tables earlier tests left would take some of its own
        try (var c = TestDb.connect()) {
            execute(c, "alter system set autovacuum = off");
            execute(c, "select pg_reload_conf()");
        }
        try {
            long xid = currentXid();
            long started = System.nanoTime();

            Thread.sleep(30_000); // the measured idle period

            used = currentXid() - xid - 1; // the second currentXid() takes one itself
            seconds = (System.nanoTime() - started) / 1e9;
        } finally {
            try (var c = TestDb.connect()) {
                execute(c, "alter system reset autovacuum");
                execute(c, "select pg_reload_conf()");
            }
        }
        System.out.printf("idle relay, 1024 shards: %d xids in %.1f s (%.1f/s), %d ticks%n", used, seconds,
                used / seconds, ticksMade() - ticks);
        // an idle tick per idlePollInterval and maintenance (its checkpoint, tick cleanup, its success) per interval:
        // no cursor row (shards / 5 a second before, 205 xids/s at 1024 shards)
        assertThat(used).as("xids").isLessThanOrEqualTo(30_000 / config.idlePollInterval().toMillis()
                + 30 / config.maintenanceInterval().toSeconds() * 2 + 5);
        assertThat(maxCursorSeq()).as("no cursor row").isEqualTo(seq);
    }

    @Test
    void manyShardsDeliverInOrderUnderLoad() throws Exception {
        // 2 instances × 4 workers over 1024 shards; a slow shard whose calls wind down at the soft deadline (1.6 s)
        // with its group's later shards unread, and a hot key whose shard fills the group's read
        config.shards(1024).workers(4).batchSize(1000).batchTimeout(Duration.ofSeconds(2));
        bootstrap();
        var slowKey = keyOnShard(7, "slow");
        var hotKey = keyOnShard(9, "hot");
        var delivered = ConcurrentHashMap.<UUID>newKeySet();
        var duplicates = new AtomicInteger();
        var outOfOrder = new ConcurrentLinkedQueue<String>();
        var lastId = new ConcurrentHashMap<String, Long>();
        var handler = new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                if (payload(record).startsWith("slow")) { // 300 ms, deaf to the wind-down's interrupt
                    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
                    while (System.nanoTime() < end) {
                        Thread.onSpinWait();
                    }
                }
                if (!delivered.add(record.messageId())) {
                    duplicates.incrementAndGet();
                }
                lastId.compute(key(record), (k, last) -> {
                    if (last != null && last >= record.id()) {
                        outOfOrder.add(k + ": " + record.id() + " after " + last);
                    }
                    return last == null ? record.id() : Math.max(last, record.id());
                });
            }
        };
        var started = List.of(start(handler), start(handler));
        var sent = ConcurrentHashMap.<UUID>newKeySet();
        var writers = Executors.newFixedThreadPool(8);
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        try {
            var done = new ArrayList<Future<?>>();
            for (int w = 0; w < 8; w++) {
                int writer = w;
                var outbox = outbox(started.get(w % 2)::wake);
                done.add(writers.submit(() -> {
                    var random = ThreadLocalRandom.current();
                    while (System.nanoTime() < until) {
                        OutboxMessage[] messages;
                        long pause;
                        if (writer == 0) { // a burst of slow records: more than one call fits before its deadline
                            messages = IntStream.range(0, 8).mapToObj(i -> msg("t", slowKey, "slow" + i))
                                    .toArray(OutboxMessage[]::new);
                            pause = 3000;
                        } else if (writer == 1) {
                            messages = IntStream.range(0, 100).mapToObj(i -> msg("t", hotKey, "hot"))
                                    .toArray(OutboxMessage[]::new);
                            pause = 20;
                        } else {
                            messages = IntStream.range(0, 10).mapToObj(i -> msg("t", "k" + random.nextInt(8192),
                                    "m")).toArray(OutboxMessage[]::new);
                            pause = 10;
                        }
                        sent.addAll(send(outbox, messages));
                        Thread.sleep(pause);
                    }
                    return null;
                }));
            }
            for (var f : done) {
                f.get(90, TimeUnit.SECONDS);
            }
        } finally {
            writers.shutdown();
        }
        System.out.printf("%d messages sent in 60 s%n", sent.size());
        await(Duration.ofSeconds(60), () -> delivered.size() >= sent.size());

        assertThat(delivered).hasSameSizeAs(sent);
        assertThat(delivered.containsAll(sent)).as("every message delivered").isTrue(); // a hash lookup each
        assertThat(duplicates).hasValue(0);
        assertThat(outOfOrder).as("per-key order").isEmpty();
        assertThat(lastId).containsKeys(slowKey, hotKey);
    }

    private void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.start();
    }

    private OutboxRelay start(OutboxHandler handler) {
        var transactions = new KoraOutboxTransactions(db);
        var relay = new OutboxRelay(transactions, config, schema, handler, TestTelemetry.kora(),
                new Maintenance(transactions, schema.names(), config, new OutboxMetrics(null)),
                new DeadLetterPass(transactions, schema.names(), config, null, new OutboxMetrics(null)));
        relays.add(relay);
        relay.start();
        assertThat(relay.running()).isTrue();
        return relay;
    }

    private PgOutbox outbox(Runnable wake) {
        return new PgOutbox(db, schema, config.shards(), wake, TestTelemetry.kora());
    }

    /** One committed transaction; Kora runs the wake-up after the commit. */
    private List<UUID> send(PgOutbox outbox, OutboxMessage... messages) {
        return db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(messages)));
    }

    private static OutboxMessage msg(String topic, String key, String payload) {
        return OutboxMessage.of(topic, key, payload.getBytes(UTF_8));
    }

    private static String key(OutboxRecord r) {
        return new String(r.key(), UTF_8);
    }

    private static String payload(OutboxRecord r) {
        return new String(r.payload(), UTF_8);
    }

    private String keyOnShard(int shard, String prefix) {
        for (int i = 0; ; i++) {
            var key = prefix + i;
            if (Hashing.shard(Hashing.keyHash("t", key.getBytes(UTF_8)), config.shards()) == shard) {
                return key;
            }
        }
    }

    /** The highest cursor {@code seq}: grows with every cursor row written, whatever maintenance deletes. */
    private long maxCursorSeq() throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select max(seq) from " + schema.names().cursor());
        }
    }

    /** The newest tick id: counts ticks made, unlike a row count, which maintenance shrinks. */
    private long ticksMade() throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select max(id) from " + schema.names().tick());
        }
    }

    /** {@code pg_current_xact_id()}: consumes one xid itself. */
    private static long currentXid() throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select pg_current_xact_id()::text::bigint");
        }
    }

    /** Every worker parked: the relay is idle. */
    private void awaitParked() throws Exception {
        await(Duration.ofSeconds(5), () -> relayThreads().size() == config.workers()
                && relayThreads().stream().allMatch(t -> t.getState() == Thread.State.TIMED_WAITING));
    }

    private static List<Thread> relayThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("outbox-relay-") && t.isAlive())
                .toList();
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void await(Duration timeout, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(5);
        }
    }
}
