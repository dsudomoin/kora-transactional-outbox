package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.internal.HeaderCodec;
import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Releases of one event contract on one table, each a runtime in this JVM: {@link Release#OLD} (N-1) reads and writes
 * v1, {@link Release#TRANSITION} (N) reads v1 and v2 but still writes v1, {@link Release#NEW} (N+1) writes v2. The
 * writer guard, the waits and the key order through rollouts, rollbacks and a canary that skips N.
 */
class ContractReleasesIT {

    private static final String TOPIC = "orders";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final OutboxSerializer<EventV1> V1 = (topic, e) -> bytes(e.id());
    private static final OutboxSerializer<Event> V2 = (topic, e) -> bytes(e.id() + "," + e.note());

    /** The event of N-1, and the class N still writes it as. */
    @OutboxContract(1)
    record EventV1(String id) {}

    /** The event from N on; read from a v1 payload it has no {@code note}. */
    @OutboxContract(2)
    record Event(String id, @Nullable String note) {

        static Event parse(byte[] payload) {
            var parts = new String(payload, UTF_8).split(",", 2);
            return new Event(parts[0], parts.length == 2 ? parts[1] : null);
        }
    }

    private enum Release {
        OLD, TRANSITION, NEW
    }

    /** A runtime of a release. */
    private record Instance(Release release, OutboxRuntime runtime) {}

    /** A record a handler delivered: by which release, its contract version, its decoded event, when. */
    private record Delivery(Release by, UUID messageId, String key, int version, Object event, long at) {

        String id() {
            return event instanceof Event e ? e.id() : ((EventV1) event).id();
        }
    }

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    // a single failed attempt would make a dead letter: a wait must spend none
    private final TestOutboxConfig config =
            new TestOutboxConfig().retry(Duration.ofMillis(50), Duration.ofMillis(500), 1);
    private final List<Instance> running = new ArrayList<>();
    private final List<Delivery> delivered = new ArrayList<>();
    /** By message id: how many times a handler gave it {@link Outcome.Waiting}, and when first. */
    private final Map<UUID, Integer> waits = new ConcurrentHashMap<>();
    private final Map<UUID, Long> firstWait = new ConcurrentHashMap<>();

    @AfterEach
    void close() {
        for (int i = running.size() - 1; i >= 0; i--) {
            running.get(i).runtime().close();
        }
    }

    /** Scenario 1: N is skipped, so while N-1 relays, N+1's first v2 write is refused before anything is written. */
    @Test
    void theNewContractIsRefusedWhileTheOldReleaseRelays() {
        var old = start(Release.OLD);
        var next = start(Release.NEW);

        assertThatThrownBy(() -> send(next, "a", "a1"))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e.version()).isEqualTo(2);
                    assertThat(e.minSupported()).isEqualTo(1);
                    assertThat(e.incompatibleInstances()).containsExactly(old.runtime().schema().instanceId());
                });
        long rows = transactions.inTx(c -> TestSql.queryLong(c, "select count(*) from " + config.table()));
        assertThat(rows).isZero();
    }

    /**
     * Scenarios 2, 3 and 5: beside N, N+1 writes v2 and both are delivered; rolled back to N, the v2 events N+1 wrote
     * are read by N, the two classes of one contract.
     */
    @Test
    void theTransitionReleaseReadsWhatTheNewOneWritesAlsoAfterItsRollback() throws Exception {
        var transition = start(Release.TRANSITION);
        var next = start(Release.NEW);

        var v1 = send(transition, "a", "a1");
        var v2 = send(next, "a", "a2");
        await(() -> delivered().size() == 2);
        assertThat(delivered()).extracting(Delivery::messageId, Delivery::version, Delivery::event)
                .containsExactly(tuple(v1, 1, new Event("a1", null)), tuple(v2, 2, new Event("a2", "v2")));

        // committed as the last N+1 instance goes: no N+1 relay ever sees them
        var rolledBack = transactions.inTx(c -> {
            var ids = List.of(send(next, "a", "a3"), send(next, "b", "b1"));
            stop(next);
            return ids;
        });
        await(() -> delivered().size() == 4);
        assertThat(delivered().subList(2, 4)).extracting(Delivery::by, Delivery::messageId, Delivery::event)
                .containsExactlyInAnyOrder(tuple(Release.TRANSITION, rolledBack.get(0), new Event("a3", "v2")),
                        tuple(Release.TRANSITION, rolledBack.get(1), new Event("b1", "v2")));
        assertThat(waits).isEmpty();
        assertThat(deadLetters(transition)).isZero();
    }

    /**
     * Scenario 4: rolled back from N+1 straight to N-1, the v2 events wait, one head per key, while N-1 relays v1;
     * none is lost or dead, and once N runs again they are delivered in key order.
     */
    @Test
    void aRollbackPastTheTransitionReleaseWaitsUntilItReturns() throws Exception {
        var next = start(Release.NEW);
        var keys = List.of("a", "b", "c");
        transactions.inTx(c -> {
            for (var key : keys) {
                for (int i = 1; i <= 3; i++) {
                    send(next, key, key + i);
                }
            }
            stop(next); // as above: no N+1 relay ever sees them
            return null;
        });

        var old = start(Release.OLD);
        var v1 = send(old, "d", "d1");
        await(() -> old.runtime().admin().status().waitingSize() == keys.size() && delivered().size() == 1);
        assertThat(delivered()).extracting(Delivery::messageId).containsExactly(v1);
        assertThat(deadLetters(old)).isZero();

        start(Release.TRANSITION); // beside N-1, which keeps taking the heads it cannot read
        await(() -> delivered().size() == keys.size() * 3 + 1);
        for (var key : keys) {
            assertThat(ids(key)).as(key).containsExactly(key + 1, key + 2, key + 3);
        }
        assertThat(delivered()).filteredOn(d -> d.version() == 2).extracting(Delivery::by)
                .containsOnly(Release.TRANSITION);
        assertThat(deadLetters(old)).isZero();
    }

    /**
     * Scenario 6: three instances replaced one by one, N-1 by N, then N by N+1, while a writer keeps writing through
     * the newest one: the guard never refuses, no record waits, each key keeps its order, nothing is dead.
     */
    @Test
    void aRollingUpdateKeepsKeyOrderAndNeverWaits() throws Exception {
        int keys = 5;
        var fleet = new ArrayList<Instance>();
        for (int i = 0; i < 3; i++) {
            fleet.add(start(Release.OLD));
        }
        var writer = new AtomicReference<>(fleet.get(2));
        var sent = new ArrayList<String>(); // the writer's alone until it is joined
        var stopping = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var writing = new Thread(() -> {
            try {
                for (int i = 0; !stopping.get(); i++) {
                    var id = "k" + i % keys + "/" + i;
                    send(writer.get(), key(id), id); // an instance closing meanwhile still writes v1
                    sent.add(id);
                    Thread.sleep(1);
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "writer");
        writing.start();
        try {
            for (var release : List.of(Release.TRANSITION, Release.NEW)) {
                for (int i = 0; i < fleet.size(); i++) {
                    var replacement = start(release);
                    writer.set(replacement);
                    stop(fleet.get(i));
                    fleet.set(i, replacement);
                    int before = delivered().size();
                    await(() -> delivered().size() >= before + 20 || failure.get() != null);
                }
            }
        } finally {
            stopping.set(true);
            writing.join();
        }

        assertThat(failure.get()).isNull();
        await(() -> delivered().stream().map(Delivery::id).distinct().count() == sent.size());
        for (int k = 0; k < keys; k++) {
            var key = "k" + k;
            // at least once: a redelivery may repeat a record, never reorder the first deliveries
            assertThat(ids(key)).as(key).containsExactlyElementsOf(sent.stream().filter(id -> key(id).equals(key))
                    .toList());
        }
        assertThat(delivered()).anyMatch(d -> d.version() == 2);
        assertThat(waits).isEmpty();
        assertThat(deadLetters(writer.get())).isZero();
    }

    /**
     * Scenario 7, a measurement: a canary of N+1 beside three instances of N-1, which skipped N, writes v2 anyway.
     * Each record waits until the canary takes it; logs the waits per record and the time from the first wait to the
     * delivery.
     */
    @Test
    void aCanaryThatSkipsTheTransitionReleaseGetsItsRecordsDeliveredAfterWaits() throws Exception {
        for (int i = 0; i < 3; i++) {
            start(Release.OLD);
        }
        var canary = start(Release.NEW);
        var typed = new TypedOutbox<>(canary.runtime().outbox(), V2, Event.class);
        // the guard refuses them while N-1 relays: written past it, as a release without the guard would write them
        var sent = transactions.inTx(c -> {
            var ids = new ArrayList<String>();
            for (int i = 0; i < 3; i++) {
                for (int k = 0; k < 4; k++) {
                    var id = "k" + k + "/" + i;
                    insertUnguarded(c, typed.message(TOPIC, key(id), new Event(id, "v2")));
                    ids.add(id);
                }
            }
            return ids;
        });

        await(() -> delivered().size() == sent.size());
        var report = new ArrayList<String>();
        long slowest = 0;
        for (var d : delivered()) {
            var since = firstWait.get(d.messageId());
            long ms = since == null ? 0 : (d.at() - since) / 1_000_000;
            slowest = Math.max(slowest, ms);
            report.add(d.id() + " " + waits.getOrDefault(d.messageId(), 0) + "x/" + ms + "ms");
        }
        System.out.printf("ContractReleasesIT canary, 3 x N-1 + 1 x N+1: %d records, %d waits, the slowest %d ms"
                + " from its first wait to its delivery; per record waits/ms: %s%n", sent.size(),
                waits.values().stream().mapToInt(Integer::intValue).sum(), slowest, String.join(", ", report));

        for (int k = 0; k < 4; k++) {
            var key = "k" + k;
            assertThat(ids(key)).as(key).containsExactly(key + "/0", key + "/1", key + "/2");
        }
        assertThat(delivered()).extracting(Delivery::by).containsOnly(Release.NEW);
        assertThat(deadLetters(canary)).isZero();
    }

    private Instance start(Release release) {
        var reads = release == Release.OLD
                ? new Reads<>(release, EventV1.class, (topic, payload) -> new EventV1(new String(payload, UTF_8)))
                : new Reads<>(release, Event.class, (topic, payload) -> Event.parse(payload));
        var instance = new Instance(release, OutboxRuntime.builder(config, transactions, null).route(reads).build());
        running.add(instance);
        instance.runtime().start();
        return instance;
    }

    private void stop(Instance instance) {
        running.remove(instance);
        instance.runtime().close();
    }

    /** Sends event {@code id} of {@code key} as {@code instance}'s release writes it: v2 from N+1, else v1. */
    private UUID send(Instance instance, String key, String id) {
        var outbox = instance.runtime().outbox();
        return transactions.inTx(c -> instance.release() == Release.NEW
                ? new TypedOutbox<>(outbox, V2, Event.class).send(TOPIC, key, new Event(id, "v2"))
                : new TypedOutbox<>(outbox, V1, EventV1.class).send(TOPIC, key, new EventV1(id)));
    }

    /** The row {@code PgOutbox} would insert for {@code m}, without its writer guard. */
    private void insertUnguarded(Connection c, OutboxMessage m) throws SQLException {
        try (var ps = c.prepareStatement("insert into " + config.table()
                + " (shard, message_id, topic, key, payload, headers) values (?, ?, ?, ?, ?, ?)")) {
            ps.setShort(1, (short) Hashing.shard(Hashing.keyHash(m.topic(), m.key()), config.shards()));
            ps.setObject(2, UuidV7.next());
            ps.setString(3, m.topic());
            ps.setBytes(4, m.key());
            ps.setBytes(5, m.payload());
            ps.setBytes(6, HeaderCodec.encode(m.headers()));
            ps.executeUpdate();
        }
    }

    private List<Delivery> delivered() {
        synchronized (delivered) {
            return List.copyOf(delivered);
        }
    }

    /** The event ids of {@code key} in the order first delivered. */
    private List<String> ids(String key) {
        return delivered().stream().filter(d -> d.key().equals(key)).map(Delivery::id).distinct().toList();
    }

    private static long deadLetters(Instance instance) {
        return instance.runtime().admin().countDeadLetters(DeadLetterFilter.any());
    }

    /** The key of event {@code id}: what precedes its {@code /}, or all of it. */
    private static String key(String id) {
        int slash = id.indexOf('/');
        return slash < 0 ? id : id.substring(0, slash);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + TIMEOUT);
            }
            Thread.sleep(10);
        }
    }

    /** A release's handler of {@link #TOPIC}: reads up to {@code type}'s contract, and notes deliveries and waits. */
    private final class Reads<T> extends TypedOutboxTopicHandler<T> {

        private final Release release;

        Reads(Release release, Class<T> type, OutboxDeserializer<T> deserializer) {
            super(Set.of(TOPIC), deserializer, type);
            this.release = release;
        }

        @Override
        protected void handle(T value, OutboxRecord record) {
            synchronized (delivered) {
                delivered.add(new Delivery(release, record.messageId(), record.keyAsString(),
                        record.contractVersion(), value, System.nanoTime()));
            }
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            var outcomes = super.handleBatch(records);
            for (int i = 0; i < outcomes.size(); i++) {
                if (outcomes.get(i) instanceof Outcome.Waiting) {
                    var id = records.get(i).messageId();
                    firstWait.putIfAbsent(id, System.nanoTime());
                    waits.merge(id, 1, Integer::sum);
                }
            }
            return outcomes;
        }
    }
}
