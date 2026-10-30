package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Releases around one local route on one table, each a runtime in this JVM: {@link Release#NEW} routes {@link #TOPIC}
 * to a handler here, {@link Release#OLD} lacks that route but has another, {@link Release#BARE} has no route at all.
 * Each hands every topic its routes do not claim to {@link #kafka}, the fallback that stands for Kafka. The route
 * guard against a canary, and the records written for the route through rollbacks: they wait, never reach the
 * fallback, and keep their key order.
 */
class RouteReleasesIT {

    private static final String TOPIC = "orders";
    /** The topic of the plain route OLD and NEW have. */
    private static final String AUDIT = "audit";
    /** A topic no route claims, which the fallback delivers. */
    private static final String KAFKA = "kafka";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final OutboxHeader LOCAL = new OutboxHeader(PgOutbox.LOCAL_HEADER, null);
    private static final OutboxSerializer<Event> SERIALIZER = (topic, e) -> bytes(e.id());

    /** The event of {@link #TOPIC}, of version 1. */
    record Event(String id) {}

    private enum Release {
        OLD, BARE, NEW
    }

    /** A runtime of a release. */
    private record Instance(Release release, OutboxRuntime runtime) {

        String id() {
            return runtime.schema().instanceId();
        }
    }

    /** A record of {@link #TOPIC} that NEW's route took. */
    private record Taken(String key, String id, List<OutboxHeader> headers) {}

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    // a single failed attempt would make a dead letter: a wait must spend none
    private final TestOutboxConfig config =
            new TestOutboxConfig().retry(Duration.ofMillis(50), Duration.ofMillis(500), 1);
    /** The fallback of every release. */
    private final RecordingHandler kafka = new RecordingHandler();
    private final List<Instance> running = new ArrayList<>();
    private final List<Taken> delivered = new ArrayList<>();
    /** Each wait a relay noted: its release, the topic and the kind. */
    private final List<String> waits = new CopyOnWriteArrayList<>();
    /** Each handler call of a relay, in order: per record its topic, its payload and its outcome. */
    private final List<List<String>> calls = new CopyOnWriteArrayList<>();

    @AfterEach
    void close() {
        for (int i = running.size() - 1; i >= 0; i--) {
            running.get(i).runtime().close();
        }
    }

    /**
     * Scenario 1: a canary of NEW beside OLD, whose row lacks the route, refuses a raw and a typed v1 send to the
     * route's topic before anything is written. Each row reports its routes, typed and plain, sorted.
     */
    @Test
    void aCanaryRefusesToWriteForItsNewRouteWhileAnOldRelayLacksIt() {
        var old = start(Release.OLD);
        var canary = start(Release.NEW);

        assertThat(routes(canary)).as("its typed route and its plain one").isEqualTo("{audit,orders}");
        assertThat(routes(old)).isEqualTo("{audit}");
        transactions.inTx(c -> {
            assertThatThrownBy(() -> canary.runtime().outbox().send(raw(TOPIC, "a"))).isInstanceOfSatisfying(
                    OutboxMissingRouteException.class, e -> assertThat(e.instances()).containsExactly(old.id()));
            assertThatThrownBy(() -> send(canary, "a", "a1")).isInstanceOfSatisfying(
                    OutboxMissingRouteException.class, e -> assertThat(e.instances()).containsExactly(old.id()));
            return null;
        });
        long rows = transactions.inTx(c -> TestSql.queryLong(c, "select count(*) from " + config.table()));
        assertThat(rows).isZero();
    }

    /** Scenario 2: rolled back to a release with another route. */
    @Test
    void aRollbackToAReleaseWithOtherRoutesWaitsUntilTheRouteReturns() throws Exception {
        rollBackAndForward(Release.OLD);
    }

    /** Scenario 3: rolled back to a release without routes, whose handler is kept from local records all the same. */
    @Test
    void aRollbackToAReleaseWithoutRoutesWaitsUntilTheRouteReturns() throws Exception {
        rollBackAndForward(Release.BARE);
    }

    /**
     * Scenario 4: mid-rollback, two OLD relays beside a NEW one. The OLD ones make the route's records wait and keep
     * taking the heads, the NEW one delivers them, each key in order.
     */
    @Test
    void aMixedFleetDeliversTheRouteOnItsNewInstancesInKeyOrder() throws Exception {
        var keys = List.of("a", "b", "c", "d");
        writeAsTheRouteGoes(keys);
        var old = start(Release.OLD);
        start(Release.OLD);
        await(() -> waiting(old) == keys.size());

        start(Release.NEW);
        await(() -> delivered().size() == keys.size() * 3);
        for (var key : keys) {
            assertThat(ids(key)).as(key).containsExactly(key + 1, key + 2, key + 3);
        }
        assertThat(waits).isNotEmpty().containsOnly("OLD orders NO_ROUTE");
        assertThat(kafka.delivered()).isEmpty();
        assertThat(deadLetters(old)).isZero();
    }

    /**
     * Scenario 5: in one call of an OLD relay the route's records wait, while {@link #KAFKA}'s, between them, go to
     * the fallback in one call of its own, in order.
     */
    @Test
    void theFallbackStillGetsItsRecordsBatchedInTheCallsWhereTheRouteWaits() throws Exception {
        writeMixed();
        var old = start(Release.OLD);
        await(() -> kafka.delivered().size() == 3 && waiting(old) == 2);

        assertThat(calls).first().isEqualTo(List.of("orders a1 waits", "kafka k1 delivered", "orders b1 waits",
                "kafka k2 delivered", "orders a2 waits", "kafka k3 delivered", "audit x1 delivered"));
        assertThat(kafka.batches()).as("one call, and none without a record for it").extracting(List::size)
                .containsExactly(3);
        assertThat(kafka.delivered()).extracting(r -> text(r.payload())).containsExactly("k1", "k2", "k3");
    }

    /**
     * Scenario 6: abandoned, the route's waiting records keep the marker as dead letters and once redriven, so an
     * OLD relay makes them wait again and never hands them to the fallback; NEW delivers them in order, marked.
     */
    @Test
    void aRedrivenRecordOfTheRouteKeepsItsMarker() throws Exception {
        writeAsTheRouteGoes(List.of("a"));
        var old = start(Release.OLD);
        await(() -> waiting(old) == 1);
        var admin = old.runtime().admin();

        assertThat(admin.abandonWaiting(TOPIC, null, "rolled back", false)).isEqualTo(new OutboxAdmin.Abandoned(1, 3));
        var letters = admin.findDeadLetters(DeadLetterFilter.any(), null, 10).letters();
        assertThat(letters).hasSize(3).allSatisfy(l -> assertThat(l.headers()).contains(LOCAL));
        int waited = waits.size();
        assertThat(admin.redriveDeadLetters(letters.stream().map(DeadLetter::id).toList())).isEqualTo(3);
        await(() -> waiting(old) == 1 && waits.size() > waited);
        assertThat(kafka.delivered()).isEmpty();

        start(Release.NEW);
        await(() -> delivered().size() == 3);
        assertThat(ids("a")).containsExactly("a1", "a2", "a3");
        assertThat(delivered()).allSatisfy(d -> assertThat(d.headers()).contains(LOCAL));
    }

    /**
     * Scenario 7: the fallback throws in the call it shares with the route's records. {@link #KAFKA}'s fail, and with
     * one attempt they are dead letters; the route's wait, no attempt spent. The audit route delivers a record of the
     * call, so the shard does not pause as for a target down, which would hand the call again unsettled.
     */
    @Test
    void aFallbackThatThrowsLeavesTheRouteWaitingWithNoAttemptSpent() throws Exception {
        kafka.throwFromBatch(() -> new IllegalStateException("broker down"));
        writeMixed();
        var old = start(Release.OLD);
        await(() -> deadLetters(old) == 3 && waiting(old) == 2);

        assertThat(calls).first().isEqualTo(List.of("orders a1 waits", "kafka k1 Retry", "orders b1 waits",
                "kafka k2 Retry", "orders a2 waits", "kafka k3 Retry", "audit x1 delivered"));
        var retried = transactions.inTx(c -> TestSql.queryString(c, "select string_agg(convert_from(payload, 'UTF8')"
                + " || ':' || attempts, ',' order by id) from " + config.table() + "_retry where topic = ?", TOPIC));
        assertThat(retried).as("payload:attempts").isEqualTo("a1:0,b1:0,a2:0");
        assertThat(kafka.delivered()).isEmpty();
    }

    /**
     * Scenario 8: writers whose read went stale, as without a heartbeat it is older than three of their 100 ms
     * maintenance intervals. The last read decides: one that found every live relay routing the topic lets a send
     * through, counted in {@code route_unchecked}; one that found a relay of unknown routes refuses, naming none.
     */
    @Test
    void aStaleReadAllowsWhatItsLastReadAllowedAndRefusesOnAnUnreportedRelay() throws Exception {
        start(Release.NEW);
        var writerConfig = new TestOutboxConfig().table(config.table()).relayEnabled(false)
                .maintenanceInterval(Duration.ofMillis(100));
        var unchecked = new CopyOnWriteArrayList<String>();
        try (var compatible = new OutboxSchema(transactions, transactions, writerConfig, Map.of(), Set.of(TOPIC));
                var unreported = new OutboxSchema(transactions, transactions, writerConfig, Map.of(), Set.of(TOPIC))) {
            compatible.start(); // reads NEW's row
            transactions.inTx(c -> {
                TestSql.execute(c, "insert into " + config.table() + "_instance (instance_id, shards, relay,"
                        + " expires_at) values ('unreported/1/0', 4, true, now() + interval '1 hour')");
                return null;
            });
            unreported.start(); // reads NEW's row and one of unknown routes
            var allowed = writer(compatible, unchecked);
            var refused = writer(unreported, unchecked);

            var refusal = new AtomicReference<OutboxMissingRouteException>();
            await(() -> { // a fresh refusal names the instance
                refusal.set(refusal(refused));
                return refusal.get().instances().isEmpty();
            });
            assertThat(refusal.get()).hasMessageContaining("in the last read not every one routes it");
            transactions.inTx(c -> allowed.send(raw(TOPIC, "a")));
            assertThat(unchecked).containsExactly(TOPIC);
        }
    }

    /**
     * The records of the route written while every relay routes it, then the fleet rolls back to {@code older}: its
     * relay makes them wait, one head per key, and never hands them to the fallback, which still delivers
     * {@link #KAFKA}'s. Rolled forward to NEW, they are delivered in key order.
     */
    private void rollBackAndForward(Release older) throws Exception {
        var keys = List.of("a", "b", "c");
        writeAsTheRouteGoes(keys);
        var old = start(older);
        var forwarded = transactions.inTx(c -> old.runtime().outbox().send(raw(KAFKA, "k")));
        await(() -> waiting(old) == keys.size() && kafka.delivered().size() == 1);
        assertThat(kafka.delivered()).extracting(OutboxRecord::messageId).containsExactly(forwarded);
        assertThat(delivered()).isEmpty();

        stop(old);
        var next = start(Release.NEW);
        await(() -> delivered().size() == keys.size() * 3);
        for (var key : keys) {
            assertThat(ids(key)).as(key).containsExactly(key + 1, key + 2, key + 3);
        }
        assertThat(waits).containsOnly(older + " orders NO_ROUTE");
        assertThat(kafka.delivered()).hasSize(1);
        assertThat(deadLetters(next)).isZero();
    }

    /** Events 1 to 3 of each of {@code keys}, written by NEW and committed as it goes: no NEW relay sees them. */
    private void writeAsTheRouteGoes(List<String> keys) {
        var next = start(Release.NEW);
        transactions.inTx(c -> {
            for (var key : keys) {
                for (int i = 1; i <= 3; i++) {
                    send(next, key, key + i);
                }
            }
            stop(next);
            return null;
        });
    }

    /**
     * As {@link #writeAsTheRouteGoes}, on one shard, so one relay call takes them all: the route's between Kafka's,
     * then one of the audit route OLD has too.
     */
    private void writeMixed() {
        config.shards(1);
        var next = start(Release.NEW);
        transactions.inTx(c -> {
            var outbox = next.runtime().outbox();
            send(next, "a", "a1");
            outbox.send(raw(KAFKA, "k1"));
            send(next, "b", "b1");
            outbox.send(raw(KAFKA, "k2"));
            send(next, "a", "a2");
            outbox.send(raw(KAFKA, "k3"));
            outbox.send(raw(AUDIT, "x1"));
            stop(next);
            return null;
        });
    }

    private Instance start(Release release) {
        var builder = OutboxRuntime.builder(config, transactions, kafka).telemetry(telemetry(release));
        if (release == Release.NEW) {
            builder.route(new Takes());
        }
        if (release != Release.BARE) {
            builder.route(new OutboxTopicHandler() {
                @Override
                public Set<String> topics() {
                    return Set.of(AUDIT);
                }

                @Override
                public void handle(OutboxRecord record) {}
            });
        }
        var instance = new Instance(release, builder.build());
        running.add(instance);
        instance.runtime().start();
        return instance;
    }

    private void stop(Instance instance) {
        running.remove(instance);
        instance.runtime().close();
    }

    /** Sends event {@code id} of {@code key} to {@link #TOPIC}, typed, as {@code instance}. */
    private void send(Instance instance, String key, String id) {
        new TypedOutbox<>(instance.runtime().outbox(), SERIALIZER, Event.class).send(TOPIC, key, new Event(id));
    }

    /** Notes {@code release}'s waits and handler calls. */
    private OutboxTelemetry telemetry(Release release) {
        return new OutboxTelemetry() {
            @Override
            public void waited(String topic, Outcome.Waiting.Kind kind) {
                waits.add(release + " " + topic + " " + kind);
            }

            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                return new Delivery() {
                    @Override
                    public void finished(List<Outcome> outcomes) {
                        var call = new ArrayList<String>();
                        for (int i = 0; i < records.size(); i++) {
                            var outcome = outcomes.get(i);
                            call.add(records.get(i).topic() + " " + text(records.get(i).payload()) + " "
                                    + (outcome instanceof Outcome.Waiting ? "waits"
                                            : outcome == Outcome.delivered() ? "delivered"
                                                    : outcome.getClass().getSimpleName()));
                        }
                        calls.add(call);
                    }
                };
            }
        };
    }

    /** A writer of its own: {@code schema}'s, with no heartbeat, so its read ages; notes its unchecked sends. */
    private PgOutbox writer(OutboxSchema schema, List<String> unchecked) {
        return new PgOutbox(transactions, schema, config.shards(), () -> {}, new OutboxTelemetry() {
            @Override
            public void routeUnchecked(String topic) {
                unchecked.add(topic);
            }
        });
    }

    /** The refusal of a raw send to {@link #TOPIC} through {@code outbox}. */
    private OutboxMissingRouteException refusal(Outbox outbox) {
        try {
            transactions.inTx(c -> outbox.send(raw(TOPIC, "a")));
        } catch (OutboxMissingRouteException e) {
            return e;
        }
        return fail("the send went in");
    }

    /** {@code routes} of the instance's row, as text. */
    private String routes(Instance instance) {
        return transactions.inTx(c -> TestSql.queryString(c, "select routes::text from " + config.table()
                + "_instance where instance_id = ?", instance.id()));
    }

    private List<Taken> delivered() {
        synchronized (delivered) {
            return List.copyOf(delivered);
        }
    }

    /** The event ids of {@code key} in the order first delivered. */
    private List<String> ids(String key) {
        return delivered().stream().filter(d -> d.key().equals(key)).map(Taken::id).distinct().toList();
    }

    private static long waiting(Instance instance) {
        return instance.runtime().admin().status().waitingSize();
    }

    private static long deadLetters(Instance instance) {
        return instance.runtime().admin().countDeadLetters(DeadLetterFilter.any());
    }

    /** A message of {@code topic} whose key and payload are {@code id}. */
    private static OutboxMessage raw(String topic, String id) {
        return new OutboxMessage(topic, bytes(id), bytes(id), List.of());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static String text(byte[] bytes) {
        return new String(bytes, UTF_8);
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

    /** NEW's route of {@link #TOPIC}: notes what it takes. */
    private final class Takes extends TypedOutboxTopicHandler<Event> {

        Takes() {
            super(Set.of(TOPIC), (topic, payload) -> new Event(text(payload)), Event.class);
        }

        @Override
        protected void handle(Event value, OutboxRecord record) {
            synchronized (delivered) {
                delivered.add(new Taken(record.keyAsString(), value.id(), record.headers()));
            }
        }
    }
}
