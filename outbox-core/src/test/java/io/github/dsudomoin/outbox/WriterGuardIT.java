package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The writer guard over real instance rows: what each instance reports, and which sends that refuses. */
class WriterGuardIT {

    record V1(String id) {}

    @OutboxContract(2)
    record V2(String id) {}

    @OutboxContract(3)
    record V3(String id) {}

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());

    @Test
    void aRelayRefusesAVersionItsOwnRouteCannotReadAndCountsIt() {
        var config = new TestOutboxConfig();
        var rejected = new CopyOnWriteArrayList<String>();
        try (var runtime = OutboxRuntime.builder(config, transactions, new RecordingHandler())
                .route(new Reads<>(V1.class, "orders")).telemetry(new OutboxTelemetry() {
                    @Override
                    public void contractRejected(String topic, String type, int version) {
                        rejected.add(topic + " " + type + " v" + version);
                    }
                }).build()) {
            runtime.start();
            var id = runtime.schema().instanceId();
            var v2 = typed(runtime, V2.class);

            transactions.inTx(c -> {
                assertThatThrownBy(() -> v2.send("orders", "k", new V2("1")))
                        .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                            assertThat(e).hasMessage("outbox contract of topic orders: " + V2.class.getName()
                                    + " writes v2, but 1 of 1 live relay instances read at most v1 (" + id + ");"
                                    + " deploy the release that reads v2 everywhere first");
                            assertThat(e.incompatibleInstances()).containsExactly(id);
                        });
                assertThatThrownBy(() -> runtime.outbox().sendAll(List.of(v2.message("orders", "k", new V2("1")))))
                        .isInstanceOfSatisfying(OutboxIncompatibleContractException.class,
                                e -> assertThat(e.type()).as("sendAll has no type").isEqualTo("?"));
                v2.send("kafka-bound", "k", new V2("1")); // no relay reports it, no route here claims it
                return null;
            });

            assertThat(rows(config)).isOne();
            assertThat(rejected).containsExactly("orders " + V2.class.getName() + " v2", "orders ? v2");
            assertThat(instance(config, id)).isEqualTo("true {\"orders\": 1}");
        }
    }

    /** No heartbeat at all: the bootstrap wrote the row and read the reports before {@code start()} returned. */
    @Test
    void aSendRightAfterTheBootstrapIsGuarded() throws Exception {
        var config = new TestOutboxConfig();
        try (var schema = new OutboxSchema(transactions, transactions, config, Map.of("orders", 1), Set.of("orders"))) {
            schema.start();
            var outbox = new PgOutbox(transactions, schema, config.shards(), () -> {}, OutboxTelemetry.NONE);
            var v2 = new TypedOutbox<>(outbox, (topic, value) -> bytes(value.id()), V2.class);

            assertThatThrownBy(() -> transactions.inTx(c -> v2.send("orders", "k", new V2("1"))))
                    .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> assertThat(
                            e.incompatibleInstances()).as("its own row, not a read never made").containsExactly(
                                    schema.instanceId()));
            assertThat(instance(config, schema.instanceId())).as("relay.enabled, though no relay runs")
                    .isEqualTo("true {\"orders\": 1}");
        }
    }

    @Test
    void aWriterOnlyInstanceIsGuardedButNotCounted() {
        var relayConfig = new TestOutboxConfig();
        var writerConfig = new TestOutboxConfig().table(relayConfig.table()).relayEnabled(false);
        try (var relay = OutboxRuntime.builder(relayConfig, transactions, new RecordingHandler())
                .route(new Reads<>(V2.class, "orders")).build();
                var writer = OutboxRuntime.builder(writerConfig, transactions, new RecordingHandler())
                        .route(new Reads<>(V1.class, "orders")).build()) {
            relay.start();
            writer.start();

            transactions.inTx(c -> typed(writer, V2.class).send("orders", "k", new V2("1")));
            assertThatThrownBy(() -> transactions.inTx(c -> typed(writer, V3.class).send("orders", "k", new V3("1"))))
                    .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                        assertThat(e).hasMessageContaining("1 of 1 live relay instances read at most v2");
                        assertThat(e.incompatibleInstances()).containsExactly(relay.schema().instanceId());
                    });
            assertThat(rows(relayConfig)).isOne();
            assertThat(instance(writerConfig, writer.schema().instanceId())).isEqualTo("false {\"orders\": 1}");
            assertThat(instance(relayConfig, relay.schema().instanceId())).isEqualTo("true {\"orders\": 2}");
        }
    }

    /**
     * A crashed relay instance, of unknown report, blocks until its row expires, then the next beat lets go. It routes
     * the topic, so the version check is what blocks.
     */
    @Test
    void anExpiredRowStopsBlocking() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).maintenanceInterval(Duration.ofMillis(100));
        try (var writer = OutboxRuntime.builder(config, transactions, new RecordingHandler())
                .route(new Reads<>(V2.class, "orders")).build()) {
            writer.start();
            transactions.inTx(c -> {
                TestSql.execute(c, "insert into " + config.table() + "_instance (instance_id, shards, relay, routes,"
                        + " expires_at) values ('crashed/1/00000000', 4, true, '{orders}', now() + interval '1"
                        + " second')");
                return null;
            });
            var v2 = typed(writer, V2.class);

            var refusal = new AtomicReference<OutboxIncompatibleContractException>();
            var refused = await(() -> { // once a beat read the row; a late beat's stale refusal names none
                refusal.set(send(v2));
                return refusal.get() != null && !refusal.get().incompatibleInstances().isEmpty();
            });
            assertThat(refusal.get())
                    .hasMessageContaining("1 of 1 live relay instances do not read it (crashed/1/00000000)");
            assertThat(refusal.get().incompatibleInstances()).containsExactly("crashed/1/00000000");
            var allowed = await(() -> send(v2) == null);

            assertThat(Duration.ofNanos(allowed - refused)).as("blocked until the row expired")
                    .isGreaterThan(Duration.ofMillis(300));
            long live = transactions.inTx(c -> TestSql.queryLong(c, "select count(*) from " + config.table()
                    + "_instance where instance_id = 'crashed/1/00000000' and expires_at > now()"));
            assertThat(live).isZero();
        }
    }

    /**
     * A canary with a new route next to a live relay instance without it: raw, version-1 typed and bulk sends to the
     * route's topic are refused and counted; a topic every relay instance routes goes in.
     */
    @Test
    void aRouteALiveRelayLacksRefusesRawAndVersion1SendsAndCountsThem() {
        var oldConfig = new TestOutboxConfig();
        var rejected = new CopyOnWriteArrayList<String>();
        try (var old = OutboxRuntime.builder(oldConfig, transactions, new RecordingHandler()).route(new Plain("audit"))
                .build();
                var canary = OutboxRuntime.builder(new TestOutboxConfig().table(oldConfig.table()), transactions,
                        new RecordingHandler()).route(new Plain("audit")).route(new Reads<>(V1.class, "orders"))
                        .telemetry(new OutboxTelemetry() {
                            @Override
                            public void routeRejected(String topic) {
                                rejected.add(topic);
                            }
                        }).build()) {
            old.start();
            canary.start(); // its bootstrap reads both rows
            var oldId = old.schema().instanceId();
            var v1 = typed(canary, V1.class);

            transactions.inTx(c -> {
                assertThatThrownBy(() -> canary.outbox().send(raw("orders")))
                        .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                            assertThat(e).hasMessage("outbox topic orders has a local route here, but 1 of 2 live"
                                    + " relay instances do not route it (" + oldId + "); deploy the release with this"
                                    + " route everywhere before writing to it");
                            assertThat(e.topic()).isEqualTo("orders");
                            assertThat(e.instances()).containsExactly(oldId);
                        });
                assertThatThrownBy(() -> v1.send("orders", "k", new V1("1")))
                        .isInstanceOf(OutboxMissingRouteException.class);
                assertThatThrownBy(() -> canary.outbox().sendAll(List.of(raw("audit"), v1.message("orders", "k",
                        new V1("1"))))).as("none of it goes in").isInstanceOf(OutboxMissingRouteException.class);
                canary.outbox().send(raw("audit"));
                return null;
            });

            assertThat(rows(oldConfig)).isOne();
            assertThat(rejected).containsExactly("orders", "orders", "orders");
        }
    }

    /** Only relay instances take records, so a writer-only instance's routes count for nothing. */
    @Test
    void aWriterOnlyInstanceIsNotCountedForARoute() {
        var relayConfig = new TestOutboxConfig();
        var writerConfig = new TestOutboxConfig().table(relayConfig.table()).relayEnabled(false);
        try (var relay = OutboxRuntime.builder(relayConfig, transactions, new RecordingHandler())
                .route(new Plain("orders")).build();
                var writer = OutboxRuntime.builder(writerConfig, transactions, new RecordingHandler())
                        .route(new Plain("orders", "billing")).build()) {
            relay.start();
            writer.start();

            transactions.inTx(c -> writer.outbox().send(raw("orders")));
            assertThatThrownBy(() -> transactions.inTx(c -> writer.outbox().send(raw("billing"))))
                    .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                        assertThat(e).hasMessageContaining("1 of 1 live relay instances do not route it");
                        assertThat(e.instances()).containsExactly(relay.schema().instanceId());
                    });
            assertThat(rows(relayConfig)).isOne();
        }
    }

    /** A crashed relay instance, of unknown routes, blocks a local send until its row expires. */
    @Test
    void anExpiredRowStopsBlockingALocalSend() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).maintenanceInterval(Duration.ofMillis(100));
        try (var writer = OutboxRuntime.builder(config, transactions, new RecordingHandler())
                .route(new Plain("orders")).build()) {
            writer.start();
            transactions.inTx(c -> {
                TestSql.execute(c, "insert into " + config.table() + "_instance (instance_id, shards, relay,"
                        + " contracts, expires_at) values ('crashed/1/00000000', 4, true, '{}', now() + interval '1"
                        + " second')");
                return null;
            });

            var refusal = new AtomicReference<OutboxMissingRouteException>();
            var refused = await(() -> { // once a beat read the row; a late beat's stale refusal names none
                refusal.set(sendRaw(writer));
                return refusal.get() != null && !refusal.get().instances().isEmpty();
            });
            assertThat(refusal.get())
                    .hasMessageContaining("1 of 1 live relay instances do not route it (crashed/1/00000000)");
            var allowed = await(() -> sendRaw(writer) == null);

            assertThat(Duration.ofNanos(allowed - refused)).as("blocked until the row expired")
                    .isGreaterThan(Duration.ofMillis(300));
        }
    }

    /**
     * Reports written by hand, with a version that is no int or one out of range, or no object at all, break neither
     * the read nor the admin status: such an instance reads none of the topic, or nothing.
     */
    @Test
    void badReportsBlockWithoutBreakingTheReadOrTheStatus() {
        var first = new TestOutboxConfig().relayEnabled(false);
        try (var tables = OutboxRuntime.builder(first, transactions, new RecordingHandler()).build()) {
            tables.start();
            transactions.inTx(c -> {
                TestSql.execute(c, "insert into " + first.table() + "_instance (instance_id, shards, relay, contracts,"
                        + " expires_at) values ('null/1/0', 4, true, '{\"t\": null, \"u\": 2}', now() + interval"
                        + " '1h'), ('text/1/0', 4, true, '{\"t\": \"x\"}', now() + interval '1h'), ('huge/1/0', 4,"
                        + " true, '{\"t\": 1e30}', now() + interval '1h'), ('list/1/0', 4, true, '[1]', now()"
                        + " + interval '1h')");
                return null;
            });
            try (var writer = OutboxRuntime.builder(new TestOutboxConfig().table(first.table()).relayEnabled(false),
                    transactions, new RecordingHandler()).build()) {
                writer.start(); // its bootstrap reads them

                assertThatThrownBy(() -> transactions.inTx(c -> typed(writer, V2.class).send("t", "k", new V2("1"))))
                        .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                            assertThat(e).hasMessageContaining("4 of 4 live relay instances do not read it");
                            assertThat(e.incompatibleInstances())
                                    .containsExactly("huge/1/0", "list/1/0", "null/1/0", "text/1/0");
                        });
                var reports = new HashMap<String, Map<String, Integer>>();
                writer.admin().status().instances().forEach(i -> reports.put(i.id(), i.contracts()));
                assertThat(reports).containsEntry("null/1/0", Map.of("t", 0, "u", 2))
                        .containsEntry("text/1/0", Map.of("t", 0)).containsEntry("huge/1/0", Map.of("t", 0))
                        .containsEntry("list/1/0", null);
            }
        }
    }

    /** The refusal of a v2 send, or null when it went in. */
    @Nullable
    private OutboxIncompatibleContractException send(TypedOutbox<V2> v2) {
        try {
            transactions.inTx(c -> v2.send("orders", "k", new V2("1")));
            return null;
        } catch (OutboxIncompatibleContractException e) {
            return e;
        }
    }

    /** The refusal of a raw send to {@code orders}, or null when it went in. */
    @Nullable
    private OutboxMissingRouteException sendRaw(OutboxRuntime runtime) {
        try {
            transactions.inTx(c -> runtime.outbox().send(raw("orders")));
            return null;
        } catch (OutboxMissingRouteException e) {
            return e;
        }
    }

    /** When {@code condition} first held, by {@link System#nanoTime()}; fails after 10 s. */
    private static long await(Condition condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.holds()) {
            assertThat(System.nanoTime()).as("condition within 10 s").isLessThan(deadline);
            Thread.sleep(20);
        }
        return System.nanoTime();
    }

    private interface Condition {
        boolean holds();
    }

    private <T extends Record> TypedOutbox<T> typed(OutboxRuntime runtime, Class<T> type) {
        return new TypedOutbox<>(runtime.outbox(), (topic, value) -> bytes(value.toString()), type);
    }

    private long rows(TestOutboxConfig config) {
        return transactions.inTx(c -> TestSql.queryLong(c, "select count(*) from " + config.table()));
    }

    /** {@code relay contracts} of the instance's row. */
    private String instance(TestOutboxConfig config, String id) {
        return transactions.inTx(c -> TestSql.queryString(c, "select relay || ' ' || contracts::text from "
                + config.table() + "_instance where instance_id = ?", id));
    }

    private static OutboxMessage raw(String topic) {
        return new OutboxMessage(topic, bytes("k"), bytes("1"), List.of());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static final class Reads<T> extends TypedOutboxTopicHandler<T> {

        Reads(Class<T> type, String... topics) {
            super(Set.of(topics), (topic, payload) -> {
                throw new AssertionError("no record is delivered");
            }, type);
        }

        @Override
        protected void handle(T value, OutboxRecord record) {}
    }

    /** A plain route that takes its records and does nothing. */
    private record Plain(Set<String> topics) implements OutboxTopicHandler {

        Plain(String... topics) {
            this(Set.of(topics));
        }

        @Override
        public void handle(OutboxRecord record) {}
    }
}
