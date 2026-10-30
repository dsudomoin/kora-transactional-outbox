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
        try (var schema = new OutboxSchema(transactions, transactions, config, Map.of("orders", 1))) {
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

    /** A crashed relay instance, of unknown report, blocks until its row expires, then the next beat lets go. */
    @Test
    void anExpiredRowStopsBlocking() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).maintenanceInterval(Duration.ofMillis(100));
        try (var writer = OutboxRuntime.builder(config, transactions, new RecordingHandler())
                .route(new Reads<>(V2.class, "orders")).build()) {
            writer.start();
            transactions.inTx(c -> {
                TestSql.execute(c, "insert into " + config.table() + "_instance (instance_id, shards, relay,"
                        + " expires_at) values ('crashed/1/00000000', 4, true, now() + interval '1 second')");
                return null;
            });
            var v2 = typed(writer, V2.class);

            var refusal = new AtomicReference<OutboxIncompatibleContractException>();
            var refused = await(() -> { // once a beat read the row; a late beat's stale refusal names none
                refusal.set(send(v2));
                return refusal.get() != null && !refusal.get().incompatibleInstances().isEmpty();
            });
            assertThat(refusal.get())
                    .hasMessageContaining("1 of 1 live relay instances read at most v0 (crashed/1/00000000)");
            assertThat(refusal.get().incompatibleInstances()).containsExactly("crashed/1/00000000");
            var allowed = await(() -> send(v2) == null);

            assertThat(Duration.ofNanos(allowed - refused)).as("blocked until the row expired")
                    .isGreaterThan(Duration.ofMillis(300));
            long live = transactions.inTx(c -> TestSql.queryLong(c, "select count(*) from " + config.table()
                    + "_instance where instance_id = 'crashed/1/00000000' and expires_at > now()"));
            assertThat(live).isZero();
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
}
