package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.TestTracing.TRACEPARENT;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACE_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.header;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

class OutboxTopicHandlerTest {

    private static final AtomicLong IDS = new AtomicLong();
    private static final OutboxHeader LOCAL = new OutboxHeader("outbox-local", null);
    private static final OutboxDeserializer<Text2> TEXT2 = (topic, payload) -> new Text2(new String(payload, UTF_8));

    /** Never connects: building a runtime touches no database. */
    private final OutboxTransactions transactions = new JdbcTransactions(new PGSimpleDataSource());

    @AfterEach
    void clear() {
        Context.clear();
    }

    @Test
    void routesClaimTheirTopicsAndTheHandlerTakesTheRest() {
        var fallback = new RecordingHandler();
        var audit = new Recorder(Set.of("audit"));
        var billing = new Recorder(Set.of("billing", "invoices"));
        var handler = build(OutboxRuntime.builder(new TestOutboxConfig(), transactions, fallback)
                .route(audit).route(billing));
        var records = List.of(record("orders", "k"), record("invoices", "k"), record("audit", "k"),
                record("billing", "k"), record("orders", null));

        var outcomes = handler.handleBatch(records);

        assertThat(outcomes).hasSize(5).containsOnly(Outcome.delivered());
        assertThat(fallback.delivered()).containsExactly(records.get(0), records.get(4));
        assertThat(audit.calls).containsExactly(List.of(records.get(2)));
        assertThat(billing.calls).as("one call per route, its topics in input order")
                .containsExactly(List.of(records.get(1), records.get(3)));
    }

    @Test
    void routeOutcomesComeBackInTheirPositions() {
        var poison = new Recorder(Set.of("audit")).failing("poison", new OutboxPermanentException("bad"));
        var handler = build(OutboxRuntime.builder(new TestOutboxConfig(), transactions, new RecordingHandler())
                .route(poison));

        var outcomes = handler.handleBatch(List.of(record("orders", "k"), record("audit", "poison"),
                record("audit", "k")));

        assertThat(outcomes.get(0)).isEqualTo(Outcome.delivered());
        assertThat(outcomes.get(1)).isInstanceOf(Outcome.Permanent.class);
        assertThat(outcomes.get(2)).isEqualTo(Outcome.delivered());
    }

    @Test
    void defaultHandleBatchIsTheSequentialLoopInEachRecordsScope() {
        var down = new IllegalStateException("down");
        var traceIds = new ArrayList<String>();
        var route = new OutboxTopicHandler() {
            @Override
            public Set<String> topics() {
                return Set.of("audit");
            }

            @Override
            public void handle(OutboxRecord record) {
                traceIds.add(OpentelemetryContext.getSpan() == null
                        ? null
                        : OpentelemetryContext.getSpan().getSpanContext().getTraceId());
                if ("k1".equals(new String(record.key(), UTF_8))) {
                    throw down;
                }
            }
        };

        var outcomes = route.handleBatch(List.of(record("audit", "k1", List.of(header("traceparent", TRACEPARENT))),
                record("audit", "k1"), record("audit", "k2")));

        assertThat(outcomes).containsExactly(Outcome.retry(down), Outcome.notAttempted(), Outcome.delivered());
        assertThat(traceIds.get(0)).isEqualTo(TRACE_ID);
        assertThat(traceIds).hasSize(2);
    }

    @Test
    void aLocalRecordReachesItsRouteButNeverTheHandler() {
        var fallback = new RecordingHandler();
        var audit = new Recorder(Set.of("audit"));
        var handler = build(OutboxRuntime.builder(new TestOutboxConfig(), transactions, fallback).route(audit));
        var routed = record("audit", "k", List.of(LOCAL));
        var unrouted = record("billing", "k", List.of(LOCAL));
        var plain = record("billing", "j");

        assertThat(handler.handleBatch(List.of(routed, unrouted, plain)))
                .containsExactly(Outcome.delivered(), noRoute("billing"), Outcome.delivered());
        assertThat(audit.calls).as("the marker stays visible to the route").containsExactly(List.of(routed));
        assertThat(fallback.delivered()).containsExactly(plain);
    }

    @Test
    void withoutRoutesTheHandlerNeverTakesALocalRecord() {
        var fallback = new RecordingHandler();
        var handler = build(OutboxRuntime.builder(new TestOutboxConfig(), transactions, fallback));
        var plain = record("billing", "j");

        assertThat(handler.handleBatch(List.of(record("billing", "k", List.of(LOCAL)), plain)))
                .containsExactly(noRoute("billing"), Outcome.delivered());
        assertThat(fallback.delivered()).containsExactly(plain);
    }

    @Test
    void withoutHandlerAnUnclaimedTopicWaits() {
        var audit = new Recorder(Set.of("audit"));
        var handler = build(OutboxRuntime.builder(new TestOutboxConfig(), transactions, null).route(audit));

        var outcomes = handler.handleBatch(List.of(record("billing", "k"), record("audit", "k")));

        assertThat(outcomes).containsExactly(new Outcome.Waiting(Outcome.Waiting.Kind.NO_ROUTE,
                "no handler for topic billing on this instance", null), Outcome.delivered());
    }

    @Test
    void typedHandlerGetsDecodedValues() {
        var typed = new Texts();
        var a = record("text", "k", "a");
        var b = record("text", "k", "b");

        assertThat(typed.handleBatch(List.of(a, b))).containsExactly(Outcome.delivered(), Outcome.delivered());
        assertThat(typed.values).containsExactly("a", "b");
        assertThat(typed.topics()).containsExactly("text");
    }

    @Test
    void tombstoneGoesToHandleTombstone() {
        var tombstones = new ArrayList<OutboxRecord>();
        var typed = new Texts() {
            @Override
            protected void handleTombstone(OutboxRecord record) {
                tombstones.add(record);
            }
        };
        var tombstone = record("text", "k", (String) null);

        assertThat(typed.handleBatch(List.of(tombstone))).containsExactly(Outcome.delivered());
        assertThat(tombstones).containsExactly(tombstone);
        assertThat(typed.values).isEmpty();
    }

    @Test
    void tombstoneWithoutOverrideIsPermanent() {
        var typed = new Texts();
        var tombstone = record("text", "k", (String) null);

        assertPermanent(typed.handleBatch(List.of(tombstone)).get(0), tombstone,
                Texts.class.getName() + " takes no tombstones");
        assertThat(typed.values).isEmpty();
    }

    @Test
    void undecodablePayloadIsPermanentAndNamesItsCause() {
        var broken = new IllegalArgumentException("not text");
        var typed = new Texts((topic, payload) -> {
            throw broken;
        });
        var record = record("text", "k", "a");

        var outcome = typed.handleBatch(List.of(record)).get(0);

        assertPermanent(outcome, record, Texts.class.getName() + " cannot decode its payload");
        assertThat(((Outcome.Permanent) outcome).error()).hasCause(broken);
        assertThat(typed.values).isEmpty();
    }

    @Test
    void deserializerWaitOrPermanentPassesThroughUnwrapped() {
        var permanent = new OutboxPermanentException("bad event");
        var typed = new Texts((topic, payload) -> {
            if (payload[0] == 'w') {
                throw new OutboxWaitException("unknown sub-type");
            }
            throw permanent;
        });

        var outcomes = typed.handleBatch(List.of(record("text", "k1", "w"), record("text", "k2", "p")));

        assertThat(outcomes).containsExactly(Outcome.waitFor("unknown sub-type"), Outcome.permanent(permanent));
        assertThat(typed.values).isEmpty();
    }

    @Test
    void deserializerReturningNullIsPermanent() {
        var typed = new Texts((topic, payload) -> null);
        var record = record("text", "k", "a");

        assertPermanent(typed.handleBatch(List.of(record)).get(0), record, "returned null");
        assertThat(typed.values).isEmpty();
    }

    @Test
    void typedHandlerReadsUpToTheContractVersionOfItsT() {
        assertThat(new Texts().contractVersion()).as("String: no @OutboxContract").isEqualTo(1);
        assertThat(new Texts2().contractVersion()).as("a direct subclass").isEqualTo(2);
        assertThat(new $Texts2__AopProxy().contractVersion()).as("a proxy-like subclass").isEqualTo(2);
        assertThat(new ViaKeyed().contractVersion()).as("through generic intermediate classes").isEqualTo(2);
        assertThat(new Base<Text2>(TEXT2) {}.contractVersion()).as("an anonymous subclass").isEqualTo(2);
        assertThat(new Unbound<>(TEXT2, Text2.class).contractVersion()).as("an explicit type").isEqualTo(2);
    }

    @Test
    void unresolvableTFailsTheConstructorPointingToTheTypeOverload() {
        assertThatThrownBy(() -> new Unbound<>(TEXT2)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(Unbound.class.getName())
                .hasMessageContaining("TypedOutboxTopicHandler(Set, OutboxDeserializer, Class) constructor");
    }

    @Test
    void recordOfANewerContractWaitsUndecodedThroughHandleAndDecode() {
        var decoded = new AtomicInteger();
        var typed = new Texts((topic, payload) -> {
            decoded.incrementAndGet();
            return new String(payload, UTF_8);
        });
        var v2 = record("text", "k1", "a", List.of(contract("2")));
        var v2Tombstone = record("text", "k2", null, List.of(contract("2")));
        var unreadable = record("text", "k3", "a", List.of(contract("x")));
        var newer = new Outcome.Waiting(Outcome.Waiting.Kind.CONTRACT,
                "contract v2 of topic text, this instance reads up to v1", null);

        assertThat(typed.handleBatch(List.of(v2, v2Tombstone, unreadable))).containsExactly(newer, newer,
                new Outcome.Waiting(Outcome.Waiting.Kind.CONTRACT, "invalid outbox-contract header: x", null));
        assertThatThrownBy(() -> typed.decode(v2)).isInstanceOfSatisfying(OutboxWaitException.class,
                e -> assertThat(e.kind()).isEqualTo(Outcome.Waiting.Kind.CONTRACT)).hasMessage(newer.reason());
        assertThat(decoded).as("the payload is never touched").hasValue(0);
        assertThat(typed.values).isEmpty();
    }

    @Test
    void handlerOfANewerContractReadsOlderRecordsBranchingOnTheirVersion() {
        var typed = new Texts2();

        var outcomes = typed.handleBatch(List.of(record("text", "k1", "a"),
                record("text", "k1", "b", List.of(contract("2"))), record("text", "k2", "c", List.of(contract("3")))));

        assertThat(outcomes).containsExactly(Outcome.delivered(), Outcome.delivered(), new Outcome.Waiting(
                Outcome.Waiting.Kind.CONTRACT, "contract v3 of topic text, this instance reads up to v2", null));
        assertThat(typed.values).containsExactly("v1 a", "v2 b");
    }

    @Test
    void handleBatchOverrideDecodesItself() {
        var typed = new Texts() {
            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                var outcomes = new ArrayList<Outcome>();
                for (var record : records) {
                    try {
                        values.add(String.valueOf(decode(record)));
                        outcomes.add(Outcome.delivered());
                    } catch (Exception e) {
                        outcomes.add(Outcome.retry(e));
                    }
                }
                return outcomes;
            }
        };

        typed.handleBatch(List.of(record("text", "k", "a"), record("text", "k", (String) null)));

        assertThat(typed.values).containsExactly("a", "null");
    }

    @Test
    void keyAsStringIsTheUtf8KeyOrNull() {
        assertThat(record("t", "ключ").keyAsString()).isEqualTo("ключ");
        assertThat(record("t", null).keyAsString()).isNull();
    }

    @Test
    void topicClaimedTwiceFailsTheBuildNamingBothClasses() {
        var builder = OutboxRuntime.builder(new TestOutboxConfig(), transactions, new RecordingHandler())
                .route(new Recorder(Set.of("audit", "billing"))).route(new Other(Set.of("billing")));

        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("billing").hasMessageContaining(Recorder.class.getName())
                .hasMessageContaining(Other.class.getName());
    }

    @Test
    void routeWithoutTopicsFailsTheBuild() {
        var builder = OutboxRuntime.builder(new TestOutboxConfig(), transactions, new RecordingHandler())
                .route(new Other(Set.of()));

        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(Other.class.getName());
    }

    @Test
    void neitherHandlerNorRoutesFailsTheBuild() {
        var builder = OutboxRuntime.builder(new TestOutboxConfig(), transactions, null);

        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("OutboxHandler").hasMessageContaining("OutboxTopicHandler");
    }

    private static OutboxHandler build(OutboxRuntime.Builder builder) {
        return builder.build().handler;
    }

    /** A record no instance reading its contract version can ever handle. */
    private static void assertPermanent(Outcome outcome, OutboxRecord record, String reason) {
        assertThat(outcome).isInstanceOfSatisfying(Outcome.Permanent.class, p -> assertThat(p.error())
                .isInstanceOf(OutboxPermanentException.class)
                .hasMessageStartingWith("cannot handle outbox record " + record.messageId() + " of topic "
                        + record.topic() + ": ")
                .hasMessageContaining(reason));
    }

    /** Decodes UTF-8 text, recording the values. */
    private static class Texts extends TypedOutboxTopicHandler<String> {

        final List<String> values = new CopyOnWriteArrayList<>();

        Texts() {
            this((topic, payload) -> new String(payload, UTF_8));
        }

        Texts(OutboxDeserializer<String> deserializer) {
            super(Set.of("text"), deserializer);
        }

        @Override
        protected void handle(String value, OutboxRecord record) {
            values.add(value);
        }
    }

    /** Version 2 of a text event, which v1 records also decode to. */
    @OutboxContract(2)
    record Text2(String text) {}

    /** Reads v1 and v2 records, recording each value with its record's version. */
    private static class Texts2 extends TypedOutboxTopicHandler<Text2> {

        final List<String> values = new CopyOnWriteArrayList<>();

        Texts2() {
            super(Set.of("text"), TEXT2);
        }

        @Override
        protected void handle(Text2 value, OutboxRecord record) {
            values.add("v" + record.contractVersion() + " " + value.text());
        }
    }

    /** What Kora's AOP makes of a handler with aspects: a plain subclass. */
    private static final class $Texts2__AopProxy extends Texts2 {}

    /** Binds T only through its type variable. */
    private abstract static class Base<E> extends TypedOutboxTopicHandler<E> {

        Base(OutboxDeserializer<E> deserializer) {
            super(Set.of("text"), deserializer);
        }

        Base(OutboxDeserializer<E> deserializer, Class<E> type) {
            super(Set.of("text"), deserializer, type);
        }

        @Override
        protected void handle(E value, OutboxRecord record) {}
    }

    /** T is its second type variable, passed on to {@link Base}'s first. */
    private abstract static class Keyed<K, E> extends Base<E> {

        Keyed(OutboxDeserializer<E> deserializer) {
            super(deserializer);
        }
    }

    private static final class ViaKeyed extends Keyed<Long, Text2> {

        ViaKeyed() {
            super(TEXT2);
        }
    }

    /** Its hierarchy never names T: erased at runtime whatever the caller writes. */
    private static final class Unbound<E> extends Base<E> {

        Unbound(OutboxDeserializer<E> deserializer) {
            super(deserializer);
        }

        Unbound(OutboxDeserializer<E> deserializer, Class<E> type) {
            super(deserializer, type);
        }
    }

    /** Records each {@code handleBatch} call over the default one; fails the records of one key. */
    private static class Recorder implements OutboxTopicHandler {

        final List<List<OutboxRecord>> calls = new CopyOnWriteArrayList<>();
        private final Set<String> topics;
        @Nullable
        private String failingKey;
        @Nullable
        private Exception failure;

        Recorder(Set<String> topics) {
            this.topics = topics;
        }

        Recorder failing(String key, Exception failure) {
            this.failingKey = key;
            this.failure = failure;
            return this;
        }

        @Override
        public Set<String> topics() {
            return topics;
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            if (failure != null && record.key() != null && failingKey.equals(new String(record.key(), UTF_8))) {
                throw failure;
            }
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            calls.add(records);
            return OutboxTopicHandler.super.handleBatch(records);
        }
    }

    private static final class Other extends Recorder {
        Other(Set<String> topics) {
            super(topics);
        }
    }

    private static OutboxRecord record(String topic, @Nullable String key) {
        return record(topic, key, List.of());
    }

    private static OutboxRecord record(String topic, @Nullable String key, @Nullable String payload) {
        return record(topic, key, payload, List.of());
    }

    private static OutboxRecord record(String topic, @Nullable String key, @Nullable String payload,
            List<OutboxHeader> headers) {
        return new OutboxRecord(IDS.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), payload == null ? null : payload.getBytes(UTF_8), headers,
                Instant.now(), 0, 0);
    }

    private static Outcome noRoute(String topic) {
        return new Outcome.Waiting(Outcome.Waiting.Kind.NO_ROUTE,
                "a record for a local handler of topic " + topic + ", which no route on this instance claims", null);
    }

    private static OutboxHeader contract(String version) {
        return new OutboxHeader("outbox-contract", version.getBytes(US_ASCII));
    }

    private static OutboxRecord record(String topic, @Nullable String key, List<OutboxHeader> headers) {
        return new OutboxRecord(IDS.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), new byte[] {1}, headers, Instant.now(), 0, 0);
    }
}
