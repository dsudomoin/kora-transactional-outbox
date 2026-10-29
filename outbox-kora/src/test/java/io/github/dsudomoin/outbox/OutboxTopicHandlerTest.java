package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.TestTracing.TRACEPARENT;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACE_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.header;
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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

class OutboxTopicHandlerTest {

    private static final AtomicLong IDS = new AtomicLong();

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
    void withoutRoutesTheHandlerIsUsedAsIs() {
        var fallback = new RecordingHandler();

        assertThat(build(OutboxRuntime.builder(new TestOutboxConfig(), transactions, fallback))).isSameAs(fallback);
    }

    @Test
    void withoutHandlerAnUnclaimedTopicCannotBeHandled() {
        var audit = new Recorder(Set.of("audit"));
        var handler = build(OutboxRuntime.builder(new TestOutboxConfig(), transactions, null).route(audit));
        var billing = record("billing", "k");

        var outcomes = handler.handleBatch(List.of(billing, record("audit", "k")));

        assertCannotHandle(outcomes.get(0), billing, "no OutboxTopicHandler claims its topic");
        assertThat(outcomes.get(1)).isEqualTo(Outcome.delivered());
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
    void tombstoneWithoutOverrideCannotBeHandled() {
        var typed = new Texts();
        var tombstone = record("text", "k", (String) null);

        assertCannotHandle(typed.handleBatch(List.of(tombstone)).get(0), tombstone,
                Texts.class.getName() + " takes no tombstones");
        assertThat(typed.values).isEmpty();
    }

    @Test
    void undecodablePayloadCannotBeHandledAndNamesItsCause() {
        var broken = new IllegalArgumentException("not text");
        var typed = new Texts((topic, payload) -> {
            throw broken;
        });
        var record = record("text", "k", "a");

        var outcome = typed.handleBatch(List.of(record)).get(0);

        assertCannotHandle(outcome, record, Texts.class.getName() + " cannot decode its payload");
        assertThat(((Outcome.Retry) outcome).error()).hasCause(broken);
        assertThat(typed.values).isEmpty();
    }

    @Test
    void deserializerReturningNullCannotBeHandled() {
        var typed = new Texts((topic, payload) -> null);
        var record = record("text", "k", "a");

        assertCannotHandle(typed.handleBatch(List.of(record)).get(0), record, "returned null");
        assertThat(typed.values).isEmpty();
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

    /** What every case of a record this instance cannot handle gives today, whatever the case. */
    private static void assertCannotHandle(Outcome outcome, OutboxRecord record, String reason) {
        assertThat(outcome).isInstanceOfSatisfying(Outcome.Retry.class, r -> assertThat(r.error())
                .isInstanceOf(IllegalStateException.class)
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
        return new OutboxRecord(IDS.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), payload == null ? null : payload.getBytes(UTF_8), List.of(),
                Instant.now(), 0, 0);
    }

    private static OutboxRecord record(String topic, @Nullable String key, List<OutboxHeader> headers) {
        return new OutboxRecord(IDS.incrementAndGet(), UUID.randomUUID(), topic,
                key == null ? null : key.getBytes(UTF_8), new byte[] {1}, headers, Instant.now(), 0, 0);
    }
}
