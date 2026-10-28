package io.github.dsudomoin.outbox.wiring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.StringValue;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestSql;
import io.github.dsudomoin.outbox.wiring.TypedTestApp.AppCode;
import io.github.dsudomoin.outbox.wiring.TypedTestApp.Custom;
import io.github.dsudomoin.outbox.wiring.TypedTestApp.OrderCreated;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlRunnable;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTest;
import ru.tinkoff.kora.test.extension.junit5.KoraAppTestConfigModifier;
import ru.tinkoff.kora.test.extension.junit5.KoraConfigModification;
import ru.tinkoff.kora.test.extension.junit5.TestComponent;

@KoraAppTest(TypedTestApp.class)
class TypedOutboxIT implements KoraAppTestConfigModifier {

    private final String table = TestDb.uniqueTable();

    @TestComponent
    private JdbcDatabase db;

    @TestComponent
    private AppCode app;

    @TestComponent
    private OutboxRelay relay;

    @TestComponent
    private RecordingHandler handler;

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db {
                  jdbcUrl = "%s"
                  username = "%s"
                  password = "%s"
                  poolName = "typed-wiring"
                }
                outbox {
                  table = %s
                  relay.enabled = false
                }
                """.formatted(TestDb.jdbcUrl(), TestDb.user(), TestDb.password(), table));
    }

    @Test
    void withoutAHookComponentDeadLettersAreKept(@TestComponent DeadLetterPass pass) throws Exception {
        handler.failAlways(r -> r.topic().equals("custom"), () -> new OutboxPermanentException("bad payload"));
        db.inTx((SqlRunnable) () -> app.custom().send("custom", "k", new Custom("dead")));

        assertThat(relay.drain()).isZero(); // ends with a run of the pass: nothing for it to hand

        assertThat(pass.active()).as("no hook in the graph").isFalse();
        try (var c = TestDb.connect()) {
            assertThat(TestSql.queryString(c, "select string_agg(status, ',') from " + table + "_dead"))
                    .isEqualTo("kept");
        }
    }

    @Test
    void eachTypedOutboxUsesItsSerializer() throws Exception {
        var header = new OutboxHeader("h", "v".getBytes(UTF_8));
        db.inTx((SqlRunnable) () -> {
            app.orders().send("orders", "u1", new OrderCreated(7, "u1"));
            app.custom().send("custom", "k", new Custom("hi"));
            app.protobuf().send("proto", "p", StringValue.of("hello"), List.of(header));
        });

        assertThat(relay.drain()).isEqualTo(3);
        Map<String, OutboxRecord> byTopic = handler.delivered().stream()
                .collect(Collectors.toMap(OutboxRecord::topic, Function.identity()));
        assertThat(new String(byTopic.get("orders").payload(), UTF_8))
                .as("JsonWriter-derived serializer").isEqualTo("{\"id\":7,\"userId\":\"u1\"}");
        assertThat(new String(byTopic.get("custom").payload(), UTF_8))
                .as("the app's serializer, not the JSON template").isEqualTo("custom:hi");
        var proto = byTopic.get("proto");
        assertThat(StringValue.parseFrom(proto.payload()).getValue()).isEqualTo("hello");
        assertThat(proto.key()).isEqualTo("p".getBytes(UTF_8));
        assertThat(proto.headers()).contains(header);
    }
}
