package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.StringValue;
import io.github.dsudomoin.outbox.protobuf.ProtobufOutbox;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TypedOutboxTest {

    @OutboxContract(2)
    record V2(String text) {}

    @OutboxContract(0)
    record Invalid() {}

    private static final OutboxSerializer<V2> V2_TEXT = (topic, value) -> value.text().getBytes(UTF_8);
    private static final OutboxHeader USER_CONTRACT = new OutboxHeader("outbox-contract", "7".getBytes(US_ASCII));

    private final List<OutboxMessage> sent = new ArrayList<>();
    private final Outbox outbox = new Outbox() {
        @Override
        public UUID send(OutboxMessage message) {
            sent.add(message);
            return UUID.randomUUID();
        }

        @Override
        public UUID send(Connection connection, OutboxMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<UUID> sendAll(List<OutboxMessage> messages) {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    void serializesValueWithTopicAndPassesKeyAndHeaders() {
        var typed = new TypedOutbox<>(outbox, (topic, value) -> (topic + ":" + value).getBytes(UTF_8), Integer.class);
        var header = new OutboxHeader("h", "v".getBytes(UTF_8));

        typed.send("t1", "k", 1);
        typed.send("t2", "k", 2, List.of(header));
        typed.send("t3", new byte[] {7}, 3, List.of(header));

        assertThat(sent).usingRecursiveFieldByFieldElementComparator().containsExactly( // arrays by content
                new OutboxMessage("t1", "k".getBytes(UTF_8), "t1:1".getBytes(UTF_8), List.of()),
                new OutboxMessage("t2", "k".getBytes(UTF_8), "t2:2".getBytes(UTF_8), List.of(header)),
                new OutboxMessage("t3", new byte[] {7}, "t3:3".getBytes(UTF_8), List.of(header)));
    }

    @Test
    void nullValueIsTombstoneWithoutCallingSerializer() {
        var typed = new TypedOutbox<Integer>(outbox, (topic, value) -> {
            throw new AssertionError("serializer called for null");
        }, Integer.class);

        typed.send("t", (String) null, null);

        assertThat(sent).singleElement().satisfies(m -> {
            assertThat(m.key()).isNull();
            assertThat(m.payload()).isNull();
        });
    }

    @Test
    void serializerReturningNullForValueThrowsInsteadOfSendingTombstone() {
        var typed = new TypedOutbox<Integer>(outbox, (topic, value) -> null, Integer.class);

        assertThatThrownBy(() -> typed.send("orders", "k", 42))
                .isInstanceOf(OutboxSerializationException.class)
                .hasMessageContaining("orders")
                .hasRootCauseInstanceOf(NullPointerException.class);
        assertThat(sent).isEmpty();
    }

    @Test
    void protobufSendsBinaryEncodingAndNullMessageAsTombstone() {
        var protobuf = new ProtobufOutbox(outbox);

        protobuf.send("p", "k", StringValue.of("hello"));
        protobuf.send("p", "k", null);

        assertThat(sent).extracting(OutboxMessage::payload)
                .containsExactly(StringValue.of("hello").toByteArray(), null);
    }

    @Test
    void serializerFailureWrappedAndNothingSent() {
        var cause = new IOException("boom");
        var typed = new TypedOutbox<Integer>(outbox, (topic, value) -> {
            throw cause;
        }, Integer.class);

        assertThatThrownBy(() -> typed.send("orders", "k", 42))
                .isInstanceOf(OutboxSerializationException.class)
                .hasMessageContaining("orders")
                .hasMessageContaining(Integer.class.getName())
                .hasCause(cause);
        assertThat(sent).isEmpty();
    }

    @Test
    void uncheckedSerializerFailureWrappedToo() {
        var cause = new IllegalStateException("bad");
        var typed = new TypedOutbox<Integer>(outbox, (topic, value) -> {
            throw cause;
        }, Integer.class);

        assertThatThrownBy(() -> typed.send("orders", "k", 42))
                .isInstanceOf(OutboxSerializationException.class)
                .hasCause(cause);
        assertThat(sent).isEmpty();
    }

    @Test
    void everySendCarriesTheDeclaredTypesVersionInPlaceOfAGivenOne() {
        var typed = new TypedOutbox<>(outbox, V2_TEXT, V2.class);
        var header = new OutboxHeader("h", "v".getBytes(UTF_8));

        typed.send("t", "k", new V2("a"));
        typed.send("t", "k", new V2("b"), List.of(USER_CONTRACT, header, USER_CONTRACT));
        typed.send("t", new byte[] {7}, new V2("c"), List.of(header));
        typed.send("t", "k", null);

        assertThat(sent).extracting(OutboxMessage::headers).containsExactly( // OutboxHeader compares by content
                List.of(contract("2")),
                List.of(header, contract("2")),
                List.of(header, contract("2")),
                List.of(contract("2"))); // the tombstone too
        assertThat(typed.typeName()).isEqualTo(V2.class.getName());
        assertThat(typed.contractVersion()).isEqualTo(2);
    }

    @Test
    void versionOneWritesNoHeaderAndDropsAGivenOne() {
        var typed = new TypedOutbox<>(outbox, (topic, value) -> new byte[] {1}, Integer.class);
        var header = new OutboxHeader("h", null);

        typed.send("t", "k", 1);
        typed.send("t", "k", 2, List.of(USER_CONTRACT, header));
        typed.send("t", (String) null, null);

        assertThat(sent).extracting(OutboxMessage::headers)
                .containsExactly(List.of(), List.of(header), List.of());
        assertThat(typed.contractVersion()).isEqualTo(1);
    }

    @Test
    void messageIsWhatSendWritesWithoutSending() {
        var typed = new TypedOutbox<>(outbox, V2_TEXT, V2.class);
        var header = new OutboxHeader("h", null);

        var messages = List.of(
                typed.message("t", "k", new V2("a")),
                typed.message("t", "k", new V2("b"), List.of(USER_CONTRACT, header)),
                typed.message("t", new byte[] {7}, null, List.of()));

        assertThat(sent).isEmpty();
        assertThat(messages).usingRecursiveFieldByFieldElementComparator().containsExactly(
                new OutboxMessage("t", "k".getBytes(UTF_8), "a".getBytes(UTF_8), List.of(contract("2"))),
                new OutboxMessage("t", "k".getBytes(UTF_8), "b".getBytes(UTF_8), List.of(header, contract("2"))),
                new OutboxMessage("t", new byte[] {7}, null, List.of(contract("2"))));
        var failing = new TypedOutbox<V2>(outbox, (topic, value) -> null, V2.class);
        assertThatThrownBy(() -> failing.message("t", "k", new V2("x"))).as("serializes like send")
                .isInstanceOf(OutboxSerializationException.class);
    }

    @Test
    void versionBelowOneIsRefusedAtConstruction() {
        assertThatThrownBy(() -> new TypedOutbox<>(outbox, (topic, value) -> new byte[0], Invalid.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("@OutboxContract of " + Invalid.class.getName() + " must be at least 1: 0");
    }

    private static OutboxHeader contract(String version) {
        return new OutboxHeader("outbox-contract", version.getBytes(US_ASCII));
    }
}
