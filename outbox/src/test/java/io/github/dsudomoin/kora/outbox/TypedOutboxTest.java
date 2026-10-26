package io.github.dsudomoin.kora.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.StringValue;
import io.github.dsudomoin.kora.outbox.protobuf.ProtobufOutbox;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TypedOutboxTest {

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
        var typed = new TypedOutbox<Integer>(outbox, (topic, value) -> (topic + ":" + value).getBytes(UTF_8));
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
        });

        typed.send("t", (String) null, null);

        assertThat(sent).singleElement().satisfies(m -> {
            assertThat(m.key()).isNull();
            assertThat(m.payload()).isNull();
        });
    }

    @Test
    void serializerReturningNullForValueThrowsInsteadOfSendingTombstone() {
        var typed = new TypedOutbox<Integer>(outbox, (topic, value) -> null);

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
        });

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
        });

        assertThatThrownBy(() -> typed.send("orders", "k", 42))
                .isInstanceOf(OutboxSerializationException.class)
                .hasCause(cause);
        assertThat(sent).isEmpty();
    }
}
