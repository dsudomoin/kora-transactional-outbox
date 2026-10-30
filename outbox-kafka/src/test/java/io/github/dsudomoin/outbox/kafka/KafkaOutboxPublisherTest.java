package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxContract;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxSerializationException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaOutboxPublisherTest {

    @OutboxContract(2)
    record Created(String id) {}

    private final UUID id = UUID.randomUUID();
    private final List<OutboxMessage> sent = new ArrayList<>();
    private final Outbox outbox = new Outbox() {
        @Override
        public UUID send(OutboxMessage message) {
            sent.add(message);
            return id;
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
    void serializesKeyAndValueAndUsesKeyBytesForOrdering() {
        var publisher = new KafkaOutboxPublisher<>(outbox, "orders", new LongSerializer(), new StringSerializer(),
                String.class);

        assertThat(publisher.send(42L, "created")).isEqualTo(id);
        assertThat(publisher.send(null, null)).isEqualTo(id);

        var keyed = sent.get(0);
        assertThat(keyed.topic()).isEqualTo("orders");
        assertThat(keyed.key()).as("the bytes Kafka partitions by").isEqualTo(new LongSerializer().serialize("x", 42L));
        assertThat(keyed.payload()).isEqualTo(bytes("created"));
        assertThat(keyed.headers()).isEmpty();
        var unkeyedTombstone = sent.get(1);
        assertThat(unkeyedTombstone.key()).isNull();
        assertThat(unkeyedTombstone.payload()).isNull();
    }

    @Test
    void headersCopied() {
        // like a schema-registry serializer: needs the topic and adds a header
        Serializer<String> typed = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, String data) {
                throw new AssertionError("the headers overload must be used");
            }

            @Override
            public byte[] serialize(String topic, Headers headers, String data) {
                headers.add("type", bytes(topic));
                return bytes(data);
            }
        };
        var publisher = new KafkaOutboxPublisher<>(outbox, "orders", new StringSerializer(), typed, String.class);
        var headers = new RecordHeaders().add("a", bytes("1")).add("b", null).add("a", bytes("2"));

        publisher.send("k", "v", headers);

        assertThat(sent.get(0).headers()).containsExactly(
                new OutboxHeader("a", bytes("1")), new OutboxHeader("b", null), new OutboxHeader("a", bytes("2")),
                new OutboxHeader("type", bytes("orders")));
        assertThat(headers.toArray()).as("the caller's headers are not modified").hasSize(3);
    }

    /** M2: a null from a serializer for a non-null key or value would silently drop ordering or write a tombstone. */
    @Test
    void serializerReturningNullForNonNullThrows() {
        Serializer<String> nulls = (topic, data) -> null;
        var nullValue = new KafkaOutboxPublisher<>(outbox, "orders", new StringSerializer(), nulls, String.class);
        var nullKey = new KafkaOutboxPublisher<>(outbox, "orders", nulls, new StringSerializer(), String.class);

        assertThatThrownBy(() -> nullValue.send("k", "v")).isInstanceOf(OutboxSerializationException.class);
        assertThatThrownBy(() -> nullKey.send("k", "v")).isInstanceOf(OutboxSerializationException.class);
        assertThat(sent).isEmpty();

        nullValue.send("k", null);
        nullKey.send(null, "v");
        assertThat(sent).extracting(OutboxMessage::payload).containsExactly(null, bytes("v"));
    }

    @Test
    void topicNameValidatedLikeKafka() {
        for (var bad : List.of("orders/created", "a b", "ä", ".", "..", "", " ", "x".repeat(250))) {
            assertThatThrownBy(() -> publisher(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
        for (var good : List.of("orders.created-v2_X", "...", "x".repeat(249))) {
            assertThat(publisher(good)).as(good).isNotNull();
        }
    }

    @Test
    void everySendCarriesTheValueTypesVersionInPlaceOfAGivenOrAddedOne() {
        // a serializer that adds its own contract header loses it too
        Serializer<Created> value = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Created data) {
                throw new AssertionError("the headers overload must be used");
            }

            @Override
            public byte[] serialize(String topic, Headers headers, Created data) {
                headers.add("outbox-contract", bytes("9"));
                return data == null ? null : bytes(data.id());
            }
        };
        var publisher = new KafkaOutboxPublisher<>(outbox, "orders", new StringSerializer(), value, Created.class);

        publisher.send("k", new Created("1"), new RecordHeaders().add("outbox-contract", bytes("7")).add("h", null));
        publisher.send("k", null);
        publisher("orders").send("k", "v", new RecordHeaders().add("outbox-contract", bytes("7")));

        assertThat(sent).extracting(OutboxMessage::headers).containsExactly(
                List.of(new OutboxHeader("h", null), contract("2")),
                List.of(contract("2")), // the tombstone too
                List.of()); // String: version 1, no header
    }

    private KafkaOutboxPublisher<String, String> publisher(String topic) {
        return new KafkaOutboxPublisher<>(outbox, topic, new StringSerializer(), new StringSerializer(), String.class);
    }

    private static OutboxHeader contract(String version) {
        return new OutboxHeader("outbox-contract", version.getBytes(US_ASCII));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
