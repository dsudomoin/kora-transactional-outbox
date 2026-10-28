package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.Outbox;
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
        var publisher = new KafkaOutboxPublisher<>(outbox, "orders", new LongSerializer(), new StringSerializer());

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
        var publisher = new KafkaOutboxPublisher<>(outbox, "orders", new StringSerializer(), typed);
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
        var nullValue = new KafkaOutboxPublisher<>(outbox, "orders", new StringSerializer(), nulls);
        var nullKey = new KafkaOutboxPublisher<>(outbox, "orders", nulls, new StringSerializer());

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
            assertThatThrownBy(() -> new KafkaOutboxPublisher<>(outbox, bad, new StringSerializer(), new StringSerializer()))
                    .as(bad)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (var good : List.of("orders.created-v2_X", "...", "x".repeat(249))) {
            assertThat(new KafkaOutboxPublisher<>(outbox, good, new StringSerializer(), new StringSerializer())).as(good)
                    .isNotNull();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
