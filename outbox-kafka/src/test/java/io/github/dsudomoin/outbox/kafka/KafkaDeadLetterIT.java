package io.github.dsudomoin.outbox.kafka;

import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxMetrics;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/** The relay and the module's dead-letter hook over one {@link KafkaOutboxSender}, against a real broker. */
class KafkaDeadLetterIT {

    private final JdbcDatabase db = TestDb.newDatabase(10);
    // the sender bounds a call by delivery.timeout.ms (20 s) + 5 s: the relay and the hook must give it longer
    private final TestOutboxConfig config = new TestOutboxConfig()
            .batchTimeout(Duration.ofSeconds(30))
            .deadLettersTimeout(Duration.ofSeconds(30));
    private final OutboxMetrics metrics = new OutboxMetrics(null);
    private KafkaOutboxSender sender;
    private OutboxSchema schema;
    private OutboxRelay relay;
    private PgOutbox outbox;

    @AfterEach
    void release() {
        if (relay != null) {
            relay.release();
        }
        if (sender != null) {
            sender.release();
        }
        db.release();
        Context.clear();
    }

    /**
     * The record is larger than its topic's {@code max.message.bytes}, so the broker refuses it for good and it dies
     * on its first attempt; its dead-letter topic has Kafka's default limit and takes it.
     */
    @Test
    void deadLetterIsPublishedToItsTopicAndDiscarded() throws Exception {
        var topic = TestKafka.createTopic("t-" + UUID.randomUUID(), 1, Map.of("max.message.bytes", "1024"));
        var dlq = TestKafka.createTopic(topic + ".dlq", 1, Map.of());
        start("{topic}.dlq");
        var payload = new byte[10 * 1024];

        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(
                OutboxMessage.of(topic, "k", payload).withHeader("h", bytes("a"))));

        var record = TestKafka.consumeUntil(dlq, Duration.ofSeconds(30), r -> !r.isEmpty()).get(0);
        assertThat(record.key()).isEqualTo(bytes("k"));
        assertThat(record.value()).isEqualTo(payload);
        assertThat(record.headers().toArray()).extracting(Header::key).containsExactly("h",
                KafkaDeadLetterHandler.DEAD_TOPIC, KafkaDeadLetterHandler.DEAD_ERROR,
                KafkaDeadLetterHandler.DEAD_ATTEMPTS, KafkaDeadLetterHandler.DEAD_FAILED_AT,
                KafkaOutboxHeaders.MESSAGE_ID);
        assertThat(header(record.headers().lastHeader("h"))).isEqualTo("a");
        assertThat(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_TOPIC))).isEqualTo(topic);
        assertThat(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_ERROR)))
                .contains("RecordTooLargeException");
        assertThat(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_ATTEMPTS))).isEqualTo("1");
        assertThat(Instant.parse(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_FAILED_AT))))
                .isBefore(Instant.now());
        assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(id);

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        long deadRows = deadRows(id);
        while (deadRows > 0 && System.nanoTime() < deadline) { // the discard commits after the publish
            Thread.sleep(20);
            deadRows = deadRows(id);
        }
        assertThat(deadRows).as("the letter was discarded").isZero();
    }

    /** Sender and hook as {@link KafkaOutboxModule} makes them. */
    private void start(String deadLetterTopic) throws Exception {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        var kafka = new KafkaOutboxConfig() {
            @Override
            public Properties driverProperties() {
                return properties;
            }

            @Override
            public String deadLetterTopic() {
                return deadLetterTopic;
            }
        };
        var module = new KafkaOutboxModule() {};
        sender = module.kafkaOutboxSender(kafka, config);
        var hook = module.kafkaDeadLetterHandler(kafka, sender, config);
        schema = new OutboxSchema(db, config);
        schema.init();
        relay = new OutboxRelay(db, config, schema, sender.handler(), metrics, null,
                new Maintenance(db, schema.names(), config, metrics),
                new DeadLetterPass(db, schema.names(), config, hook, metrics));
        relay.init();
        outbox = new PgOutbox(db, schema, config.shards(), relay::wake, metrics);
    }

    private long deadRows(UUID messageId) throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + schema.names().dead() + " where message_id = '" + messageId
                    + "'");
        }
    }

    private static String header(Header header) {
        return new String(header.value(), UTF_8);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
