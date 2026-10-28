package io.github.dsudomoin.outbox.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testcontainers.kafka.KafkaContainer;

/** One Kafka broker per test JVM, started on first use; Testcontainers removes it at exit. */
public final class TestKafka {

    private TestKafka() {}

    private static final class Broker {
        // production brokers do not create topics on first use either
        static final KafkaContainer INSTANCE = new KafkaContainer("apache/kafka:3.9.1")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

        static {
            INSTANCE.start();
        }
    }

    public static String bootstrapServers() {
        return Broker.INSTANCE.getBootstrapServers();
    }

    /** A new topic with a unique name. */
    public static String createTopic(int partitions) {
        return createTopic("t-" + UUID.randomUUID(), partitions, Map.of());
    }

    /** A new topic {@code name} with topic-level {@code configs}, such as {@code max.message.bytes}. */
    public static String createTopic(String name, int partitions, Map<String, String> configs) {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1).configs(configs)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("cannot create topic " + name, e);
        }
        return name;
    }

    /**
     * Freezes the broker for {@code duration}: its connections stay open but nothing answers. Unlike a stop/start, the
     * mapped port survives.
     */
    public static void pause(Duration duration) throws InterruptedException {
        var docker = Broker.INSTANCE.getDockerClient();
        var id = Broker.INSTANCE.getContainerId();
        docker.pauseContainerCmd(id).exec();
        try {
            Thread.sleep(duration.toMillis());
        } finally {
            docker.unpauseContainerCmd(id).exec();
        }
    }

    /**
     * Reads {@code topic} from the beginning with a new consumer group until {@code done} holds for everything read
     * so far, in per-partition offset order; fails after {@code timeout}.
     */
    public static List<ConsumerRecord<byte[], byte[]>> consumeUntil(
            String topic, Duration timeout, Predicate<List<ConsumerRecord<byte[], byte[]>>> done) {
        var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "g-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            while (!done.test(records)) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("not consumed within " + timeout + "; got " + records.size() + " records");
                }
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
        }
        return records;
    }
}
