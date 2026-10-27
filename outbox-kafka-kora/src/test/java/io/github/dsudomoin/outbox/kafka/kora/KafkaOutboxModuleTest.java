package io.github.dsudomoin.outbox.kafka.kora;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxConfig;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxSender;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** The warnings {@link KafkaOutboxModule} gives when a Kafka call can outlast the caller's own time limit. */
class KafkaOutboxModuleTest {

    private static final String TEMPLATE = "{topic}.dlq";

    /**
     * A call can take delivery.timeout.ms + 5 s + max.block.ms; the relay gives it batchTimeout. The defaults fit:
     * 20 s + 5 s + 5 s against 30 s.
     */
    @Test
    void warnsOnlyWhenACallCanOutlastBatchTimeout() {
        KafkaOutboxConfig defaults = () -> props("bootstrap.servers", "localhost:9092");
        KafkaOutboxConfig slowMetadata = () -> props("bootstrap.servers", "localhost:9092", "max.block.ms", 10000);
        var module = new KafkaOutboxModule() {};
        var relayDefault = outboxConfig(new OutboxConfig.RelayConfig() {}.batchTimeout());
        try (var err = new CapturedStderr()) {
            module.kafkaOutboxSender(defaults, relayDefault).release();
            module.kafkaOutboxSender(slowMetadata, outboxConfig(Duration.ofSeconds(35))).release();
            module.kafkaOutboxSender(slowMetadata, null).release(); // no relay in the app
            assertThat(err.text()).doesNotContain("batchTimeout");
        }
        try (var err = new CapturedStderr()) {
            module.kafkaOutboxSender(slowMetadata, relayDefault).release();
            assertThat(err.text()).contains("WARN").contains("batchTimeout");
        }
    }

    /**
     * A sender's call can take delivery.timeout.ms + 5 s + max.block.ms; the dead-letter pass gives a hook call
     * {@code outbox.deadLetters.timeout}. The defaults fit: 20 s + 5 s + 5 s against 30 s.
     */
    @Test
    void warnsOnlyWhenACallCanOutlastTheHookTimeout() {
        var module = new KafkaOutboxModule() {};
        var outbox = new OutboxConfig() {};
        var defaults = kafkaConfig(TEMPLATE, props("bootstrap.servers", "dlq-test:9092"));
        var slow = kafkaConfig(TEMPLATE, props("bootstrap.servers", "dlq-test:9092", "max.block.ms", "10000"));
        var off = kafkaConfig(null, props("bootstrap.servers", "dlq-test:9092", "max.block.ms", "10000"));
        try (var err = new CapturedStderr()) {
            module.kafkaDeadLetterHandler(defaults, new KafkaOutboxSender(defaults), outbox);
            module.kafkaDeadLetterHandler(slow, new KafkaOutboxSender(slow), null); // no relay in the app
            module.kafkaDeadLetterHandler(off, new KafkaOutboxSender(off), outbox); // no calls
            assertThat(err.text()).doesNotContain("deadLetters.timeout");
        }
        try (var err = new CapturedStderr()) {
            module.kafkaDeadLetterHandler(slow, new KafkaOutboxSender(slow), outbox);
            assertThat(err.text()).contains("WARN").contains("outbox.deadLetters.timeout");
        }
    }

    private static OutboxConfig outboxConfig(Duration batchTimeout) {
        return new OutboxConfig() {
            @Override
            public RelayConfig relay() {
                return new RelayConfig() {
                    @Override
                    public Duration batchTimeout() {
                        return batchTimeout;
                    }
                };
            }
        };
    }

    private static KafkaOutboxConfig kafkaConfig(@Nullable String template, Properties driverProperties) {
        return new KafkaOutboxConfig() {
            @Override
            public Properties driverProperties() {
                return driverProperties;
            }

            @Override
            public String deadLetterTopic() {
                return template;
            }
        };
    }

    private static Properties props(Object... keyValues) {
        var p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            p.put(keyValues[i], keyValues[i + 1]);
        }
        return p;
    }
}
