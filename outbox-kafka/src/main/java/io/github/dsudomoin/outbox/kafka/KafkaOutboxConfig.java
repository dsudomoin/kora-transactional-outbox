package io.github.dsudomoin.outbox.kafka;

import jakarta.annotation.Nullable;
import java.util.Properties;

/**
 * The Kafka handler's settings, for {@link KafkaOutboxSender}. The Kora adapter reads them from HOCON path
 * {@code outbox.kafka}, keys named as here.
 */
public interface KafkaOutboxConfig {

    /** Kafka producer properties; see {@link KafkaOutboxSender} for the ones it forces or defaults. */
    Properties driverProperties();

    /**
     * Where dead letters are published, {@code {topic}} standing for the letter's topic, e.g. {@code "{topic}.dlq"};
     * absent (the default), the dead-letter topic is off and letters are kept. Checked at startup; see
     * {@link KafkaDeadLetterHandler}.
     */
    @Nullable
    default String deadLetterTopic() {
        return null;
    }
}
