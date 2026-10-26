package io.github.dsudomoin.kora.outbox.kafka;

import java.util.Properties;
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/** HOCON path {@code outbox.kafka}. */
@ConfigValueExtractor
public interface KafkaOutboxConfig {

    /** Kafka producer properties; see {@link KafkaOutboxSender} for the ones it forces or defaults. */
    Properties driverProperties();
}
