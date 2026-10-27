package io.github.dsudomoin.outbox.kafka.kora;

import io.github.dsudomoin.outbox.kafka.KafkaOutboxConfig;
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/** {@link KafkaOutboxConfig} read from HOCON path {@code outbox.kafka}, with the same keys and defaults. */
@ConfigValueExtractor
public interface KoraKafkaOutboxConfig extends KafkaOutboxConfig {}
