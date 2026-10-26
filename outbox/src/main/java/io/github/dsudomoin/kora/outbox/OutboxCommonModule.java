package io.github.dsudomoin.kora.outbox;

import io.github.dsudomoin.kora.outbox.internal.OutboxMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Nullable;
import ru.tinkoff.kora.config.common.Config;
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor;

/**
 * What {@link OutboxModule} and {@link InboxModule} share: config at {@code outbox}, where every key is optional, and
 * metrics, which are on when the app has a {@link MeterRegistry}. An app may extend both modules.
 */
public interface OutboxCommonModule {

    default OutboxConfig outboxConfig(Config config, ConfigValueExtractor<OutboxConfig> extractor) {
        return extractor.extract(config.get("outbox")); // a missing block yields the defaults
    }

    default OutboxMetrics outboxMetrics(@Nullable MeterRegistry registry) {
        return new OutboxMetrics(registry);
    }
}
