package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.internal.GuardedTelemetry;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;
import ru.tinkoff.kora.config.common.Config;
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;

/**
 * What {@link OutboxModule} and {@link InboxModule} share: config at {@code outbox}, where every key is optional, and
 * telemetry: metrics when the app has a {@link MeterRegistry}, delivery spans when it has a {@link Tracer}; and the
 * app's {@link JdbcConnectionFactory} transactions. An app may extend both modules.
 */
public interface OutboxCommonModule {

    default OutboxConfig outboxConfig(Config config, ConfigValueExtractor<KoraOutboxConfig> extractor) {
        return extractor.extract(config.get("outbox")); // a missing block yields the defaults
    }

    default OutboxTransactions outboxTransactions(JdbcConnectionFactory db) {
        return new KoraOutboxTransactions(db);
    }

    default OutboxTelemetry outboxTelemetry(@Nullable MeterRegistry registry, @Nullable Tracer tracer) {
        return GuardedTelemetry.of(new KoraOutboxTelemetry(new OutboxMetrics(registry), tracer));
    }
}
