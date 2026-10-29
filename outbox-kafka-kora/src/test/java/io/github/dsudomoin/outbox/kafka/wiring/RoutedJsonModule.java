package io.github.dsudomoin.outbox.kafka.wiring;

import io.github.dsudomoin.outbox.kora.OutboxJsonModule;
import ru.tinkoff.kora.common.Module;

/** In every app of this source set: {@link RoutedAuditHandler}, which each of them gets, needs its deserializer. */
@Module
public interface RoutedJsonModule extends OutboxJsonModule {}
