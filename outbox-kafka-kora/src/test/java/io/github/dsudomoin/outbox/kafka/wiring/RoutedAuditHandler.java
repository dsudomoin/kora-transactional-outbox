package io.github.dsudomoin.outbox.kafka.wiring;

import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import ru.tinkoff.kora.common.Component;
import ru.tinkoff.kora.json.common.annotation.Json;

/**
 * A typed topic handler as an app writes one: its deserializer comes from {@code OutboxJsonModule}. Kora puts every
 * {@code @Component} class of this source set into every app of it, hence {@link RoutedJsonModule}.
 */
@Component
public final class RoutedAuditHandler extends TypedOutboxTopicHandler<RoutedAuditHandler.AuditEvent> {

    static final String TOPIC = "routed-audit";

    @Json
    public record AuditEvent(String action) {}

    final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    public RoutedAuditHandler(OutboxDeserializer<AuditEvent> deserializer) {
        super(Set.of(TOPIC), deserializer);
    }

    @Override
    protected void handle(AuditEvent event, OutboxRecord record) {
        events.add(event);
    }
}
