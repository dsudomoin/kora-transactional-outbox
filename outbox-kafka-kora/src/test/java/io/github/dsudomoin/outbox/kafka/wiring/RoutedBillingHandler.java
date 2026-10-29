package io.github.dsudomoin.outbox.kafka.wiring;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import ru.tinkoff.kora.common.Component;

/** A plain topic handler claiming two topics; in every app of this source set, as {@link RoutedAuditHandler}. */
@Component
public final class RoutedBillingHandler implements OutboxTopicHandler {

    static final String BILLING = "routed-billing";
    static final String INVOICES = "routed-invoices";

    final List<OutboxRecord> handled = new CopyOnWriteArrayList<>();

    @Override
    public Set<String> topics() {
        return Set.of(BILLING, INVOICES);
    }

    @Override
    public void handle(OutboxRecord record) {
        handled.add(record);
    }
}
