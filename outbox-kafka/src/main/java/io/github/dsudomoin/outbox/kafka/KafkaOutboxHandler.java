package io.github.dsudomoin.outbox.kafka;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxPermanentException;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import java.util.List;
import org.apache.kafka.common.KafkaException;

/**
 * The relay's handler over a {@link KafkaOutboxSender} ({@link KafkaOutboxSender#handler()}): {@code handleBatch} is
 * {@link KafkaOutboxSender#send}; {@code handle} sends one record and throws {@link OutboxPermanentException} or a
 * {@link KafkaException} unless it was delivered. A type of its own, so that an app's handler can replace
 * {@link KafkaOutboxModule}'s default.
 *
 * <p>A decorator (logging, metrics) must delegate {@code handleBatch} to {@code handleBatch}, not only {@code handle}:
 * {@link io.github.dsudomoin.outbox.OutboxHandler}'s default {@code handleBatch} calls {@code handle} once per
 * record, one acks=all round trip each, and turns a cooldown's {@code NotAttempted} into {@code Retry}, which spends
 * the record's attempts and drops its {@code notBefore}.
 */
public final class KafkaOutboxHandler implements OutboxHandler {

    private final KafkaOutboxSender sender;

    KafkaOutboxHandler(KafkaOutboxSender sender) {
        this.sender = sender;
    }

    @Override
    public void handle(OutboxRecord record) {
        var outcome = sender.send(List.of(record)).get(0);
        if (outcome instanceof Outcome.Permanent p) {
            throw new OutboxPermanentException("Kafka rejected outbox record " + record.id(), p.error());
        }
        if (outcome instanceof Outcome.Retry r) {
            throw new KafkaException("Kafka send of outbox record " + record.id() + " failed", r.error());
        }
        if (outcome instanceof Outcome.NotAttempted n) {
            throw new KafkaException(
                    "outbox record " + record.id() + ": its topic or partition cools down until " + n.notBefore());
        }
    }

    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        return sender.send(records);
    }
}
