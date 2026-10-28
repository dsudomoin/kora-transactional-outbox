package io.github.dsudomoin.outbox.kafka;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxModule;
import jakarta.annotation.Nullable;
import ru.tinkoff.kora.common.DefaultComponent;
import ru.tinkoff.kora.config.common.Config;
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor;

/**
 * Relays the outbox to Kafka: provides {@link KafkaOutboxSender}, as the default {@link OutboxHandler} a
 * {@link KafkaOutboxHandler} and, as the default {@link OutboxDeadLetterHandler}, a {@link KafkaDeadLetterHandler}.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, KafkaOutboxModule {}
 * }</pre>
 * Config at {@code outbox.kafka}; {@code driverProperties} needs at least {@code bootstrap.servers}:
 * <pre>{@code
 * outbox.kafka.driverProperties { "bootstrap.servers" = "localhost:9092" }
 * }</pre>
 * The relay takes the app's one untagged {@code OutboxHandler}. The app's own, if it has one, replaces the default,
 * whether it is a component of an {@code OutboxHandler} subtype ({@code @Component class Router implements
 * OutboxHandler}) or declared as exactly {@code OutboxHandler}, like a factory routing some topics elsewhere:
 * <pre>{@code
 * default OutboxHandler outboxHandler(KafkaOutboxSender kafka, @Tag(Audit.class) AuditHandler audit) {
 *     return OutboxHandler.byTopic(Map.of("audit", audit), kafka.handler());
 * }
 * }</pre>
 * Keep it the only untagged {@code OutboxHandler} component of the app: tag sub-handlers that implement
 * {@code OutboxHandler} (as {@code audit} above) or build them inside the factory, and inject
 * {@link KafkaOutboxSender} rather than {@link KafkaOutboxHandler}. Otherwise Kora 1.x can fail the build with
 * "More than one component matches dependency type: OutboxHandler". A handler wrapping {@code kafka.handler()} (for
 * logging or metrics) must delegate {@code handleBatch} as well as {@code handle}; see {@link KafkaOutboxHandler}.
 *
 * <p>Dead letters stay in the dead table unless {@code outbox.kafka.deadLetterTopic} is set:
 * <pre>{@code
 * outbox.kafka.deadLetterTopic = "{topic}.dlq"
 * }</pre>
 * An app's own {@code OutboxDeadLetterHandler} component replaces that hook, the key set or not.
 *
 * @see OutboxModule
 */
public interface KafkaOutboxModule {

    default KafkaOutboxConfig kafkaOutboxConfig(Config config, ConfigValueExtractor<KafkaOutboxConfig> extractor) {
        return extractor.extract(config.get("outbox.kafka"));
    }

    /** {@code outbox} is the relay's config, absent when the app does not include {@link OutboxModule}. */
    default KafkaOutboxSender kafkaOutboxSender(KafkaOutboxConfig config, @Nullable OutboxConfig outbox) {
        var sender = new KafkaOutboxSender(config);
        if (outbox != null) {
            sender.warnIfLongerThan("outbox.relay.batchTimeout", outbox.relay().batchTimeout(), "The relay interrupts"
                    + " a call at 80% of batchTimeout; the sender then stops at once, keeps acked records as delivered"
                    + " and retries the rest, so slow sends end as retries");
        }
        return sender;
    }

    @DefaultComponent
    default KafkaOutboxHandler kafkaOutboxHandler(KafkaOutboxSender sender) {
        return sender.handler();
    }

    /**
     * Publishes dead letters with {@code outbox.kafka.deadLetterTopic} set, is inactive without; {@code outbox} as for
     * {@link #kafkaOutboxSender}.
     */
    @DefaultComponent
    default KafkaDeadLetterHandler kafkaDeadLetterHandler(KafkaOutboxConfig config, KafkaOutboxSender sender,
            @Nullable OutboxConfig outbox) {
        var hook = new KafkaDeadLetterHandler(sender, config.deadLetterTopic());
        if (hook.active() && outbox != null) {
            sender.warnIfLongerThan("outbox.deadLetters.timeout", outbox.deadLetters().timeout(), "The dead-letter"
                    + " pass abandons a hook call at its timeout and hands its letters again after a backoff, so slow"
                    + " dead-letter sends end as retries and letters acked meanwhile repeat on their dead-letter"
                    + " topic");
        }
        return hook;
    }
}
