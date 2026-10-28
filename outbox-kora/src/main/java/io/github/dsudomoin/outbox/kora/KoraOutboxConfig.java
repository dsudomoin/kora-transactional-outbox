package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxConfig;
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/**
 * {@link OutboxConfig} read from HOCON path {@code outbox}: the same keys and defaults; every key is optional. The
 * nested blocks are their Kora counterparts, so Kora's config processor reads each of them too.
 */
@ConfigValueExtractor
public interface KoraOutboxConfig extends OutboxConfig {

    @Override
    default KoraRelayConfig relay() {
        return new KoraRelayConfig() {};
    }

    @Override
    default KoraPartitionsConfig partitions() {
        return new KoraPartitionsConfig() {};
    }

    @Override
    default KoraInboxConfig inbox() {
        return new KoraInboxConfig() {};
    }

    @Override
    default KoraDeadLettersConfig deadLetters() {
        return new KoraDeadLettersConfig() {};
    }

    @Override
    default KoraRecoveryConfig recovery() {
        return new KoraRecoveryConfig() {};
    }

    @Override
    default KoraSchemaConfig schema() {
        return new KoraSchemaConfig() {};
    }

    @ConfigValueExtractor
    interface KoraRelayConfig extends RelayConfig {

        @Override
        default KoraRetryConfig retry() {
            return new KoraRetryConfig() {};
        }

        @Override
        default KoraRequeueConfig requeue() {
            return new KoraRequeueConfig() {};
        }
    }

    @ConfigValueExtractor
    interface KoraRetryConfig extends RetryConfig {}

    @ConfigValueExtractor
    interface KoraRequeueConfig extends RequeueConfig {}

    @ConfigValueExtractor
    interface KoraPartitionsConfig extends PartitionsConfig {

        @Override
        default KoraTailConversionConfig tailConversion() {
            return new KoraTailConversionConfig() {};
        }
    }

    @ConfigValueExtractor
    interface KoraTailConversionConfig extends TailConversionConfig {}

    @ConfigValueExtractor
    interface KoraInboxConfig extends InboxConfig {}

    @ConfigValueExtractor
    interface KoraDeadLettersConfig extends DeadLettersConfig {}

    @ConfigValueExtractor
    interface KoraRecoveryConfig extends RecoveryConfig {}

    @ConfigValueExtractor
    interface KoraSchemaConfig extends SchemaConfig {}
}
