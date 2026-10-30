package io.github.dsudomoin.outbox;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The version of a payload type's contract, at least 1; a type without it is version 1. {@link TypedOutbox} and
 * {@code KafkaOutboxPublisher} write the version of their declared type into each message's {@code outbox-contract}
 * header (none for version 1), and {@link OutboxRecord#contractVersion()} reads it back. Not inherited: a subclass
 * declares its own.
 * <pre>{@code
 * @OutboxContract(2)
 * @Json
 * public record OrderCreated(long id, String userId, String currency) {}
 * }</pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface OutboxContract {

    int value();
}
