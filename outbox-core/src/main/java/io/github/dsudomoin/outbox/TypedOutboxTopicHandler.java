package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.Contracts;
import jakarta.annotation.Nullable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;

/**
 * An {@link OutboxTopicHandler} that gets each payload decoded by an {@link OutboxDeserializer}: the value goes to
 * {@link #handle(Object, OutboxRecord)}, a record with a {@code null} payload (a tombstone) to
 * {@link #handleTombstone}. The mirror of {@link TypedOutbox}.
 * <pre>{@code
 * @Component
 * public final class AuditHandler extends TypedOutboxTopicHandler<AuditEvent> {
 *
 *     private final AuditClient client;
 *
 *     public AuditHandler(OutboxDeserializer<AuditEvent> deserializer, AuditClient client) {
 *         super(Set.of("audit"), deserializer);
 *         this.client = client;
 *     }
 *
 *     @Override
 *     protected void handle(AuditEvent event, OutboxRecord record) {
 *         client.post(record.messageId().toString(), event);
 *     }
 * }
 * }</pre>
 * In a Kora app with {@code OutboxJsonModule}, the deserializer of a {@code @Json} class comes from its
 * {@code JsonReader}. Kotlin: {@code override fun handle(value: AuditEvent, record: OutboxRecord)}; the value is never
 * null, tombstones never reach it.
 *
 * <p>The handler reads contract versions up to {@code T}'s {@link OutboxContract} (1 without it). A record of a newer
 * version waits ({@link Outcome.Waiting.Kind#CONTRACT}) for an instance that reads it, its payload untouched; older
 * ones arrive as usual, so a handler of version 2 branches on {@link OutboxRecord#contractVersion()}. A payload that
 * does not decode, and a tombstone the handler does not take, can never succeed: they move to the dead table.
 */
public abstract class TypedOutboxTopicHandler<T> implements OutboxTopicHandler {

    private final Set<String> topics;
    private final OutboxDeserializer<T> deserializer;
    private final int contractVersion;

    /** {@code T} is the one the subclass declares, read through generic intermediate classes and proxies. */
    protected TypedOutboxTopicHandler(Set<String> topics, OutboxDeserializer<T> deserializer) {
        this.topics = Set.copyOf(topics);
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
        this.contractVersion = Contracts.version(typeArgument(getClass()));
    }

    /** For a class hierarchy that does not name {@code T}: {@code type} is {@code T}. */
    protected TypedOutboxTopicHandler(Set<String> topics, OutboxDeserializer<T> deserializer, Class<T> type) {
        this.topics = Set.copyOf(topics);
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
        this.contractVersion = Contracts.version(Objects.requireNonNull(type, "type"));
    }

    @Override
    public final Set<String> topics() {
        return topics;
    }

    /** The highest contract version this handler reads: {@code T}'s {@link OutboxContract}, 1 without it. */
    @InternalApi
    public final int contractVersion() {
        return contractVersion;
    }

    /**
     * Decodes the payload ({@link #decode}) for {@link #handle(Object, OutboxRecord)}; a tombstone goes to
     * {@link #handleTombstone}.
     */
    @Override
    public final void handle(OutboxRecord record) throws Exception {
        var value = decode(record);
        if (value == null) {
            handleTombstone(record);
        } else {
            handle(value, record);
        }
    }

    /** Delivers one record, as {@link OutboxHandler#handle}; {@code value} is its decoded payload, never null. */
    protected abstract void handle(T value, OutboxRecord record) throws Exception;

    /**
     * Delivers a record with a {@code null} payload, as {@link OutboxHandler#handle}. Override to take them; by
     * default it moves to the dead table.
     */
    protected void handleTombstone(OutboxRecord record) throws Exception {
        throw CannotHandle.permanent(record, getClass().getName() + " takes no tombstones", null);
    }

    /**
     * The record's decoded payload, {@code null} for a tombstone; for {@code handleBatch} overrides. Checks the
     * contract version first and throws what {@link #handle(OutboxRecord)} throws: {@link OutboxWaitException} for a
     * newer version than this handler reads, {@link OutboxPermanentException} for a payload that does not decode.
     */
    @Nullable
    protected final T decode(OutboxRecord record) throws Exception {
        int version = record.contractVersion();
        if (version > contractVersion) {
            throw CannotHandle.waiting(Outcome.Waiting.Kind.CONTRACT, "contract v" + version + " of topic "
                    + record.topic() + ", this instance reads up to v" + contractVersion);
        }
        var payload = record.payload();
        if (payload == null) {
            return null;
        }
        T value;
        try {
            value = deserializer.deserialize(record.topic(), payload);
        } catch (OutboxWaitException | OutboxPermanentException e) {
            throw e;
        } catch (Exception e) {
            throw CannotHandle.permanent(record, getClass().getName() + " cannot decode its payload", e);
        }
        if (value == null) {
            throw CannotHandle.permanent(record, getClass().getName() + " cannot decode its payload: "
                    + deserializer.getClass().getName() + " returned null", null);
        }
        return value;
    }

    /**
     * The class {@code handler}'s hierarchy binds to {@code T}, following type variables down through generic
     * intermediate classes; a proxy subclass ({@code $X__AopProxy extends X}) adds no binding and changes nothing.
     */
    private static Class<?> typeArgument(Class<?> handler) {
        var bindings = new HashMap<TypeVariable<?>, Type>();
        for (Class<?> c = handler; c != TypedOutboxTopicHandler.class; c = c.getSuperclass()) {
            if (c.getGenericSuperclass() instanceof ParameterizedType superclass) {
                var variables = c.getSuperclass().getTypeParameters();
                var arguments = superclass.getActualTypeArguments();
                for (int i = 0; i < variables.length; i++) { // a variable of c was bound by c's subclass, if at all
                    bindings.put(variables[i], bindings.getOrDefault(arguments[i], arguments[i]));
                }
            }
        }
        var type = bindings.get(TypedOutboxTopicHandler.class.getTypeParameters()[0]);
        if (type instanceof Class<?> c) {
            return c;
        }
        if (type instanceof ParameterizedType parameterized) {
            return (Class<?>) parameterized.getRawType();
        }
        throw new IllegalArgumentException("cannot tell T of " + handler.getName() + " extending"
                + " TypedOutboxTopicHandler<T> from its class hierarchy (found " + type + "); pass it to the"
                + " TypedOutboxTopicHandler(Set, OutboxDeserializer, Class) constructor");
    }
}
