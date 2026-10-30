package io.github.dsudomoin.outbox;

import java.util.List;

/**
 * A send refused before anything was written: its {@code outbox-contract} header gives a version of its topic that a
 * live relay instance cannot read, so the record would wait there. The caller's transaction decides what happens
 * next; deploy the release whose handlers read the version everywhere first. Thrown by {@link Outbox} and the typed
 * writers built on it, for versions above 1 only.
 */
public final class OutboxIncompatibleContractException extends RuntimeException {

    private final String topic;
    private final String type;
    private final int version;
    private final int minSupported;
    private final List<String> incompatibleInstances;

    /** Internal, unstable: the outbox throws it. */
    @InternalApi
    public OutboxIncompatibleContractException(String message, String topic, String type, int version,
            int minSupported, List<String> incompatibleInstances) {
        super(message);
        this.topic = topic;
        this.type = type;
        this.version = version;
        this.minSupported = minSupported;
        this.incompatibleInstances = List.copyOf(incompatibleInstances);
    }

    public String topic() {
        return topic;
    }

    /** The class name of the type the message was written as; {@code ?} for a message sent through {@link Outbox}. */
    public String type() {
        return type;
    }

    /** The contract version the message carries. */
    public int version() {
        return version;
    }

    /**
     * The highest version every live relay instance reads: the lowest of their reports, 0 when one reports none of the
     * topic, and 0 when this instance has not read their reports lately.
     */
    public int minSupported() {
        return minSupported;
    }

    /** The ids of the live relay instances that read below {@link #version()}, sorted; empty for a stale read. */
    public List<String> incompatibleInstances() {
        return incompatibleInstances;
    }
}
