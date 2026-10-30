package io.github.dsudomoin.outbox;

import java.util.List;

/**
 * A send refused before anything was written: a route of this instance claims its topic, so its record is marked for
 * a route, but a live relay instance does not route the topic, and would make the record wait. The caller's
 * transaction decides what happens next; deploy the release with the route everywhere, and let the heartbeat confirm
 * it, before writing to the topic. Thrown by {@link Outbox} and the typed writers built on it, whatever the version.
 */
public final class OutboxMissingRouteException extends RuntimeException {

    private final String topic;
    private final List<String> instances;

    /** Internal, unstable: the outbox throws it. */
    @InternalApi
    public OutboxMissingRouteException(String message, String topic, List<String> instances) {
        super(message);
        this.topic = topic;
        this.instances = List.copyOf(instances);
    }

    public String topic() {
        return topic;
    }

    /**
     * The ids of the live relay instances that do not route {@link #topic()}, or whose routes are unknown, sorted;
     * empty for a stale read.
     */
    public List<String> instances() {
        return instances;
    }
}
