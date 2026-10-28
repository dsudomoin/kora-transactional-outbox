package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.observation.Observation;
import java.util.List;

/**
 * The observation of one relay step, current on the thread that steps (a relay worker, or the caller of
 * {@code OutboxRelay.drain()}) while the step runs.
 */
public final class OutboxStepContext extends Observation.Context {

    private final List<Integer> shards;

    OutboxStepContext(List<Integer> shards) {
        this.shards = List.copyOf(shards);
    }

    /** The shards whose locks the step holds. */
    public List<Integer> shards() {
        return shards;
    }
}
