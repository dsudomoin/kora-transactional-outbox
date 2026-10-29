package io.github.dsudomoin.outbox;

import java.util.List;
import java.util.Set;

/**
 * Delivers the records of its own {@link #topics()}; the {@link OutboxHandler} delivers every other topic. The runtime
 * routes them ({@link OutboxRuntime.Builder#route}; in a Kora app {@code OutboxModule} takes every untagged component
 * of this type), as {@link OutboxHandler#byTopic} does: each route gets its records in their original order, in one
 * call per route, one route after another.
 * <pre>{@code
 * @Component
 * public final class AuditHandler implements OutboxTopicHandler {
 *     public Set<String> topics() { return Set.of("audit"); }
 *     public void handle(OutboxRecord record) throws Exception { client.post(record.payload()); }
 * }
 * }</pre>
 * Not an {@code OutboxHandler}: each such component would be one more untagged {@code OutboxHandler}, and Kora 1.x
 * fails the build with "More than one component matches dependency type: OutboxHandler".
 */
public interface OutboxTopicHandler {

    /** The topics it delivers, read once when the runtime is built. No two routes may claim one topic. */
    Set<String> topics();

    /** As {@link OutboxHandler#handle}. */
    void handle(OutboxRecord record) throws Exception;

    /**
     * As {@link OutboxHandler#handleBatch}, for records of its topics; the default is the same sequential
     * {@link #handle} in each record's scope.
     */
    default List<Outcome> handleBatch(List<OutboxRecord> records) {
        OutboxHandler sequential = this::handle; // OutboxHandler's default loop, not a copy of it
        return sequential.handleBatch(records);
    }
}
