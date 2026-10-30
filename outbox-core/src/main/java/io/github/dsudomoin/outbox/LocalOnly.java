package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.BatchClassifier;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * The runtime's handler, the one behind its routes, kept from local records: those a writer marked for a route of
 * their topic (a header {@code outbox-local} with a null value), which no route here claims, since this instance runs
 * a release without it. Such a record waits ({@link Outcome.Waiting.Kind#NO_ROUTE}) for an instance with the route,
 * and is never forwarded (to Kafka). {@link OutboxRuntime.Builder#build()} puts every handler behind one, with routes
 * or without.
 */
final class LocalOnly implements OutboxHandler {

    private final OutboxHandler delegate;
    private final RouteCalls calls = new RouteCalls("handler");

    LocalOnly(OutboxHandler delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /** A local record throws {@link OutboxWaitException} ({@code NO_ROUTE}); any other goes to the delegate. */
    @Override
    public void handle(OutboxRecord record) throws Exception {
        if (local(record)) {
            throw CannotHandle.waiting(Outcome.Waiting.Kind.NO_ROUTE, reason(record));
        }
        delegate.handle(record);
    }

    /**
     * In order: a local record waits and blocks its (topic, key), a later record of a blocked key is not attempted
     * (unkeyed records never block each other), and the rest go to the delegate's {@code handleBatch} in one call,
     * their outcomes back in their positions. That call fails only its own records ({@link RouteCalls}): a throw or an
     * invalid list leaves the local records waiting. A call with no local record is the delegate's own: the same list,
     * its answer or exception as they are.
     */
    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        if (records.stream().noneMatch(LocalOnly::local)) {
            return delegate.handleBatch(records);
        }
        var outcomes = new Outcome[records.size()];
        var blocked = new HashSet<Object>();
        var rest = new ArrayList<OutboxRecord>(records.size());
        var positions = new ArrayList<Integer>(records.size());
        for (int i = 0; i < records.size(); i++) {
            var record = records.get(i);
            var key = BatchClassifier.orderingKey(record);
            if (local(record)) {
                outcomes[i] = new Outcome.Waiting(Outcome.Waiting.Kind.NO_ROUTE, reason(record), null);
                blocked.add(key);
            } else if (blocked.contains(key)) {
                outcomes[i] = Outcome.notAttempted();
            } else {
                rest.add(record);
                positions.add(i);
            }
        }
        if (!rest.isEmpty()) {
            var delegated = calls.handleBatch(delegate, rest);
            for (int i = 0; i < positions.size(); i++) {
                outcomes[positions.get(i)] = delegated.get(i);
            }
        }
        return Arrays.asList(outcomes);
    }

    /** The delegate's: what {@link OutboxHandler#byTopic} names when the delegate's answer is invalid. */
    @Override
    public String toString() {
        return delegate.toString();
    }

    /** A loop, not a stream: it runs for every record the relay hands. */
    private static boolean local(OutboxRecord record) {
        for (var h : record.headers()) {
            if (h.value() == null && h.name().equals(PgOutbox.LOCAL_HEADER)) {
                return true;
            }
        }
        return false;
    }

    private static String reason(OutboxRecord record) {
        return "a record for a local handler of topic " + record.topic() + ", which no route on this instance claims";
    }
}
