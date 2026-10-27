package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.micrometer.observation.Observation;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Puts a delivery observation's trace context into the record handed to the handler; micrometer-tracing only here. */
final class TraceInjector {

    private static final Propagator.Setter<List<OutboxHeader>> SETTER =
            (carrier, key, value) -> carrier.add(new OutboxHeader(key, value.getBytes(UTF_8)));

    private final Tracer tracer;
    private final Propagator propagator;
    private final Set<String> fields;

    TraceInjector(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
        this.fields = Set.copyOf(propagator.fields());
    }

    /** {@code record} with its trace headers replaced by those of {@code observation}'s span; as is without a span. */
    OutboxRecord inject(Observation observation, OutboxRecord record) {
        try (var scope = observation.openScope()) {
            var span = tracer.currentSpan();
            if (span == null) {
                return record;
            }
            var headers = new ArrayList<OutboxHeader>(record.headers().size() + fields.size());
            for (var h : record.headers()) {
                if (!fields.contains(h.name())) {
                    headers.add(h);
                }
            }
            propagator.inject(span.context(), headers, SETTER);
            return record.withHeaders(headers);
        }
    }
}
