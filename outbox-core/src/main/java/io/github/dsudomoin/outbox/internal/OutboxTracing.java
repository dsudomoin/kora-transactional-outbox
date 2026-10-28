package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

/** W3C {@code traceparent}/{@code tracestate} headers, bridged to Kora's {@link OpentelemetryContext}. */
public final class OutboxTracing {

    public static final String TRACEPARENT = "traceparent";
    public static final String TRACESTATE = "tracestate";

    private static final W3CTraceContextPropagator W3C = W3CTraceContextPropagator.getInstance();

    private static final TextMapSetter<List<OutboxHeader>> SETTER =
            (headers, name, value) -> headers.add(new OutboxHeader(name, value.getBytes(UTF_8)));

    private static final TextMapGetter<List<OutboxHeader>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(List<OutboxHeader> headers) {
            return headers.stream().map(OutboxHeader::name).toList();
        }

        @Nullable
        @Override
        public String get(@Nullable List<OutboxHeader> headers, String name) {
            if (headers != null) {
                for (int i = headers.size() - 1; i >= 0; i--) { // last wins, as Kafka's lastHeader
                    var h = headers.get(i);
                    if (h.name().equals(name)) {
                        return h.value() == null ? null : new String(h.value(), UTF_8);
                    }
                }
            }
            return null;
        }
    };

    private OutboxTracing() {}

    /** Trace headers of the span in the current Kora context; empty without a valid span. */
    public static List<OutboxHeader> currentHeaders() {
        var otel = OpentelemetryContext.get(Context.current()).getContext();
        if (!hasValidSpan(otel)) {
            return List.of();
        }
        var headers = new ArrayList<OutboxHeader>(2);
        W3C.inject(otel, headers, SETTER);
        return headers;
    }

    public static io.opentelemetry.context.Context extract(List<OutboxHeader> headers) {
        return W3C.extract(io.opentelemetry.context.Context.root(), headers, GETTER);
    }

    /**
     * {@code headers} without trace headers, followed by those of {@code ctx}; other headers keep their order. Without
     * a valid span in {@code ctx}, {@code headers} are returned unchanged.
     */
    public static List<OutboxHeader> replaceTraceHeaders(
            List<OutboxHeader> headers, io.opentelemetry.context.Context ctx) {
        if (!hasValidSpan(ctx)) {
            return headers;
        }
        var result = new ArrayList<OutboxHeader>(headers.size() + 2);
        for (var h : headers) {
            if (!h.name().equals(TRACEPARENT) && !h.name().equals(TRACESTATE)) {
                result.add(h);
            }
        }
        W3C.inject(ctx, result, SETTER);
        return result;
    }

    /** Runs {@code call} in a forked Kora context carrying {@code otel}; the caller's context is restored after. */
    public static <T> T withKoraContext(io.opentelemetry.context.Context otel, Callable<T> call) throws Exception {
        var previous = Context.current();
        var forked = previous.fork();
        forked.inject(); // before set(): it writes trace ids into the MDC of the current context
        try {
            OpentelemetryContext.set(forked, new OpentelemetryContext(otel));
            return call.call();
        } finally {
            previous.inject();
        }
    }

    private static boolean hasValidSpan(@Nullable io.opentelemetry.context.Context ctx) {
        return ctx != null && Span.fromContext(ctx).getSpanContext().isValid();
    }
}
