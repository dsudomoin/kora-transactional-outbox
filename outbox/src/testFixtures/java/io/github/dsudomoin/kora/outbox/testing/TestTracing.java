package io.github.dsudomoin.kora.outbox.testing;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

/** A fixed sampled span without an SDK; tests that activate it reset the thread with {@code Context.clear()}. */
public final class TestTracing {

    public static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    public static final String SPAN_ID = "b7ad6b7169203331";
    /** W3C {@code traceparent} value of {@link #TRACE_ID}/{@link #SPAN_ID}, sampled. */
    public static final String TRACEPARENT = "00-" + TRACE_ID + "-" + SPAN_ID + "-01";

    private TestTracing() {}

    public static io.opentelemetry.context.Context otel(String traceId, String spanId, TraceState state) {
        var span = Span.wrap(SpanContext.create(traceId, spanId, TraceFlags.getSampled(), state));
        return io.opentelemetry.context.Context.root().with(span);
    }

    /** Puts the {@link #TRACE_ID}/{@link #SPAN_ID} span into the current Kora context. */
    public static void activateSpan() {
        OpentelemetryContext.set(
                Context.current(), new OpentelemetryContext(otel(TRACE_ID, SPAN_ID, TraceState.getDefault())));
    }

    public static OutboxHeader header(String name, String value) {
        return new OutboxHeader(name, value.getBytes(UTF_8));
    }
}
