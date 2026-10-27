package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestTracing.SPAN_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACEPARENT;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACE_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.header;
import static io.github.dsudomoin.outbox.testing.TestTracing.otel;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.TestTracing;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.TraceState;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

class OutboxTracingTest {

    @BeforeEach
    @AfterEach
    void clearContext() {
        Context.clear();
    }

    @Test
    void noSpanNoHeaders() {
        assertThat(OutboxTracing.currentHeaders()).isEmpty();
    }

    @Test
    void activeSpanInjectsTraceparent() {
        TestTracing.activateSpan();

        assertThat(OutboxTracing.currentHeaders()).containsExactly(header("traceparent", TRACEPARENT));
    }

    @Test
    void extractRoundTrip() {
        var state = TraceState.builder().put("vendor", "value").build();
        var headers = OutboxTracing.replaceTraceHeaders(List.of(), otel(TRACE_ID, SPAN_ID, state));
        assertThat(headers).contains(header("tracestate", "vendor=value"));

        var extracted = Span.fromContext(OutboxTracing.extract(headers)).getSpanContext();

        assertThat(extracted.getTraceId()).isEqualTo(TRACE_ID);
        assertThat(extracted.getSpanId()).isEqualTo(SPAN_ID);
        assertThat(extracted.isSampled()).isTrue();
        assertThat(extracted.isRemote()).isTrue();
        assertThat(extracted.getTraceState().get("vendor")).isEqualTo("value");
    }

    @Test
    void extractWithoutTraceHeadersHasNoSpan() {
        var ctx = OutboxTracing.extract(List.of(header("a", "1")));

        assertThat(Span.fromContext(ctx).getSpanContext().isValid()).isFalse();
    }

    @Test
    void replaceTraceHeadersReplacesOnlyTraceHeaders() {
        var old = OutboxTracing.replaceTraceHeaders(
                List.of(), otel("11111111111111111111111111111111", "2222222222222222", TraceState.builder()
                        .put("old", "1")
                        .build()));
        var headers = List.of(header("a", "1"), old.get(0), header("b", "2"), old.get(1), header("a", "3"));

        var replaced = OutboxTracing.replaceTraceHeaders(headers, otel(TRACE_ID, SPAN_ID, TraceState.getDefault()));

        assertThat(replaced)
                .containsExactly(
                        header("a", "1"), header("b", "2"), header("a", "3"), header("traceparent", TRACEPARENT));
    }

    @Test
    void replaceTraceHeadersWithoutSpanKeepsHeaders() {
        var headers = List.of(header("a", "1"), header("traceparent", TRACEPARENT), header("tracestate", "k=v"));

        assertThat(OutboxTracing.replaceTraceHeaders(headers, io.opentelemetry.context.Context.root()))
                .isEqualTo(headers);
    }

    @Test
    void withKoraContextSetsSpanForTheCallAndRestores() throws Exception {
        var outer = Context.current();

        var traceIdInside = OutboxTracing.withKoraContext(
                otel(TRACE_ID, SPAN_ID, TraceState.getDefault()), OpentelemetryContext::getTraceId);

        assertThat(traceIdInside).isEqualTo(TRACE_ID);
        assertThat(Context.current()).isSameAs(outer);
        assertThat(OpentelemetryContext.getSpan()).isNull();
    }
}
