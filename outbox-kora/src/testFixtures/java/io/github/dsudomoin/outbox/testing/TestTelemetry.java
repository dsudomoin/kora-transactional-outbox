package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.kora.KoraOutboxTelemetry;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;

/** The Kora app's telemetry, as {@code OutboxCommonModule} builds it, for components a test builds by hand. */
public final class TestTelemetry {

    private TestTelemetry() {}

    /** No meters, no spans; the Kora context still carries each record's trace. */
    public static OutboxTelemetry kora() {
        return kora(null, null);
    }

    public static OutboxTelemetry kora(@Nullable MeterRegistry registry, @Nullable Tracer tracer) {
        return new KoraOutboxTelemetry(new OutboxMetrics(registry), tracer);
    }
}
