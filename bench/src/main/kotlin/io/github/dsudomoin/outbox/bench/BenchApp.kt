package io.github.dsudomoin.outbox.bench

import io.github.dsudomoin.outbox.kora.InboxModule
import io.github.dsudomoin.outbox.OutboxHandler
import io.github.dsudomoin.outbox.kora.OutboxModule
import io.github.dsudomoin.outbox.OutboxRecord
import io.github.dsudomoin.outbox.Outcome
import io.github.dsudomoin.outbox.kora.OutboxJsonModule
import io.github.dsudomoin.outbox.kafka.kora.KafkaOutboxModule
import io.github.dsudomoin.outbox.kafka.KafkaOutboxSender
import io.github.dsudomoin.outbox.OutboxTelemetry
import io.github.dsudomoin.outbox.micrometer.ObservationOutboxTelemetry
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.transport.ReceiverContext
import io.micrometer.observation.transport.SenderContext
import io.micrometer.tracing.handler.DefaultTracingObservationHandler
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler
import io.micrometer.tracing.otel.bridge.OtelBaggageManager
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext
import io.micrometer.tracing.otel.bridge.OtelPropagator
import io.micrometer.tracing.otel.bridge.OtelTracer
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.trace.samplers.Sampler
import ru.tinkoff.kora.application.graph.GraphInterceptor
import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.common.annotation.ConfigSource
import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule
import ru.tinkoff.kora.http.server.undertow.UndertowHttpServerModule
import ru.tinkoff.kora.json.module.JsonModule
import ru.tinkoff.kora.kafka.common.KafkaModule
import ru.tinkoff.kora.logging.logback.LogbackModule
import ru.tinkoff.kora.micrometer.module.MetricsModule
import ru.tinkoff.kora.opentelemetry.tracing.OpentelemetryTracingModule
import ru.tinkoff.kora.opentelemetry.tracing.exporter.grpc.OpentelemetryGrpcExporterModule

@KoraApp
interface BenchApp :
    HoconConfigModule,
    LogbackModule,
    JdbcDatabaseModule,
    UndertowHttpServerModule,
    MetricsModule,
    OpentelemetryTracingModule,
    OpentelemetryGrpcExporterModule,
    KafkaModule,
    JsonModule,
    OutboxModule,
    OutboxJsonModule,
    InboxModule,
    KafkaOutboxModule {

    /** Exactly `OutboxHandler`, so it replaces the module's default Kafka handler. */
    fun outboxHandler(kafka: KafkaOutboxSender, config: BenchConfig, metrics: BenchMetrics): OutboxHandler =
        when (config.handler()) {
            "kafka" -> if (config.handlerDelayMs() > 0) DelayedHandler(kafka.handler(), config.handlerDelayMs()) else kafka.handler()
            "noop" -> CountingHandler(metrics.noopDelivered)
            else -> throw IllegalArgumentException("bench.handler must be kafka or noop, got ${config.handler()}")
        }

    fun sampler(config: BenchConfig): Sampler = Sampler.parentBased(Sampler.traceIdRatioBased(config.traceRatio()))
}

/**
 * `bench.telemetry = observation` replaces the module's telemetry with [ObservationOutboxTelemetry]: the outbox's
 * meters, a timer per observation and spans through micrometer-tracing's bridge over Kora's OpenTelemetry tracer (same
 * sampler); deliveries of unsampled traces are not observed, the library's default. `observation-all` observes every
 * delivery (`observeUnsampled(true)`). The writer's span lives only in Kora's context, so each `outbox enqueue` span
 * is a trace root.
 */
@Component
class TelemetrySwitch(
    private val config: BenchConfig,
    private val registry: MeterRegistry,
    private val tracer: Tracer,
) : GraphInterceptor<OutboxTelemetry> {
    override fun init(value: OutboxTelemetry): OutboxTelemetry = when (config.telemetry()) {
        "kora" -> value
        "observation" -> observationTelemetry(registry, tracer, observeUnsampled = false)
        "observation-all" -> observationTelemetry(registry, tracer, observeUnsampled = true)
        else -> throw IllegalArgumentException(
            "bench.telemetry must be kora, observation or observation-all, got ${config.telemetry()}")
    }

    override fun release(value: OutboxTelemetry) = value
}

/** The setup of an app on Micrometer Observation with micrometer-tracing's OpenTelemetry bridge. */
private fun observationTelemetry(registry: MeterRegistry, otel: Tracer, observeUnsampled: Boolean): OutboxTelemetry {
    val current = OtelCurrentTraceContext()
    val tracer = OtelTracer(otel, current, {}, OtelBaggageManager(current, listOf(), listOf()))
    val propagator = OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel)
    val observations = ObservationRegistry.create()
    observations.observationConfig()
        .observationHandler(ObservationHandler.FirstMatchingCompositeObservationHandler(
            PropagatingSenderTracingObservationHandler<SenderContext<*>>(tracer, propagator),
            PropagatingReceiverTracingObservationHandler<ReceiverContext<*>>(tracer, propagator),
            DefaultTracingObservationHandler(tracer)))
        .observationHandler(DefaultMeterObservationHandler(registry))
    return ObservationOutboxTelemetry.builder(observations, OutboxMetrics(registry)).tracing(tracer, propagator)
        .observeUnsampled(observeUnsampled).build()
}

fun main() {
    KoraApplication.run { BenchAppGraph.graph() }
}

@ConfigSource("bench")
interface BenchConfig {
    /** `kafka` or `noop` (counts and drops: pure outbox throughput). */
    fun handler(): String

    /** `kora` (the module's telemetry), `observation` or `observation-all` (Observation, see [TelemetrySwitch]). */
    fun telemetry(): String

    /** Root span sampling ratio; children follow their parent. */
    fun traceRatio(): Double

    /** Delay per record before the Kafka handler gets it (slow-handler scenario); 0 = none. */
    fun handlerDelayMs(): Long

    /** Percent of load-generator orders whose transaction sends its message and then rolls back (chaos runs). */
    fun rollbackPercent(): Double

    /** Defaults for `POST /load`. */
    fun load(): LoadConfig

    @ConfigValueExtractor
    interface LoadConfig {
        fun rate(): Int
        fun writers(): Int
        fun keys(): Int
        fun messagesPerTx(): Int
        fun payloadBytes(): Int
    }
}

/** Delivers nothing: every record counts as delivered. */
class CountingHandler(private val delivered: Counter) : OutboxHandler {
    override fun handle(record: OutboxRecord) = delivered.increment()

    override fun handleBatch(records: List<OutboxRecord>): List<Outcome> {
        delivered.increment(records.size.toDouble())
        return List(records.size) { Outcome.delivered() }
    }
}

/**
 * Sleeps `delayMs` per record, then hands the whole batch to [delegate]'s `handleBatch` (not record by record, which
 * would turn Kafka's batched sends into one send per record). An interrupt (the relay's batch deadline) ends the wait
 * at once: nothing was handed over, so every record is NotAttempted, and the interrupt stays set.
 */
class DelayedHandler(private val delegate: OutboxHandler, private val delayMs: Long) : OutboxHandler {
    override fun handle(record: OutboxRecord) {
        Thread.sleep(delayMs)
        delegate.handle(record)
    }

    override fun handleBatch(records: List<OutboxRecord>): List<Outcome> {
        try {
            Thread.sleep(delayMs * records.size)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            return List(records.size) { Outcome.notAttempted() }
        }
        return delegate.handleBatch(records)
    }
}
