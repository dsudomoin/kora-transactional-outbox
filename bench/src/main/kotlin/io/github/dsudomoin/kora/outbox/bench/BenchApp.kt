package io.github.dsudomoin.kora.outbox.bench

import io.github.dsudomoin.kora.outbox.InboxModule
import io.github.dsudomoin.kora.outbox.OutboxHandler
import io.github.dsudomoin.kora.outbox.OutboxModule
import io.github.dsudomoin.kora.outbox.OutboxRecord
import io.github.dsudomoin.kora.outbox.Outcome
import io.github.dsudomoin.kora.outbox.json.OutboxJsonModule
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxModule
import io.github.dsudomoin.kora.outbox.kafka.KafkaOutboxSender
import io.micrometer.core.instrument.Counter
import io.opentelemetry.sdk.trace.samplers.Sampler
import ru.tinkoff.kora.application.graph.KoraApplication
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

fun main() {
    KoraApplication.run { BenchAppGraph.graph() }
}

@ConfigSource("bench")
interface BenchConfig {
    /** `kafka` or `noop` (counts and drops: pure outbox throughput). */
    fun handler(): String

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
