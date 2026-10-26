package io.github.dsudomoin.kora.outbox.bench

import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.http.common.HttpMethod
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.common.annotation.Query
import ru.tinkoff.kora.http.server.common.HttpServerResponseException
import ru.tinkoff.kora.http.server.common.annotation.HttpController
import ru.tinkoff.kora.json.common.annotation.Json

@Json
data class Stats(
    val handler: String,
    val running: Boolean,
    val load: LoadSettings?,
    val transactions: Long,
    val failed: Long,
    val rolledBack: Long,
    val sent: Long,
    val received: Long,
    val duplicates: Long,
    val orderViolations: Long,
    val noopDelivered: Long,
)

@Component
@HttpController
class BenchController(
    private val load: LoadGenerator,
    private val writer: OrderWriter,
    private val metrics: BenchMetrics,
    private val config: BenchConfig,
) {
    @HttpRoute(method = HttpMethod.POST, path = "/load")
    @Json
    fun startLoad(
        @Query("rate") rate: Int?,
        @Query("writers") writers: Int?,
        @Query("keys") keys: Int?,
        @Query("messagesPerTx") messagesPerTx: Int?,
        @Query("payloadBytes") payloadBytes: Int?,
    ): Stats {
        val defaults = config.load()
        val settings = try {
            LoadSettings(
                rate ?: defaults.rate(),
                writers ?: defaults.writers(),
                keys ?: defaults.keys(),
                messagesPerTx ?: defaults.messagesPerTx(),
                payloadBytes ?: defaults.payloadBytes(),
            )
        } catch (e: IllegalArgumentException) {
            throw HttpServerResponseException.of(400, e.message ?: "bad load settings")
        }
        load.start(settings)
        return stats()
    }

    @HttpRoute(method = HttpMethod.DELETE, path = "/load")
    @Json
    fun stopLoad(): Stats {
        load.stop()
        return stats()
    }

    @HttpRoute(method = HttpMethod.POST, path = "/orders")
    @Json
    fun createOrder(@Query("userId") userId: String): OrderEvent =
        writer.createOrder(userId, null)

    @HttpRoute(method = HttpMethod.POST, path = "/poison")
    @Json
    fun poison(@Query("count") count: Int?): Stats {
        if (count != null && count !in 1..10_000) throw HttpServerResponseException.of(400, "count must be in 1..10000")
        writer.sendPoison(count ?: 1)
        return stats()
    }

    @HttpRoute(method = HttpMethod.GET, path = "/stats")
    @Json
    fun stats(): Stats {
        val settings = load.settings
        return Stats(
            handler = config.handler(),
            running = settings != null,
            load = settings,
            transactions = metrics.tx.count(),
            failed = metrics.txFailed.count().toLong(),
            rolledBack = metrics.rolledBack.count().toLong(),
            sent = metrics.sent.count().toLong(),
            received = metrics.received.count().toLong(),
            duplicates = metrics.duplicates.count().toLong(),
            orderViolations = metrics.orderViolations.count().toLong(),
            noopDelivered = metrics.noopDelivered.count().toLong(),
        )
    }
}
