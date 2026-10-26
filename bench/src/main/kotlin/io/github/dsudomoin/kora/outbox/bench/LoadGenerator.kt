package io.github.dsudomoin.kora.outbox.bench

import org.slf4j.LoggerFactory
import ru.tinkoff.kora.application.graph.Lifecycle
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.json.common.annotation.Json
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport

@Json
data class LoadSettings(val rate: Int, val writers: Int, val keys: Int, val messagesPerTx: Int, val payloadBytes: Int) {
    init {
        require(rate >= 0) { "rate must be >= 0 (0 = unlimited)" }
        require(writers in 1..256) { "writers must be in 1..256" }
        require(keys >= 1) { "keys must be >= 1" }
        require(messagesPerTx >= 1) { "messagesPerTx must be >= 1" }
        require(payloadBytes >= 0) { "payloadBytes must be >= 0" }
    }
}

/**
 * Shared pacing for all writers: each reservation takes the next free slot; `rate = 0` never waits. Slots missed
 * while every writer was busy stay usable for [BURST], so a slow transaction does not lower the average rate.
 */
class TokenBucket(rate: Int) {
    private val nanosPerToken = if (rate == 0) 0.0 else 1e9 / rate
    private var next = System.nanoTime()

    /** The `System.nanoTime()` at which `tokens` may be spent. */
    @Synchronized
    fun reserve(tokens: Int): Long {
        val slot = maxOf(next, System.nanoTime() - BURST)
        next = slot + (tokens * nanosPerToken).toLong()
        return slot
    }

    private companion object {
        val BURST = TimeUnit.SECONDS.toNanos(1)
    }
}

/**
 * Writer threads (platform, a fixed pool of `writers`): each loops one transaction after another,
 * `messagesPerTx = 1` an order via [OrderWriter.createOrder] (`bench.rollbackPercent` of them rolled back after the
 * send), more a bulk `sendAll`.
 */
@Component
class LoadGenerator(
    private val writer: OrderWriter,
    private val metrics: BenchMetrics,
    private val config: BenchConfig,
) : Lifecycle {
    private class Run(val settings: LoadSettings, val pool: ExecutorService) {
        @Volatile var stopped = false
    }

    @Volatile private var run: Run? = null

    val settings: LoadSettings? get() = run?.settings

    @Synchronized
    fun start(settings: LoadSettings) {
        stop()
        val bucket = TokenBucket(settings.rate)
        val pad = if (settings.messagesPerTx == 1) {
            writer.pad("u${settings.keys - 1}", settings.payloadBytes)
        } else {
            writer.pad(UUID.randomUUID().toString(), settings.payloadBytes)
        }
        val n = AtomicInteger()
        val pool = Executors.newFixedThreadPool(settings.writers) { Thread(it, "bench-writer-${n.incrementAndGet()}") }
        val current = Run(settings, pool)
        repeat(settings.writers) { pool.execute { loop(current, bucket, pad) } }
        run = current
        log.info("load started: {}", settings)
    }

    @Synchronized
    fun stop() {
        val current = run ?: return
        run = null
        current.stopped = true
        current.pool.shutdown()
        if (!current.pool.awaitTermination(30, TimeUnit.SECONDS)) {
            current.pool.shutdownNow()
        }
        log.info("load stopped")
    }

    private fun loop(run: Run, bucket: TokenBucket, pad: String?) {
        val s = run.settings
        while (!run.stopped) {
            val slot = bucket.reserve(s.messagesPerTx)
            while (!run.stopped && slot - System.nanoTime() > 0) {
                LockSupport.parkNanos(minOf(slot - System.nanoTime(), MAX_PARK))
            }
            if (run.stopped) return
            try {
                val random = ThreadLocalRandom.current()
                if (s.messagesPerTx > 1) {
                    writer.sendBulk(s.messagesPerTx, pad)
                } else if (random.nextDouble(100.0) < config.rollbackPercent()) {
                    writer.createRolledBackOrder("u${random.nextInt(s.keys)}", pad)
                } else {
                    writer.createOrder("u${random.nextInt(s.keys)}", pad)
                }
            } catch (e: Exception) {
                metrics.txFailed.increment()
                log.warn("writer transaction failed: {}", e.toString())
                Thread.sleep(1000) // ponytail: fixed back-off; a DB outage logs once per writer per second
            }
        }
    }

    override fun init() {}

    override fun release() = stop()

    private companion object {
        val MAX_PARK = TimeUnit.MILLISECONDS.toNanos(100) // so stop() is not stuck behind a far slot
        val log = LoggerFactory.getLogger(LoadGenerator::class.java)
    }
}
