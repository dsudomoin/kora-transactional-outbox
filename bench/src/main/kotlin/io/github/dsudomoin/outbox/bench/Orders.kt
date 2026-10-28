package io.github.dsudomoin.outbox.bench

import io.github.dsudomoin.outbox.Outbox
import io.github.dsudomoin.outbox.OutboxMessage
import io.github.dsudomoin.outbox.TypedOutbox
import io.github.dsudomoin.outbox.kafka.KafkaOutboxConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import ru.tinkoff.kora.application.graph.Lifecycle
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.Context
import ru.tinkoff.kora.database.common.annotation.Query
import ru.tinkoff.kora.database.common.annotation.Repository
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.database.jdbc.JdbcRepository
import ru.tinkoff.kora.json.common.JsonWriter
import ru.tinkoff.kora.json.common.annotation.Json
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

const val TOPIC = "orders"
const val TOPIC_PARTITIONS = 12

/** Kafka payload. `seq` grows per user in commit order; 0 = bulk message, not order-checked. */
@Json
data class OrderEvent(val orderId: Long, val userId: String, val seq: Long, val pad: String? = null)

/** A user's next `seq` and a fresh `orders.id`, drawn in one round trip. */
data class NextOrder(val n: Long, val orderId: Long)

@Repository
interface OrderRepository : JdbcRepository {
    @Query("insert into orders(id, user_id, amount, message_id) values (:id, :userId, :amount, :messageId)")
    fun insert(id: Long, userId: String, amount: Long, messageId: UUID)

    /** Row-locked until commit, so a user's seq follows commit order (orders.id does not). */
    @Query("insert into user_seq(user_id, n) values (:userId, 1) on conflict (user_id) do update set n = user_seq.n + 1 returning n, nextval(pg_get_serial_sequence('orders', 'id')) as order_id")
    fun nextOrder(userId: String): NextOrder

    @Query("insert into bench_rolled_back(message_id) values (:messageId)")
    fun rolledBack(messageId: UUID)
}

@Component
class BenchMetrics(registry: MeterRegistry) {
    val tx: Timer = Timer.builder("bench.tx")
        .publishPercentileHistogram()
        .minimumExpectedValue(Duration.of(100, ChronoUnit.MICROS))
        .register(registry)
    val txFailed: Counter = registry.counter("bench.tx.failed")
    val rolledBack: Counter = registry.counter("bench.tx.rolled_back")
    val sent: Counter = registry.counter("bench.messages.sent")
    val received: Counter = registry.counter("bench.consumer.received")
    val duplicates: Counter = registry.counter("bench.consumer.duplicates")
    val orderViolations: Counter = registry.counter("bench.consumer.order_violations")
    val noopDelivered: Counter = registry.counter("bench.noop.delivered")
}

/**
 * Business tables and the `orders` topic, before anything writes or consumes. `orders.message_id` is the id the send
 * returned in the order's transaction; `bench_rolled_back` holds the ids of messages whose transaction rolled back, and
 * `bench_consumed` the first-time order messages in the consumer's commit order; the chaos scenarios reconcile them
 * against the inbox in SQL.
 */
@Component
class BenchSetup(private val db: JdbcConnectionFactory, private val kafka: KafkaOutboxConfig) : Lifecycle {
    override fun init() {
        db.inTx<Unit> { c: Connection ->
            c.createStatement().use {
                it.execute(
                    """
                    create table if not exists orders(id bigserial primary key, user_id text, amount bigint,
                        message_id uuid not null, created_at timestamptz default now());
                    alter table orders add column if not exists message_id uuid; -- tables made before message_id
                    create table if not exists user_seq(user_id text primary key, n bigint);
                    create table if not exists bench_rolled_back(message_id uuid primary key);
                    create table if not exists bench_consumed(
                        id bigserial, message_id uuid not null, user_id text not null, seq bigint not null)
                    """.trimIndent()
                )
            }
        }
        val bootstrap = kafka.driverProperties()[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG]!!
        val topic = NewTopic(TOPIC, Optional.of(TOPIC_PARTITIONS), Optional.empty())
        Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap)).use { admin ->
            try {
                admin.createTopics(listOf(topic)).all().get(30, TimeUnit.SECONDS)
            } catch (e: ExecutionException) {
                if (e.cause !is TopicExistsException) throw e
            }
            try { // only a hint: metadata may lag right after creation, never fail startup over it
                val partitions = admin.describeTopics(listOf(TOPIC)).allTopicNames()
                    .get(30, TimeUnit.SECONDS).getValue(TOPIC).partitions().size
                if (partitions != TOPIC_PARTITIONS) {
                    log.warn("topic {} has {} partitions, the bench expects {}", TOPIC, partitions, TOPIC_PARTITIONS)
                }
            } catch (e: Exception) {
                if (e is InterruptedException) throw e
                log.warn("cannot check partitions of topic {}: {}", TOPIC, e.toString())
            }
        }
    }

    override fun release() {}

    private companion object {
        val log = LoggerFactory.getLogger(BenchSetup::class.java)
    }
}

@Component
class OrderWriter(
    private val db: JdbcConnectionFactory,
    private val orders: OrderRepository,
    private val outbox: Outbox,
    private val events: TypedOutbox<OrderEvent>,
    private val json: JsonWriter<OrderEvent>,
    private val tracer: Tracer,
    private val metrics: BenchMetrics,
    private val kafka: KafkaOutboxConfig,
    @Suppress("UNUSED_PARAMETER") setup: BenchSetup,
) {
    /** One order: business row, per-user seq and its message in one transaction. */
    fun createOrder(userId: String, pad: String?): OrderEvent = transaction(1) { order(userId, pad).first }

    /**
     * An order whose transaction sends its message and then rolls back. The message id goes to `bench_rolled_back`
     * after the rollback, in a transaction of its own: never two connections per writer, so a pool smaller than the
     * writers cannot deadlock on it. A crash in between leaves that id out of the phantom check, never in it wrongly.
     */
    fun createRolledBackOrder(userId: String, pad: String?) {
        val id = try {
            transaction(1) { throw RolledBack(order(userId, pad).second) }
        } catch (e: RolledBack) {
            e.messageId
        }
        db.inTx<Unit> { _: Connection -> orders.rolledBack(id) }
        metrics.rolledBack.increment()
    }

    /** The order's statements: seq and id, the message, the business row carrying the message id. */
    private fun order(userId: String, pad: String?): Pair<OrderEvent, UUID> {
        val next = orders.nextOrder(userId)
        val event = OrderEvent(next.orderId, userId, next.n, pad)
        val messageId = events.send(TOPIC, userId, event)
        orders.insert(next.orderId, userId, ThreadLocalRandom.current().nextLong(1, 100_000), messageId)
        return event to messageId
    }

    private class RolledBack(val messageId: UUID) : RuntimeException(null, null, false, false)

    /** `count` messages with distinct random keys in one `sendAll`, no business rows. */
    fun sendBulk(count: Int, pad: String?) = transaction(count) {
        outbox.sendAll(List(count) {
            val key = UUID.randomUUID().toString()
            OutboxMessage.of(TOPIC, key, json.toByteArray(OrderEvent(0, key, 0, pad)))
        })
    }

    /** Messages over the producer's max.request.size: Kafka rejects them, the relay moves them to dead. */
    fun sendPoison(count: Int) {
        // HOCON numbers arrive as Number, not String, so no getProperty here
        val limit = kafka.driverProperties()["max.request.size"]?.toString()?.toInt() ?: 1_048_576
        repeat(count) {
            db.inTx<Unit> { _: Connection ->
                outbox.send(OutboxMessage.of(TOPIC, "poison-" + UUID.randomUUID(), ByteArray(limit + 1024)))
            }
        }
    }

    /** Padding that brings a message with this key to about `payloadBytes`. */
    fun pad(sampleKey: String, payloadBytes: Int): String? {
        val base = json.toByteArray(OrderEvent(1_000_000, sampleKey, 1_000, "")).size
        return if (payloadBytes > base) "x".repeat(payloadBytes - base) else null
    }

    /** A `bench.order.create` span (root on writer threads, child under HTTP) around a timed transaction. */
    private fun <T> transaction(messages: Int, body: () -> T): T {
        val ctx = Context.current()
        val parent = OpentelemetryContext.get(ctx)
        val span = tracer.spanBuilder("bench.order.create").setParent(parent.context).startSpan()
        OpentelemetryContext.set(ctx, parent.add(span))
        val start = System.nanoTime()
        try {
            val result = db.inTx<T> { _: Connection -> body() }
            metrics.tx.record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
            metrics.sent.increment(messages.toDouble())
            return result
        } catch (e: Exception) {
            span.setStatus(StatusCode.ERROR)
            span.recordException(e)
            throw e
        } finally {
            span.end()
            OpentelemetryContext.set(ctx, parent)
        }
    }
}
