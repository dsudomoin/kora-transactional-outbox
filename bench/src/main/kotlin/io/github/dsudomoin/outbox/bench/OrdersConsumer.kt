package io.github.dsudomoin.outbox.bench

import io.github.dsudomoin.outbox.Inbox
import io.github.dsudomoin.outbox.kafka.KafkaOutboxHeaders
import org.apache.kafka.clients.consumer.ConsumerRecords
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.json.common.JsonReader
import ru.tinkoff.kora.kafka.common.annotation.KafkaListener
import ru.tinkoff.kora.kafka.common.consumer.telemetry.KafkaConsumerTelemetry.KafkaConsumerRecordsTelemetryContext
import java.sql.Connection
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Deduplicates through the inbox (one transaction and one `tryAcceptAll` per poll) and checks that each user's `seq`
 * strictly grows over first-time messages. Order state is applied after commit, keeping the max, so a rolled-back poll
 * or a partition handed to another thread cannot raise a false violation. The same transaction appends the poll's
 * first-time order messages to `bench_consumed` (one INSERT), so the order check can be redone in SQL across instances
 * and kills: a user's rows there, in id order, must have growing `seq`.
 */
@Component
class OrdersConsumer(
    private val db: JdbcConnectionFactory,
    private val inbox: Inbox,
    private val json: JsonReader<OrderEvent>,
    private val metrics: BenchMetrics,
    @Suppress("UNUSED_PARAMETER") setup: BenchSetup, // the topic exists before the consumer subscribes
) {
    private val lastSeq = ConcurrentHashMap<String, Long>()

    @KafkaListener("kafka.consumer.orders")
    fun process(
        records: ConsumerRecords<String, ByteArray>,
        telemetry: KafkaConsumerRecordsTelemetryContext<String, ByteArray>,
    ) {
        val seen = HashMap<String, Long>()
        var accepted = 0
        var violations = 0
        val consumed = Consumed()
        db.inTx<Unit> { c: Connection ->
            val ids = records.map { KafkaOutboxHeaders.messageId(it) }
            val fresh = inbox.tryAcceptAll(ids.filterNotNull()).toHashSet() // one statement per poll
            for ((i, record) in records.withIndex()) {
                val span = telemetry.get(record) // "orders process", parented by the record's traceparent
                var error: Exception? = null
                try {
                    val id = ids[i]
                    if (id == null || fresh.remove(id)) { // each first-time id once, even if repeated in the poll
                        accepted++
                        val event = json.read(record.value())
                        if (event.seq > 0) {
                            val last = maxOf(seen[event.userId] ?: 0, lastSeq[event.userId] ?: 0)
                            if (event.seq <= last) violations++ else seen[event.userId] = event.seq
                            if (id != null) consumed.add(id, event)
                        }
                    }
                } catch (e: Exception) {
                    error = e
                    throw e
                } finally {
                    span.close(error)
                }
            }
            consumed.insert(c)
        }
        seen.forEach { (user, seq) -> lastSeq.merge(user, seq, ::maxOf) }
        metrics.received.increment(accepted.toDouble())
        metrics.duplicates.increment((records.count() - accepted).toDouble())
        metrics.orderViolations.increment(violations.toDouble())
    }
}

/** First-time order messages of one poll, in poll order, for `bench_consumed`. */
private class Consumed {
    private val ids = ArrayList<UUID>()
    private val users = ArrayList<String>()
    private val seqs = ArrayList<Long>()

    fun add(id: UUID, event: OrderEvent) {
        ids += id
        users += event.userId
        seqs += event.seq
    }

    fun insert(c: Connection) {
        if (ids.isEmpty()) return
        c.prepareStatement(
            "insert into bench_consumed(message_id, user_id, seq) select m, u, s" +
                " from unnest(?::uuid[], ?::text[], ?::bigint[]) with ordinality t(m, u, s, o) order by o"
        ).use {
            it.setArray(1, c.createArrayOf("uuid", ids.toTypedArray()))
            it.setArray(2, c.createArrayOf("text", users.toTypedArray()))
            it.setArray(3, c.createArrayOf("bigint", seqs.toTypedArray()))
            it.executeUpdate()
        }
    }
}
