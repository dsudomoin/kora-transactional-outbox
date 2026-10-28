package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_METRICS_PUSH_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.BatchClassifier;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers relayed records to Kafka. Each {@link #send} borrows one pooled idempotent producer. The first
 * send that fails after Kafka appended it closes the producer with a zero timeout from inside its callback: that aborts
 * every queued batch, so no later record of a failed (topic, key) is written after it, and the producer is
 * discarded. A send Kafka refuses before appending it (metadata wait in {@code partitionsFor} or {@code send}, a
 * client-side {@link RecordTooLargeException} or {@link BufferExhaustedException}) has nothing to abort: the producer
 * stays open, and the call skips the rest of the record's topic (topic faults) or (topic, key). After a metadata
 * timeout the call also skips topics its producer has not resolved before: one metadata wait per call, plus one per
 * topic the producer wrongly takes as resolved (see {@link Pooled}). Records of a topic or partition that recently
 * failed on its own are not attempted until its {@link Cooldown} ends: a partition cools after a retriable error
 * Kafka's I/O thread reports for one of its records, a topic after a metadata timeout,
 * {@link TopicAuthorizationException} or {@link UnknownTopicOrPartitionException}. Unkeyed records are left to Kafka's
 * partitioner unless a partition of their topic cools; then the sender picks one partition per call among those with a
 * leader that do not cool.
 *
 * <p>Producer properties are {@code driverProperties} with byte-array serializers, {@code enable.idempotence=true} and
 * {@code acks=all} forced; {@code max.block.ms} defaults to 5000, {@code enable.metrics.push} to false,
 * {@code delivery.timeout.ms} to 20000 and then, if unset, {@code request.timeout.ms} to 15000 (Kafka requires
 * {@code delivery.timeout.ms >= linger.ms + request.timeout.ms}). {@code transactional.id}, {@code partitioner.class}
 * and {@code partitioner.ignore.keys=true} are rejected: ordering and the cooldown need Kafka's per-key partitioning. A
 * call hands records over and waits for acks until {@code delivery.timeout.ms} + 5 s from entry (25 s by default); a
 * send or metadata wait still blocking then can add up to {@code max.block.ms} (30 s in all by default). The relay
 * interrupts a handler call at 80 % of its {@code batchTimeout} (24 s of 30 s by default) and waits for the outcomes
 * until {@code batchTimeout}. The sender honours the interrupt at once: it stops handing records over, closes the
 * producer if records are in flight, and returns acked records as {@code Delivered}, unacked ones as {@code Retry} and
 * the rest as {@code NotAttempted}. Running past the soft cut is therefore safe, but turns slow sends into retries.
 * {@code KafkaOutboxModule} logs a WARN at startup when {@code delivery.timeout.ms} + 5 s + {@code max.block.ms}
 * exceeds {@code batchTimeout} itself.
 */
public final class KafkaOutboxSender implements AutoCloseable {

    private static final Duration WAIT_SLACK = Duration.ofSeconds(5);
    private static final Duration RELEASE_TIMEOUT = Duration.ofSeconds(5);
    private static final Logger log = LoggerFactory.getLogger(KafkaOutboxSender.class);

    private final Supplier<Producer<byte[], byte[]>> factory;
    private final Duration wait;
    private final Duration longestCall;
    private final Clock clock;
    private final Deque<Pooled> pool = new ConcurrentLinkedDeque<>();
    private final Cooldown<TopicPartition> cooldown;
    private final Cooldown<String> topicCooldown;
    private volatile boolean released;

    public KafkaOutboxSender(KafkaOutboxConfig config) {
        this(config, KafkaProducer::new);
    }

    /**
     * {@code producers} makes each producer the sender pools, from the validated properties (the forced and defaulted
     * ones set): the application's own Kafka client factory, e.g. one that adds its metrics or interceptors.
     */
    public KafkaOutboxSender(KafkaOutboxConfig config,
            Function<Map<String, Object>, Producer<byte[], byte[]>> producers) {
        this(producerConfig(config.driverProperties()), producers);
    }

    private KafkaOutboxSender(ProducerConfig config,
            Function<Map<String, Object>, Producer<byte[], byte[]>> producers) {
        this(() -> producers.apply(config.originals()), Duration.ofMillis(config.getInt(DELIVERY_TIMEOUT_MS_CONFIG)),
                Duration.ofMillis(config.getLong(MAX_BLOCK_MS_CONFIG)), Clock.systemUTC());
    }

    /** For tests: {@code clock} drives the cooldowns. */
    KafkaOutboxSender(Supplier<Producer<byte[], byte[]>> factory, Duration deliveryTimeout, Clock clock) {
        this(factory, deliveryTimeout, Duration.ZERO, clock);
    }

    private KafkaOutboxSender(Supplier<Producer<byte[], byte[]>> factory, Duration deliveryTimeout, Duration maxBlock,
            Clock clock) {
        this.factory = factory;
        this.clock = clock;
        this.wait = deliveryTimeout.plus(WAIT_SLACK);
        this.longestCall = wait.plus(maxBlock); // a send or metadata wait may still block at the deadline
        this.cooldown = new Cooldown<>(deliveryTimeout); // a probe resolves within delivery.timeout.ms
        this.topicCooldown = new Cooldown<>(deliveryTimeout);
    }

    /** Validates {@code user} and returns the producer configuration (fails at startup, not on the first send). */
    static ProducerConfig producerConfig(Properties user) {
        // before kafka-clients 4, which has no default for it, rejects it as a ConfigException
        require(user.get(BOOTSTRAP_SERVERS_CONFIG) != null, BOOTSTRAP_SERVERS_CONFIG + " is required");
        require(!user.containsKey(TRANSACTIONAL_ID_CONFIG), TRANSACTIONAL_ID_CONFIG + " is not supported");
        require(!user.containsKey(PARTITIONER_CLASS_CONFIG),
                PARTITIONER_CLASS_CONFIG + " is not supported: ordering and the cooldown need Kafka's key hashing");
        var acks = user.get(ACKS_CONFIG);
        require(acks == null || Set.of("all", "-1").contains(String.valueOf(acks).trim()), "acks must be all, got " + acks);
        var idempotence = user.get(ENABLE_IDEMPOTENCE_CONFIG);
        require(idempotence == null || !"false".equalsIgnoreCase(String.valueOf(idempotence).trim()),
                ENABLE_IDEMPOTENCE_CONFIG + " must be true");
        var maxInFlight = user.get(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION); // before Kafka rejects it less clearly
        require(maxInFlight == null || Integer.parseInt(String.valueOf(maxInFlight).trim()) <= 5,
                MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " must be at most 5, got " + maxInFlight);

        var properties = new Properties();
        properties.putAll(user);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ACKS_CONFIG, "all");
        properties.putIfAbsent(MAX_BLOCK_MS_CONFIG, "5000"); // metadata normally takes milliseconds
        // with push on, kafka-clients 3.9 logs an ERROR from the I/O thread after each close(Duration.ZERO)
        properties.putIfAbsent(ENABLE_METRICS_PUSH_CONFIG, "false");
        if (!properties.containsKey(DELIVERY_TIMEOUT_MS_CONFIG)) {
            properties.put(DELIVERY_TIMEOUT_MS_CONFIG, "20000");
            properties.putIfAbsent(REQUEST_TIMEOUT_MS_CONFIG, "15000"); // Kafka's 30 s default exceeds 20 s
        }

        var config = new ProducerConfig(properties); // Kafka's own validation, e.g. retries=0 with idempotence
        require(!config.getList(BOOTSTRAP_SERVERS_CONFIG).isEmpty(), BOOTSTRAP_SERVERS_CONFIG + " is required");
        require(!config.getBoolean(PARTITIONER_IGNORE_KEYS_CONFIG),
                PARTITIONER_IGNORE_KEYS_CONFIG + " must be false: ordering and the cooldown need Kafka's key hashing");
        int delivery = config.getInt(DELIVERY_TIMEOUT_MS_CONFIG);
        long lingerAndRequest = config.getLong(LINGER_MS_CONFIG) + config.getInt(REQUEST_TIMEOUT_MS_CONFIG);
        require(delivery >= lingerAndRequest, DELIVERY_TIMEOUT_MS_CONFIG + " (" + delivery + ") must be at least "
                + LINGER_MS_CONFIG + " + " + REQUEST_TIMEOUT_MS_CONFIG + " (" + lingerAndRequest + ")");
        return config;
    }

    /**
     * One outcome per record, in list order. Records never handed to Kafka (after a failure closed the
     * producer, after an interrupt or past the deadline) are {@code NotAttempted}. Honours interrupts: returns early
     * and keeps the flag.
     */
    public List<Outcome> send(List<OutboxRecord> records) {
        return sendAndReport(records).outcomes();
    }

    /**
     * {@link #send}'s outcomes, and whether its producer was closed by a record Kafka refused for good after the
     * append ({@code Permanent} from the I/O thread): the records in flight with it then failed for that alone.
     */
    record Sent(List<Outcome> outcomes, boolean closedByRefusal) {}

    Sent sendAndReport(List<OutboxRecord> records) {
        if (records.isEmpty()) {
            return new Sent(List.of(), false);
        }
        long deadline = System.nanoTime() + wait.toNanos();
        var pooled = pool.pollFirst();
        if (pooled == null) {
            pooled = new Pooled(factory.get());
        }
        var call = new Call(pooled.producer, records.size());
        try {
            var partitions = sendAll(call, records, deadline, pooled);
            if (!call.await(deadline)) {
                call.close(Thread.currentThread().isInterrupted() ? "interrupted" : "not acknowledged within " + wait);
            }
            return new Sent(outcomes(call, records, partitions), call.closedByRefusal());
        } catch (RuntimeException | Error e) {
            call.close("aborted by " + e); // unknown state: never reuse the producer
            throw e;
        } finally {
            if (!call.closed) {
                giveBack(pooled);
            }
        }
    }

    private void giveBack(Pooled pooled) {
        pool.offerFirst(pooled);
        if (released && pool.remove(pooled)) { // close() has drained the pool already
            closeQuietly(pooled.producer, RELEASE_TIMEOUT);
        }
    }

    /**
     * Logs a WARN when a call's own budget exceeds {@code limit}, the value of {@code setting} that bounds a caller's
     * calls; {@code effect} says what then becomes of slow ones. Such calls are not lost: the caller interrupts them
     * and the sender returns their real outcomes at once, but slow sends then end as retries.
     */
    public void warnIfLongerThan(String setting, Duration limit, String effect) {
        if (longestCall.compareTo(limit) > 0) {
            log.warn("A Kafka outbox call can take up to {} (delivery.timeout.ms + 5 s + max.block.ms), longer than "
                    + "{} ({}). {}. Raise {} or lower the producer timeouts in outbox.kafka.driverProperties",
                    longestCall, setting, limit, effect, setting);
        }
    }

    /** Steps 1–3; returns the partition of each record the sender chose or computed (null: Kafka chooses). */
    private TopicPartition[] sendAll(Call call, List<OutboxRecord> records, long deadline, Pooled pooled) {
        var partitions = new TopicPartition[records.size()];
        var partitionInfos = new HashMap<String, List<PartitionInfo>>();
        var unkeyed = new HashMap<String, Optional<TopicPartition>>(); // per topic, sticky for the call
        // decided once per topic and partition: another worker's success must not unblock a key whose earlier record
        // we skipped
        var coolingUntil = new HashMap<Object, Optional<Instant>>();
        var refused = new HashSet<Object>(); // topics and (topic, key)s whose record Kafka refused before appending it
        // the rest stays unsent (NotAttempted); an interrupt closes the producer only if records are in flight
        for (int i = 0; i < records.size() && !call.closed && !Thread.currentThread().isInterrupted()
                && System.nanoTime() < deadline; i++) {
            var record = records.get(i);
            var topic = record.topic();
            call.started();
            try {
                if (!refused.isEmpty()
                        && (refused.contains(topic) || refused.contains(BatchClassifier.orderingKey(record)))) {
                    call.complete(i, Outcome.notAttempted(), false);
                    continue;
                }
                if (call.metadataTimedOut && !partitionInfos.containsKey(topic)
                        && !pooled.resolved(topic, clock.instant())) {
                    // one metadata wait per call; before the cooldown lookup, which would lease the topic to us
                    var until = topicCooldown.peek(topic, clock.instant());
                    call.complete(i, until == null ? Outcome.notAttempted() : Outcome.notAttempted(until), false);
                    continue;
                }
                var topicUntil = coolingUntil.computeIfAbsent(topic,
                        t -> Optional.ofNullable(topicCooldown.coolingUntil(topic, clock.instant())));
                if (topicUntil.isPresent()) {
                    call.complete(i, Outcome.notAttempted(topicUntil.get()), false);
                    continue;
                }
                var infos = partitionInfos.get(topic);
                if (infos == null) {
                    try {
                        infos = call.producer.partitionsFor(topic);
                    } catch (InterruptException e) {
                        throw e;
                    } catch (RuntimeException e) { // a metadata failure: the record was never handed to send()
                        pooled.forget(topic);
                        refuse(call, i, record, e, true, coolingUntil, refused);
                        continue;
                    }
                    partitionInfos.put(topic, infos);
                    pooled.used(topic, clock.instant());
                    if (System.nanoTime() >= deadline) { // a slow lookup: send() could block max.block.ms again
                        call.complete(i, Outcome.notAttempted(), false);
                        break;
                    }
                }
                var topicPartitions = infos;
                var partition = record.key() != null
                        ? new TopicPartition(topic, BuiltInPartitioner.partitionForKey(record.key(), infos.size()))
                        : unkeyed.computeIfAbsent(topic, t -> unkeyedPartition(topic, topicPartitions, coolingUntil))
                                .orElse(null);
                if (partition != null) {
                    partitions[i] = partition;
                    var until = coolingUntil.computeIfAbsent(partition,
                            p -> Optional.ofNullable(cooldown.coolingUntil(partition, clock.instant())));
                    if (until.isPresent()) {
                        call.complete(i, Outcome.notAttempted(until.get()), false);
                        continue;
                    }
                }
                var explicit = record.key() == null && partition != null ? partition.partition() : null;
                call.producer.send(producerRecord(record, explicit), call.callback(i));
                if (call.refused != null) {
                    refuse(call, i, record, call.refused, false, coolingUntil, refused);
                    call.refused = null;
                }
            } catch (InterruptException e) { // waiting for metadata or buffer space: the record joined no batch
                call.complete(i, Outcome.notAttempted(), false);
            } catch (RuntimeException e) {
                call.failed(i, e, null);
            }
        }
        return partitions;
    }

    /**
     * I1: empty while no partition of {@code topic} has a cooldown entry, so Kafka's partitioner picks one; otherwise
     * a random partition with a leader that does not cool, or, when all of them cool, the one cooling least long.
     */
    private Optional<TopicPartition> unkeyedPartition(String topic, List<PartitionInfo> infos,
            Map<Object, Optional<Instant>> coolingUntil) {
        if (infos.stream().noneMatch(p -> cooldown.contains(new TopicPartition(topic, p.partition())))) {
            return Optional.empty();
        }
        TopicPartition earliest = null;
        Instant earliestUntil = null;
        int start = ThreadLocalRandom.current().nextInt(infos.size());
        for (int k = 0; k < infos.size(); k++) {
            var info = infos.get((start + k) % infos.size());
            if (info.leader() == null) {
                continue;
            }
            var partition = new TopicPartition(topic, info.partition());
            var until = coolingUntil.computeIfAbsent(partition,
                    p -> Optional.ofNullable(cooldown.coolingUntil(partition, clock.instant()))); // may lease it
            if (until.isEmpty()) {
                return Optional.of(partition);
            }
            if (earliest == null || until.get().isBefore(earliestUntil)) {
                earliest = partition;
                earliestUntil = until.get();
            }
        }
        return Optional.ofNullable(earliest);
    }

    /**
     * C1: Kafka refused record {@code i} before appending it, so the producer has nothing to abort and stays open.
     * The rest of the call skips the record's topic after a metadata failure (cooling the topic after a metadata
     * timeout or a {@link #topicFault}), otherwise its (topic, key); a client-side {@link RecordTooLargeException} is
     * {@code Permanent} and skips nothing. Before the append a {@link TimeoutException} is a metadata wait, unless it
     * is a {@link BufferExhaustedException}; after one, the call looks up no topic its producer has not resolved before,
     * so it waits {@code max.block.ms} for metadata once, plus once per topic wrongly taken as resolved
     * ({@link Pooled}).
     */
    private void refuse(Call call, int i, OutboxRecord record, Exception e, boolean metadata,
            Map<Object, Optional<Instant>> coolingUntil, Set<Object> refused) {
        call.complete(i, outcome(e), false);
        var topic = record.topic();
        boolean metadataTimeout = e instanceof TimeoutException && !(e instanceof BufferExhaustedException);
        call.metadataTimedOut |= metadataTimeout;
        if (metadataTimeout || topicFault(e)) {
            coolingUntil.put(topic, Optional.of(topicCooldown.failed(topic, clock.instant())));
        } else if (metadata || e instanceof InvalidTopicException) {
            refused.add(topic);
        } else if (!(e instanceof RecordTooLargeException)) {
            refused.add(BatchClassifier.orderingKey(record));
        }
    }

    /**
     * M3: errors that say the partition is unhealthy (timeouts, not-enough-replicas, not-leader, …). Not a generic
     * {@link KafkaException}, which a batch-mate of an invalid record gets (Sender.java:806-822).
     */
    private static boolean partitionFault(Throwable e) {
        return e instanceof RetriableException && !(e instanceof BufferExhaustedException);
    }

    /** Errors of the whole topic, not of a partition: they cool the topic. */
    private static boolean topicFault(Throwable e) {
        return e instanceof TopicAuthorizationException || e instanceof UnknownTopicOrPartitionException;
    }

    /** Steps 5–6. */
    private List<Outcome> outcomes(Call call, List<OutboxRecord> records, TopicPartition[] partitions) {
        var now = clock.instant();
        var result = new ArrayList<Outcome>(partitions.length);
        var failed = new HashSet<TopicPartition>();
        var failedTopics = new HashSet<String>();
        var notSent = Outcome.notAttempted();
        Outcome unfinished = null;
        synchronized (call) {
            for (int i = 0; i < partitions.length; i++) {
                var outcome = call.outcomes[i];
                if (outcome == null && i >= call.started) {
                    outcome = notSent;
                } else if (outcome == null) {
                    if (unfinished == null) {
                        unfinished = Outcome.retry(new KafkaException("not acknowledged: " + call.closeReason));
                    }
                    outcome = unfinished;
                }
                result.add(outcome);
                var partition = partitions[i] != null ? partitions[i] : call.partitionChosenByKafka(i);
                if (outcome == Outcome.delivered()) {
                    topicCooldown.succeeded(records.get(i).topic());
                    if (partition != null) {
                        cooldown.succeeded(partition);
                    }
                } else if (call.ownFailure[i] && outcome instanceof Outcome.Retry retry) {
                    if (partition != null && partitionFault(retry.error())) {
                        failed.add(partition);
                    }
                    if (topicFault(retry.error())) {
                        failedTopics.add(records.get(i).topic());
                    }
                }
            }
        }
        failed.forEach(p -> cooldown.failed(p, now));
        failedTopics.forEach(t -> topicCooldown.failed(t, now));
        return result;
    }

    private static ProducerRecord<byte[], byte[]> producerRecord(OutboxRecord record, @Nullable Integer partition) {
        var headers = new ArrayList<Header>(record.headers().size() + 1);
        for (var h : record.headers()) {
            headers.add(new RecordHeader(h.name(), h.value()));
        }
        headers.add(new RecordHeader(KafkaOutboxHeaders.MESSAGE_ID, record.messageId().toString().getBytes(UTF_8)));
        return new ProducerRecord<>(record.topic(), partition, null, record.key(), record.payload(), headers);
    }

    private static Outcome outcome(Exception e) {
        return e instanceof RecordTooLargeException || e instanceof InvalidRecordException
                        || e instanceof InvalidTopicException
                ? Outcome.permanent(e)
                : Outcome.retry(e);
    }

    /** The relay's handler over this sender: {@link KafkaOutboxHandler}. */
    public KafkaOutboxHandler handler() {
        return new KafkaOutboxHandler(this);
    }

    @Override
    public void close() {
        released = true;
        for (Pooled pooled; (pooled = pool.pollFirst()) != null; ) {
            closeQuietly(pooled.producer, RELEASE_TIMEOUT);
        }
    }

    private static void closeQuietly(Producer<byte[], byte[]> producer, Duration timeout) {
        try {
            producer.close(timeout);
        } catch (InterruptException e) { // closed anyway, only the I/O thread was not joined; the flag stays set
            log.debug("Interrupted while closing an outbox Kafka producer", e);
        } catch (RuntimeException e) {
            log.warn("Failed to close an outbox Kafka producer", e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("outbox.kafka.driverProperties: " + message);
        }
    }

    /**
     * A pooled producer and the topics it has fetched metadata for, by last lookup: {@code partitionsFor} does not wait
     * for those. Kafka drops a topic unused for {@code metadata.max.idle.ms} (5 min by default) from its metadata, so
     * an entry is trusted for 5 min after its last lookup, and at most 10 000 are kept, the least recently used dropped
     * first. A topic wrongly taken as resolved (deleted since, or dropped by a {@code metadata.max.idle.ms} below
     * 5 min) can cost a call that already timed out one more metadata wait, and is then forgotten; one wrongly taken as
     * unresolved costs no wait: Kafka answers its lookup from cache, and after a timeout the call skips it. Used by
     * one call at a time.
     */
    private static final class Pooled {
        private static final Duration IDLE = Duration.ofMinutes(5);
        private static final int MAX_TOPICS = 10_000;

        final Producer<byte[], byte[]> producer;
        private final Map<String, Instant> lastUse = new LinkedHashMap<>() { // least recently used first
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Instant> eldest) {
                return size() > MAX_TOPICS;
            }
        };

        Pooled(Producer<byte[], byte[]> producer) {
            this.producer = producer;
        }

        boolean resolved(String topic, Instant now) {
            var used = lastUse.get(topic);
            if (used != null && now.isBefore(used.plus(IDLE))) {
                return true;
            }
            lastUse.remove(topic);
            return false;
        }

        void used(String topic, Instant now) {
            lastUse.remove(topic); // re-inserted last
            lastUse.put(topic, now);
        }

        void forget(String topic) {
            lastUse.remove(topic);
        }
    }

    /** State of one {@link #send} call, shared with the producer's callbacks. */
    private static final class Call {
        final Producer<byte[], byte[]> producer;
        final Thread caller = Thread.currentThread();
        final Outcome[] outcomes; // guarded by this, like ownFailure, metadata, pending, started, closeReason, closedBy
        final boolean[] ownFailure;
        final RecordMetadata[] metadata;
        int pending;
        int started; // records 0 until started were handed to the loop body, in order
        volatile boolean closed; // written under this
        String closeReason = "";
        @Nullable
        Exception closedBy; // the failure whose failed() closed the call, if one did
        @Nullable
        Exception refused; // caller thread only: what send() reported through the callback before appending
        boolean metadataTimedOut; // caller thread only

        Call(Producer<byte[], byte[]> producer, int size) {
            this.producer = producer;
            this.outcomes = new Outcome[size];
            this.ownFailure = new boolean[size];
            this.metadata = new RecordMetadata[size];
        }

        synchronized void started() {
            pending++;
            started++;
        }

        synchronized void complete(int i, Outcome outcome, boolean own) {
            record(i, outcome, own, null);
        }

        /** Holds this. The first completion of a record wins. */
        private void record(int i, Outcome outcome, boolean own, @Nullable RecordMetadata kafka) {
            if (outcomes[i] == null) {
                outcomes[i] = outcome;
                ownFailure[i] = own;
                metadata[i] = kafka;
                pending--;
                notifyAll();
            }
        }

        /**
         * Holds this. Where Kafka's partitioner put record {@code i}, from its callback: on failure too, where
         * KafkaProducer's AppendCallbacks fills in the partition the accumulator chose (KafkaProducer.java:1575-1576,
         * RecordAccumulator.java:322).
         */
        @Nullable
        TopicPartition partitionChosenByKafka(int i) {
            var m = metadata[i];
            return m != null && m.partition() >= 0 ? new TopicPartition(m.topic(), m.partition()) : null;
        }

        /**
         * KafkaProducer.doSend reports an {@code ApiException} raised before the append through the callback, on the
         * caller thread inside {@code send()} (KafkaProducer.java:1105-1111); only {@code maybeAddPartition} runs after
         * the append, and it throws none of these types. The caller handles them when {@code send()} returns.
         */
        Callback callback(int i) {
            return (kafka, e) -> {
                if (e == null) {
                    synchronized (this) {
                        record(i, Outcome.delivered(), false, kafka);
                    }
                } else if (Thread.currentThread() == caller && (e instanceof TimeoutException
                        || e instanceof RecordTooLargeException || e instanceof InvalidTopicException
                        || e instanceof TopicAuthorizationException)) {
                    refused = e;
                } else {
                    failed(i, e, kafka);
                }
            };
        }

        /**
         * Records the failure and marks the call closed in one step, so the caller never sees the outcome without the
         * close and never pools the producer. The record's own failure is one the I/O thread reports before our close
         * (after it, an abort); one thrown on the caller thread cools nothing.
         */
        void failed(int i, Exception e, @Nullable RecordMetadata kafka) {
            boolean closeProducer;
            synchronized (this) {
                record(i, outcome(e), !closed && Thread.currentThread() != caller, kafka);
                closeProducer = markClosed("an earlier send failed: " + e);
                if (closeProducer) {
                    closedBy = e;
                }
            }
            if (closeProducer) {
                closeQuietly(producer, Duration.ZERO);
            }
        }

        synchronized boolean closedByRefusal() {
            return closedBy != null && outcome(closedBy) instanceof Outcome.Permanent;
        }

        /** Idempotent. Callers must not hold this monitor: closing from the caller joins the I/O thread. */
        void close(String reason) {
            boolean closeProducer;
            synchronized (this) {
                closeProducer = markClosed(reason);
            }
            if (closeProducer) {
                closeQuietly(producer, Duration.ZERO);
            }
        }

        /** Holds this. True for the first caller, who then closes the producer outside the monitor. */
        private boolean markClosed(String reason) {
            if (closed) {
                return false;
            }
            closed = true;
            closeReason = reason;
            notifyAll();
            return true;
        }

        /** True once every started record completed or the call closed; false on deadline or interrupt (flag kept). */
        synchronized boolean await(long deadlineNanos) {
            while (pending > 0 && !closed) {
                long left = deadlineNanos - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                try {
                    NANOSECONDS.timedWait(this, left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }
}
