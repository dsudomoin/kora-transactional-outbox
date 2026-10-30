package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.US_ASCII;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxIncompatibleContractException;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The writer guard, one per {@link OutboxSchema}: {@link PgOutbox} refuses a message whose {@code outbox-contract}
 * header gives a version v above 1 with {@link OutboxIncompatibleContractException}, before anything is written,
 * unless every live relay instance ({@code relay}, {@code expires_at} ahead: any of them can take any shard) reports
 * its topic at v or above in {@code contracts}; one whose report is null (unknown) or lacks the topic reads none of
 * it. A topic that no live relay instance reports and no typed route of this instance claims is not checked: its
 * records are forwarded or handled raw, as a Kafka-bound one is. Without a live relay instance nothing is refused. The
 * reports are as the bootstrap, then each beat of this instance's {@link Heartbeat}, last read them: a read older than
 * {@value OutboxSchema#LIVE_INTERVALS} {@code maintenanceInterval}s refuses every such message, as nothing stands
 * behind it. A refusal is counted ({@link OutboxTelemetry#contractRejected}) and logged at WARN with every instance
 * id, at most once a minute.
 */
final class ContractGuard {

    private static final Logger log = LoggerFactory.getLogger(ContractGuard.class);
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    /** How many instance ids the exception's message names; its list and the WARN name all. */
    private static final int NAMED = 2;

    private final Map<String, Integer> contracts;
    private final Duration staleAfter;
    private final AtomicLong warned = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    @Nullable
    private volatile Snapshot snapshot;

    /** {@code contracts}: this instance's typed routes, the version each reads by topic. */
    ContractGuard(Map<String, Integer> contracts, Duration maintenanceInterval) {
        this.contracts = Map.copyOf(contracts);
        this.staleAfter = maintenanceInterval.multipliedBy(OutboxSchema.LIVE_INTERVALS);
    }

    /** What this instance's row reports: the version its typed routes read, by topic. */
    Map<String, Integer> contracts() {
        return contracts;
    }

    /** Reads the live relay instances' reports, in a statement of its own. */
    void refresh(Connection c, TableNames names) throws SQLException {
        long readAt = System.nanoTime();
        var reports = new HashMap<String, Map<String, Integer>>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select instance_id, contracts is not null,"
                + " array(select key from jsonb_each(contracts) order by key), array(select value::int from"
                + " jsonb_each(contracts) order by key) from " + names.instance() + " where relay and expires_at"
                + " > now()")) {
            while (rs.next()) {
                Map<String, Integer> report = null;
                if (rs.getBoolean(2)) {
                    var topics = (String[]) rs.getArray(3).getArray();
                    var versions = (Integer[]) rs.getArray(4).getArray();
                    report = new HashMap<>();
                    for (int i = 0; i < topics.length; i++) {
                        report.put(topics[i], versions[i]);
                    }
                }
                reports.put(rs.getString(1), report);
            }
        }
        observed(new Snapshot(readAt, reports));
    }

    void observed(Snapshot read) {
        snapshot = read;
    }

    /** Throws {@link OutboxIncompatibleContractException} when {@code message} must not be written (see above). */
    void require(TableNames names, OutboxMessage message, String type, OutboxTelemetry telemetry) {
        int version = version(message.headers());
        if (version <= 1) {
            return;
        }
        var topic = message.topic();
        var s = snapshot;
        if (s == null || System.nanoTime() - s.readAt > staleAfter.toNanos()) {
            throw refused(names, telemetry, new OutboxIncompatibleContractException("outbox contract of topic " + topic
                    + ": " + type + " writes v" + version + ", but this instance has not read the live relay"
                    + " instances' contracts for " + staleAfter + " (its heartbeat fails?); refused until it reads"
                    + " them again", topic, type, version, 0, List.of()));
        }
        var lowest = s.lowest.get(topic);
        if (lowest == null) { // no live relay instance reports it
            if (!contracts.containsKey(topic) || s.reports.isEmpty()) {
                return;
            }
            lowest = 0;
        }
        if (lowest >= version) {
            return;
        }
        var below = new ArrayList<String>();
        s.reports.forEach((id, report) -> {
            if (Snapshot.read(report, topic) < version) {
                below.add(id);
            }
        });
        var named = String.join(", ", below.subList(0, Math.min(NAMED, below.size())))
                + (below.size() > NAMED ? ", +" + (below.size() - NAMED) + " more" : "");
        throw refused(names, telemetry, new OutboxIncompatibleContractException("outbox contract of topic " + topic
                + ": " + type + " writes v" + version + ", but " + below.size() + " of " + s.reports.size()
                + " live relay instances read at most v" + lowest + " (" + named + "); deploy the release that reads v"
                + version + " everywhere first", topic, type, version, lowest, below));
    }

    private OutboxIncompatibleContractException refused(TableNames names, OutboxTelemetry telemetry,
            OutboxIncompatibleContractException e) {
        telemetry.contractRejected(e.topic(), e.type(), e.version());
        long now = System.nanoTime();
        long last = warned.get();
        if (now - last >= WARN_INTERVAL && warned.compareAndSet(last, now)) {
            log.warn("outbox {} refused a send: {}; the instances: {}; refusals are counted in"
                    + " outbox.messages.contract_rejected, this WARN repeats at most once a minute", names.base(),
                    e.getMessage(), e.incompatibleInstances());
        }
        return e;
    }

    /**
     * The version the last {@code outbox-contract} header gives, as {@link io.github.dsudomoin.outbox.OutboxRecord}
     * reads it: 1 without one, at most 0 for a value it rejects.
     */
    private static int version(List<OutboxHeader> headers) {
        for (int i = headers.size() - 1; i >= 0; i--) {
            var header = headers.get(i);
            if (header.name().equals(Contracts.HEADER)) {
                var value = header.value() == null ? null : new String(header.value(), US_ASCII);
                try {
                    return Integer.parseInt(value);
                } catch (NumberFormatException e) { // a null value too
                    return 0;
                }
            }
        }
        return 1;
    }

    /** One read of the live relay instances' reports; {@code readAt}: {@link System#nanoTime()} before it ran. */
    static final class Snapshot {

        final long readAt;
        /** By instance id, sorted: its report, the version it reads by topic; null when unknown. */
        final SortedMap<String, Map<String, Integer>> reports;
        /** For each topic an instance reports: the lowest version any reads, 0 when one reads none of it. */
        final Map<String, Integer> lowest;

        Snapshot(long readAt, Map<String, Map<String, Integer>> reports) {
            this.readAt = readAt;
            this.reports = Collections.unmodifiableSortedMap(new TreeMap<>(reports));
            var lowest = new HashMap<String, Integer>();
            for (var report : reports.values()) {
                if (report != null) {
                    for (var topic : report.keySet()) {
                        lowest.computeIfAbsent(topic, t -> reports.values().stream()
                                .mapToInt(r -> read(r, t)).min().orElseThrow());
                    }
                }
            }
            this.lowest = Map.copyOf(lowest);
        }

        /** The version {@code report} reads of {@code topic}, 0 for none. */
        static int read(@Nullable Map<String, Integer> report, String topic) {
            var version = report == null ? null : report.get(topic);
            return version == null ? 0 : version;
        }
    }
}
