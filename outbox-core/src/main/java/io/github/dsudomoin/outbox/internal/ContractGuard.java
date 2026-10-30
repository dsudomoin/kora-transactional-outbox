package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxIncompatibleContractException;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The writer guard, one per {@link OutboxSchema}: {@link PgOutbox} refuses a message whose {@code outbox-contract}
 * header gives a version v above 1 with {@link OutboxIncompatibleContractException}, before anything is written,
 * unless every live relay instance ({@code relay}, {@code expires_at} ahead: any of them can take any shard) reports
 * its topic at v or above in {@code contracts}; one that lacks the topic, or gives a version that is no positive int,
 * reads none of it. A topic that no live relay instance reports and no typed route of this instance claims is not
 * checked, as its records are forwarded or handled raw (a Kafka-bound one), unless a live relay instance's report is
 * unknown ({@code contracts} null or not an object): such an instance is incompatible with every version above 1.
 * Without a live relay instance nothing is refused. The reports are as the bootstrap, then each beat of this
 * instance's {@link Heartbeat}, last read them: a read older than {@value OutboxSchema#LIVE_INTERVALS}
 * {@code maintenanceInterval}s refuses every such message, as nothing stands behind it. A refusal is counted
 * ({@link OutboxTelemetry#contractRejected}) and logged at WARN with every instance id, at most once a minute. A
 * header that gives no version (null, not a decimal int, not positive) is refused with
 * {@link IllegalArgumentException}: the relay would make the record wait for good. The guard also holds this
 * instance's routes, which its row reports, and each read takes the live relay instances' {@code routes} too.
 */
final class ContractGuard {

    private static final Logger log = LoggerFactory.getLogger(ContractGuard.class);
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    /** How many instance ids the exception's message names; its list and the WARN name all. */
    private static final int NAMED = 2;
    /** {@code contracts} when it is an object, else null, so the reads below never fail on a report written by hand. */
    private static final String OBJECT = "case jsonb_typeof(contracts) when 'object' then contracts end";
    /**
     * An instance row's report in three columns, which {@link #report} reads: whether it is known, then its topics and
     * their versions as text, in one order.
     */
    static final String REPORT = "jsonb_typeof(contracts) = 'object', array(select key from jsonb_each_text(" + OBJECT
            + ") order by key), array(select value from jsonb_each_text(" + OBJECT + ") order by key)";
    /**
     * An instance row's {@code routes} as {@link #routes(ResultSet, int)} reads it: its topics sorted, whatever the
     * database's locale, with no null and in one dimension whatever was written by hand; null when unknown.
     */
    static final String ROUTES = "case when routes is not null then array(select t from unnest(routes) t where t is"
            + " not null order by t collate \"C\") end";

    private final Map<String, Integer> contracts;
    private final SortedSet<String> routes;
    private final Duration staleAfter;
    private final AtomicLong warned = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    @Nullable
    private volatile Snapshot snapshot;

    /**
     * {@code contracts}: this instance's typed routes, the version each reads by topic; {@code routes}: every topic a
     * route of this instance claims, typed or plain.
     */
    ContractGuard(Map<String, Integer> contracts, Set<String> routes, Duration maintenanceInterval) {
        this.contracts = Map.copyOf(contracts);
        this.routes = Collections.unmodifiableSortedSet(new TreeSet<>(routes));
        this.staleAfter = maintenanceInterval.multipliedBy(OutboxSchema.LIVE_INTERVALS);
    }

    /** What this instance's row reports: the version its typed routes read, by topic. */
    Map<String, Integer> contracts() {
        return contracts;
    }

    /** What this instance's row reports in {@code routes}: every topic a route of it claims, sorted. */
    SortedSet<String> routes() {
        return routes;
    }

    /** Reads the live relay instances' reports, in a statement of its own. */
    void refresh(Connection c, TableNames names) throws SQLException {
        long readAt = System.nanoTime();
        var reports = new HashMap<String, Map<String, Integer>>();
        var routed = new HashMap<String, Set<String>>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select instance_id, " + REPORT + ", " + ROUTES
                + " from " + names.instance() + " where relay and expires_at > now()")) {
            while (rs.next()) {
                var id = rs.getString(1);
                reports.put(id, report(rs, 2));
                var topics = routes(rs, 5);
                routed.put(id, topics == null ? null : Set.copyOf(topics));
            }
        }
        observed(new Snapshot(readAt, reports, routed));
    }

    /**
     * The report in {@link #REPORT}'s columns from {@code first} on: the version read by topic, null when unknown; a
     * version that is no positive int reads 0.
     */
    @Nullable
    static Map<String, Integer> report(ResultSet rs, int first) throws SQLException {
        if (!rs.getBoolean(first)) {
            return null;
        }
        var topics = (String[]) rs.getArray(first + 1).getArray();
        var versions = (String[]) rs.getArray(first + 2).getArray();
        var report = new HashMap<String, Integer>();
        for (int i = 0; i < topics.length; i++) {
            report.put(topics[i], Contracts.parse(versions[i]));
        }
        return report;
    }

    /** The {@link #ROUTES} column {@code column}: the topics, sorted; null when unknown. */
    @Nullable
    static List<String> routes(ResultSet rs, int column) throws SQLException {
        var routes = rs.getArray(column);
        return routes == null ? null : List.of((String[]) routes.getArray());
    }

    void observed(Snapshot read) {
        snapshot = read;
    }

    /** The last read, null before the first. */
    @Nullable
    Snapshot snapshot() {
        return snapshot;
    }

    /**
     * Throws {@link OutboxIncompatibleContractException} when {@code message} must not be written, and
     * {@link IllegalArgumentException} for a header that gives no version (see above).
     */
    void require(TableNames names, OutboxMessage message, String type, OutboxTelemetry telemetry) {
        var topic = message.topic();
        int version = Contracts.version(message.headers());
        if (version == 0) {
            throw new IllegalArgumentException("invalid " + Contracts.HEADER + " header of a message to topic " + topic
                    + ": " + Contracts.value(message.headers()) + "; it must be a positive decimal int, or absent for"
                    + " version 1");
        }
        if (version == 1) {
            return;
        }
        var s = snapshot;
        if (s == null || System.nanoTime() - s.readAt > staleAfter.toNanos()) {
            throw refused(names, telemetry, new OutboxIncompatibleContractException("outbox contract of topic " + topic
                    + ": " + type + " writes v" + version + ", but this instance has not read the live relay"
                    + " instances' contracts for " + staleAfter.toSeconds() + " s (its heartbeat fails?); refused until"
                    + " it reads them again", topic, type, version, 0, List.of()));
        }
        var lowest = s.lowest.get(topic);
        // no live relay instance reports it and no route here claims it: only an unknown report stands in the way
        boolean unguarded = lowest == null && !contracts.containsKey(topic);
        if (unguarded && !s.unknown || s.reports.isEmpty() || lowest != null && lowest >= version) {
            return;
        }
        var below = new ArrayList<String>();
        int most = 0; // the highest version any of them reads, so the message holds for each one it names
        for (var e : s.reports.entrySet()) {
            int read = Snapshot.read(e.getValue(), topic);
            if (unguarded ? e.getValue() == null : read < version) {
                below.add(e.getKey());
                most = Math.max(most, read);
            }
        }
        var named = String.join(", ", below.subList(0, Math.min(NAMED, below.size())))
                + (below.size() > NAMED ? ", +" + (below.size() - NAMED) + " more" : "");
        throw refused(names, telemetry, new OutboxIncompatibleContractException("outbox contract of topic " + topic
                + ": " + type + " writes v" + version + ", but " + below.size() + " of " + s.reports.size()
                + " live relay instances " + (most == 0 ? "do not read it" : "read at most v" + most) + " (" + named
                + "); deploy the release that reads v" + version + " everywhere first", topic, type, version,
                lowest == null ? 0 : lowest, below));
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

    /** One read of the live relay instances' reports; {@code readAt}: {@link System#nanoTime()} before it ran. */
    static final class Snapshot {

        final long readAt;
        /** By instance id, sorted: its report, the version it reads by topic; null when unknown. */
        final SortedMap<String, Map<String, Integer>> reports;
        /** For each topic an instance reports: the lowest version any reads, 0 when one reads none of it. */
        final Map<String, Integer> lowest;
        /** A report is unknown. */
        final boolean unknown;
        /** By instance id, sorted: the topics its routes claim; null when unknown. */
        final SortedMap<String, Set<String>> routes;

        Snapshot(long readAt, Map<String, Map<String, Integer>> reports, Map<String, Set<String>> routes) {
            this.readAt = readAt;
            this.reports = Collections.unmodifiableSortedMap(new TreeMap<>(reports));
            this.routes = Collections.unmodifiableSortedMap(new TreeMap<>(routes));
            this.unknown = this.reports.containsValue(null); // the copy's: Map.of refuses to look for null
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
