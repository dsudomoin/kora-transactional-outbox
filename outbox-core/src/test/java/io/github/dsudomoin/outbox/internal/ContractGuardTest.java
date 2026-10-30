package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxIncompatibleContractException;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxMissingRouteException;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ContractGuardTest {

    private static final Duration INTERVAL = Duration.ofSeconds(30);
    /** The age of a stale read: more than three intervals. */
    private static final Duration STALE = Duration.ofSeconds(100);
    private static final TableNames NAMES = TableNames.of("outbox");

    private final List<String> rejected = new ArrayList<>();
    private final List<String> routeRejected = new ArrayList<>();
    private final List<String> unchecked = new ArrayList<>();
    private final OutboxTelemetry telemetry = new OutboxTelemetry() {
        @Override
        public void contractRejected(String topic, String type, int version) {
            rejected.add(topic + " " + type + " v" + version);
        }

        @Override
        public void routeRejected(String topic) {
            routeRejected.add(topic);
        }

        @Override
        public void routeUnchecked(String topic) {
            unchecked.add(topic);
        }
    };

    /** Neither check covers it: never read, or read stale without a live relay instance, it still goes in. */
    @Test
    void aVersion1MessageOfATopicNoRouteHereClaimsIsNeverChecked() {
        var neverRead = new ContractGuard(Map.of("orders", 2), Set.of("orders"), INTERVAL);
        var noRelay = routed(Set.of("orders"), Map.of(), STALE);

        for (var guard : List.of(neverRead, noRelay)) {
            assertThatCode(() -> {
                require(guard, message("kafka-bound", null), "Created");
                require(guard, message("kafka-bound", "1"), "Created");
            }).doesNotThrowAnyException();
        }
        assertThat(rejected).isEmpty();
        assertThat(routeRejected).isEmpty();
        assertThat(unchecked).isEmpty();
    }

    @Test
    void aTopicNoRelayReportsAndNoRouteHereClaimsIsNotChecked() {
        var guard = guard(Map.of("audit", 1), Map.of("a", Map.of("orders", 1)));

        assertThatCode(() -> require(guard, message("kafka-bound", "2"), "Created")).as("forwarded or handled raw")
                .doesNotThrowAnyException();
    }

    @Test
    void everyRelayAtTheVersionOrAboveAllowsIt() {
        var guard = guard(Map.of(), Map.of("a", Map.of("orders", 2), "b", Map.of("orders", 3)));

        assertThatCode(() -> require(guard, message("orders", "2"), "Created")).doesNotThrowAnyException();
        assertThatThrownBy(() -> require(guard, message("orders", "3"), "Created"))
                .isInstanceOf(OutboxIncompatibleContractException.class)
                .hasMessage("outbox contract of topic orders: Created writes v3, but 1 of 2 live relay instances read"
                        + " at most v2 (a); deploy the release that reads v3 everywhere first");
    }

    @Test
    void oneRelayBelowRefusesIt() {
        var guard = guard(Map.of("orders", 2), Map.of("a", Map.of("orders", 2), "b", Map.of("orders", 1)));

        assertThatThrownBy(() -> require(guard, message("orders", "2"), "Created"))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e.minSupported()).isEqualTo(1);
                    assertThat(e.incompatibleInstances()).containsExactly("b");
                });
        assertThat(routeRejected).as("both route it").isEmpty();
    }

    @Test
    void aRelayWithoutAReportOrWithoutTheTopicReadsNoneOfIt() {
        var reports = new HashMap<String, Map<String, Integer>>();
        reports.put("a", Map.of("orders", 2));
        reports.put("unknown", null);
        reports.put("other", Map.of("audit", 5));
        var guard = guard(Map.of(), reports);

        assertThatThrownBy(() -> require(guard, message("orders", "2"), "Created"))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e.minSupported()).isZero();
                    assertThat(e.incompatibleInstances()).containsExactly("other", "unknown");
                    assertThat(e).hasMessageContaining("2 of 3 live relay instances do not read it (other, unknown)");
                });
    }

    /** {@code a} routes {@code orders} too, plainly: the version check is what refuses. */
    @Test
    void aTopicOnlyARouteHereClaimsIsCheckedAgainstEveryRelay() {
        var claimed = guard(Map.of("orders", 2), Map.of("a", Map.of("audit", 1)));
        var noRelay = guard(Map.of("orders", 1), Map.of());

        assertThatThrownBy(() -> require(claimed, message("orders", "2"), "Created"))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class,
                        e -> assertThat(e.incompatibleInstances()).containsExactly("a"));
        assertThatCode(() -> require(noRelay, message("orders", "2"), "Created"))
                .as("no live relay instance: nothing refuses").doesNotThrowAnyException();
    }

    /**
     * Stale, the last read decides a version above 1: no read, no live relay instance, an unknown report and one
     * below each refuse, naming the read's age and no instance.
     */
    @Test
    void aStaleReadRefusesAVersionAbove1ItCannotVouchFor() {
        record Case(String name, ContractGuard guard, List<String> topics, String why) {}
        var unknown = new HashMap<String, Map<String, Integer>>();
        unknown.put("a", Map.of("orders", 5));
        unknown.put("unknown", null);
        var lacking = "has not read the live relay instances' reports for 100 s (its heartbeat fails?), and in the last"
                + " read not every one reads v2; refused until a fresh read allows it";
        var both = List.of("orders", "kafka-bound");
        var cases = List.of(
                new Case("never read", new ContractGuard(Map.of(), Set.of(), INTERVAL), both,
                        "has never read the live relay instances' reports (its heartbeat fails?); refused until a"
                                + " fresh read allows it"),
                new Case("no relay", aged(guard(Map.of(), Map.of()), STALE), both,
                        "has not read the live relay instances' reports for 100 s (its heartbeat fails?), and the last"
                                + " read found no live relay instance; refused until a fresh read allows it"),
                new Case("unknown", aged(guard(Map.of(), unknown), STALE), both, lacking),
                new Case("below", aged(guard(Map.of(), Map.of("a", Map.of("orders", 5), "b", Map.of("orders", 1))),
                        STALE), List.of("orders"), lacking));

        for (var c : cases) {
            for (var topic : c.topics()) {
                assertThatThrownBy(() -> require(c.guard(), message(topic, "2"), "Created")).as(c.name() + " " + topic)
                        .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                            assertThat(e).hasMessage("outbox contract of topic " + topic + ": Created writes v2, but"
                                    + " this instance " + c.why());
                            assertThat(e.minSupported()).isZero();
                            assertThat(e.incompatibleInstances()).isEmpty();
                        });
            }
        }
        assertThat(rejected).hasSize(7);
        assertThat(unchecked).isEmpty();
    }

    /** Stale, a last read that allowed it lets it through, counted; a version-1 message of it is not even counted. */
    @Test
    void aStaleReadThatAllowedAVersionLetsItThroughUnchecked() {
        var guard = aged(guard(Map.of(), Map.of("a", Map.of("orders", 2), "b", Map.of("orders", 3))), STALE);

        try (var err = new CapturedStderr()) {
            assertThatCode(() -> {
                require(guard, message("orders", "2"), "Created");
                require(guard, message("kafka-bound", "2"), "Created");
                require(guard, message("orders", null), "Created");
            }).doesNotThrowAnyException();
            var text = err.text();
            assertThat(text.split("through unchecked", -1)).as("one WARN within a minute").hasSize(2);
            assertThat(text).contains("outbox outbox let a send to topic orders through unchecked: this instance has"
                    + " not read the live relay instances' reports for 100 s (its heartbeat fails?), and the last read"
                    + " allowed it; such sends are counted in outbox.messages.route_unchecked");
        }
        assertThat(unchecked).containsExactly("orders", "kafka-bound");
        assertThatThrownBy(() -> require(guard, message("orders", "3"), "Created"))
                .isInstanceOf(OutboxIncompatibleContractException.class);
        assertThat(unchecked).as("a refusal is not let through").hasSize(2);
    }

    @Test
    void theRefusalNamesTheTopicTypeVersionsAndInstances() {
        var reports = new HashMap<String, Map<String, Integer>>();
        for (int i = 0; i < 7; i++) {
            reports.put("host-" + i + "/1/0000000" + i, Map.of("orders", 2));
        }
        reports.put("host-b/7/0000000b", Map.of("orders", 1));
        reports.put("host-a/7/0000000a", Map.of("orders", 1));
        reports.put("host-c/7/0000000c", Map.of("orders", 1));
        var guard = guard(Map.of(), reports);

        assertThatThrownBy(() -> require(guard, message("orders", "2"), "com.acme.OrderCreated"))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e).hasMessage("outbox contract of topic orders: com.acme.OrderCreated writes v2, but 3"
                            + " of 10 live relay instances read at most v1 (host-a/7/0000000a, host-b/7/0000000b, +1"
                            + " more); deploy the release that reads v2 everywhere first");
                    assertThat(e.topic()).isEqualTo("orders");
                    assertThat(e.type()).isEqualTo("com.acme.OrderCreated");
                    assertThat(e.version()).isEqualTo(2);
                    assertThat(e.minSupported()).isEqualTo(1);
                    assertThat(e.incompatibleInstances())
                            .containsExactly("host-a/7/0000000a", "host-b/7/0000000b", "host-c/7/0000000c");
                });
    }

    /** The highest version among the instances named, so it holds for each; the fleet's lowest is minSupported. */
    @Test
    void theMessageNamesTheHighestVersionTheBlockingInstancesRead() {
        var reports = new HashMap<String, Map<String, Integer>>();
        reports.put("a", Map.of("orders", 3));
        reports.put("b", Map.of("orders", 1));
        reports.put("c", Map.of("orders", 2));
        reports.put("d", null);
        reports.put("e", Map.of("audit", 1));
        var guard = guard(Map.of(), reports);

        assertThatThrownBy(() -> require(guard, message("orders", "3"), "Created"))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e).hasMessageContaining("4 of 5 live relay instances read at most v2 (b, c, +2 more)");
                    assertThat(e.minSupported()).isZero();
                    assertThat(e.incompatibleInstances()).containsExactly("b", "c", "d", "e");
                });
    }

    /** Fail closed: an instance whose report is unknown may forward or handle anything, so nothing above v1 passes. */
    @Test
    void anUnknownReportGuardsEveryTopic() {
        var reports = new HashMap<String, Map<String, Integer>>();
        reports.put("a", Map.of("orders", 2));
        reports.put("b", Map.of());
        reports.put("unknown", null);
        var guard = guard(Map.of(), reports);

        assertThatThrownBy(() -> require(guard, message("kafka-bound", "2"), "Created"))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e).hasMessage("outbox contract of topic kafka-bound: Created writes v2, but 1 of 3 live"
                            + " relay instances do not read it (unknown); deploy the release that reads v2 everywhere"
                            + " first");
                    assertThat(e.minSupported()).isZero();
                    assertThat(e.incompatibleInstances()).containsExactly("unknown");
                });
        assertThatCode(() -> require(guard, message("kafka-bound", null), "Created")).doesNotThrowAnyException();
    }

    @Test
    void theLastHeaderCounts() {
        var guard = guard(Map.of(), Map.of("a", Map.of("orders", 1)));
        var headers = List.of(header("2"), header("1"));

        assertThatCode(() -> require(guard, new OutboxMessage("orders", null, null, headers), "?"))
                .doesNotThrowAnyException();
    }

    /** The relay would make such a record wait for good: refused at once, whatever the reports, and not counted. */
    @Test
    void aHeaderThatGivesNoVersionIsRefused() {
        var guard = new ContractGuard(Map.of(), Set.of("orders"), INTERVAL); // never read
        for (var value : Arrays.asList(null, "", "x", "0", "-1", " 2", "99999999999")) {
            var headers = List.of(header("2"), new OutboxHeader(Contracts.HEADER,
                    value == null ? null : value.getBytes(US_ASCII)));

            assertThatThrownBy(() -> require(guard, new OutboxMessage("orders", null, null, headers), "?")).as(value)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("invalid outbox-contract header of a message to topic orders: " + value + "; it must"
                            + " be a positive decimal int, or absent for version 1");
        }
        assertThat(rejected).isEmpty();
        assertThat(routeRejected).isEmpty();
    }

    @Test
    void eachRefusalIsCountedAndTheWarnNamesEveryInstanceOnceAMinute() {
        var reports = new HashMap<String, Map<String, Integer>>();
        reports.put("a", Map.of("orders", 1));
        reports.put("b", Map.of("orders", 1));
        reports.put("c", null);
        var guard = guard(Map.of(), reports);

        try (var err = new CapturedStderr()) {
            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> require(guard, message("orders", "2"), "Created"))
                        .isInstanceOf(OutboxIncompatibleContractException.class);
            }
            var text = err.text();
            assertThat(text.split("refused a send", -1)).as("one WARN within a minute").hasSize(2);
            assertThat(text).contains("outbox outbox refused a send: outbox contract of topic orders")
                    .contains("the instances: [a, b, c]").contains("outbox.messages.contract_rejected");
        }
        assertThat(rejected).containsExactly("orders Created v2", "orders Created v2");
    }

    /** Raw or typed at version 1, a message of a topic a route here claims needs every live relay to route it. */
    @Test
    void aLocalMessageOfAnyVersionNeedsEveryLiveRelayToRouteIt() {
        var everywhere = routed(Set.of("orders"), Map.of("a", Set.of("orders", "audit"), "b", Set.of("orders")),
                Duration.ZERO);
        var notOnB = routed(Set.of("orders"), Map.of("a", Set.of("orders", "audit"), "b", Set.of("audit")),
                Duration.ZERO);

        assertThatCode(() -> {
            require(everywhere, message("orders", null), "?");
            require(everywhere, message("orders", "1"), "Created");
        }).doesNotThrowAnyException();
        for (var version : Arrays.asList(null, "1")) {
            assertThatThrownBy(() -> require(notOnB, message("orders", version), "Created")).as(version)
                    .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                        assertThat(e).hasMessage("outbox topic orders has a local route here, but 1 of 2 live relay"
                                + " instances do not route it (b); deploy the release with this route everywhere"
                                + " before writing to it");
                        assertThat(e.topic()).isEqualTo("orders");
                        assertThat(e.instances()).containsExactly("b");
                    });
        }
        assertThat(routeRejected).containsExactly("orders", "orders");
        assertThat(unchecked).as("fresh").isEmpty();
    }

    /** Fail closed: an instance whose routes are unknown routes nothing. */
    @Test
    void aRelayWhoseRoutesAreUnknownRoutesNothing() {
        var routes = new HashMap<String, Set<String>>();
        routes.put("a", Set.of("orders"));
        routes.put("unknown", null);
        var guard = routed(Set.of("orders"), routes, Duration.ZERO);

        assertThatThrownBy(() -> require(guard, message("orders", null), "?"))
                .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                    assertThat(e).hasMessageContaining("1 of 2 live relay instances do not route it (unknown)");
                    assertThat(e.instances()).containsExactly("unknown");
                });
    }

    @Test
    void theRouteRefusalNamesTwoInstancesThenHowManyMore() {
        var routes = new HashMap<String, Set<String>>();
        for (int i = 0; i < 7; i++) {
            routes.put("host-" + i + "/1/0000000" + i, Set.of("orders"));
        }
        routes.put("host-b/7/0000000b", Set.of());
        routes.put("host-a/7/0000000a", Set.of("audit"));
        routes.put("host-c/7/0000000c", null);
        var guard = routed(Set.of("orders"), routes, Duration.ZERO);

        assertThatThrownBy(() -> require(guard, message("orders", null), "?"))
                .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                    assertThat(e).hasMessage("outbox topic orders has a local route here, but 3 of 10 live relay"
                            + " instances do not route it (host-a/7/0000000a, host-b/7/0000000b, +1 more); deploy the"
                            + " release with this route everywhere before writing to it");
                    assertThat(e.instances())
                            .containsExactly("host-a/7/0000000a", "host-b/7/0000000b", "host-c/7/0000000c");
                });
    }

    @Test
    void withoutALiveRelayAFreshReadPassesAndAStaleOneRefuses() {
        var fresh = routed(Set.of("orders"), Map.of(), Duration.ZERO);
        var stale = routed(Set.of("orders"), Map.of(), STALE);

        assertThatCode(() -> require(fresh, message("orders", null), "?")).doesNotThrowAnyException();
        assertThatThrownBy(() -> require(stale, message("orders", null), "?"))
                .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                    assertThat(e).hasMessage("outbox topic orders has a local route here, but this instance has not"
                            + " read the live relay instances' reports for 100 s (its heartbeat fails?), and the last"
                            + " read found no live relay instance; refused until a fresh read allows it");
                    assertThat(e.instances()).isEmpty();
                });
        assertThat(routeRejected).containsExactly("orders");
    }

    /** Stale, the last read decides a local message: no read, an unknown routes report and one without it refuse. */
    @Test
    void aStaleReadRefusesALocalMessageItCannotVouchFor() {
        var unknown = new HashMap<String, Set<String>>();
        unknown.put("a", Set.of("orders"));
        unknown.put("unknown", null);
        var neverRead = new ContractGuard(Map.of(), Set.of("orders"), INTERVAL);
        var lacking = "outbox topic orders has a local route here, but this instance has not read the live relay"
                + " instances' reports for 100 s (its heartbeat fails?), and in the last read not every one routes it;"
                + " refused until a fresh read allows it";

        assertThatThrownBy(() -> require(neverRead, message("orders", null), "?"))
                .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                    assertThat(e).hasMessage("outbox topic orders has a local route here, but this instance has never"
                            + " read the live relay instances' reports (its heartbeat fails?); refused until a fresh"
                            + " read allows it");
                    assertThat(e.instances()).isEmpty();
                });
        for (var guard : List.of(routed(Set.of("orders"), unknown, STALE),
                routed(Set.of("orders"), Map.of("a", Set.of("orders"), "b", Set.of("audit")), STALE))) {
            assertThatThrownBy(() -> require(guard, message("orders", "1"), "Created"))
                    .isInstanceOfSatisfying(OutboxMissingRouteException.class, e -> {
                        assertThat(e).hasMessage(lacking);
                        assertThat(e.instances()).as("a stale read names none").isEmpty();
                    });
        }
        assertThat(routeRejected).hasSize(3);
        assertThat(unchecked).isEmpty();
    }

    /** One count per message let through, whether one check or both took the stale read. */
    @Test
    void aStaleReadThatShowedTheRouteEverywhereLetsItThroughCountedOncePerMessage() {
        var guard = aged(guard(Map.of("orders", 2), Map.of("a", Map.of("orders", 2), "b", Map.of("orders", 3))),
                STALE);

        try (var err = new CapturedStderr()) {
            assertThatCode(() -> {
                require(guard, message("orders", null), "?");
                require(guard, message("orders", "2"), "Created");
            }).doesNotThrowAnyException();
            assertThat(err.text().split("through unchecked", -1)).as("one WARN within a minute").hasSize(2);
        }
        assertThat(unchecked).containsExactly("orders", "orders");
        assertThat(routeRejected).isEmpty();
        assertThat(rejected).isEmpty();
    }

    /** It runs first: a message both checks refuse is refused for its route, and counted only there. */
    @Test
    void theRouteCheckRunsBeforeTheVersionCheck() {
        var reports = Map.of("a", Map.of("orders", 1));
        var notRouted = new ContractGuard(Map.of("orders", 2), Set.of("orders"), INTERVAL);
        notRouted.observed(new ContractGuard.Snapshot(System.nanoTime(), reports, Map.of("a", Set.of("audit"))));
        var routed = guard(Map.of("orders", 2), reports);

        assertThatThrownBy(() -> require(notRouted, message("orders", "2"), "Created"))
                .isInstanceOf(OutboxMissingRouteException.class);
        assertThat(routeRejected).containsExactly("orders");
        assertThat(rejected).isEmpty();
        assertThatThrownBy(() -> require(routed, message("orders", "2"), "Created"))
                .isInstanceOf(OutboxIncompatibleContractException.class);
        assertThat(rejected).containsExactly("orders Created v2");
    }

    @Test
    void eachRouteRefusalIsCountedAndTheWarnNamesEveryInstanceOnceAMinute() {
        var routes = new HashMap<String, Set<String>>();
        routes.put("a", Set.of("orders"));
        routes.put("b", Set.of());
        routes.put("c", null);
        var guard = routed(Set.of("orders"), routes, Duration.ZERO);

        try (var err = new CapturedStderr()) {
            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> require(guard, message("orders", null), "?"))
                        .isInstanceOf(OutboxMissingRouteException.class);
            }
            var text = err.text();
            assertThat(text.split("refused a send", -1)).as("one WARN within a minute").hasSize(2);
            assertThat(text).contains("outbox outbox refused a send: outbox topic orders has a local route here")
                    .contains("the instances: [b, c]").contains("outbox.messages.route_rejected");
        }
        assertThat(routeRejected).containsExactly("orders", "orders");
    }

    /** No route here claims it, so it is written unmarked, though a relay routes it: a WARN, never a refusal. */
    @Test
    void anUnmarkedMessageOfATopicARelayRoutesIsWarnedOnceAMinute() {
        var guard = routed(Set.of(), Map.of("a", Set.of("orders"), "b", Set.of()), Duration.ZERO);

        try (var err = new CapturedStderr()) {
            assertThatCode(() -> {
                require(guard, message("kafka-bound", null), "?");
                require(guard, message("orders", "2"), "Created");
            }).doesNotThrowAnyException();
            assertThat(err.text()).as("whatever the version").contains("wrote to topic orders unmarked");
            assertThatCode(() -> {
                require(guard, message("orders", null), "?");
                require(guard, message("orders", "1"), "Created");
            }).doesNotThrowAnyException();
            var text = err.text();
            assertThat(text.split("unmarked", -1)).as("one WARN within a minute, none for kafka-bound").hasSize(2);
            assertThat(text).contains("outbox outbox wrote to topic orders unmarked, as no route here claims it, while"
                    + " a live relay instance routes it: register that route here too");
        }
        assertThat(routeRejected).isEmpty();
        assertThat(unchecked).isEmpty();
    }

    /** A route refusal, a contract refusal, a send let through unchecked and an unmarked one, within a minute. */
    @Test
    void eachWarnHasALimitOfItsOwn() {
        var guard = new ContractGuard(Map.of("orders", 2), Set.of("orders", "billing"), INTERVAL);
        guard.observed(new ContractGuard.Snapshot(System.nanoTime() - STALE.toNanos(),
                Map.of("a", Map.of("orders", 1)), Map.of("a", Set.of("orders", "audit"))));

        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> require(guard, message("billing", null), "?"))
                    .isInstanceOf(OutboxMissingRouteException.class);
            assertThatThrownBy(() -> require(guard, message("orders", "2"), "Created"))
                    .isInstanceOf(OutboxIncompatibleContractException.class);
            require(guard, message("orders", null), "?");
            require(guard, message("audit", null), "?");

            assertThat(err.text()).contains("outbox.messages.route_rejected")
                    .contains("outbox.messages.contract_rejected").contains("outbox.messages.route_unchecked")
                    .contains("wrote to topic audit unmarked");
        }
    }

    /** As {@link PgOutbox} calls it: local when a route of the guard's instance claims the topic. */
    private void require(ContractGuard guard, OutboxMessage message, String type) {
        guard.require(NAMES, message, type, guard.routes().contains(message.topic()), telemetry);
    }

    /**
     * A fresh read of {@code reports}; each instance routes the topics it reports and every topic a route of the
     * guard's instance claims, so only the version check refuses.
     */
    private static ContractGuard guard(Map<String, Integer> contracts, Map<String, Map<String, Integer>> reports) {
        var routes = new HashMap<String, Set<String>>();
        reports.forEach((id, report) -> {
            var topics = new HashSet<>(contracts.keySet());
            topics.addAll(report == null ? Set.of() : report.keySet());
            routes.put(id, topics);
        });
        var guard = new ContractGuard(contracts, contracts.keySet(), INTERVAL);
        guard.observed(new ContractGuard.Snapshot(System.nanoTime(), reports, routes));
        return guard;
    }

    /**
     * A guard whose instance's routes claim {@code here}, all plain, over a read {@code age} ago of {@code routes};
     * each instance reports no typed topic.
     */
    private static ContractGuard routed(Set<String> here, Map<String, Set<String>> routes, Duration age) {
        var reports = new HashMap<String, Map<String, Integer>>();
        routes.keySet().forEach(id -> reports.put(id, Map.of()));
        var guard = new ContractGuard(Map.of(), here, INTERVAL);
        guard.observed(new ContractGuard.Snapshot(System.nanoTime() - age.toNanos(), reports, routes));
        return guard;
    }

    /** {@code guard} with its read made {@code age} old. */
    private static ContractGuard aged(ContractGuard guard, Duration age) {
        var s = guard.snapshot();
        guard.observed(new ContractGuard.Snapshot(s.readAt - age.toNanos(), s.reports, s.routes));
        return guard;
    }

    private static OutboxMessage message(String topic, String version) {
        var headers = version == null ? List.<OutboxHeader>of() : List.of(header(version));
        return new OutboxMessage(topic, null, "p".getBytes(UTF_8), headers);
    }

    private static OutboxHeader header(String version) {
        return new OutboxHeader(Contracts.HEADER, version.getBytes(US_ASCII));
    }
}
