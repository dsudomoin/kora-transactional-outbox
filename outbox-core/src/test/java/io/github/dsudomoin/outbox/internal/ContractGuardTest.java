package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxIncompatibleContractException;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ContractGuardTest {

    private static final Duration INTERVAL = Duration.ofSeconds(30);
    private static final TableNames NAMES = TableNames.of("outbox");

    private final List<String> rejected = new ArrayList<>();
    private final OutboxTelemetry telemetry = new OutboxTelemetry() {
        @Override
        public void contractRejected(String topic, String type, int version) {
            rejected.add(topic + " " + type + " v" + version);
        }
    };

    @Test
    void version1IsNeverChecked() {
        var guard = new ContractGuard(Map.of("orders", 1), INTERVAL); // never read, as if stale

        assertThatCode(() -> {
            guard.require(NAMES, message("orders", null), "Created", telemetry);
            guard.require(NAMES, message("orders", "1"), "Created", telemetry);
        }).doesNotThrowAnyException();
        assertThat(rejected).isEmpty();
    }

    @Test
    void aTopicNoRelayReportsAndNoRouteHereClaimsIsNotChecked() {
        var guard = guard(Map.of("audit", 1), Map.of("a", Map.of("orders", 1)));

        assertThatCode(() -> guard.require(NAMES, message("kafka-bound", "2"), "Created", telemetry))
                .as("forwarded or handled raw").doesNotThrowAnyException();
    }

    @Test
    void everyRelayAtTheVersionOrAboveAllowsIt() {
        var guard = guard(Map.of(), Map.of("a", Map.of("orders", 2), "b", Map.of("orders", 3)));

        assertThatCode(() -> guard.require(NAMES, message("orders", "2"), "Created", telemetry))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> guard.require(NAMES, message("orders", "3"), "Created", telemetry))
                .isInstanceOf(OutboxIncompatibleContractException.class)
                .hasMessage("outbox contract of topic orders: Created writes v3, but 1 of 2 live relay instances read"
                        + " at most v2 (a); deploy the release that reads v3 everywhere first");
    }

    @Test
    void oneRelayBelowRefusesIt() {
        var guard = guard(Map.of("orders", 2), Map.of("a", Map.of("orders", 2), "b", Map.of("orders", 1)));

        assertThatThrownBy(() -> guard.require(NAMES, message("orders", "2"), "Created", telemetry))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e.minSupported()).isEqualTo(1);
                    assertThat(e.incompatibleInstances()).containsExactly("b");
                });
    }

    @Test
    void aRelayWithoutAReportOrWithoutTheTopicReadsNoneOfIt() {
        var reports = new HashMap<String, Map<String, Integer>>();
        reports.put("a", Map.of("orders", 2));
        reports.put("unknown", null);
        reports.put("other", Map.of("audit", 5));
        var guard = guard(Map.of(), reports);

        assertThatThrownBy(() -> guard.require(NAMES, message("orders", "2"), "Created", telemetry))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e.minSupported()).isZero();
                    assertThat(e.incompatibleInstances()).containsExactly("other", "unknown");
                    assertThat(e).hasMessageContaining("2 of 3 live relay instances do not read it (other, unknown)");
                });
    }

    @Test
    void aTopicOnlyARouteHereClaimsIsCheckedAgainstEveryRelay() {
        var claimed = guard(Map.of("orders", 2), Map.of("a", Map.of("audit", 1)));
        var noRelay = guard(Map.of("orders", 1), Map.of());

        assertThatThrownBy(() -> claimed.require(NAMES, message("orders", "2"), "Created", telemetry))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class,
                        e -> assertThat(e.incompatibleInstances()).containsExactly("a"));
        assertThatCode(() -> noRelay.require(NAMES, message("orders", "2"), "Created", telemetry))
                .as("no live relay instance: nothing refuses").doesNotThrowAnyException();
    }

    @Test
    void aReadOlderThanThreeIntervalsRefusesEveryVersionAbove1() {
        var guard = new ContractGuard(Map.of(), INTERVAL);
        guard.observed(new ContractGuard.Snapshot(System.nanoTime() - INTERVAL.multipliedBy(3).toNanos() - 1,
                Map.of("a", Map.of("orders", 5))));
        var neverRead = new ContractGuard(Map.of(), INTERVAL);

        for (var g : List.of(guard, neverRead)) {
            assertThatThrownBy(() -> g.require(NAMES, message("kafka-bound", "2"), "Created", telemetry))
                    .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                        assertThat(e).hasMessage("outbox contract of topic kafka-bound: Created writes v2, but this"
                                + " instance has not read the live relay instances' contracts for 90 s (its"
                                + " heartbeat fails?); refused until it reads them again");
                        assertThat(e.minSupported()).isZero();
                        assertThat(e.incompatibleInstances()).isEmpty();
                    });
        }
        assertThatCode(() -> guard.require(NAMES, message("orders", null), "Created", telemetry))
                .doesNotThrowAnyException();
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

        assertThatThrownBy(() -> guard.require(NAMES, message("orders", "2"), "com.acme.OrderCreated", telemetry))
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

        assertThatThrownBy(() -> guard.require(NAMES, message("orders", "3"), "Created", telemetry))
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

        assertThatThrownBy(() -> guard.require(NAMES, message("kafka-bound", "2"), "Created", telemetry))
                .isInstanceOfSatisfying(OutboxIncompatibleContractException.class, e -> {
                    assertThat(e).hasMessage("outbox contract of topic kafka-bound: Created writes v2, but 1 of 3 live"
                            + " relay instances do not read it (unknown); deploy the release that reads v2 everywhere"
                            + " first");
                    assertThat(e.minSupported()).isZero();
                    assertThat(e.incompatibleInstances()).containsExactly("unknown");
                });
        assertThatCode(() -> guard.require(NAMES, message("kafka-bound", null), "Created", telemetry))
                .doesNotThrowAnyException();
    }

    @Test
    void theLastHeaderCounts() {
        var guard = guard(Map.of(), Map.of("a", Map.of("orders", 1)));
        var headers = List.of(header("2"), header("1"));

        assertThatCode(() -> guard.require(NAMES, new OutboxMessage("orders", null, null, headers), "?", telemetry))
                .doesNotThrowAnyException();
    }

    /** The relay would make such a record wait for good: refused at once, whatever the reports, and not counted. */
    @Test
    void aHeaderThatGivesNoVersionIsRefused() {
        var guard = new ContractGuard(Map.of(), INTERVAL); // never read
        for (var value : Arrays.asList(null, "", "x", "0", "-1", " 2", "99999999999")) {
            var headers = List.of(header("2"), new OutboxHeader(Contracts.HEADER,
                    value == null ? null : value.getBytes(US_ASCII)));

            assertThatThrownBy(() -> guard.require(NAMES, new OutboxMessage("orders", null, null, headers), "?",
                    telemetry)).as(value).isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("invalid outbox-contract header of a message to topic orders: " + value + "; it must"
                            + " be a positive decimal int, or absent for version 1");
        }
        assertThat(rejected).isEmpty();
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
                assertThatThrownBy(() -> guard.require(NAMES, message("orders", "2"), "Created", telemetry))
                        .isInstanceOf(OutboxIncompatibleContractException.class);
            }
            var text = err.text();
            assertThat(text.split("refused a send", -1)).as("one WARN within a minute").hasSize(2);
            assertThat(text).contains("outbox outbox refused a send: outbox contract of topic orders")
                    .contains("the instances: [a, b, c]").contains("outbox.messages.contract_rejected");
        }
        assertThat(rejected).containsExactly("orders Created v2", "orders Created v2");
    }

    private static ContractGuard guard(Map<String, Integer> contracts, Map<String, Map<String, Integer>> reports) {
        var guard = new ContractGuard(contracts, INTERVAL);
        guard.observed(new ContractGuard.Snapshot(System.nanoTime(), reports));
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
