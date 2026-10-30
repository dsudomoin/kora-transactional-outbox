package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OutboxRecordTest {

    @Test
    void shardDefaultsToUnknownAndSurvivesWithHeaders() {
        var built = new OutboxRecord(1, UUID.randomUUID(), "t", null, null, List.of(), Instant.EPOCH, 0, 0);
        var read = new OutboxRecord(1, UUID.randomUUID(), "t", null, null, List.of(), Instant.EPOCH, 0, 0, 7);

        assertThat(built.shard()).isEqualTo(-1);
        assertThat(read.withHeaders(List.of(new OutboxHeader("h", null))).shard()).isEqualTo(7);
    }

    @Test
    void contractVersionIsTheLastContractHeaderOrOne() {
        assertThat(withHeaders().contractVersion()).isEqualTo(1);
        assertThat(withHeaders(new OutboxHeader("h", null)).contractVersion()).isEqualTo(1);
        assertThat(withHeaders(contract("3")).contractVersion()).isEqualTo(3);
        assertThat(withHeaders(contract("x"), new OutboxHeader("h", null), contract("2")).contractVersion())
                .as("the last one wins").isEqualTo(2);
    }

    @Test
    void unreadableContractVersionWaits() {
        for (var value : Arrays.asList(null, "", "x", "0", "-1", " 2", "99999999999")) {
            var record = withHeaders(contract("2"), contract(value));

            assertThatThrownBy(record::contractVersion).as(value)
                    .isInstanceOfSatisfying(OutboxWaitException.class,
                            e -> assertThat(e.kind()).isEqualTo(Outcome.Waiting.Kind.CONTRACT))
                    .hasMessage("invalid outbox-contract header: " + value);
        }
    }

    private static OutboxHeader contract(@Nullable String value) {
        return new OutboxHeader("outbox-contract", value == null ? null : value.getBytes(US_ASCII));
    }

    private static OutboxRecord withHeaders(OutboxHeader... headers) {
        return new OutboxRecord(1, UUID.randomUUID(), "t", null, null, List.of(headers), Instant.EPOCH, 0, 0);
    }
}
