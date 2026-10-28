package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
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
}
