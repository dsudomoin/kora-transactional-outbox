package io.github.dsudomoin.kora.outbox.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.Outcome;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RecordingHandlerTest {

    private final OutboxRecord a = new OutboxRecord(1, UUID.randomUUID(), "a", null, null, List.of(), Instant.now(), 0);
    private final OutboxRecord b = new OutboxRecord(2, UUID.randomUUID(), "b", null, null, List.of(), Instant.now(), 0);

    @Test
    void scriptsFailuresBatchThrowsAndDelay() {
        var handler = new RecordingHandler();
        var once = new IllegalStateException("once");
        var always = new IllegalStateException("always");
        handler.failNext(r -> r == a, () -> once);
        handler.failAlways(r -> r == b, () -> always);
        handler.throwFromBatch(() -> new RuntimeException("first call"));
        handler.throwFromBatch(() -> new AssertionError("second call"));

        assertThatThrownBy(() -> handler.handleBatch(List.of(a))).hasMessage("first call");
        assertThatThrownBy(() -> handler.handleBatch(List.of(a))).isInstanceOf(AssertionError.class);
        assertThat(handler.handleBatch(List.of(a, b))).containsExactly(Outcome.retry(once), Outcome.retry(always));
        assertThat(handler.handleBatch(List.of(a, b))).containsExactly(Outcome.delivered(), Outcome.retry(always));
        assertThat(handler.delivered()).containsExactly(a);
        assertThat(handler.batches()).hasSize(2);

        handler.delay(Duration.ofMillis(200));
        long start = System.nanoTime();
        handler.handleBatch(List.of(a));
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(200));

        handler.reset();
        assertThat(handler.delivered()).isEmpty();
        assertThat(handler.batches()).isEmpty();
        assertThat(handler.handleBatch(List.of(b))).containsExactly(Outcome.delivered());
    }
}
