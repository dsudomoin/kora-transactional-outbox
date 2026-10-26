package io.github.dsudomoin.kora.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffTest {

    private static final Duration INITIAL = Duration.ofSeconds(1);
    private static final Duration MAX = Duration.ofMinutes(5);

    @Test
    void doubles() {
        assertThat(Backoff.delay(0, INITIAL, MAX)).isEqualTo(Duration.ofSeconds(1));
        assertThat(Backoff.delay(1, INITIAL, MAX)).isEqualTo(Duration.ofSeconds(2));
        assertThat(Backoff.delay(3, INITIAL, MAX)).isEqualTo(Duration.ofSeconds(8));
    }

    @Test
    void capped() {
        assertThat(Backoff.delay(20, INITIAL, MAX)).isEqualTo(MAX);
        assertThat(Backoff.delay(0, Duration.ofMinutes(10), MAX)).isEqualTo(MAX);
    }

    @Test
    void negativeFailuresCountAsNone() {
        assertThat(Backoff.delay(-1, INITIAL, MAX)).isEqualTo(INITIAL);
        assertThat(Backoff.delay(Integer.MIN_VALUE, INITIAL, MAX)).isEqualTo(INITIAL);
    }

    @Test
    void noOverflowAt1000Failures() {
        assertThat(Backoff.delay(1000, INITIAL, MAX)).isEqualTo(MAX);
        assertThat(Backoff.delay(Integer.MAX_VALUE, INITIAL, MAX)).isEqualTo(MAX);
        assertThat(Backoff.delay(63, Duration.ofNanos(1), MAX)).isEqualTo(MAX);
    }
}
