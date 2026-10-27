package io.github.dsudomoin.kora.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class TailConversionWindowTest {

    @Test
    void aWindowWithinADayIncludesItsStartButNotItsEnd() {
        var window = XidPartitions.Window.parse("03:00-05:00");

        assertThat(window.contains(LocalTime.of(3, 0))).isTrue();
        assertThat(window.contains(LocalTime.of(4, 59, 59))).isTrue();
        assertThat(window.contains(LocalTime.of(5, 0))).isFalse();
        assertThat(window.contains(LocalTime.of(2, 59, 59))).isFalse();
        assertThat(window.contains(LocalTime.of(15, 0))).isFalse();
    }

    @Test
    void aWindowMayCrossMidnight() {
        var window = XidPartitions.Window.parse("23:00-01:00");

        assertThat(window.contains(LocalTime.of(23, 0))).isTrue();
        assertThat(window.contains(LocalTime.MIDNIGHT)).isTrue();
        assertThat(window.contains(LocalTime.of(0, 59))).isTrue();
        assertThat(window.contains(LocalTime.of(1, 0))).isFalse();
        assertThat(window.contains(LocalTime.of(22, 59))).isFalse();
        assertThat(window.contains(LocalTime.NOON)).isFalse();
    }

    @Test
    void anEmptyWindowIsNone() {
        assertThat(XidPartitions.Window.parse("")).isNull();
    }

    @Test
    void badFormatsAreRejected() {
        for (var bad : List.of("3:00-05:00", "03:00-5:00", "24:00-01:00", "03:60-04:00", "03:00", "03:00 - 05:00",
                "03:00-03:00", "03.00-05.00", " ", "night")) {
            assertThatThrownBy(() -> XidPartitions.Window.parse(bad)).as(bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("HH:mm-HH:mm");
        }
    }
}
