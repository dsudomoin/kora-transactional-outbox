package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PgSnapshotTest {

    @Test
    void parsesEmptyXip() {
        var snapshot = PgSnapshot.parse("736:736:");
        assertThat(snapshot.xmin()).isEqualTo(736);
        assertThat(snapshot.xmax()).isEqualTo(736);
        assertThat(snapshot.xip()).isEmpty();
    }

    @Test
    void parsesXipList() {
        var snapshot = PgSnapshot.parse("100:210:150,160");
        assertThat(snapshot.xmin()).isEqualTo(100);
        assertThat(snapshot.xmax()).isEqualTo(210);
        assertThat(snapshot.xip()).containsExactly(150, 160);
    }

    @Test
    void textRoundTrip() {
        for (String text : new String[] {"736:736:", "100:210:150,160", "100:210:150"}) {
            assertThat(PgSnapshot.parse(text).text()).isEqualTo(text);
        }
    }
}
