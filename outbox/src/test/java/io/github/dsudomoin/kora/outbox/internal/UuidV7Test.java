package io.github.dsudomoin.kora.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UuidV7Test {

    @Test
    void versionAndVariantBits() {
        UUID uuid = UuidV7.next();
        assertThat(uuid.version()).isEqualTo(7);
        assertThat(uuid.variant()).isEqualTo(2);
        assertThat(UuidV7.isV7(uuid)).isTrue();
        assertThat(UuidV7.isV7(UUID.randomUUID())).isFalse();
    }

    @Test
    void timestampWithin5msOfNow() {
        UUID uuid = UuidV7.next();
        assertThat(UuidV7.timestampMillis(uuid)).isCloseTo(System.currentTimeMillis(), within(5L));
    }

    @Test
    void monotonicWithinSameMillisecondIsNotRequiredButUnique() {
        var ids = new HashSet<UUID>();
        for (int i = 0; i < 100_000; i++) {
            ids.add(UuidV7.next());
        }
        assertThat(ids).hasSize(100_000);
    }

    @Test
    void lowerBoundSortsBeforeEveryIdOfThatMillisecond() {
        for (int i = 0; i < 1000; i++) {
            UUID id = UuidV7.next();
            long ms = UuidV7.timestampMillis(id);
            assertThat(compareAsPostgres(UuidV7.lowerBound(ms), id)).isNegative();
            assertThat(compareAsPostgres(id, UuidV7.lowerBound(ms + 1))).isNegative();
        }
        // smallest possible v7 id of a millisecond: version and variant set, every random bit zero
        long ms = System.currentTimeMillis();
        UUID smallest = new UUID((ms << 16) | 0x7000L, 0x8000_0000_0000_0000L);
        assertThat(compareAsPostgres(UuidV7.lowerBound(ms), smallest)).isNegative();
        assertThat(UuidV7.timestampMillis(UuidV7.lowerBound(ms))).isEqualTo(ms);
    }

    // Postgres orders uuid values bytewise (unsigned); UUID.compareTo compares signed longs.
    private static int compareAsPostgres(UUID a, UUID b) {
        return Arrays.compareUnsigned(bytes(a), bytes(b));
    }

    private static byte[] bytes(UUID uuid) {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
