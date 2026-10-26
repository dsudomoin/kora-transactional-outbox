package io.github.dsudomoin.kora.outbox.internal;

import java.security.SecureRandom;
import java.util.UUID;

/** RFC 9562 UUIDv7: 48-bit Unix millis, version 7, variant 2, 74 random bits. */
public final class UuidV7 {

    // message ids are dedup ids: a collision silently drops a message, so use the CSPRNG like UUID.randomUUID
    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {}

    public static UUID next() {
        long msb = (System.currentTimeMillis() << 16) | 0x7000L | (RANDOM.nextLong() & 0x0FFFL);
        long lsb = (RANDOM.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L;
        return new UUID(msb, lsb);
    }

    public static boolean isV7(UUID uuid) {
        return uuid.version() == 7 && uuid.variant() == 2;
    }

    public static long timestampMillis(UUID uuid) {
        return uuid.getMostSignificantBits() >>> 16;
    }

    /** Sorts (bytewise, as Postgres does) before every v7 id of {@code epochMillis} and after those of earlier ms. */
    public static UUID lowerBound(long epochMillis) {
        return new UUID(epochMillis << 16, 0L);
    }
}
