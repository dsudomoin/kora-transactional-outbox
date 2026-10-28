package io.github.dsudomoin.outbox.internal;

import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/** (topic, key) hash and shard. Frozen: rows already written were sharded with it (HashingTest pins it). */
public final class Hashing {

    private Hashing() {}

    public static int keyHash(String topic, @Nullable byte[] key) {
        return fmix32(31 * topic.hashCode() + Arrays.hashCode(key));
    }

    public static int shard(int keyHash, int shards) {
        return Math.floorMod(keyHash, shards);
    }

    public static int randomShard(int shards) {
        return ThreadLocalRandom.current().nextInt(shards);
    }

    // murmur3 finalizer
    private static int fmix32(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}
