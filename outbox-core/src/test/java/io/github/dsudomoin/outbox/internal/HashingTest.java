package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HashingTest {

    // Frozen: shards of rows already written depend on these values. Never change them.
    @Test
    void goldenValues() {
        int keyed = Hashing.keyHash("orders", "user-1".getBytes(UTF_8));
        assertThat(keyed).isEqualTo(-1529465446);
        assertThat(Hashing.keyHash("orders", null)).isEqualTo(1244240942);
        assertThat(Hashing.shard(keyed, 32)).isEqualTo(26);
    }

    @Test
    void sameTopicKeySameShard() {
        int first = Hashing.keyHash("orders", "user-1".getBytes(UTF_8));
        int second = Hashing.keyHash("orders", "user-1".getBytes(UTF_8));
        assertThat(second).isEqualTo(first);
        assertThat(Hashing.shard(second, 32)).isEqualTo(Hashing.shard(first, 32));
    }

    @Test
    void differentTopicSameKeyUsuallyDifferentHash() {
        int differ = 0;
        for (int i = 0; i < 1000; i++) {
            byte[] key = ("user-" + i).getBytes(UTF_8);
            if (Hashing.keyHash("orders", key) != Hashing.keyHash("payments", key)) {
                differ++;
            }
        }
        assertThat(differ).isGreaterThanOrEqualTo(990);
    }

    @Test
    void shardDistributionIsEven() {
        int shards = 32;
        int keys = 100_000;
        int[] counts = new int[shards];
        for (int i = 0; i < keys; i++) {
            counts[Hashing.shard(Hashing.keyHash("orders", ("k" + i).getBytes(UTF_8)), shards)]++;
        }
        double mean = (double) keys / shards;
        for (int count : counts) {
            assertThat((double) count).isBetween(mean * 0.9, mean * 1.1);
        }
    }

    @Test
    void randomShardStaysInRange() {
        for (int i = 0; i < 1000; i++) {
            assertThat(Hashing.randomShard(32)).isBetween(0, 31);
        }
    }
}
