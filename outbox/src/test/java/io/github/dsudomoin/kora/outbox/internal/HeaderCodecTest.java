package io.github.dsudomoin.kora.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxHeader;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class HeaderCodecTest {

    @Test
    void roundTripPreservesOrderDuplicatesAndNullValues() {
        var headers = List.of(
                new OutboxHeader("b", "1".getBytes(UTF_8)),
                new OutboxHeader("a", null),
                new OutboxHeader("b", new byte[0]),
                new OutboxHeader("b", new byte[] {0, -1, 127, -128}));

        assertThat(HeaderCodec.decode(HeaderCodec.encode(headers))).containsExactlyElementsOf(headers);
    }

    @Test
    void emptyEncodesToNull() {
        assertThat(HeaderCodec.encode(List.of())).isNull();
    }

    @Test
    void nullDecodesToEmpty() {
        assertThat(HeaderCodec.decode(null)).isEmpty();
    }

    @Test
    void nonAsciiNames() {
        var headers = List.of(new OutboxHeader("заголовок-ü-日本", "значение".getBytes(UTF_8)));

        assertThat(HeaderCodec.decode(HeaderCodec.encode(headers))).containsExactlyElementsOf(headers);
    }

    @Test
    void headerCountAboveU16Rejected() {
        var header = new OutboxHeader("h", null);
        assertThat(HeaderCodec.decode(HeaderCodec.encode(Collections.nCopies(65535, header)))).hasSize(65535);
        assertThatThrownBy(() -> HeaderCodec.encode(Collections.nCopies(65536, header)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nameLongerThanU16BytesRejected() {
        var longest = List.of(new OutboxHeader("a".repeat(65535), null));
        assertThat(HeaderCodec.decode(HeaderCodec.encode(longest))).containsExactlyElementsOf(longest);
        assertThatThrownBy(() -> HeaderCodec.encode(List.of(new OutboxHeader("a".repeat(65536), null))))
                .isInstanceOf(IllegalArgumentException.class);
        // the limit counts UTF-8 bytes: 32768 two-byte chars = 65536 bytes
        assertThatThrownBy(() -> HeaderCodec.encode(List.of(new OutboxHeader("я".repeat(32768), null))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeValueLengthOtherThanMinusOneRejected() {
        byte[] corrupt = ByteBuffer.allocate(2 + 2 + 1 + 4)
                .putShort((short) 1)
                .putShort((short) 1)
                .put((byte) 'a')
                .putInt(-2)
                .array();

        assertThatThrownBy(() -> HeaderCodec.decode(corrupt)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void valueLengthBeyondTheStoredBytesIsRejectedWithoutAllocatingIt() {
        for (int length : new int[] {Integer.MAX_VALUE, 1 << 30, 2}) {
            var corrupt = ByteBuffer.allocate(2 + 2 + 1 + 4 + 1)
                    .putShort((short) 1).putShort((short) 1).put((byte) 'a').putInt(length).put((byte) 0).array();

            assertThatThrownBy(() -> HeaderCodec.decode(corrupt)).as("length %d", length)
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("corrupt headers");
        }
    }
}
