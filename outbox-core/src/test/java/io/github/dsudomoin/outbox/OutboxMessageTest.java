package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OutboxMessageTest {

    @Test
    void topicLongerThan249Rejected() {
        assertThatCode(() -> OutboxMessage.of("t".repeat(249), null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> OutboxMessage.of("t".repeat(250), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keyLongerThan1024BytesRejected() {
        assertThatCode(() -> OutboxMessage.of("t", new byte[1024], null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> OutboxMessage.of("t", new byte[1025], null))
                .isInstanceOf(IllegalArgumentException.class);
        // the limit counts UTF-8 bytes, not chars: 513 two-byte chars = 1026 bytes
        assertThatThrownBy(() -> OutboxMessage.of("t", "я".repeat(513), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void blankTopicRejected() {
        assertThatThrownBy(() -> OutboxMessage.of("", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OutboxMessage.of(" \t", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OutboxMessage.of(null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unkeyedFactoryHasNullKey() {
        byte[] payload = "p".getBytes(UTF_8);
        var message = OutboxMessage.of("t", payload);
        assertThat(message.topic()).isEqualTo("t");
        assertThat(message.key()).isNull();
        assertThat(message.payload()).isSameAs(payload);
        assertThat(message.headers()).isEmpty();
    }

    @Test
    void stringKeyIsUtf8() {
        assertThat(OutboxMessage.of("t", "ключ-1", null).key()).isEqualTo("ключ-1".getBytes(UTF_8));
        assertThat(OutboxMessage.of("t", (String) null, null).key()).isNull();
    }

    @Test
    void headersAreCopiedAndOrdered() {
        var b1 = new OutboxHeader("b", "1".getBytes(UTF_8));
        var a2 = new OutboxHeader("a", "2".getBytes(UTF_8));
        var b3 = new OutboxHeader("b", null);
        var source = new ArrayList<>(List.of(b1, a2, b3));

        var message = new OutboxMessage("t", null, null, source);
        source.clear();

        assertThat(message.headers()).containsExactly(b1, a2, b3);
        assertThatThrownBy(() -> message.headers().add(b1)).isInstanceOf(UnsupportedOperationException.class);

        var withC = message.withHeader("c", "4".getBytes(UTF_8));
        assertThat(withC.headers()).containsExactly(b1, a2, b3, new OutboxHeader("c", "4".getBytes(UTF_8)));
        assertThat(message.headers()).containsExactly(b1, a2, b3);
    }

    @Test
    void nullPayloadAllowed() {
        var message = OutboxMessage.of("t", "k", null);
        assertThat(message.payload()).isNull();
        assertThat(message.headers()).isEmpty();
    }
}
