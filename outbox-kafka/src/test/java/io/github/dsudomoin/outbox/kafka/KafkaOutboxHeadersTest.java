package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class KafkaOutboxHeadersTest {

    @Test
    void messageIdIsTheLastHeaderOrNull() {
        var id = UUID.randomUUID();

        assertThat(KafkaOutboxHeaders.messageId(new RecordHeaders())).isNull();
        assertThat(KafkaOutboxHeaders.messageId(new RecordHeaders().add(KafkaOutboxHeaders.MESSAGE_ID, null))).isNull();
        assertThat(KafkaOutboxHeaders.messageId(
                new RecordHeaders().add(KafkaOutboxHeaders.MESSAGE_ID, "not a uuid".getBytes(UTF_8)))).isNull();
        // the sender appends its header after the message's own ones
        assertThat(KafkaOutboxHeaders.messageId(new RecordHeaders()
                .add(KafkaOutboxHeaders.MESSAGE_ID, UUID.randomUUID().toString().getBytes(UTF_8))
                .add(KafkaOutboxHeaders.MESSAGE_ID, id.toString().getBytes(UTF_8)))).isEqualTo(id);
    }
}
