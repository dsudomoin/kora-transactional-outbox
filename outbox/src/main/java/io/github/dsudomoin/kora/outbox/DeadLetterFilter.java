package io.github.dsudomoin.kora.outbox;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Which dead letters an {@link OutboxAdmin} call means: a letter matches when every non-null field does.
 * {@code errorContains} is a plain substring of the error text, never a pattern ({@code %} and {@code _} match
 * themselves); {@code failedFrom} is inclusive, {@code failedTo} exclusive. All null ({@link #any()}) matches every
 * letter.
 */
public record DeadLetterFilter(
        @Nullable String topic,
        @Nullable byte[] key,
        @Nullable String errorContains,
        @Nullable Instant failedFrom,
        @Nullable Instant failedTo,
        @Nullable DeadLetter.Status status) {

    /** Every letter. */
    public static DeadLetterFilter any() {
        return new DeadLetterFilter(null, null, null, null, null, null);
    }

    /** True when it matches every letter: the bulk redrive and discard refuse it. */
    public boolean isEmpty() {
        return topic == null && key == null && errorContains == null && failedFrom == null && failedTo == null
                && status == null;
    }

    /** The key in hex, so an audit line shows it. */
    @Override
    public String toString() {
        return "DeadLetterFilter[topic=" + topic + ", key=" + (key == null ? null : HexFormat.of().formatHex(key))
                + ", errorContains=" + errorContains + ", failedFrom=" + failedFrom + ", failedTo=" + failedTo
                + ", status=" + status + "]";
    }
}
