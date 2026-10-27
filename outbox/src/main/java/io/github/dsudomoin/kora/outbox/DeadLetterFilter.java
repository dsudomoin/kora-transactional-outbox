package io.github.dsudomoin.kora.outbox;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Which dead letters an {@link OutboxAdmin} call means: a letter matches when every non-null field does.
 * {@code errorContains} is a plain substring of the error text, never a pattern ({@code %} and {@code _} match
 * themselves); {@code failedFrom} is inclusive, {@code failedTo} exclusive. A blank {@code topic} or
 * {@code errorContains} counts as absent in the reads; the bulk redrive and discard refuse it, as dropped it would
 * widen the call to every letter the rest of the filter matches. All null ({@link #any()}) matches every letter.
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

    /** True when it matches every letter (a blank string counts as absent): the bulk redrive and discard refuse it. */
    public boolean isEmpty() {
        return blank(topic) && key == null && blank(errorContains) && failedFrom == null && failedTo == null
                && status == null;
    }

    private static boolean blank(@Nullable String s) {
        return s == null || s.isBlank();
    }

    /** The key in hex, so an audit line shows it. */
    @Override
    public String toString() {
        return "DeadLetterFilter[topic=" + topic + ", key=" + (key == null ? null : HexFormat.of().formatHex(key))
                + ", errorContains=" + errorContains + ", failedFrom=" + failedFrom + ", failedTo=" + failedTo
                + ", status=" + status + "]";
    }
}
