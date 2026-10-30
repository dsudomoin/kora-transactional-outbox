package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.US_ASCII;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxContract;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** The {@code outbox-contract} header: the {@link OutboxContract} version of the type a payload was written as. */
public final class Contracts {

    public static final String HEADER = "outbox-contract";

    private Contracts() {}

    /** {@code type}'s {@link OutboxContract} version, 1 without the annotation. */
    public static int version(Class<?> type) {
        var contract = type.getAnnotation(OutboxContract.class);
        if (contract == null) {
            return 1;
        }
        if (contract.value() < 1) {
            throw new IllegalArgumentException(
                    "@OutboxContract of " + type.getName() + " must be at least 1: " + contract.value());
        }
        return contract.value();
    }

    /**
     * The version the last {@code outbox-contract} header of {@code headers} gives: 1 without one, 0 when its value is
     * null, not a decimal int or not positive.
     */
    public static int version(List<OutboxHeader> headers) {
        var header = last(headers);
        return header == null ? 1 : parse(header.value() == null ? null : new String(header.value(), US_ASCII));
    }

    /** The last {@code outbox-contract} header's value as text, for a message; null without one or for null. */
    @Nullable
    public static String value(List<OutboxHeader> headers) {
        var header = last(headers);
        return header == null || header.value() == null ? null : new String(header.value(), US_ASCII);
    }

    /** {@code value} as a version: a positive decimal int, else 0. */
    static int parse(@Nullable String value) {
        try {
            return Math.max(Integer.parseInt(value), 0);
        } catch (NumberFormatException e) { // a null value too
            return 0;
        }
    }

    @Nullable
    private static OutboxHeader last(List<OutboxHeader> headers) {
        for (int i = headers.size() - 1; i >= 0; i--) {
            if (headers.get(i).name().equals(HEADER)) {
                return headers.get(i);
            }
        }
        return null;
    }

    /** {@code headers} without their {@code outbox-contract} headers, plus one for {@code version} if above 1. */
    public static List<OutboxHeader> stamp(List<OutboxHeader> headers, int version) {
        var stamped = new ArrayList<>(headers);
        stamped.removeIf(h -> h.name().equals(HEADER));
        if (version > 1) {
            stamped.add(new OutboxHeader(HEADER, Integer.toString(version).getBytes(US_ASCII)));
        }
        return stamped;
    }

    /** {@code outbox.send(message)}, naming {@code type}, the class the payload was written as, to its writer guard. */
    public static UUID send(Outbox outbox, OutboxMessage message, String type) {
        return outbox instanceof PgOutbox pg ? pg.send(message, type) : outbox.send(message);
    }
}
