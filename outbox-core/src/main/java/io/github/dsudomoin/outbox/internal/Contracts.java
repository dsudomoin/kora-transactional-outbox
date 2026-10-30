package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.US_ASCII;

import io.github.dsudomoin.outbox.OutboxContract;
import io.github.dsudomoin.outbox.OutboxHeader;
import java.util.ArrayList;
import java.util.List;

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

    /** {@code headers} without their {@code outbox-contract} headers, plus one for {@code version} if above 1. */
    public static List<OutboxHeader> stamp(List<OutboxHeader> headers, int version) {
        var stamped = new ArrayList<>(headers);
        stamped.removeIf(h -> h.name().equals(HEADER));
        if (version > 1) {
            stamped.add(new OutboxHeader(HEADER, Integer.toString(version).getBytes(US_ASCII)));
        }
        return stamped;
    }
}
