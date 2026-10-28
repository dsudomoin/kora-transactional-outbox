package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/** Kafka-compatible header: order is kept and duplicate names are allowed. */
public record OutboxHeader(String name, @Nullable byte[] value) {

    public OutboxHeader {
        Objects.requireNonNull(name, "name");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof OutboxHeader h && name.equals(h.name) && Arrays.equals(value, h.value);
    }

    @Override
    public int hashCode() {
        return 31 * name.hashCode() + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "OutboxHeader[name=" + name + ", value=" + Arrays.toString(value) + "]";
    }
}
