package io.github.dsudomoin.kora.outbox.internal;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Postgres {@code pg_snapshot} in its text form {@code xmin:xmax:xip1,xip2} (xip list may be empty). */
public record PgSnapshot(long xmin, long xmax, long[] xip) {

    public static PgSnapshot parse(String text) {
        String[] parts = text.split(":", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("not a pg_snapshot: " + text);
        }
        long[] xip = parts[2].isEmpty()
                ? new long[0]
                : Arrays.stream(parts[2].split(",")).mapToLong(Long::parseLong).toArray();
        return new PgSnapshot(Long.parseLong(parts[0]), Long.parseLong(parts[1]), xip);
    }

    public String text() {
        return xmin + ":" + xmax + ":" + Arrays.stream(xip).mapToObj(Long::toString).collect(Collectors.joining(","));
    }
}
