package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;
import java.util.ArrayList;
import java.util.List;

/**
 * The observation of one {@code send} or {@code sendAll} call. Its carrier collects the trace headers the
 * application's propagating sender handler injects; the call's messages store them unless they carry their own.
 */
public final class OutboxEnqueueContext extends SenderContext<List<OutboxHeader>> {

    OutboxEnqueueContext() {
        super((carrier, key, value) -> {
            if (carrier != null) {
                carrier.add(new OutboxHeader(key, value.getBytes(UTF_8)));
            }
        }, Kind.PRODUCER);
        setCarrier(new ArrayList<>(2));
    }
}
