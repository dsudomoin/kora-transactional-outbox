package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.internal.RecordScope;
import java.util.concurrent.Callable;

/** Outside a relay call, a record's call runs in a Kora context carrying its trace, as within one. */
public final class KoraRecordScope implements RecordScope.Fallback {

    @Override
    public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
        return KoraOutboxTelemetry.inKoraContext(record, call);
    }
}
