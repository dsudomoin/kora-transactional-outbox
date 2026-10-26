package io.github.dsudomoin.kora.outbox.admin;

import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiDelegate;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiResponses.GetStatusApiResponse;
import io.github.dsudomoin.kora.outbox.admin.model.Status;
import io.github.dsudomoin.kora.outbox.internal.Maintenance;

/** The admin API over the app's outbox. */
public final class OutboxAdminDelegate implements OutboxAdminApiDelegate {

    private final Maintenance maintenance;

    public OutboxAdminDelegate(Maintenance maintenance) {
        this.maintenance = maintenance;
    }

    @Override
    public GetStatusApiResponse getStatus() {
        return new GetStatusApiResponse.GetStatus200ApiResponse(new Status(maintenance.halted()));
    }
}
