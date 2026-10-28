package io.github.dsudomoin.outbox.admin.kora;

import io.github.dsudomoin.outbox.InternalApi;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiControllerModule;
import io.github.dsudomoin.outbox.admin.kora.api.OutboxAdminApiDelegate;
import io.github.dsudomoin.outbox.admin.kora.model.DeadLetterStatus;
import io.github.dsudomoin.outbox.admin.kora.model.Filter;
import io.github.dsudomoin.outbox.admin.kora.model.ModelError;
import io.github.dsudomoin.outbox.admin.kora.model.OrphanKind;
import io.github.dsudomoin.outbox.admin.kora.model.Selection;
import java.time.OffsetDateTime;
import java.util.List;
import ru.tinkoff.kora.http.server.common.handler.BlockingRequestExecutor;
import ru.tinkoff.kora.http.server.common.handler.StringParameterReader;
import ru.tinkoff.kora.json.common.JsonReader;
import ru.tinkoff.kora.json.common.JsonWriter;

/**
 * The admin HTTP API on the app's public HTTP server, routes under {@code /kora-outbox/admin/v1}; contract at
 * {@code openapi/kora-outbox-admin.yaml} on the classpath. Needs {@code OutboxModule}, an HTTP server module, a JSON
 * module and one {@link OutboxAdminAuthorizer} component.
 * <pre>{@code
 * @KoraApp
 * interface App extends HoconConfigModule, JdbcDatabaseModule, OutboxModule, UndertowHttpServerModule, JsonModule,
 *         OutboxAdminHttpModule {}
 * }</pre>
 */
public interface OutboxAdminHttpModule extends OutboxAdminApiControllerModule {

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default OutboxAdminInterceptor outboxAdminInterceptor(OutboxAdminAuthorizer authorizer,
            BlockingRequestExecutor executor, JsonWriter<ModelError> errorWriter) {
        return new OutboxAdminInterceptor(authorizer, executor, errorWriter);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default OutboxAdminApiDelegate outboxAdminDelegate(OutboxAdmin admin) {
        return new OutboxAdminDelegate(admin);
    }

    // The generator's enum readers and writers (by the contract's values, "pending", not the constant's name) are
    // @Component classes, which an app's graph never finds in a library jar: without these it falls back to
    // Enum::name for the query parameter and generates its own JSON codecs, with a warning, in every app build.

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default StringParameterReader<DeadLetterStatus> outboxAdminDeadLetterStatusParameterReader() {
        return new DeadLetterStatus.StringParameterReader();
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default JsonReader<DeadLetterStatus> outboxAdminDeadLetterStatusJsonReader(JsonReader<String> strings) {
        return new DeadLetterStatus.JsonReader(strings);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default JsonWriter<DeadLetterStatus> outboxAdminDeadLetterStatusJsonWriter(JsonWriter<String> strings) {
        return new DeadLetterStatus.JsonWriter(strings);
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    default JsonWriter<OrphanKind> outboxAdminOrphanKindJsonWriter(JsonWriter<String> strings) {
        return new OrphanKind.JsonWriter(strings);
    }

    /**
     * Internal type, unstable: for the adapters of this repository. Refuses a field the contract does not name, which
     * the generated reader would skip.
     */
    @InternalApi
    default JsonReader<Selection> outboxAdminSelectionJsonReader(JsonReader<List<Long>> ids,
            JsonReader<Filter> filter) {
        return RequestReaders.selection(ids, filter);
    }

    /**
     * Internal type, unstable: for the adapters of this repository. Refuses a field the contract does not name, which
     * the generated reader would skip.
     */
    @InternalApi
    default JsonReader<Filter> outboxAdminFilterJsonReader(JsonReader<OffsetDateTime> times,
            JsonReader<DeadLetterStatus> statuses) {
        return RequestReaders.filter(times, statuses);
    }
}
