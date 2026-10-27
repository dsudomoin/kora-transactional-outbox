package io.github.dsudomoin.kora.outbox.admin;

import io.github.dsudomoin.kora.outbox.OutboxAdmin;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiControllerModule;
import io.github.dsudomoin.kora.outbox.admin.api.OutboxAdminApiDelegate;
import io.github.dsudomoin.kora.outbox.admin.model.DeadLetterStatus;
import io.github.dsudomoin.kora.outbox.admin.model.ModelError;
import io.github.dsudomoin.kora.outbox.admin.model.OrphanKind;
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

    default OutboxAdminInterceptor outboxAdminInterceptor(OutboxAdminAuthorizer authorizer,
            BlockingRequestExecutor executor, JsonWriter<ModelError> errorWriter) {
        return new OutboxAdminInterceptor(authorizer, executor, errorWriter);
    }

    default OutboxAdminApiDelegate outboxAdminDelegate(OutboxAdmin admin) {
        return new OutboxAdminDelegate(admin);
    }

    // The generator's enum readers and writers (by the contract's values, "pending", not the constant's name) are
    // @Component classes, which an app's graph never finds in a library jar: without these it falls back to
    // Enum::name for the query parameter and generates its own JSON codecs, with a warning, in every app build.

    default StringParameterReader<DeadLetterStatus> outboxAdminDeadLetterStatusParameterReader() {
        return new DeadLetterStatus.StringParameterReader();
    }

    default JsonReader<DeadLetterStatus> outboxAdminDeadLetterStatusJsonReader(JsonReader<String> strings) {
        return new DeadLetterStatus.JsonReader(strings);
    }

    default JsonWriter<DeadLetterStatus> outboxAdminDeadLetterStatusJsonWriter(JsonWriter<String> strings) {
        return new DeadLetterStatus.JsonWriter(strings);
    }

    default JsonWriter<OrphanKind> outboxAdminOrphanKindJsonWriter(JsonWriter<String> strings) {
        return new OrphanKind.JsonWriter(strings);
    }
}
