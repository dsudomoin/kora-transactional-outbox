package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.GuardedTelemetry;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.OwnerTransactions;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.internal.PgOutboxAdmin;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The outbox assembled from public parts: config, the application's transactions, the handler and, optionally, topic
 * routes in front of it, a dead-letter hook and telemetry. {@link #start()} creates or checks the tables, then starts
 * the relay with its maintenance and dead-letter pass; {@link #close()} stops the relay. The components exist from
 * {@code start()} on, built with the shard count the bootstrap resolved.
 * <pre>{@code
 * var transactions = new JdbcTransactions(dataSource);
 * var runtime = OutboxRuntime.builder(config, transactions, handler).build();
 * runtime.start();
 * transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("orders", key, payload)));
 * runtime.close();
 * }</pre>
 * Handlers that claim their own topics go in front of the handler, which delivers every other topic:
 * <pre>{@code
 * OutboxRuntime.builder(config, transactions, kafka.handler())
 *         .route(new AuditHandler(client))
 *         .route(new BillingHandler(billing))
 *         .build();
 * }</pre>
 */
public final class OutboxRuntime implements AutoCloseable {

    static final String OWNER_NEEDS_MIGRATIONS = "outbox.schema.bootstrap must be false when an owner connection source"
            + " is set: objects it created would leave the application role without rights on them";
    static final String NO_HANDLER = "the outbox has neither an OutboxHandler nor an OutboxTopicHandler route: nothing"
            + " would deliver its records (in a Kora app the OutboxHandler comes from KafkaOutboxModule or the app's"
            + " own component)";

    private final OutboxConfig config;
    private final OutboxTransactions transactions;
    /** The builder's handler, with its routes in front of it. Package-private for tests. */
    final OutboxHandler handler;
    /** The topics the routes claim, the only ones {@link #outbox()} takes; null with a handler behind them. */
    @Nullable
    private final Set<String> claimedTopics;
    private final OutboxDeadLetterHandler deadLetterHandler;
    private final OutboxTelemetry telemetry;
    /** The transactions with the owner's new connections, or {@link #transactions} without an owner source. */
    private final OutboxTransactions ddl;
    @Nullable
    private volatile Parts parts;

    private OutboxRuntime(Builder builder, OutboxHandler handler, @Nullable Set<String> claimedTopics) {
        this.config = builder.config;
        this.transactions = builder.transactions;
        this.handler = handler;
        this.claimedTopics = claimedTopics;
        this.deadLetterHandler = builder.deadLetterHandler;
        this.telemetry = GuardedTelemetry.of(builder.telemetry);
        this.ddl = OwnerTransactions.of(transactions, builder.owner);
    }

    /**
     * {@code handler} delivers every topic no {@link Builder#route} claims. It may be null when there are routes:
     * {@link #outbox()} then refuses a topic none claims with an {@link IllegalArgumentException}, before anything is
     * written, and a record of such a topic that another release wrote waits
     * ({@link Outcome.Waiting.Kind#NO_ROUTE}) for an instance with its route.
     */
    public static Builder builder(OutboxConfig config, OutboxTransactions transactions,
            @Nullable OutboxHandler handler) {
        return new Builder(config, transactions, handler);
    }

    /** Sends in the caller's transaction. */
    public Outbox outbox() {
        return parts().outbox;
    }

    public OutboxRelay relay() {
        return parts().relay;
    }

    public OutboxAdmin admin() {
        return parts().admin;
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    public OutboxSchema schema() {
        return parts().schema;
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    public Maintenance maintenance() {
        return parts().maintenance;
    }

    /** Internal type, unstable: for the adapters of this repository. */
    @InternalApi
    public DeadLetterPass deadLetterPass() {
        return parts().deadLetters;
    }

    /**
     * Creates or checks the tables, then starts the relay (unless {@code relay.enabled} is false), its maintenance
     * and its dead-letter pass. Once.
     */
    public synchronized void start() {
        if (parts != null) {
            throw new IllegalStateException("OutboxRuntime.start() has run already");
        }
        var schema = new OutboxSchema(transactions, ddl, config);
        try {
            schema.start();
        } catch (SQLException e) {
            throw transactions.translate(e);
        }
        try {
            var resolved = schema.config(); // with the shard count the bootstrap resolved
            var names = schema.names();
            var maintenance = new Maintenance(ddl, names, resolved, telemetry);
            var deadLetters = new DeadLetterPass(transactions, names, resolved, deadLetterHandler, telemetry);
            var relay = new OutboxRelay(transactions, resolved, schema, handler, telemetry, maintenance, deadLetters);
            var outbox = new PgOutbox(transactions, schema, resolved.shards(), relay::wake, telemetry, claimedTopics);
            var admin = new PgOutboxAdmin(transactions, ddl, names, resolved, maintenance, relay::wake);
            relay.start();
            parts = new Parts(schema, maintenance, deadLetters, relay, outbox, admin);
        } catch (RuntimeException | Error e) {
            schema.close(); // this instance's row, which the bootstrap wrote
            throw e;
        }
    }

    /**
     * Stops the relay within {@code relay.shutdownTimeout}, then deletes this instance's row; nothing without
     * {@link #start()}.
     */
    @Override
    public synchronized void close() {
        var p = parts;
        if (p != null) {
            p.relay.close();
            p.schema.close();
        }
    }

    private Parts parts() {
        var p = parts;
        if (p == null) {
            throw new IllegalStateException("OutboxRuntime.start() has not run");
        }
        return p;
    }

    private record Parts(OutboxSchema schema, Maintenance maintenance, DeadLetterPass deadLetters, OutboxRelay relay,
            Outbox outbox, OutboxAdmin admin) {}

    public static final class Builder {

        private final OutboxConfig config;
        private final OutboxTransactions transactions;
        @Nullable
        private final OutboxHandler handler;
        private final List<OutboxTopicHandler> routes = new ArrayList<>();
        private OutboxDeadLetterHandler deadLetterHandler = OutboxDeadLetterHandler.NONE;
        private OutboxTelemetry telemetry = OutboxTelemetry.NONE;
        @Nullable
        private ConnectionSource owner;

        private Builder(OutboxConfig config, OutboxTransactions transactions, @Nullable OutboxHandler handler) {
            this.config = Objects.requireNonNull(config, "config");
            this.transactions = Objects.requireNonNull(transactions, "transactions");
            this.handler = handler;
        }

        /**
         * Sends the records of {@code route}'s topics to it instead of the handler, as {@link OutboxHandler#byTopic}
         * does. {@link #build()} fails when two routes claim one topic, or a route claims none.
         */
        public Builder route(OutboxTopicHandler route) {
            routes.add(Objects.requireNonNull(route, "route"));
            return this;
        }

        /** Where dead letters go; without one, every dead letter is kept for the admin API. */
        public Builder deadLetterHandler(OutboxDeadLetterHandler hook) {
            this.deadLetterHandler = Objects.requireNonNull(hook, "hook");
            return this;
        }

        public Builder telemetry(OutboxTelemetry telemetry) {
            this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
            return this;
        }

        /**
         * Connections of the role that owns the tables, for everything that runs DDL or holds the session locks: the
         * startup, maintenance (partitions, orphans, the xid-regression recovery), {@link OutboxAdmin#status()},
         * {@link OutboxAdmin#restoreOrphan} and {@link OutboxAdmin#recoverXidRegression()}; {@code transactions} then
         * needs DML only. Requires {@code outbox.schema.bootstrap = false}: the migrations own the base tables.
         */
        public Builder ownerConnections(ConnectionSource owner) {
            this.owner = Objects.requireNonNull(owner, "owner");
            return this;
        }

        /** Validates the config and the routes; touches no database. */
        public OutboxRuntime build() {
            OutboxSchema.validate(config);
            if (owner != null && config.schema().bootstrap()) {
                throw new IllegalArgumentException(OWNER_NEEDS_MIGRATIONS);
            }
            var claims = new HashMap<String, OutboxTopicHandler>();
            var routed = routed(claims);
            return new OutboxRuntime(this, routed, handler != null ? null : claims.keySet());
        }

        /** Fills {@code claims} with each claimed topic's route. */
        private OutboxHandler routed(Map<String, OutboxTopicHandler> claims) {
            if (routes.isEmpty()) {
                if (handler == null) {
                    throw new IllegalArgumentException(NO_HANDLER);
                }
                return handler;
            }
            var table = new HashMap<String, OutboxHandler>();
            for (var route : routes) {
                var topics = route.topics();
                if (topics.isEmpty()) {
                    throw new IllegalArgumentException(route.getClass().getName() + " claims no topics");
                }
                var adapted = adapt(route); // one per route: byTopic hands a route all its topics in one call
                for (var topic : topics) {
                    var other = claims.putIfAbsent(topic, route);
                    if (other != null) {
                        throw new IllegalArgumentException("outbox topic " + topic + " is claimed by both "
                                + other.getClass().getName() + " and " + route.getClass().getName());
                    }
                    table.put(topic, adapted);
                }
            }
            return OutboxHandler.byTopic(table, handler != null ? handler : record -> {
                throw CannotHandle.waiting(record, Outcome.Waiting.Kind.NO_ROUTE,
                        "no handler for topic " + record.topic() + " on this instance");
            });
        }

        private static OutboxHandler adapt(OutboxTopicHandler route) {
            return new OutboxHandler() {
                @Override
                public void handle(OutboxRecord record) throws Exception {
                    route.handle(record);
                }

                @Override
                public List<Outcome> handleBatch(List<OutboxRecord> records) {
                    return route.handleBatch(records);
                }

                @Override
                public String toString() {
                    return route.toString();
                }
            };
        }
    }
}
