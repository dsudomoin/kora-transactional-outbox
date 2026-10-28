package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.GuardedTelemetry;
import io.github.dsudomoin.outbox.internal.OwnerTransactions;
import io.github.dsudomoin.outbox.internal.PgInbox;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.util.Objects;

/**
 * The consumer-side {@link Inbox} without the outbox: {@link #start()} creates or checks its table and schedules its
 * maintenance, {@link #close()} stops it. The inbox exists from {@code start()} on.
 */
public final class InboxRuntime implements AutoCloseable {

    private final OutboxTransactions transactions;
    private final PgInbox inbox;
    private volatile boolean started;

    private InboxRuntime(OutboxTransactions transactions, PgInbox inbox) {
        this.transactions = transactions;
        this.inbox = inbox;
    }

    public static Builder builder(OutboxConfig config, OutboxTransactions transactions) {
        return new Builder(config, transactions);
    }

    /** Deduplicates in the caller's transaction. */
    public Inbox inbox() {
        if (!started) {
            throw new IllegalStateException("InboxRuntime.start() has not run");
        }
        return inbox;
    }

    /** Creates or checks the inbox table and schedules its maintenance. Once. */
    public synchronized void start() {
        if (started) {
            throw new IllegalStateException("InboxRuntime.start() has run already");
        }
        try {
            inbox.start();
        } catch (SQLException e) {
            throw transactions.translate(e);
        }
        started = true;
    }

    /** Stops the inbox maintenance; nothing without {@link #start()}. */
    @Override
    public synchronized void close() {
        if (started) {
            inbox.close();
        }
    }

    public static final class Builder {

        private final OutboxConfig config;
        private final OutboxTransactions transactions;
        private OutboxTelemetry telemetry = OutboxTelemetry.NONE;
        @Nullable
        private ConnectionSource owner;

        private Builder(OutboxConfig config, OutboxTransactions transactions) {
            this.config = Objects.requireNonNull(config, "config");
            this.transactions = Objects.requireNonNull(transactions, "transactions");
        }

        public Builder telemetry(OutboxTelemetry telemetry) {
            this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
            return this;
        }

        /**
         * Connections of the role that owns the inbox table, for its startup and maintenance (the range DDL);
         * {@code transactions} then needs DML only. Requires {@code outbox.schema.bootstrap = false}.
         */
        public Builder ownerConnections(ConnectionSource owner) {
            this.owner = Objects.requireNonNull(owner, "owner");
            return this;
        }

        /** Validates the inbox keys of the config; touches no database. */
        public InboxRuntime build() {
            if (owner != null && config.schema().bootstrap()) {
                throw new IllegalArgumentException(OutboxRuntime.OWNER_NEEDS_MIGRATIONS);
            }
            return new InboxRuntime(transactions, new PgInbox(transactions, OwnerTransactions.of(transactions, owner),
                    config, GuardedTelemetry.of(telemetry)));
        }
    }
}
