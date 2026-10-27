package io.github.dsudomoin.outbox;

import io.github.dsudomoin.outbox.internal.GuardedTelemetry;
import io.github.dsudomoin.outbox.internal.PgInbox;
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

        private Builder(OutboxConfig config, OutboxTransactions transactions) {
            this.config = Objects.requireNonNull(config, "config");
            this.transactions = Objects.requireNonNull(transactions, "transactions");
        }

        public Builder telemetry(OutboxTelemetry telemetry) {
            this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
            return this;
        }

        /** Validates the inbox keys of the config; touches no database. */
        public InboxRuntime build() {
            return new InboxRuntime(transactions, new PgInbox(transactions, config, GuardedTelemetry.of(telemetry)));
        }
    }
}
