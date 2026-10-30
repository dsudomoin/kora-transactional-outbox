package io.github.dsudomoin.outbox;

/** The handler a runtime builds, for the tests of other packages. */
public final class BuiltHandler {

    private BuiltHandler() {}

    /** {@code builder}'s handler: its routes in front of the builder's handler; touches no database. */
    public static OutboxHandler of(OutboxRuntime.Builder builder) {
        return builder.build().handler;
    }
}
