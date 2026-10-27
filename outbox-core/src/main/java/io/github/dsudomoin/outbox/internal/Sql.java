package io.github.dsudomoin.outbox.internal;

import java.sql.SQLException;

/** JDBC callbacks that may throw {@link SQLException}. */
public final class Sql {

    private Sql() {}

    @FunctionalInterface
    public interface SqlFunction0<T> {
        T apply() throws SQLException;
    }

    @FunctionalInterface
    public interface SqlFunction1<A, T> {
        T apply(A a) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlConsumer<A> {
        void accept(A a) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlRunnable {
        void run() throws SQLException;
    }
}
