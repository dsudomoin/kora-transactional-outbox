package io.github.dsudomoin.outbox.testing;

import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** The tests' Postgres, started once per JVM, and plain {@link DataSource}s over it, without any framework. */
public final class TestDataSources {

    private TestDataSources() {}

    private static final class Pg {
        static final PostgreSQLContainer INSTANCE = new PostgreSQLContainer("postgres:16-alpine")
                .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=500");

        static {
            INSTANCE.start();
        }
    }

    public static String jdbcUrl() {
        return Pg.INSTANCE.getJdbcUrl();
    }

    public static String user() {
        return Pg.INSTANCE.getUsername();
    }

    public static String password() {
        return Pg.INSTANCE.getPassword();
    }

    /** A new physical connection per {@code getConnection()}. */
    public static DataSource newDataSource() {
        var ds = new PGSimpleDataSource();
        ds.setUrl(jdbcUrl());
        ds.setUser(user());
        ds.setPassword(password());
        return ds;
    }
}
