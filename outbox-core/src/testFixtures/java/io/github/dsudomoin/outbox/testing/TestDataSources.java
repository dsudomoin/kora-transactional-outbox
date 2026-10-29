package io.github.dsudomoin.outbox.testing;

import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The tests' Postgres, started once per JVM, and plain {@link DataSource}s over it, without any framework. 16 unless
 * {@code -Doutbox.test.postgres} names another image; {@code -Doutbox.test.containerCpus=1} gives it one CPU, as a
 * small CI runner would.
 */
public final class TestDataSources {

    private TestDataSources() {}

    private static final class Pg {
        static final PostgreSQLContainer INSTANCE =
                new PostgreSQLContainer(System.getProperty("outbox.test.postgres", "postgres:16-alpine"))
                        .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=500")
                        .withCreateContainerCmdModifier(cmd -> {
                            var cpus = System.getProperty("outbox.test.containerCpus");
                            if (cpus != null) {
                                cmd.getHostConfig().withNanoCPUs((long) (Double.parseDouble(cpus) * 1e9));
                            }
                        });

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
