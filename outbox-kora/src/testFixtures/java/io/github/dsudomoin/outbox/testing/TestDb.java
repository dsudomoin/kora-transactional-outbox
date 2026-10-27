package io.github.dsudomoin.outbox.testing;

import jakarta.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetryFactory;
import ru.tinkoff.kora.database.jdbc.ConnectionContext;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseConfig;
import ru.tinkoff.kora.telemetry.common.TelemetryConfig;

/** One Postgres 16 container per test JVM, started on first use; Testcontainers removes it at exit. */
public final class TestDb {

    private static final AtomicInteger POOLS = new AtomicInteger();

    private TestDb() {}

    public static String jdbcUrl() {
        return TestDataSources.jdbcUrl();
    }

    public static String user() {
        return TestDataSources.user();
    }

    public static String password() {
        return TestDataSources.password();
    }

    /** Kora pool over the test container; the caller closes it with {@code release()}. */
    public static TestDatabase newDatabase(int maxPoolSize) {
        return newDatabase(maxPoolSize, new Properties());
    }

    /** {@link #newDatabase(int)} with pgjdbc connection properties, e.g. {@code defaultRowFetchSize}. */
    public static TestDatabase newDatabase(int maxPoolSize, Properties dsProperties) {
        return newDatabase(jdbcUrl(), user(), password(), maxPoolSize, dsProperties);
    }

    /** {@link #newDatabase(int)} logged in as {@code role}, made by {@link #newLoginRole()}. */
    public static TestDatabase newDatabase(int maxPoolSize, String role) {
        return newDatabase(jdbcUrl(), role, role, maxPoolSize, new Properties());
    }

    /** Kora pool over any database, e.g. the bench stand's; the caller closes it with {@code release()}. */
    public static TestDatabase newDatabase(String jdbcUrl, String user, String password, int maxPoolSize) {
        return newDatabase(jdbcUrl, user, password, maxPoolSize, new Properties());
    }

    /**
     * A new login role, its password its name, that may create tables in {@code public} and has no other privilege:
     * no superuser, no {@code pg_read_all_stats}, as an application role usually is.
     */
    public static String newLoginRole() throws SQLException {
        var role = String.format("app_%010x", ThreadLocalRandom.current().nextLong(1L << 40));
        try (var c = connect(); var st = c.createStatement()) {
            st.execute("create role " + role + " login password '" + role + "'");
            st.execute("grant create on schema public to " + role);
        }
        return role;
    }

    private static TestDatabase newDatabase(
            String jdbcUrl, String user, String password, int maxPoolSize, Properties dsProperties) {
        var poolName = "test-" + POOLS.incrementAndGet();
        var config = new JdbcDatabaseConfig() {
            @Override
            public String username() {
                return user;
            }

            @Override
            public String password() {
                return password;
            }

            @Override
            public String jdbcUrl() {
                return jdbcUrl;
            }

            @Override
            public String poolName() {
                return poolName;
            }

            @Nullable
            @Override
            public String schema() {
                return null;
            }

            @Override
            public int maxPoolSize() {
                return maxPoolSize;
            }

            @Nullable
            @Override
            public Duration initializationFailTimeout() {
                return null;
            }

            @Override
            public Properties dsProperties() {
                return dsProperties;
            }

            @Nullable
            @Override
            public TelemetryConfig telemetry() {
                return null; // read only by the DataBaseTelemetryFactory constructor, not the one used below
            }
        };
        return new TestDatabase(config);
    }

    /** {@code ob_} + 10 random lowercase hex digits. */
    public static String uniqueTable() {
        return TestSql.uniqueTable();
    }

    /** Plain autocommit JDBC connection, outside any Kora pool. */
    public static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), user(), password());
    }

    /** {@link #connect()} with pgjdbc connection properties, e.g. {@code ApplicationName}, or as another role. */
    public static Connection connect(Properties properties) throws SQLException {
        var p = new Properties();
        p.setProperty("user", user());
        p.setProperty("password", password());
        p.putAll(properties);
        return DriverManager.getConnection(jdbcUrl(), p);
    }

    /**
     * {@code db} as seen through a transaction-pooling proxy: {@code select pg_backend_pid()} reports another backend
     * on every statement; everything else runs on {@code db}. New connections record in {@code sql} each statement
     * they run or prepare.
     */
    public static JdbcConnectionFactory transactionPooled(JdbcConnectionFactory db, Collection<String> sql) {
        var pid = new AtomicInteger(1_000_000);
        return new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                return db.withConnection(callback);
            }

            @Nullable
            @Override
            public Connection currentConnection() {
                return db.currentConnection();
            }

            @Nullable
            @Override
            public ConnectionContext currentConnectionContext() {
                return db.currentConnectionContext();
            }

            @Override
            public Connection newConnection() {
                var c = db.newConnection();
                return delegate(Connection.class, c, (name, args) -> {
                    if (name.equals("prepareStatement")) {
                        sql.add((String) args[0]);
                    }
                    if (!name.equals("createStatement") || args != null) {
                        return null;
                    }
                    var st = c.createStatement();
                    return delegate(Statement.class, st, (stName, stArgs) -> {
                        if (!stName.equals("executeQuery") && !stName.equals("execute")) {
                            return null;
                        }
                        String text = (String) stArgs[0];
                        sql.add(text);
                        if (!text.equals("select pg_backend_pid()")) {
                            return null;
                        }
                        String other = "select " + pid.incrementAndGet();
                        return stName.equals("execute") ? st.execute(other) : st.executeQuery(other);
                    });
                });
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return db.telemetry();
            }
        };
    }

    /**
     * {@code db} whose new connections lose their server session right after their first {@code commit()}: the commit
     * goes through, then the backend is terminated, and waited for until gone, so the next statement fails.
     */
    public static JdbcConnectionFactory killedAfterCommit(JdbcConnectionFactory db) {
        return new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                return db.withConnection(callback);
            }

            @Nullable
            @Override
            public Connection currentConnection() {
                return db.currentConnection();
            }

            @Nullable
            @Override
            public ConnectionContext currentConnectionContext() {
                return db.currentConnectionContext();
            }

            @Override
            public Connection newConnection() {
                var c = db.newConnection();
                var committed = new AtomicBoolean();
                return delegate(Connection.class, c, (name, args) -> {
                    if (!name.equals("commit") || committed.getAndSet(true)) {
                        return null;
                    }
                    int pid;
                    try (var st = c.createStatement(); var rs = st.executeQuery("select pg_backend_pid()")) {
                        rs.next();
                        pid = rs.getInt(1);
                    }
                    c.commit();
                    try (var other = connect(); var st = other.createStatement()) {
                        st.execute("select pg_terminate_backend(" + pid + ")");
                        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                        while (true) {
                            try (var rs = st.executeQuery("select count(*) from pg_stat_activity where pid = " + pid)) {
                                rs.next();
                                if (rs.getInt(1) == 0) {
                                    break;
                                }
                            }
                            if (System.nanoTime() - deadline > 0) {
                                throw new IllegalStateException("backend " + pid + " still alive 10 s after"
                                        + " pg_terminate_backend");
                            }
                            Thread.sleep(5);
                        }
                    }
                    return Boolean.TRUE; // non-null: the delegate's commit is not called again
                });
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return db.telemetry();
            }
        };
    }

    private interface Intercept {
        /** The result instead of the delegate's, or null to call the delegate. */
        @Nullable
        Object result(String method, Object[] args) throws Exception;
    }

    private static <T> T delegate(Class<T> type, T target, Intercept intercept) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            var result = intercept.result(method.getName(), args);
            if (result != null) {
                return result;
            }
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }));
    }

    /** Consumes {@code n} transaction ids with autocommit statements. */
    public static void burnXids(int n) throws SQLException {
        try (var c = connect(); var st = c.createStatement()) {
            for (int i = 0; i < n; i++) {
                st.execute("select pg_current_xact_id()");
            }
        }
    }
}
