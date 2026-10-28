package io.github.dsudomoin.outbox.testing;

import java.sql.Connection;
import java.sql.SQLException;

/** One-statement JDBC helpers for assertions. */
public final class TestSql {

    private TestSql() {}

    public static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    public static long queryLong(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    public static String queryString(Connection c, String sql, String... args) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setString(i + 1, args[i]);
            }
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}
