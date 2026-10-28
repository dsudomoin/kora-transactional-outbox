package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BatchReaderTest {

    @Test
    void bindLeavesOutTransactionsStillInFlightAtTheTarget() throws Exception {
        var xip = new AtomicReference<Object>();
        var connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("createArrayOf")) {
                        xip.set(args[1]);
                    }
                    return null;
                });
        var statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> method.getName().equals("getConnection") ? connection : null);

        // 12 is still running at the target, so none of its rows can be visible there
        new BatchReader(TableNames.of("outbox")).bind(statement, 0, new PgSnapshot(10, 20, new long[] {11, 12, 15}),
                new PgSnapshot(12, 30, new long[] {12, 25}), 0, 100);

        assertThat((Object[]) xip.get()).containsExactly("11", "15");
    }
}
