package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.kora.outbox.testing.TestSql.queryLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The Postgres behaviour the partition tail's design rests on, pinned on 16 and 17 so a future major that changes it
 * fails here first: why the tail is never detached concurrently, why a hot tail is attached as a range by a scan and
 * never behind a check, why there is no default partition, and why the swap never queues for its lock. A table shaped
 * as the outbox: {@code partition by range (xid)}, ranges below H = 3000 and the tail {@code [H, MAXVALUE)}.
 */
class PgFactsIT {

    private static final Map<String, PostgreSQLContainer> SERVERS = new ConcurrentHashMap<>();
    private static final String H = "3000";

    @AfterAll
    static void stop() {
        SERVERS.values().forEach(PostgreSQLContainer::stop);
    }

    /**
     * While a concurrent detach of the tail waits in its second phase for a lock holder, nothing at or above H has a
     * partition and nothing can be attached there: the window cannot be closed early.
     */
    @ParameterizedTest
    @ValueSource(strings = {"postgres:16-alpine", "postgres:17-alpine"})
    void aPendingTailRefusesInsertsAndOverlappingAttaches(String image) throws Exception {
        try (var holder = connect(image); var c = connect(image)) {
            outbox(c, false);
            holder.setAutoCommit(false); // a writer open in a range, as at any moment
            insert(holder, 2500);
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var detach = new Thread(() -> {
                try (var d = connect(image)) {
                    execute(d, "alter table o detach partition o_tail concurrently");
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            detach.start();
            while (queryLong(c, "select count(*) from pg_inherits where inhrelid = 'o_tail'::regclass"
                    + " and inhdetachpending") == 0) {
                assertThat(errors).isEmpty();
                Thread.sleep(10);
            }

            assertThatThrownBy(() -> insert(c, 3005)).isInstanceOfSatisfying(SQLException.class,
                    e -> assertThat(e.getSQLState()).as(e.getMessage()).isEqualTo("23514"));
            execute(c, "create table o_x3000 (like o including defaults)");
            assertThatThrownBy(() -> execute(c, "alter table o attach partition o_x3000 for values from ('3000')"
                    + " to ('4000')")).hasMessageContaining("would overlap partition \"o_tail\"");

            holder.commit();
            detach.join();
            assertThat(errors).isEmpty();
        }
    }

    /**
     * Only a validated check spares the attach its scan: a {@code NOT VALID} one does not count, so any check that
     * skips the scan of a tail holding rows refuses the rows above its bound from the moment it is validated.
     */
    @ParameterizedTest
    @ValueSource(strings = {"postgres:16-alpine", "postgres:17-alpine"})
    void onlyAValidatedCheckSparesTheAttachItsScan(String image) throws Exception {
        try (var c = connect(image)) {
            for (var check : List.of("none", "not valid", "validated")) {
                outbox(c, false);
                insert(c, 3005);
                if (!check.equals("none")) {
                    execute(c, "alter table o_tail add constraint b check (xid >= '3000' and xid < '4000') not valid");
                }
                if (check.equals("validated")) {
                    execute(c, "alter table o_tail validate constraint b");
                }
                var warnings = new ArrayList<String>();
                c.setAutoCommit(false);
                try (var st = c.createStatement()) {
                    st.execute("set local client_min_messages = debug1");
                    st.execute("alter table o detach partition o_tail");
                    st.execute("alter table o attach partition o_tail for values from ('3000') to ('4000')");
                    for (var w = st.getWarnings(); w != null; w = w.getNextWarning()) {
                        warnings.add(w.getMessage());
                    }
                    c.commit();
                } finally {
                    c.setAutoCommit(true);
                }

                if (check.equals("validated")) {
                    assertThat(warnings).as(check).contains("partition constraint for table \"o_tail\" is implied by"
                            + " existing constraints").doesNotContain("verifying table \"o_tail\"");
                } else {
                    assertThat(warnings).as(check).contains("verifying table \"o_tail\"");
                }
            }
        }
    }

    /** With a default partition, no partition of the parent can be detached concurrently, the default's included. */
    @ParameterizedTest
    @ValueSource(strings = {"postgres:16-alpine", "postgres:17-alpine"})
    void aDefaultPartitionForbidsConcurrentDetaches(String image) throws Exception {
        try (var c = connect(image)) {
            outbox(c, true);

            for (var partition : List.of("o_x0", "o_default")) {
                assertThatThrownBy(() -> execute(c, "alter table o detach partition " + partition + " concurrently"))
                        .as(partition).hasMessageContaining("cannot detach partitions concurrently when a default"
                                + " partition exists");
            }
        }
    }

    /**
     * A queued ACCESS EXCLUSIVE request on the parent makes every later writer queue behind it, so a writer whose own
     * {@code lock_timeout} is shorter than the wait fails: the swap only ever takes the lock without waiting.
     */
    @ParameterizedTest
    @ValueSource(strings = {"postgres:16-alpine", "postgres:17-alpine"})
    void aQueuedExclusiveRequestFailsAWriterWithAShortLockTimeout(String image) throws Exception {
        try (var holder = connect(image); var swap = connect(image); var writer = connect(image)) {
            outbox(holder, false);
            holder.setAutoCommit(false);
            insert(holder, 2500);
            var errors = new ConcurrentLinkedQueue<Throwable>();
            swap.setAutoCommit(false);
            var request = new Thread(() -> {
                try {
                    execute(swap, "set local lock_timeout = '3s'");
                    execute(swap, "lock table only o in access exclusive mode");
                } catch (SQLException e) {
                    if (!"55P03".equals(e.getSQLState())) {
                        errors.add(e);
                    }
                }
                try {
                    swap.rollback();
                } catch (SQLException e) {
                    errors.add(e);
                }
            });
            request.start();
            try (var w = connect(image)) {
                while (queryLong(w, "select count(*) from pg_locks where relation = 'o'::regclass"
                        + " and mode = 'AccessExclusiveLock' and not granted") == 0) {
                    assertThat(errors).isEmpty();
                    Thread.sleep(10);
                }
            }

            execute(writer, "set lock_timeout = '100ms'");
            assertThatThrownBy(() -> insert(writer, 2600)).isInstanceOfSatisfying(SQLException.class,
                    e -> assertThat(e.getSQLState()).as(e.getMessage()).isEqualTo("55P03"));

            holder.commit();
            request.join();
            assertThat(errors).isEmpty();
        }
    }

    /** The outbox's shape, made anew: ranges [0, 3000) by 1000 and the tail [3000, MAXVALUE), or a default. */
    private static void outbox(Connection c, boolean withDefault) throws SQLException {
        execute(c, "drop table if exists o, o_tail, o_x3000 cascade");
        execute(c, "create table o (id bigint generated always as identity, shard smallint not null,"
                + " xid xid8 not null default pg_current_xact_id(), message_id uuid not null default gen_random_uuid(),"
                + " topic text not null default 't', payload bytea) partition by range (xid)");
        execute(c, "create index o_shard_xid on o (shard, xid)");
        for (int from = 0; from < 3000; from += 1000) {
            execute(c, "create table o_x" + from + " partition of o for values from ('" + from + "') to ('"
                    + (from + 1000) + "')");
        }
        execute(c, withDefault ? "create table o_default partition of o default"
                : "create table o_tail partition of o for values from ('" + H + "') to (maxvalue)");
    }

    /** A row routed by its {@code xid}, as a writer's whose xid is that. */
    private static void insert(Connection c, long xid) throws SQLException {
        execute(c, "insert into o (shard, xid) values (0, '" + xid + "')");
    }

    private static Connection connect(String image) throws SQLException {
        var pg = SERVERS.computeIfAbsent(image, i -> {
            var container = new PostgreSQLContainer(i);
            container.start();
            return container;
        });
        return DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    }
}
