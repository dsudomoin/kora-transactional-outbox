package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The previous release ({@code feature/requeue} at {@code 5fc60dd}) against the database this version leaves: in a
 * mixed fleet during a rolling deploy, and after a full upgrade is rolled back, its relay starts each step from the
 * latest cursor row's {@code tick_id} (its target's while a batch is in progress), reads a tick's {@code shards}, and
 * inserts ticks with it. Its statements are copied verbatim below.
 */
class RollbackCompatibilityIT {

    private static final int SHARDS = 4;
    private static final String COLUMNS =
            "shard, tick_id, target_tick_id, last_id, paused_until, failures, retry_due_at, parked_due_at";

    private final JdbcDatabase db = TestDb.newDatabase(10);
    private final TestOutboxConfig config = new TestOutboxConfig().shards(SHARDS).relayEnabled(false)
            .xidsPerPartition(1000).premake(10).maintenanceInterval(Duration.ofSeconds(1))
            .cursorAnchorInterval(Duration.ofSeconds(2));
    private final List<OutboxRelay> relays = new ArrayList<>();
    private OutboxSchema schema;
    private TableNames names;

    @AfterEach
    void release() {
        relays.forEach(OutboxRelay::release);
        db.release();
    }

    // 5fc60dd Cursors#positions, verbatim
    private String oldPositions() {
        return "select c.*, c.tick_id < p.latest and case when l.created_at is null or t.created_at is null"
                + " or l.created_at < t.created_at or l.created_at >= t.created_at + p.idle then true"
                + " else exists (select 1 from " + names.tick() + " n where n.id > c.tick_id and n.id <= p.latest"
                + " and (n.shards is null or c.shard = any(n.shards))) end"
                + " from (select ?::bigint latest, ? * interval '1 microsecond' idle, ?::int shards) p"
                + " cross join generate_series(0, p.shards - 1) s cross join lateral (select " + COLUMNS + " from "
                + names.cursor() + " where shard = s order by seq desc limit 1) c"
                + " left join " + names.tick() + " t on t.id = c.tick_id"
                + " left join " + names.tick() + " l on l.id = p.latest order by s";
    }

    // 5fc60dd Cursors#latest, the first statement of a step after its lock (ShardProcessor#tryProcess), verbatim
    private String oldLatest() {
        return "select " + COLUMNS + " from " + names.cursor() + " where shard = ? order by seq desc limit 1";
    }

    // 5fc60dd Ticks#get, by which the step reads its cursor's tick and its target (ShardProcessor#tryProcess):
    // SELECT + names.tick() + " where id = ?", failing without a row
    private String oldTick() {
        return "select id, snapshot::text, created_at, max_id from " + names.tick() + " where id = ?";
    }

    // 5fc60dd Ticks#insert, the tick row after its probe, verbatim
    private String oldTickInsert() {
        return "insert into " + names.tick() + " (snapshot, max_id, shards) values (?::pg_snapshot, ?, ?::smallint[])";
    }

    @Test
    void thePreviousReleaseResumesFromTheCursorsThisOneLeaves() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.init();
        names = schema.names();
        var handler = new RecordingHandler();
        var maintenance = new Maintenance(db, names, config, new OutboxMetrics(null));
        var relay = relay(handler, maintenance);
        var outbox = new PgOutbox(db, schema, SHARDS, () -> {}, TestTelemetry.kora());
        long started = System.nanoTime();

        // every shard busy once, then one shard alone: the others go idle, moved on by checkpoints only
        send(outbox, IntStream.range(0, 40).mapToObj(i -> msg("k" + i)).toArray(OutboxMessage[]::new));
        assertThat(relay.drain()).isEqualTo(40);
        assertThat(maintenance.runOnce()).isTrue();
        TestDb.burnXids(3000);
        send(outbox, msg("solo"));
        assertThat(relay.drain()).isOne();
        assertThat(maintenance.runOnce()).isTrue();
        try (var c = TestDb.connect()) { // a tick of the previous release, in a mixed fleet: no dirty, so capped
            try (var ps = c.prepareStatement(oldTickInsert())) {
                ps.setString(1, queryString(c, "select pg_current_snapshot()::text"));
                ps.setLong(2, queryLong(c, "select coalesce(max(id), 0) from " + names.base()));
                ps.setString(3, "{" + shardOf(c, "solo") + "}");
                assertThat(ps.executeUpdate()).isOne();
            }
        }
        assertThat(relay.drain()).isZero();
        assertThat(maintenance.runOnce()).isTrue();
        assertThat(maintenance.runOnce()).isTrue();
        makeTick();

        // an anchor interval passes: anchors, then a tick cleanup up to them
        Thread.sleep(Math.max(0, 2500 - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)));
        makeTick();
        assertThat(maintenance.runOnce()).isTrue();
        long cleaned;
        try (var c = TestDb.connect()) {
            cleaned = queryLong(c, "select min(id) from " + names.tick());
        }

        // then work left for the old reader: a batch in progress, rows behind a tick, rows after the latest tick
        var delivered = new HashSet<UUID>();
        handler.delivered().forEach(r -> delivered.add(r.messageId()));
        var inBatch = send(outbox, msg("batch"), msg("batch"), msg("batch"), msg("batch"));
        long target = makeTick();
        try (var c = TestDb.connect()) {
            int shard = shardOf(c, "batch");
            var cursors = new Cursors(names, SHARDS);
            var cursor = cursors.latest(c, shard);
            long secondId = queryLong(c, "select id from " + names.base() + " where message_id = '" + inBatch.get(1)
                    + "'");
            // as a step that handed the first two rows leaves it
            cursors.append(c, cursor.cursor().withProgress(cursor.effectiveTickId(), target, secondId));
            delivered.add(inBatch.get(0));
            delivered.add(inBatch.get(1));
        }
        send(outbox, msg("behind"), msg("behind2"));
        makeTick();
        assertThat(maintenance.runOnce()).isTrue();
        send(outbox, msg("untick"));

        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select min(id) from " + names.tick())).as("cleanup ran").isGreaterThan(0);
            long latest = queryLong(c, "select max(id) from " + names.tick());
            // the previous release's position read runs on this schema
            try (var ps = c.prepareStatement(oldPositions())) {
                ps.setLong(1, latest);
                ps.setLong(2, TimeUnit.MICROSECONDS.convert(Duration.ofSeconds(5)));
                ps.setInt(3, SHARDS);
                try (var rs = ps.executeQuery()) {
                    int n = 0;
                    while (rs.next()) {
                        n++;
                    }
                    assertThat(n).isEqualTo(SHARDS);
                }
            }
            var undelivered = undelivered(c, delivered);
            assertThat(undelivered).as("left for the old reader").hasSizeGreaterThanOrEqualTo(5);
            for (int shard = 0; shard < SHARDS; shard++) {
                var cursor = oldLatest(c, shard);
                assertThat(tickExists(c, cursor.tickId())).as("shard %s starts from tick %s (cleanup to %s)", shard,
                        cursor.tickId(), cleaned).isTrue();
                if (cursor.targetTickId() != null) {
                    assertThat(tickExists(c, cursor.targetTickId())).as("shard %s target", shard).isTrue();
                }
                // every committed undelivered row of the shard is one the old reader reads from its cursor:
                // not visible in its tick, and in a batch in progress past lastId or not visible in the target
                try (var ps = c.prepareStatement("select o.message_id from " + names.base() + " o join "
                        + names.tick() + " p on p.id = ? left join " + names.tick() + " t on t.id = ?"
                        + " where o.shard = ? and o.message_id = any(?) and (pg_visible_in_snapshot(o.xid, p.snapshot)"
                        + " or t.id is not null and o.id <= ? and pg_visible_in_snapshot(o.xid, t.snapshot))")) {
                    ps.setLong(1, cursor.tickId());
                    ps.setObject(2, cursor.targetTickId(), Types.BIGINT);
                    ps.setInt(3, shard);
                    ps.setArray(4, c.createArrayOf("uuid", undelivered.toArray()));
                    ps.setLong(5, cursor.lastId());
                    try (var rs = ps.executeQuery()) {
                        assertThat(rs.next()).as("shard %s: a row the old reader would skip", shard).isFalse();
                    }
                }
            }
        }
        // and this version still delivers all of it
        relay.drain();
        var all = new HashSet<UUID>(delivered);
        handler.delivered().stream().map(OutboxRecord::messageId).forEach(all::add);
        try (var c = TestDb.connect()) {
            assertThat(undelivered(c, all)).isEmpty();
        }
    }

    /**
     * A rollback to 5fc60dd in each state this version may leave the tail in: empty, hot after a swap that failed, and
     * hot over maxTailScan. No check is on any partition, so the previous release's maintenance (its extension below)
     * and writers go on with no human step: every insert succeeds while xids run far past any bound, a hot tail
     * taking the rows. Upgrading again, this version's maintenance heals the tail by itself.
     */
    @Test
    void aRollbackInEveryTailStateKeepsInsertsWorkingAndTheUpgradeHealsIt() throws Exception {
        for (var state : List.of("empty", "hot", "hot over maxTailScan")) {
            var cfg = new TestOutboxConfig().shards(SHARDS).relayEnabled(false).xidsPerPartition(1000).premake(10)
                    .maxTailScan(state.equals("hot over maxTailScan") ? 0 : 16L * 1024 * 1024)
                    .tailConversion("03:00-05:00", Duration.ofSeconds(5));
            schema = new OutboxSchema(db, cfg);
            schema.init();
            names = schema.names();
            var partitions = new XidPartitions(names, cfg.partitions());
            partitions.clock = clockAt(12, 0); // outside the window
            try (var holder = TestDb.connect(); var c = TestDb.connect()) {
                if (!state.equals("empty")) { // rows in the tail, then a swap that fails on a young lock holder
                    TestDb.burnXids((int) (tailLower(c) - currentXid(c)) + 10);
                    insert(c);
                    holder.setAutoCommit(false);
                    execute(holder, "lock table only " + names.base() + " in access share mode");
                    try {
                        partitions.ensurePremade(c);
                    } catch (SQLException e) {
                        assertThat(e.getSQLState()).isEqualTo("55P03");
                    }
                    holder.commit();
                    assertThat(queryLong(c, "select count(*) from " + names.tail())).as(state).isOne();
                    if (state.equals("hot")) { // over maxTailScan, no swap is tried outside the window
                        assertThat(PartitionDdl.standalone(c, names.base(), "_x[0-9]+"))
                                .as("the failed swap's ranges made ahead").isNotEmpty();
                    }
                }
                long tail = tailLower(c);

                var failed = new ArrayList<SQLException>();
                for (int i = 0; i < 30; i++) { // the previous release, 30 ranges on
                    TestDb.burnXids(1000);
                    try {
                        insert(c);
                    } catch (SQLException e) {
                        failed.add(e);
                    }
                    previousFinishPendingDetach(c);
                    previousExtension(c, cfg.xidsPerPartition(), cfg.premake());
                    previousDropOrphans(c);
                }

                assertThat(failed).as("%s: failed inserts", state).isEmpty();
                assertThat(PartitionDdl.standalone(c, names.base(), "_x[0-9]+")).as("%s: 5fc60dd dropped the empty"
                        + " ranges made ahead, as anyone else's", state).isEmpty();
                assertThat(queryLong(c, "select count(*) from pg_constraint where contype = 'c' and conrelid in"
                        + " (select inhrelid from pg_inherits where inhparent = '" + names.base() + "'::regclass)"))
                        .as("%s: checks", state).isZero();
                if (!state.equals("empty")) {
                    assertThat(tailLower(c)).as("%s: the previous release leaves a hot tail be", state)
                            .isEqualTo(tail);
                }
                partitions.clock = clockAt(4, 0); // upgraded again; an oversized tail waits for the window
                for (int run = 0; run < 5; run++) { // maintenance runs: four ranges a swap until premake are ahead
                    partitions.ensurePremade(c);
                }

                assertThat(queryLong(c, "select count(*) from " + names.tail())).as("%s: healed", state).isZero();
                // its own xids moved the counter on meanwhile, over these 1000-xid ranges: no longer due is the point
                assertThat(partitions.premadeAhead(c)).as(state).isGreaterThan(cfg.premake() / 2);
                insert(c);
            }
        }
    }

    /**
     * 5fc60dd XidPartitions#ensurePremade for an attached tail, its statements verbatim, without its long-transaction
     * check (this test has none): a hot tail (rows, or the current xid within a range of it) is left be; else, once
     * half of premake is used, the empty tail is detached concurrently and dropped, ranges are created until premake
     * lie above the current xid's, and a new tail is attached.
     */
    private void previousExtension(Connection c, long size, int premake) throws SQLException {
        long current = currentXid(c);
        Long upper = null;
        int ahead = 0;
        for (var b : PartitionDdl.listRanges(c, names.base())) {
            if (b.to() != null) {
                ahead += Long.parseLong(b.from()) > current ? 1 : 0;
                upper = upper == null ? Long.parseLong(b.to()) : Math.max(upper, Long.parseLong(b.to()));
            }
        }
        if (ahead > premake / 2 || PartitionDdl.hasRows(c, names.tail()) || current + size > tailLower(c)) {
            return;
        }
        PartitionDdl.detach(c, names.base(), names.tail(), PartitionDdl.DETACH_LOCK_TIMEOUT);
        assertThat(PartitionDdl.dropIfEmptyElseAttach(c, names.base(), names.tail(), Long.toString(upper), null,
                PartitionDdl.DETACH_LOCK_TIMEOUT)).isTrue();
        long floor = Math.floorDiv(current, size) * size;
        long from = upper;
        if (from < floor) {
            PartitionDdl.createAndAttach(c, names.base(), names.partitionPrefix() + from, Long.toString(from),
                    Long.toString(floor), PartitionDdl.CREATE_LOCK_TIMEOUT);
            from = floor;
        }
        for (long target = floor + (premake + 1) * size; from < target; from += size) {
            PartitionDdl.createAndAttach(c, names.base(), names.partitionPrefix() + from, Long.toString(from),
                    Long.toString(from + size), PartitionDdl.CREATE_LOCK_TIMEOUT);
        }
        PartitionDdl.createAndAttach(c, names.base(), names.tail(), Long.toString(from), null,
                PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    /** 5fc60dd XidPartitions#finishPendingDetach, its statements verbatim: nothing pending after this version. */
    private void previousFinishPendingDetach(Connection c) throws SQLException {
        var pending = PartitionDdl.pendingDetach(c, names.base());
        if (pending != null) {
            PartitionDdl.detach(c, names.base(), pending, PartitionDdl.DETACH_LOCK_TIMEOUT);
        }
    }

    /**
     * 5fc60dd XidPartitions#dropOrphans, its statements verbatim: every standalone {@code <base>_x<digits>}, so also
     * the ranges this version made ahead of its tail, is dropped when empty unless recorded as relayed.
     */
    private void previousDropOrphans(Connection c) throws SQLException {
        var marked = new HashSet<String>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select table_name from " + names.dropping())) {
            while (rs.next()) {
                marked.add(rs.getString(1));
            }
        }
        for (var table : PartitionDdl.standalone(c, names.base(), "_x[0-9]+")) {
            if (!marked.contains(names.unqualified(table))) {
                PartitionDdl.dropUnlessFound(c, table, "select 1 from {table} limit 1", null,
                        PartitionDdl.DETACH_LOCK_TIMEOUT);
            }
        }
    }

    private void insert(Connection c) throws SQLException {
        execute(c, "insert into " + names.base() + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
    }

    private long tailLower(Connection c) throws SQLException {
        return PartitionDdl.listRanges(c, names.base()).stream().filter(b -> b.to() == null)
                .mapToLong(b -> Long.parseLong(b.from())).findFirst().orElseThrow();
    }

    private static long currentXid(Connection c) throws SQLException {
        return queryLong(c, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint");
    }

    private static Clock clockAt(int hour, int minute) {
        var zone = ZoneId.systemDefault();
        return Clock.fixed(LocalDate.now(zone).atTime(hour, minute).atZone(zone).toInstant(), zone);
    }

    private Cursor oldLatest(Connection c, int shard) throws SQLException {
        try (var ps = c.prepareStatement(oldLatest())) {
            ps.setInt(1, shard);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return new Cursor(rs.getInt(1), rs.getLong(2), rs.getObject(3, Long.class), rs.getLong(4), null,
                        rs.getInt(6), null, null);
            }
        }
    }

    private boolean tickExists(Connection c, long id) throws SQLException {
        try (var ps = c.prepareStatement(oldTick())) {
            ps.setLong(1, id);
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** The committed rows still in the outbox whose message is not in {@code delivered}. */
    private Set<UUID> undelivered(Connection c, Set<UUID> delivered) throws SQLException {
        var result = new HashSet<UUID>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select message_id from " + names.base())) {
            while (rs.next()) {
                var id = rs.getObject(1, UUID.class);
                if (!delivered.contains(id)) {
                    result.add(id);
                }
            }
        }
        return result;
    }

    private int shardOf(Connection c, String key) throws SQLException {
        try (var ps = c.prepareStatement("select shard from " + names.base() + " where key = ? limit 1")) {
            ps.setBytes(1, key.getBytes(UTF_8));
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return rs.getInt(1);
            }
        }
    }

    private long makeTick() throws SQLException {
        try (var c = TestDb.connect()) {
            var ticks = new Ticks(names, SHARDS);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            return ticks.latest(c).id();
        }
    }

    private static String queryString(Connection c, String sql) throws SQLException {
        return io.github.dsudomoin.outbox.testing.TestSql.queryString(c, sql);
    }

    private OutboxRelay relay(RecordingHandler handler, Maintenance maintenance) {
        var relay = new OutboxRelay(db, config, schema, handler, TestTelemetry.kora(), maintenance,
                new DeadLetterPass(db, names, config, null, new OutboxMetrics(null)));
        relays.add(relay);
        return relay;
    }

    private List<UUID> send(PgOutbox outbox, OutboxMessage... messages) {
        return db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(messages)));
    }

    private static OutboxMessage msg(String key) {
        return OutboxMessage.of("t", key, key.getBytes(UTF_8));
    }
}
