package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestTracing.header;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Follower;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Head;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Keep;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Park;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Unpark;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParkedStoreIT {

    /** Postgres keeps microseconds. */
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);
    /** After every due time the tests write. */
    private static final Instant LATER = NOW.plus(Duration.ofMinutes(10));

    private final TestDatabase db = TestDb.newDatabase(2);
    private TableNames names;
    private ParkedStore store;
    private RetryStore retries;
    private Connection c;
    private long nextId;

    @BeforeEach
    void bootstrap() throws Exception {
        var schema = new OutboxSchema(db, new TestOutboxConfig().shards(2));
        schema.init();
        names = schema.names();
        retries = new RetryStore(names, false);
        store = new ParkedStore(names, retries);
        c = TestDb.connect();
    }

    @AfterEach
    void release() throws SQLException {
        c.close();
        db.release();
    }

    @Test
    void applyParksMainStreamRecordsAndReParksParkedOnes() throws SQLException {
        var keyed = record("t", "k", List.of(header("h", "v"), new OutboxHeader("null", null)));
        var unkeyed = record("u", null);
        var followed = record("t", "f");
        var head = record("t", "h");
        var follower = record("t", "h");
        var dead = record("t", "d");
        var failing = record("t", "x");
        var carried = record("t", "x");
        var poison = record("t", "p");
        store.apply(c, 0, List.of(
                new Park(keyed, 1, NOW.plusSeconds(1)),
                new Head(head, 1, NOW, "e"),
                new Follower(follower),
                new Dead(dead, 1, "boom"),
                new Park(unkeyed, 1, NOW.plusSeconds(2)),
                new Park(followed, 1, NOW.plusSeconds(3)),
                new Park(failing, 1, NOW.plusSeconds(4)),
                new Park(carried, 1, NOW.plusSeconds(5)),
                new Park(poison, 1, NOW.plusSeconds(6))), Set.of());

        assertThat(parkedRows()).as("main-stream rows other than Park are not its business").containsExactly(
                keyed.id() + ":0:0:1:" + NOW.plusSeconds(1),
                unkeyed.id() + ":0:0:1:" + NOW.plusSeconds(2),
                followed.id() + ":0:0:1:" + NOW.plusSeconds(3),
                failing.id() + ":0:0:1:" + NOW.plusSeconds(4),
                carried.id() + ":0:0:1:" + NOW.plusSeconds(5),
                poison.id() + ":0:0:1:" + NOW.plusSeconds(6));
        assertThat(retries.size(c)).isZero();
        assertThat(retries.deadSize(c)).isZero();
        var stored = store.due(c, 0, LATER, 10).get(0);
        assertSameRecord(stored, keyed, 0, 1);

        // followed: blocked by a retry head of its key in a retry-pass call; carried: by failing, whose move takes it
        store.apply(c, 0, List.of(new Park(stored, 2, NOW), new Unpark(unkeyed), new Follower(followed),
                        new Head(failing, 2, LATER, "e"), new Follower(carried), new Dead(poison, 1, "boom")),
                Set.of(keyed.id(), unkeyed.id(), followed.id(), failing.id(), carried.id(), poison.id()));

        assertThat(parkedRows()).as("re-parked at the new due, even an earlier one; the follower untouched")
                .containsExactly(keyed.id() + ":0:0:2:" + NOW, followed.id() + ":0:0:1:" + NOW.plusSeconds(3));
        assertThat(retryRows()).containsExactly(
                failing.id() + ":0:2:1:" + LATER + ":e", carried.id() + ":0:0:1:null:null");
        assertThat(deadRow(poison.id())).isEqualTo("0|1|boom|p");
    }

    @Test
    void keepNeverMovesADueTimeEarlier() throws SQLException {
        var late = record("t", "a");
        var early = record("t", "b");
        store.apply(c, 0, List.of(new Park(late, 3, NOW.plusSeconds(10)), new Park(early, 1, NOW)), Set.of());

        store.apply(c, 0, List.of(new Keep(late, NOW.plusSeconds(5)), new Keep(early, NOW.plusSeconds(5))),
                Set.of(late.id(), early.id()));

        assertThat(parkedRows()).containsExactly(
                late.id() + ":0:0:3:" + NOW.plusSeconds(10), early.id() + ":0:0:1:" + NOW.plusSeconds(5));
    }

    @Test
    void unparkOfAMissingRowFails() throws SQLException {
        var parked = record("t", "k");
        var other = record("t", "j");
        store.apply(c, 0, List.of(new Park(parked, 1, NOW), new Park(other, 1, NOW)), Set.of());
        var gone = record("t", "k");

        assertThatThrownBy(() -> store.unpark(c, List.of(gone))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.apply(c, 0, List.of(new Unpark(gone)), Set.of(gone.id())))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.size(c)).isEqualTo(2);

        store.unpark(c, List.of());
        store.unpark(c, List.of(parked));

        assertThat(parkedRows()).containsExactly(other.id() + ":0:0:1:" + NOW);
    }

    @Test
    void toRetryMakesTheRowTheHeadDemotesNewerHeadsAndCarriesNewerParkedRows() throws SQLException {
        var p5 = record(5, "t", "k", 1, 2);
        var p7 = record(7, "t", "k", 2, 3);
        var p9 = record(9, "t", "k", 4, 5);
        var r8 = record(8, "t", "k", 1, 0);
        var r10 = record(10, "t", "k", 0, 0);
        // the same key bytes under another topic and shard, and another key of the topic: none of them move
        var otherTopic = record(11, "u", "k", 0, 0);
        var otherKey = record(12, "t", "j", 0, 0);
        var otherShard = record(13, "t", "k", 0, 0);
        var otherHead = record(14, "t", "j", 1, 0);
        var otherShardHead = record(15, "t", "k", 1, 0);
        store.apply(c, 0, List.of(new Park(p5, 2, NOW), new Park(p7, 3, NOW), new Park(p9, 5, NOW),
                new Park(otherTopic, 1, NOW), new Park(otherKey, 1, NOW)), Set.of());
        store.apply(c, 1, List.of(new Park(otherShard, 1, NOW)), Set.of());
        retries.apply(c, 0, List.of(new Head(r8, 1, NOW, "e8"), new Follower(r10),
                new Head(otherHead, 1, NOW, "ej")));
        retries.apply(c, 1, List.of(new Head(otherShardHead, 1, NOW, "es")));
        var next = NOW.plusSeconds(30);

        store.toRetry(c, 0, p7, 3, next, "e7");

        assertThat(retryRows()).containsExactly(
                "7:0:3:3:" + next + ":e7",
                "8:0:1:0:null:e8",
                "9:0:4:5:null:null",
                "10:0:0:0:null:null",
                "14:0:1:0:" + NOW + ":ej",
                "15:1:1:0:" + NOW + ":es");
        assertThat(parkedIds()).containsExactly(5L, 11L, 12L, 13L);
        assertSameRecord(retries.dueHeads(c, 0, next, 1).get(0), p7, 3, 3);
        assertThat(retries.followers(c, List.of(p7), 10).get(7L)).extracting(OutboxRecord::id)
                .containsExactly(8L, 9L, 10L);
    }

    @Test
    void toRetryOfAnUnkeyedRowMovesOnlyIt() throws SQLException {
        var u = record("t", null);
        var v = record("t", null);
        var w = record("t", null);
        store.apply(c, 0, List.of(new Park(u, 1, NOW), new Park(v, 1, NOW)), Set.of());
        retries.apply(c, 0, List.of(new Head(w, 1, NOW, "ew")));

        store.toRetry(c, 0, u, 1, LATER, "eu");

        assertThat(retryRows()).containsExactly(
                u.id() + ":0:1:1:" + LATER + ":eu", w.id() + ":0:1:0:" + NOW + ":ew");
        assertThat(parkedIds()).containsExactly(v.id());
    }

    @Test
    void toRetryOfAMissingRowFails() throws SQLException {
        var parked = record("t", "k");
        var gone = record("t", "k");
        // rows the statement moves and demotes although the row itself is missing
        var newer = record("t", "k");
        var head = record("t", "k");
        store.apply(c, 0, List.of(new Park(parked, 1, NOW), new Park(newer, 1, NOW)), Set.of());
        retries.apply(c, 0, List.of(new Head(head, 1, NOW, "e")));

        assertThatThrownBy(() -> store.toRetry(c, 0, gone, 1, LATER, "e")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void toDeadAndFromRetryMoveExactlyOneRow() throws SQLException {
        var parked = record("t", "k", List.of(header("h", "v")));
        var newer = record("t", "k");
        var head = record(100, "t", "r", 3, 1);
        var follower = record("t", "r");
        store.apply(c, 0, List.of(new Park(parked, 2, NOW), new Park(newer, 1, NOW)), Set.of());
        retries.apply(c, 1, List.of(new Head(head, 3, NOW, "e"), new Follower(follower)));

        store.toDead(c, parked, 4, "x\u0000y");
        store.fromRetry(c, head, 2, LATER);

        assertThat(deadRow(parked.id())).isEqualTo("0|4|x?y|k");
        try (var ps = c.prepareStatement(
                "select message_id, payload, headers, created_at from " + names.dead() + " where id = ?")) {
            ps.setLong(1, parked.id());
            try (var rs = ps.executeQuery()) {
                rs.next();
                assertThat(rs.getObject(1, UUID.class)).isEqualTo(parked.messageId());
                assertThat(rs.getBytes(2)).isEqualTo(parked.payload());
                assertThat(HeaderCodec.decode(rs.getBytes(3))).isEqualTo(parked.headers());
                assertThat(rs.getObject(4, OffsetDateTime.class).toInstant()).isEqualTo(parked.createdAt());
            }
        }
        assertThat(parkedRows()).as("attempts kept from the retry table").containsExactly(
                newer.id() + ":0:0:1:" + NOW, head.id() + ":1:3:2:" + LATER);
        assertSameRecord(store.due(c, 1, LATER, 10).get(0), head, 3, 2);
        assertThat(retryRows()).containsExactly(follower.id() + ":1:0:0:null:null");

        assertThatThrownBy(() -> store.toDead(c, parked, 1, "e")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.fromRetry(c, head, 3, NOW)).isInstanceOf(IllegalStateException.class);
        assertThat(store.size(c)).isEqualTo(2);
        assertThat(retries.deadSize(c)).isEqualTo(1);
    }

    @Test
    void dueReturnsDueRowsInIdOrderUpToLimit() throws SQLException {
        var a = record("t", "a");
        var b = record("t", "b");
        var cc = record("t", null);
        var otherShard = record("t", "a");
        var e = record("t", "a");
        store.apply(c, 0, List.of(new Park(a, 1, NOW), new Park(b, 1, NOW.plusSeconds(10)),
                new Park(cc, 2, NOW.minusSeconds(1)), new Park(e, 1, NOW.minusSeconds(5))), Set.of());
        store.apply(c, 1, List.of(new Park(otherShard, 1, NOW)), Set.of());

        assertThat(store.due(c, 0, NOW, 10)).extracting(OutboxRecord::id).containsExactly(a.id(), cc.id(), e.id());
        assertThat(store.due(c, 0, NOW, 2)).extracting(OutboxRecord::id).containsExactly(a.id(), cc.id());
        assertThat(store.due(c, 0, LATER, 10)).extracting(OutboxRecord::id)
                .containsExactly(a.id(), b.id(), cc.id(), e.id());
        assertThat(store.due(c, 0, NOW.minusSeconds(6), 10)).isEmpty();
        assertSameRecord(store.due(c, 0, NOW, 10).get(1), cc, 0, 2);
    }

    @Test
    void ofKeysMatchesTopicAndKeyExactlyOldestFirst() throws SQLException {
        var k1 = record("t", "k");
        var j1 = record("t", "j");
        var otherTopic = record("u", "k");
        var k2 = record("t", "k");
        var longerKey = record("t", "kk");
        var unkeyed = record("t", null);
        var otherShard = record("t", "k");
        var k3 = record("t", "k");
        store.apply(c, 0, List.of(new Park(k1, 1, LATER), new Park(j1, 1, LATER), new Park(otherTopic, 1, LATER),
                new Park(k2, 2, LATER), new Park(longerKey, 1, LATER), new Park(unkeyed, 1, LATER),
                new Park(k3, 1, LATER)), Set.of());
        store.apply(c, 1, List.of(new Park(otherShard, 1, LATER)), Set.of());
        var chunkK = record("t", "k");
        var chunkJ = record("t", "j");

        assertThat(store.ofKeys(c, 0, List.of(chunkK, record("t", null)), 10)).as("an unkeyed record matches nothing")
                .extracting(OutboxRecord::id).containsExactly(k1.id(), k2.id(), k3.id());
        assertThat(store.ofKeys(c, 0, List.of(chunkK, record("t", "k")), 2)).as("the two oldest, once each")
                .extracting(OutboxRecord::id).containsExactly(k1.id(), k2.id());
        assertThat(store.ofKeys(c, 0, List.of(chunkJ, chunkK), 10)).extracting(OutboxRecord::id)
                .containsExactly(k1.id(), j1.id(), k2.id(), k3.id());
        assertThat(store.ofKeys(c, 0, List.of(), 10)).isEmpty();
        assertSameRecord(store.ofKeys(c, 0, List.of(chunkK), 10).get(1), k2, 0, 2);
    }

    @Test
    void minDueAndSize() throws SQLException {
        assertThat(store.minDue(c, 0)).isNull();
        assertThat(store.size(c)).isZero();
        store.apply(c, 0, List.of(new Park(record("t", "a"), 1, NOW.plusSeconds(20)),
                new Park(record("t", null), 1, NOW.plusSeconds(10))), Set.of());
        store.apply(c, 1, List.of(new Park(record("t", "c"), 1, NOW)), Set.of());

        assertThat(store.minDue(c, 0)).isEqualTo(NOW.plusSeconds(10));
        assertThat(store.minDue(c, 1)).isEqualTo(NOW);
        assertThat(store.size(c)).isEqualTo(3);
    }

    private void assertSameRecord(OutboxRecord actual, OutboxRecord expected, int attempts, int requeues) {
        assertThat(actual.id()).isEqualTo(expected.id());
        assertThat(actual.messageId()).isEqualTo(expected.messageId());
        assertThat(actual.topic()).isEqualTo(expected.topic());
        assertThat(actual.key()).isEqualTo(expected.key());
        assertThat(actual.payload()).isEqualTo(expected.payload());
        assertThat(actual.headers()).isEqualTo(expected.headers());
        assertThat(actual.createdAt()).isEqualTo(expected.createdAt());
        assertThat(actual.attempts()).isEqualTo(attempts);
        assertThat(actual.requeues()).isEqualTo(requeues);
    }

    /** {@code id:shard:attempts:requeues:due_at} in id order. */
    private List<String> parkedRows() throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(
                "select id, shard, attempts, requeues, due_at from " + names.parked() + " order by id")) {
            var rows = new ArrayList<String>();
            while (rs.next()) {
                rows.add(rs.getLong(1) + ":" + rs.getShort(2) + ":" + rs.getInt(3) + ":" + rs.getInt(4) + ":"
                        + instant(rs.getObject(5, OffsetDateTime.class)));
            }
            return rows;
        }
    }

    private List<Long> parkedIds() throws SQLException {
        return parkedRows().stream().map(r -> Long.parseLong(r.split(":", 2)[0])).toList();
    }

    /** {@code id:shard:attempts:requeues:next_attempt_at:last_error} in id order. */
    private List<String> retryRows() throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select id, shard, attempts, requeues,"
                + " next_attempt_at, last_error from " + names.retry() + " order by id")) {
            var rows = new ArrayList<String>();
            while (rs.next()) {
                rows.add(rs.getLong(1) + ":" + rs.getShort(2) + ":" + rs.getInt(3) + ":" + rs.getInt(4) + ":"
                        + instant(rs.getObject(5, OffsetDateTime.class)) + ":" + rs.getString(6));
            }
            return rows;
        }
    }

    @Nullable
    private static Instant instant(@Nullable OffsetDateTime t) {
        return t == null ? null : t.toInstant();
    }

    /** {@code shard|attempts|error|key}. */
    private String deadRow(long id) throws SQLException {
        try (var ps = c.prepareStatement("select shard, attempts, error, key from " + names.dead() + " where id = ?")) {
            ps.setLong(1, id);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).as("dead row %s", id).isTrue();
                return rs.getShort(1) + "|" + rs.getInt(2) + "|" + rs.getString(3) + "|"
                        + new String(rs.getBytes(4), UTF_8);
            }
        }
    }

    private OutboxRecord record(String topic, @Nullable String key) {
        return record(topic, key, List.of());
    }

    private OutboxRecord record(String topic, @Nullable String key, List<OutboxHeader> headers) {
        return record(++nextId, topic, key, headers, 0, 0);
    }

    private OutboxRecord record(long id, String topic, @Nullable String key, int attempts, int requeues) {
        nextId = Math.max(nextId, id);
        return record(id, topic, key, List.of(), attempts, requeues);
    }

    private static OutboxRecord record(long id, String topic, @Nullable String key, List<OutboxHeader> headers,
            int attempts, int requeues) {
        return new OutboxRecord(id, UUID.randomUUID(), topic, key == null ? null : key.getBytes(UTF_8),
                ("payload" + id).getBytes(UTF_8), headers, NOW.minusSeconds(60), attempts, requeues);
    }
}
