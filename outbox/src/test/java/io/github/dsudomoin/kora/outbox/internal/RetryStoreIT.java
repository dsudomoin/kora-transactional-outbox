package io.github.dsudomoin.kora.outbox.internal;

import static io.github.dsudomoin.kora.outbox.testing.TestTracing.header;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.kora.outbox.OutboxHeader;
import io.github.dsudomoin.kora.outbox.OutboxRecord;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Follower;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Head;
import io.github.dsudomoin.kora.outbox.internal.BatchClassifier.Park;
import io.github.dsudomoin.kora.outbox.testing.TestDb;
import io.github.dsudomoin.kora.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

class RetryStoreIT {

    /** Postgres keeps microseconds. */
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);
    /** After every due time the tests write. */
    private static final Instant LATER = NOW.plus(Duration.ofMinutes(10));

    private final JdbcDatabase db = TestDb.newDatabase(2);
    private TableNames names;
    private RetryStore store;
    private Connection c;
    private long nextId;

    @BeforeEach
    void bootstrap() throws Exception {
        var schema = new OutboxSchema(db, new TestOutboxConfig().shards(2));
        schema.init();
        names = schema.names();
        store = new RetryStore(names);
        c = TestDb.connect();
    }

    @AfterEach
    void release() throws SQLException {
        c.close();
        db.release();
    }

    @Test
    void applyThenDueHeadsReturnsOnlyDueHeadsInIdOrder() throws SQLException {
        var unkeyedHead = record("u", null, List.of(header("h", "v"), new OutboxHeader("null", null)));
        var keyedHead = record("t", "k");
        var follower = record("t", "k");
        var notDue = record("t", "k2");
        var dead = record("t", "k3");
        var otherShard = record("t", "k");
        store.apply(c, 0, List.of(
                new Head(keyedHead, 1, NOW.minusSeconds(1), "e1"),
                new Head(unkeyedHead, 2, NOW, "e2"),
                new Follower(follower),
                new Head(notDue, 1, NOW.plusSeconds(1), "e3"),
                new Dead(dead, 1, "boom")));
        store.apply(c, 1, List.of(new Head(otherShard, 1, NOW, "e")));

        var due = store.dueHeads(c, 0, NOW, 10);

        assertThat(due).extracting(OutboxRecord::id).containsExactly(unkeyedHead.id(), keyedHead.id());
        assertSameRecord(due.get(0), unkeyedHead, 2);
        assertSameRecord(due.get(1), keyedHead, 1);
        assertThat(store.dueHeads(c, 0, NOW, 1)).extracting(OutboxRecord::id).containsExactly(unkeyedHead.id());
        assertThat(store.dueHeads(c, 0, LATER, 10)).extracting(OutboxRecord::id)
                .containsExactly(unkeyedHead.id(), keyedHead.id(), notDue.id());
        assertThat(store.size(c)).isEqualTo(5);
        assertThat(store.deadSize(c)).isEqualTo(1);
        assertThat(retryRow(follower.id())).isEqualTo("0|null|null");
        assertThat(retryRow(keyedHead.id())).isEqualTo("1|" + NOW.minusSeconds(1) + "|e1");
        assertThat(deadRow(dead.id())).isEqualTo("0|1|boom|k3");
    }

    @Test
    void followersOfEachKeyedHeadInIdOrderUpToPerKey() throws SQLException {
        var a = record("t", "a");
        var b = record("t", "b");
        var otherTopic = record("u", "a");
        var a1 = record("t", "a");
        var otherShard = record("t", "a");
        var b1 = record("t", "b");
        var a2 = record("t", "a");
        var unkeyed = record("t", null);
        var a3 = record("t", "a");
        store.apply(c, 0, List.of(new Head(a, 1, NOW, "e"), new Head(b, 1, NOW, "e"), new Head(otherTopic, 1, NOW,
                "e"), new Follower(a1), new Follower(b1), new Follower(a2), new Head(unkeyed, 1, NOW, "e"),
                new Follower(a3)));
        store.apply(c, 1, List.of(new Follower(otherShard)));

        var followers = store.followers(c, List.of(a, b, unkeyed), 2);

        assertThat(followers).containsOnlyKeys(a.id(), b.id());
        assertThat(followers.get(a.id())).extracting(OutboxRecord::id).containsExactly(a1.id(), a2.id());
        assertThat(followers.get(b.id())).extracting(OutboxRecord::id).containsExactly(b1.id());
        assertSameRecord(followers.get(b.id()).get(0), b1, 0);
        assertThat(store.followers(c, List.of(b1), 5)).as("only rows after the given one").isEmpty();
    }

    @Test
    void deleteThenPromoteMakesTheNextRowTheHead() throws SQLException {
        // lower ids than f1: promoting by anything less than (shard, topic, key) picks one of them
        var otherTopicFollower = record("u", "k");
        var otherKeyFollower = record("t", "k2");
        var otherShardFollower = record("t", "k");
        var head = record("t", "k");
        var f1 = record("t", "k");
        var f2 = record("t", "k");
        var unkeyed = record("t", null);
        store.apply(c, 1, List.of(new Follower(otherShardFollower)));
        store.apply(c, 0, List.of(
                new Follower(otherTopicFollower),
                new Follower(otherKeyFollower),
                new Head(head, 1, NOW, "e"),
                new Follower(f1),
                new Follower(f2),
                new Head(unkeyed, 1, NOW, "e")));

        store.delete(c, store.dueHeads(c, 0, NOW, 1));
        assertThat(store.promote(c, 0, head, NOW)).isTrue();

        assertThat(store.dueHeads(c, 0, NOW, 10)).as("promoted at the caller's now")
                .extracting(OutboxRecord::id).containsExactly(f1.id(), unkeyed.id());
        assertThat(store.dueHeads(c, 0, NOW, 10).get(0).attempts()).isZero();
        assertThat(retryRow(f1.id())).isEqualTo("0|" + NOW + "|null");
        assertThat(store.dueHeads(c, 1, LATER, 10)).isEmpty();
        assertThat(retryRow(f2.id())).startsWith("0|null|");

        store.delete(c, List.of(unkeyed));
        assertThat(store.promote(c, 0, unkeyed, NOW)).as("nothing follows an unkeyed row").isFalse();
        assertThat(store.size(c)).isEqualTo(5);
        assertThat(store.dueHeads(c, 0, LATER, 10)).extracting(OutboxRecord::id).containsExactly(f1.id());
    }

    @Test
    void deadFromRetryMovesTheRowAndPromoteFindsTheNext() throws SQLException {
        var head = record("t", "k", List.of(header("h", "v")));
        var follower = record("t", "k");
        store.apply(c, 1, List.of(new Head(head, 2, NOW, "e"), new Follower(follower)));

        store.deadFromRetry(c, store.dueHeads(c, 1, NOW, 10).get(0), 3, "gave up");
        assertThat(store.dueHeads(c, 1, LATER, 10)).as("not promoted by itself").isEmpty();
        assertThat(store.promote(c, 1, head, NOW)).isTrue();
        assertThat(store.promote(c, 0, head, NOW)).as("another shard").isFalse();

        assertThat(store.size(c)).isEqualTo(1);
        assertThat(store.deadSize(c)).isEqualTo(1);
        assertThat(deadRow(head.id())).isEqualTo("1|3|gave up|k");
        assertThat(store.dueHeads(c, 1, NOW, 10)).extracting(OutboxRecord::id).containsExactly(follower.id());
        try (var ps = c.prepareStatement(
                "select message_id, topic, payload, headers, created_at from " + names.dead() + " where id = ?")) {
            ps.setLong(1, head.id());
            try (var rs = ps.executeQuery()) {
                rs.next();
                assertThat(rs.getObject(1, UUID.class)).isEqualTo(head.messageId());
                assertThat(rs.getString(2)).isEqualTo("t");
                assertThat(rs.getBytes(3)).isEqualTo(head.payload());
                assertThat(HeaderCodec.decode(rs.getBytes(4))).isEqualTo(head.headers());
                assertThat(rs.getObject(5, OffsetDateTime.class).toInstant()).isEqualTo(head.createdAt());
            }
        }
    }

    @Test
    void rescheduleUpdatesAttemptsAndDue() throws SQLException {
        var head = record("t", "k");
        store.apply(c, 0, List.of(new Head(head, 1, NOW, "first")));
        var next = NOW.plusSeconds(30);

        store.reschedule(c, store.dueHeads(c, 0, NOW, 10).get(0), 4, next, "second");

        assertThat(store.dueHeads(c, 0, NOW, 10)).isEmpty();
        var due = store.dueHeads(c, 0, next, 10);
        assertThat(due).extracting(OutboxRecord::id).containsExactly(head.id());
        assertThat(due.get(0).attempts()).isEqualTo(4);
        assertThat(retryRow(head.id())).isEqualTo("4|" + next + "|second");
    }

    @Test
    void nullErrorKeepsLastError() throws SQLException {
        var head = record("t", "k");
        var other = record("t", "j");
        store.apply(c, 0, List.of(new Head(head, 1, NOW, "first"), new Head(other, 2, NOW, "other")));

        store.reschedule(c, head, 1, LATER, null);
        store.deadFromRetry(c, other, 2, null);

        assertThat(retryRow(head.id())).isEqualTo("1|" + LATER + "|first");
        assertThat(deadRow(other.id())).isEqualTo("0|2|other|j");
    }

    @Test
    void minDue() throws SQLException {
        assertThat(store.minDue(c, 0)).isNull();
        store.apply(c, 0, List.of(
                new Follower(record("t", "f")),
                new Head(record("t", "a"), 1, NOW.plusSeconds(20), "e"),
                new Head(record("t", "b"), 1, NOW.plusSeconds(10), "e")));
        store.apply(c, 1, List.of(new Head(record("t", "c"), 1, NOW, "e")));

        assertThat(store.minDue(c, 0)).isEqualTo(NOW.plusSeconds(10));
        assertThat(store.minDue(c, 1)).isEqualTo(NOW);
    }

    @Test
    void errorTextIsTruncatedAndNulFree() throws SQLException {
        var head = record("t", "k");
        var dead = record("t", "d");
        var longError = "x\u0000y" + "z".repeat(3000);
        store.apply(c, 0, List.of(new Head(head, 1, NOW, longError), new Dead(dead, 1, longError)));
        assertThat(lastError(head.id())).hasSize(2000).startsWith("x?yzzz");
        store.reschedule(c, head, 2, NOW, longError.replace("x", "w"));
        assertThat(lastError(head.id())).as("reschedule").hasSize(2000).startsWith("w?yzzz");
        store.deadFromRetry(c, head, 3, longError);

        try (var st = c.createStatement(); var rs = st.executeQuery("select error from " + names.dead())) {
            for (int i = 0; i < 2; i++) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).hasSize(2000).startsWith("x?yzzz");
            }
        }
    }

    @Test
    void missingHeadFailsLoudly() throws SQLException {
        var head = record("t", "k");
        store.apply(c, 0, List.of(new Head(head, 1, NOW, "e")));
        var gone = record("t", "k");

        assertThatThrownBy(() -> store.delete(c, List.of(gone))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.deadFromRetry(c, gone, 2, "e")).isInstanceOf(IllegalStateException.class);
        assertThat(store.size(c)).isEqualTo(1);
        assertThat(store.deadSize(c)).isZero();
    }

    @Test
    void applyRejectsActionsOfOtherTables() throws SQLException {
        var head = new Head(record("t", "k"), 1, NOW, "e");
        var park = new Park(record("t", "k2"), 1, NOW);

        assertThatThrownBy(() -> store.apply(c, 0, List.of(head, park))).isInstanceOf(IllegalStateException.class);
        assertThat(store.size(c)).as("nothing written").isZero();
    }

    private String lastError(long id) throws SQLException {
        return retryRow(id).split("\\|", 3)[2];
    }

    private void assertSameRecord(OutboxRecord actual, OutboxRecord expected, int attempts) {
        assertThat(actual.messageId()).isEqualTo(expected.messageId());
        assertThat(actual.topic()).isEqualTo(expected.topic());
        assertThat(actual.key()).isEqualTo(expected.key());
        assertThat(actual.payload()).isEqualTo(expected.payload());
        assertThat(actual.headers()).isEqualTo(expected.headers());
        assertThat(actual.createdAt()).isEqualTo(expected.createdAt());
        assertThat(actual.attempts()).isEqualTo(attempts);
    }

    /** {@code attempts|next_attempt_at|last_error}. */
    private String retryRow(long id) throws SQLException {
        try (var ps = c.prepareStatement(
                "select attempts, next_attempt_at, last_error from " + names.retry() + " where id = ?")) {
            ps.setLong(1, id);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).as("retry row %s", id).isTrue();
                var next = rs.getObject(2, OffsetDateTime.class);
                return rs.getInt(1) + "|" + (next == null ? null : next.toInstant()) + "|" + rs.getString(3);
            }
        }
    }

    /** {@code shard|attempts|error|key}, failed_at must be set. */
    private String deadRow(long id) throws SQLException {
        try (var ps = c.prepareStatement(
                "select shard, attempts, error, key, failed_at is not null from " + names.dead() + " where id = ?")) {
            ps.setLong(1, id);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).as("dead row %s", id).isTrue();
                assertThat(rs.getBoolean(5)).isTrue();
                return rs.getShort(1) + "|" + rs.getInt(2) + "|" + rs.getString(3) + "|"
                        + new String(rs.getBytes(4), UTF_8);
            }
        }
    }

    private OutboxRecord record(String topic, @Nullable String key) {
        return record(topic, key, List.of());
    }

    private OutboxRecord record(String topic, @Nullable String key, List<OutboxHeader> headers) {
        long id = ++nextId;
        return new OutboxRecord(id, UUID.randomUUID(), topic, key == null ? null : key.getBytes(UTF_8),
                ("payload" + id).getBytes(UTF_8), headers, NOW.minusSeconds(60), 0, 0);
    }
}
