package io.github.dsudomoin.kora.outbox.internal;

import io.github.dsudomoin.kora.outbox.DeadLetter;
import io.github.dsudomoin.kora.outbox.OutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Dead rows as {@link DeadLetter}s and their redrive: what {@link DeadLetterPass} and {@link PgOutboxAdmin} share. */
final class DeadLetters {

    /** The columns {@link #read} maps, in its order. */
    static final String COLUMNS = "id, message_id, topic, key, payload, headers, created_at, attempts,"
            + " failed_at, error, status, redrives, handler_attempts, due_at";

    /** A letter with its headers in the stored encoding, as a redrive inserts them. */
    record Claimed(DeadLetter letter, @Nullable byte[] headers) {}

    /** Letters by their key lock ({@link #tryKeyLocks}): ready to redrive, or held by another transaction. */
    record Split(List<Claimed> ready, List<Claimed> held) {}

    private final Resender resender;
    private final int keyNamespace;
    private final String lockTimeout;
    private final String heldKeys;
    private final String remember;
    private final String delete;

    DeadLetters(TableNames names, OutboxConfig config) {
        this.resender = new Resender(names, config);
        this.keyNamespace = names.keyNamespace();
        this.lockTimeout = "set local lock_timeout = '" + config.partitions().swapLockTimeout().toMillis() + "ms'";
        // one row whatever the driver's fetch size, so every hash is tried (see Resender.LOCK_ALL); never waits
        this.heldKeys = "select array_agg(h) filter (where not pg_try_advisory_xact_lock(?, h))"
                + " from unnest(?::int[]) h";
        // one row per message, should two of its letters be redriven together
        this.remember = "insert into " + names.redriven() + " (message_id, redrives)"
                + " select m, max(r) from unnest(?::uuid[], ?::int[]) u(m, r) group by m"
                + " on conflict (message_id) do update set redrives = excluded.redrives, at = now()";
        this.delete = "delete from " + names.dead() + " where id = any(?)";
    }

    /** The row at {@code rs}, selected as {@link #COLUMNS}. */
    static Claimed read(ResultSet rs) throws SQLException {
        var headers = rs.getBytes(6);
        var dueAt = rs.getObject(14, OffsetDateTime.class);
        return new Claimed(new DeadLetter(
                rs.getLong(1),
                rs.getObject(2, UUID.class),
                rs.getString(3),
                rs.getBytes(4),
                rs.getBytes(5),
                HeaderCodec.decode(headers),
                rs.getObject(7, OffsetDateTime.class).toInstant(),
                rs.getInt(8),
                rs.getObject(9, OffsetDateTime.class).toInstant(),
                rs.getString(10),
                DeadLetter.Status.valueOf(rs.getString(11).toUpperCase(Locale.ROOT)),
                rs.getInt(12),
                rs.getInt(13),
                dueAt == null ? null : dueAt.toInstant()), headers);
    }

    /**
     * Tries the key lock of each distinct key without waiting ({@code pg_try_advisory_xact_lock}, held until
     * {@code tx} ends): the unkeyed letters and those whose lock {@code tx} now holds are ready, those whose key
     * another transaction holds, an open {@code send} of it, are held.
     */
    Split tryKeyLocks(Connection tx, List<Claimed> letters) throws SQLException {
        var hashes = letters.stream()
                .map(c -> c.letter().key() == null ? null : Hashing.keyHash(c.letter().topic(), c.letter().key()))
                .toList();
        var held = held(tx, hashes);
        var ready = new ArrayList<Claimed>();
        var waiting = new ArrayList<Claimed>();
        for (int i = 0; i < letters.size(); i++) {
            (hashes.get(i) != null && held.contains(hashes.get(i)) ? waiting : ready).add(letters.get(i));
        }
        return new Split(ready, waiting);
    }

    /** Tries the key lock of each distinct hash without waiting; returns the hashes another transaction holds. */
    private Set<Integer> held(Connection tx, List<Integer> hashes) throws SQLException {
        var distinct = hashes.stream().filter(Objects::nonNull).distinct().toArray(Integer[]::new);
        if (distinct.length == 0) {
            return Set.of();
        }
        try (var ps = tx.prepareStatement(heldKeys)) {
            ps.setInt(1, keyNamespace);
            ps.setArray(2, tx.createArrayOf("int4", distinct));
            try (var rs = ps.executeQuery()) {
                rs.next();
                var held = rs.getArray(1);
                return held == null ? Set.of() : Set.of((Integer[]) held.getArray());
            }
        }
    }

    /**
     * Back into the outbox ({@link Resender}), the redrive counted in {@code <redriven>}, the dead rows deleted. Their
     * key locks are {@code tx}'s already ({@link #tryKeyLocks}), so taken again without waiting; any other lock the
     * insert waits for is waited for at most {@code partitions.swapLockTimeout} each.
     */
    void redrive(Connection tx, List<Claimed> letters) throws SQLException {
        if (letters.isEmpty()) {
            return;
        }
        limitLockWaits(tx); // after the claim: the insert waits for a lock no longer than the swap
        resender.insertAll(tx, letters.stream().map(c -> new Resender.Row(c.letter().messageId(),
                c.letter().topic(), c.letter().key(), c.letter().payload(), c.headers())).toList());
        try (var ps = tx.prepareStatement(remember)) {
            ps.setArray(1, tx.createArrayOf("uuid", letters.stream().map(c -> c.letter().messageId()).toArray()));
            ps.setArray(2, tx.createArrayOf("int4", letters.stream().map(c -> c.letter().redrives() + 1).toArray()));
            ps.executeUpdate();
        }
        delete(tx, letters.stream().map(c -> c.letter().id()).toList());
    }

    /** Each lock wait of the rest of {@code tx} at most {@code partitions.swapLockTimeout}, as a writer's at a swap. */
    void limitLockWaits(Connection tx) throws SQLException {
        try (var st = tx.createStatement()) {
            st.execute(lockTimeout);
        }
    }

    void delete(Connection tx, List<Long> ids) throws SQLException {
        byIds(tx, delete, ids);
    }

    /** {@code sql} with the one parameter {@code ids}, a bigint array; nothing when there are none. */
    static void byIds(Connection tx, String sql, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        try (var ps = tx.prepareStatement(sql)) {
            ps.setArray(1, tx.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        }
    }
}
