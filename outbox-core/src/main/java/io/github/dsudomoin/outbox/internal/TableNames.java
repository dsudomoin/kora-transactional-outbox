package io.github.dsudomoin.outbox.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Every SQL identifier the library interpolates, derived from one validated base name. The 41-char limit keeps
 * {@code <base>_x<20-digit xid>} within Postgres' 63-byte identifier limit. The base is lower-cased, as Postgres
 * folds unquoted identifiers, so {@code Outbox} and {@code outbox} share tables. Lock namespaces must come from
 * {@link #resolve}: {@link #of} hashes the spelling, and {@code outbox} and {@code public.outbox} are one table.
 */
public record TableNames(
        String base,
        String tick,
        String cursor,
        String retry,
        String dead,
        String halt,
        String tail,
        String partitionPrefix,
        int keyNamespace,
        int internalNamespace) {

    public static final int LOCK_SCHEMA = 1;
    public static final int LOCK_TICK = 2;
    public static final int LOCK_MAINTENANCE = 3;
    public static final int LOCK_INBOX_MAINTENANCE = 4;
    public static final int LOCK_SWAP = 5;
    /** The dead table's index build, {@link DeadIndexBuilder}: a session lock, only ever tried. */
    public static final int LOCK_DEAD_INDEX = 6;
    public static final int LOCK_SHARD_BASE = 1000;

    private static final Pattern NAME =
            Pattern.compile("(?:[A-Za-z_][A-Za-z0-9_]{0,40}\\.)?[A-Za-z_][A-Za-z0-9_]{0,40}");

    public TableNames {
        requireValid(base);
    }

    public static TableNames of(String base) {
        base = requireValid(base).toLowerCase(Locale.ROOT);
        return withLocksOf(base, base);
    }

    /**
     * These names with the lock namespaces of the table they denote on {@code c}, hashed from its canonical
     * {@code schema.name}, so every spelling of one table shares its locks. An unqualified name denotes the table in
     * {@code current_schema()}, where {@code CREATE TABLE} puts it; once the table exists, {@code to_regclass} must
     * resolve the name to that same table. Not the oid: a dump and restore changes it.
     */
    public TableNames resolve(Connection c) throws SQLException {
        try (var ps = c.prepareStatement("select current_schema(), (select n.nspname || '.' || r.relname"
                + " from pg_class r join pg_namespace n on n.oid = r.relnamespace where r.oid = to_regclass(?))")) {
            ps.setString(1, base);
            try (var rs = ps.executeQuery()) {
                rs.next();
                String schema = rs.getString(1);
                String existing = rs.getString(2);
                if (base.indexOf('.') < 0 && schema == null) {
                    throw new IllegalStateException("table " + base + " is unqualified, but the search_path names"
                            + " no existing schema; qualify it as schema." + base);
                }
                String canonical = base.indexOf('.') < 0 ? schema + "." + base : base;
                if (existing != null && !existing.equals(canonical)) {
                    throw new IllegalStateException("table " + base + " resolves to " + existing + ", but it would be"
                            + " created as " + canonical + ", in current_schema(): the table exists only in a later"
                            + " search_path schema, or a temporary table of that name shadows it; qualify the name as"
                            + " schema." + unqualified(base) + " or fix the search_path");
                }
                return withLocksOf(base, canonical);
            }
        }
    }

    private static TableNames withLocksOf(String base, String lockName) {
        int keyNamespace = ("outbox-key:" + lockName).hashCode();
        int internalNamespace = ("outbox-internal:" + lockName).hashCode();
        if (internalNamespace == keyNamespace) {
            internalNamespace++;
        }
        return new TableNames(
                base,
                base + "_tick",
                base + "_cursor",
                base + "_retry",
                base + "_dead",
                base + "_halt",
                base + "_tail",
                base + "_x",
                keyNamespace,
                internalNamespace);
    }

    /** One row: when a maintenance run last completed every step, on any instance. */
    public String maintenance() {
        return base + "_maintenance";
    }

    /** Maintenance's checkpoint: the latest tick of its run and the shards busy at it ({@link Cursors}). */
    public String checkpoint() {
        return base + "_checkpoint";
    }

    /** One row per running instance, upserted by its {@link Heartbeat}. */
    public String instance() {
        return base + "_instance";
    }

    /** Requeued records, waiting for the next record of their key or their due time; they block nothing. */
    public String parked() {
        return base + "_parked";
    }

    /** The ranges maintenance has decided to drop as relayed, from just before their detach until their drop. */
    public String dropping() {
        return base + "_dropping";
    }

    /** How many times each message was redriven, kept for {@code outbox.deadLetters.redriveHistory}. */
    public String redriven() {
        return base + "_redriven";
    }

    /**
     * {@code <base>_o<digits>}: a range the xid-regression recovery set aside ({@link XidRecovery}), as long as a
     * {@link #partitionPrefix()} name.
     */
    public String setAsidePrefix() {
        return base + "_o";
    }

    /** The next tail, created beside the current one before the swap that replaces it. */
    public String tailNext() {
        return base + "_tail_next";
    }

    public String unqualified(String name) {
        return name.substring(name.indexOf('.') + 1);
    }

    private static String requireValid(String base) {
        if (base == null || !NAME.matcher(base).matches()) {
            throw new IllegalArgumentException(
                    "table name must match [schema.]name with parts [A-Za-z_][A-Za-z0-9_]{0,40}: " + base);
        }
        return base;
    }
}
