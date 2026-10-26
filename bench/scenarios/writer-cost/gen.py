"""Writes the pgbench scripts of the writer-side cost comparison (RESULTS.md, «Стоимость для писателя»).

gen.py OUT_DIR PARTITION SEQUENCE: PARTITION is the bench_wc partition that holds the current xid and SEQUENCE its
identity sequence (both looked up by writer-cost.sh); they are used by the *_direct variants only.

ours_* use the exact statements PgOutbox emits (keyed send; sendAll = sorted distinct lock statement + one unnest
insert); plain_* are plain inserts into a baseline table. Bound parameters the library sends are literals or
pgbench variables here; arrays that the library builds on the client are built server-side, identically per variant,
and measured alone by gen_* so their cost can be subtracted.
"""
import os
import sys

OUT, PART, SEQ = sys.argv[1], sys.argv[2], sys.argv[3]
OURS = 'bench_wc'
PLAIN = 'bench_wc_plain'
NS = 12345
MID = "'01930000-0000-7000-8000-000000000000'::uuid"  # the library binds a client-made UUIDv7; no index on it
HEADERS = "'\\x" + '61' * 70 + "'::bytea"  # ~ an encoded traceparent header
P300 = "'\\x" + 'ab' * 300 + "'::bytea"
P100 = "'\\x" + 'ab' * 100 + "'::bytea"
N = 10000
KEYS_BYTEA = f"array(select convert_to(gen_random_uuid()::text, 'UTF8') from generate_series(1, {N}))"
KEYS_TEXT = f"array(select gen_random_uuid()::text from generate_series(1, {N}))"
HASHES = f"array(select (random() * 4294967295 - 2147483648)::int from generate_series(1, {N}))"
SHARDS = f"array(select (random() * 3)::smallint from generate_series(1, {N}))"
UNNEST_OURS = (f"unnest({SHARDS}, array_fill({MID}, array[{N}]), array_fill('orders'::text, array[{N}]), {KEYS_BYTEA}, "
               f"array_fill({P100}, array[{N}]), array_fill({HEADERS}, array[{N}])) with ordinality "
               "u(shard, mid, topic, key, payload, headers, ord) order by u.ord")
COLS = "u.shard, u.mid, u.topic, u.key, u.payload, u.headers"

files = {
    # 1 message per transaction (autocommit: one statement = one transaction)
    'ours_1.sql': f"""\\set k random(1, 10000)
\\set s random(0, 31)
insert into {OURS} (shard, message_id, topic, key, payload, headers) select :s, {MID}, 'orders', convert_to('u' || :k, 'UTF8'), {P300}, {HEADERS} from (select pg_advisory_xact_lock({NS}, :k)) l;
""",
    'plain_1.sql': f"""\\set k random(1, 10000)
insert into {PLAIN} (topic, key, payload, headers) values ('orders', 'u' || :k, {P300}, {HEADERS});
""",
    # 10 000 messages per transaction
    'ours_10k.sql': f"""BEGIN;
select pg_advisory_xact_lock({NS}, h) from (select distinct unnest({HASHES}) as h order by 1) s;
insert into {OURS} (shard, message_id, topic, key, payload, headers) select {COLS} from {UNNEST_OURS};
END;
""",
    'plain_10k.sql': f"""BEGIN;
insert into {PLAIN} (topic, key, payload, headers) select u.topic, u.key, u.payload, u.headers from unnest(array_fill('orders'::text, array[{N}]), {KEYS_TEXT}, array_fill({P100}, array[{N}]), array_fill({HEADERS}, array[{N}])) with ordinality u(topic, key, payload, headers, ord) order by u.ord;
END;
""",
    # the server-side array building alone, to subtract
    'gen_ours_10k.sql': f"""select cardinality({HASHES}) + cardinality({SHARDS}) + cardinality({KEYS_BYTEA});
""",
    'gen_plain_10k.sql': f"""select cardinality({KEYS_TEXT});
""",
    'gen_hashes_10k.sql': f"""select cardinality({HASHES});
""",
    # decomposition of ours
    'ours_1_nolock.sql': f"""\\set k random(1, 10000)
\\set s random(0, 31)
insert into {OURS} (shard, message_id, topic, key, payload, headers) values (:s, {MID}, 'orders', convert_to('u' || :k, 'UTF8'), {P300}, {HEADERS});
""",
    'ours_1_direct.sql': f"""\\set k random(1, 10000)
\\set s random(0, 31)
insert into {PART} (id, shard, message_id, topic, key, payload, headers) values (nextval('{SEQ}'), :s, {MID}, 'orders', convert_to('u' || :k, 'UTF8'), {P300}, {HEADERS});
""",
    'ours_10k_nolock.sql': f"""BEGIN;
insert into {OURS} (shard, message_id, topic, key, payload, headers) select {COLS} from {UNNEST_OURS};
END;
""",
    'ours_10k_direct.sql': f"""BEGIN;
insert into {PART} (id, shard, message_id, topic, key, payload, headers) select nextval('{SEQ}'), {COLS} from {UNNEST_OURS};
END;
""",
    'lock_10k.sql': f"""BEGIN;
select pg_advisory_xact_lock({NS}, h) from (select distinct unnest({HASHES}) as h order by 1) s;
END;
""",
}
os.makedirs(OUT, exist_ok=True)
for name, text in files.items():
    with open(os.path.join(OUT, name), 'w') as f:
        f.write(text)
print(' '.join(files))
