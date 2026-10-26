#!/usr/bin/env bash
# writer-cost: our enqueue statements against a plain insert into a baseline table, with pgbench inside the
# bench Postgres container (RESULTS.md, «Стоимость для писателя»). Nothing it does is destructive outside its own tables, and
# it acts only on the bench stack. Everything it does:
#  - starts the bench app once (consumer off, noop handler) and stops it by its recorded PID; at startup the app
#    creates the outbox table bench_wc (the library), the inbox if missing, the business tables orders, user_seq,
#    bench_rolled_back and bench_consumed if missing, and the Kafka topic `orders` if missing (client API);
#  - creates bench_wc_plain; truncates bench_wc and bench_wc_plain before each run; `drop` drops both;
#  - in the Postgres container (kora-outbox-bench-postgres-1): runs pgbench, a server-wide CHECKPOINT and
#    pg_stat_statements_reset() before each run, and writes the generated SQL files to /tmp/bench-writer-cost there
#    (left in place after `drop`; the generated copies on the host are in bench/build/scenarios/writer-cost);
#  - posts one Grafana annotation per run.
#   writer-cost.sh all                          setup, every run of the RESULTS section in its order, drop
#   writer-cost.sh setup | drop
#   writer-cost.sh run NAME CLIENTS SECONDS     one variant (gen.py lists them) on freshly truncated tables
# Each run is a Grafana annotation tagged writer-cost; per-statement timings come from pg_stat_statements.
source "$(dirname "$0")/../lib.sh"
W=$(cd "$(dirname "$0")" && pwd)
OUT=$LOGS/writer-cost # generated SQL on the host
IN=/tmp/bench-writer-cost # the same files inside the container

setup() {
  drop
  app_start writer-cost CONSUMER_THREADS=0 BENCH_HANDLER=noop OUTBOX_TABLE=bench_wc
  app_stop
  psql_q "create table bench_wc_plain (id bigserial primary key, xid xid8 not null default pg_current_xact_id(),
      topic text, key text, payload bytea, headers bytea, created_at timestamptz default now());
    create index bench_wc_plain_xid on bench_wc_plain (xid);"
}

# the partition holding the next xid and the identity sequence, for the *_direct variants; then (re)generates the SQL
generate() {
  local part seq
  part=$(psql_q "select c.relname from pg_inherits i join pg_class c on c.oid = i.inhrelid,
      lateral (select substring(pg_get_expr(c.relpartbound, c.oid) from \$\$FROM \\('(\\d+)'\\)\$\$)::numeric lo,
        substring(pg_get_expr(c.relpartbound, c.oid) from \$\$TO \\('(\\d+)'\\)\$\$)::numeric hi) b,
      lateral (select pg_snapshot_xmax(pg_current_snapshot())::text::numeric x) n
    where i.inhparent = 'bench_wc'::regclass and n.x >= b.lo and n.x < b.hi")
  seq=$(psql_q "select pg_get_serial_sequence('bench_wc', 'id')")
  [ -n "$part" ] && [ -n "$seq" ] || { log "bench_wc missing: run setup first"; exit 1; }
  python3 "$W/gen.py" "$OUT" "$part" "$seq" > /dev/null
}

run_variant() { # run_variant NAME CLIENTS SECONDS
  generate
  psql_q "truncate bench_wc; truncate bench_wc_plain; checkpoint" > /dev/null
  docker exec "$PG" mkdir -p "$IN"
  docker exec -i "$PG" sh -c "cat > $IN/$1.sql" < "$OUT/$1.sql"
  psql_q "select pg_stat_statements_reset()" > /dev/null
  local id; id=$(mark writer-cost "writer cost: pgbench $1 ($2 clients, $3 s)")
  bash "$W/run.sh" "$IN" "$1" "$2" "$3"
  mark_end "$id"
  psql_q "select calls, round(mean_exec_time::numeric, 3) as mean_ms, round(wal_bytes / calls) as wal_per_call,
      left(regexp_replace(query, '\s+', ' ', 'g'), 60) from pg_stat_statements
    where query ~* '^(insert|select (pg_advisory|cardinality))' order by total_exec_time desc limit 3"
}

drop() { psql_q "drop table if exists bench_wc_plain" > /dev/null; drop_outbox bench_wc; }

case ${1:-} in
setup) setup ;;
drop) drop ;;
run) run_variant "$2" "$3" "$4" ;;
all)
  setup
  run_variant ours_1 32 60; run_variant plain_1 32 60
  run_variant ours_10k 2 60; run_variant plain_10k 2 60
  run_variant gen_ours_10k 2 30; run_variant gen_plain_10k 2 30
  for v in ours_10k plain_10k ours_10k_nolock lock_10k gen_hashes_10k gen_ours_10k gen_plain_10k ours_10k_direct; do
    run_variant "$v" 2 30
  done
  for v in ours_1_nolock ours_1_direct plain_1; do run_variant "$v" 32 60; done
  drop
  ;;
*) sed -n '2,8p' "$0"; exit 2 ;;
esac
