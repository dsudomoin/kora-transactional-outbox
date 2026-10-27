#!/usr/bin/env bash
# shard-cost: what the shard count costs at the library defaults (the §10.2 criteria of the zero-touch operations
# design), for the shard counts in the arguments or RUNS (default 32), each phase on a fresh outbox table. First the
# quiet phases: the stand's background with the app stopped, then idle (relay on, no load, no consumer) for every shard
# count, so no idle window follows a load phase (autovacuum, vacuum after drops and full-page writes of a load run would
# land in it). Then per shard count the load phases: bulk (2 writers x 10 000 messages, noop handler, no consumer) and
# low-load at 100 and 1000 msg/s (order mode, Kafka handler, consumer 12 threads, as low-load.sh). The relay at the
# library defaults: workers 4 and batchSize 1000 set (the bench app's own defaults are 8 and 2000), every other outbox
# setting unset. Each phase: WARMUP s, then a STEADY s window. PHASES (default "background idle load") picks the
# phases to run. WAL_SPLIT=1 splits each idle window's WAL with pg_waldump (wal_split below). Appends its section (in
# Russian) to RESULTS.
source "$(dirname "$0")/lib.sh"
WARMUP=${WARMUP:-60}
STEADY=${STEADY:-180}
RUNS=${*:-${RUNS:-32}}
PHASES=${PHASES:-background idle load}
WAL_SPLIT=${WAL_SPLIT:-}
unset OUTBOX_SHARDS OUTBOX_TABLE OUTBOX_RELAY_WORKERS OUTBOX_RELAY_BATCH_SIZE OUTBOX_RELAY_MAX_TICKS \
  OUTBOX_RELAY_MIN_TICK OUTBOX_RELAY_IDLE_POLL OUTBOX_XIDS_PER_PARTITION OUTBOX_MAINTENANCE_INTERVAL
DEFAULTS=(OUTBOX_RELAY_WORKERS=4 OUTBOX_RELAY_BATCH_SIZE=1000)

# pg_now: "next-xid WAL-bytes commits" now: the next xid of the cluster (consuming none), the WAL position and the
# database's committed transactions (read-only ones included).
pg_now() {
  psql_q "select pg_snapshot_xmax(pg_current_snapshot()), pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint,
    (select xact_commit from pg_stat_database where datname = current_database())" | tr '|' ' '
}

# counters_start / counters_stop: the pg_now deltas over the window as XID_S WAL_S COMMIT_S (per second), WAL_B (bytes).
counters_start() { read -r X0 W0 C0 <<< "$(pg_now)"; T0=$(now); }
counters_stop() {
  local x1 w1 c1
  read -r x1 w1 c1 <<< "$(pg_now)"; T1=$(now)
  WAL_B=$((w1 - W0))
  XID_S=$(calc "($x1 - $X0) / ($T1 - $T0)")
  WAL_S=$(calc "$WAL_B / ($T1 - $T0)")
  COMMIT_S=$(calc "($c1 - $C0) / ($T1 - $T0)")
}

# sizes BASE: "partitions rest" in bytes: the partitions of BASE, and its tick, cursor, retry, dead and (once they
# exist) parked and checkpoint tables.
sizes() {
  psql_q "select (select coalesce(sum(pg_total_relation_size(relid)), 0) from pg_partition_tree('$1')),
    pg_total_relation_size('${1}_tick') + pg_total_relation_size('${1}_cursor')
      + pg_total_relation_size('${1}_retry') + pg_total_relation_size('${1}_dead')
      + coalesce(pg_total_relation_size(to_regclass('${1}_parked')), 0)
      + coalesce(pg_total_relation_size(to_regclass('${1}_checkpoint')), 0)" | tr '|' ' '
}
mb() { f1 "$(calc "$1 / 1048576")"; }

drop_all() { drop_outbox "$1"; psql_q "drop table if exists ${1}_parked" > /dev/null; }

# analyzed: "relname:autoanalyze_count:autovacuum_count" of every user table, one per line, for a before/after diff.
analyzed() {
  psql_q "select relname || ':' || autoanalyze_count || ':' || autovacuum_count from pg_stat_user_tables order by 1"
}

# wal_split BASE LSN0 LSN1 SECONDS BEFORE: the WAL of [LSN0, LSN1) by pg_waldump (read-only, in the Postgres container)
# as "outbox catalog rest" B/s. outbox: records on a relfilenode of BASE's objects (tables, partitions, indexes,
# sequences and their TOAST tables and TOAST indexes) and the commit records of the transactions that wrote them;
# catalog: records on pg_catalog relations (with their TOAST); rest: everything else. Sets WAL_AUTO to the user tables
# autovacuum analysed or vacuumed between BEFORE (an analyzed snapshot) and now, as "name +analyses/+vacuums".
WAL_SPLIT_PY='
import re, sys
own, cat, secs = set(sys.argv[1].split()), set(sys.argv[2].split()), float(sys.argv[3])
recs, xids = [], set()
for line in sys.stdin:
    m = re.match(r"rmgr: (\S+)\s+len \(rec/tot\):\s+\d+/\s*(\d+), tx:\s+(\d+), .*?desc: \S+(.*)", line)
    if m:
        rels = re.findall(r"rel \d+/\d+/(\d+)", m.group(4))
        recs.append((m.group(1), int(m.group(2)), int(m.group(3)), rels))
        if any(r in own for r in rels) and int(m.group(3)):
            xids.add(int(m.group(3)))
out = {"outbox": 0, "catalog": 0, "rest": 0}
for rmgr, tot, tx, rels in recs:
    if any(r in own for r in rels) or (rmgr == "Transaction" and tx in xids):
        out["outbox"] += tot
    elif any(r in cat for r in rels):
        out["catalog"] += tot
    else:
        out["rest"] += tot
print(" ".join(str(round(out[k] / secs)) for k in ("outbox", "catalog", "rest")))
'
filenodes() { # filenodes WHERE: the relfilenodes of the relations matching WHERE, with their TOAST tables and indexes
  psql_q "with own as (select oid, reltoastrelid from pg_class where $1)
    select string_agg(n::text, ' ') from (select pg_relation_filenode(x) n from (select oid x from own
      union select reltoastrelid from own where reltoastrelid <> 0
      union select indexrelid from pg_index where indrelid in (select reltoastrelid from own)) s) f
    where n is not null"
}
wal_split() {
  local own cat dir
  own=$(filenodes "relname like '${1}%' and relnamespace = current_schema()::regnamespace")
  cat=$(filenodes "relnamespace = 'pg_catalog'::regnamespace")
  dir=$(psql_q "show data_directory")/pg_wal
  WAL_PARTS=$(docker exec -u postgres "$PG" pg_waldump -p "$dir" -s "$2" -e "$3" 2> /dev/null |
    python3 -c "$WAL_SPLIT_PY" "$own" "$cat" "$4")
  WAL_AUTO=$(diff <(echo "$5") <(analyzed) | python3 -c '
import sys
old, new = {}, {}
for line in sys.stdin:
    if line[:2] in ("< ", "> "):
        name, a, v = line[2:].strip().rsplit(":", 2)
        (old if line[0] == "<" else new)[name] = (int(a), int(v))
print(", ".join(f"{n} +{a - old.get(n, (0, 0))[0]}/+{v - old.get(n, (0, 0))[1]}" for n, (a, v) in sorted(new.items()))
      or "нет")')
}
span() { echo "$(utc "$1" | cut -c12-) – $(utc "$2" | cut -c12-)"; }
row() { log "$1: $2"; printf -v "$1" '%s%s\n' "${!1}" "$2"; } # row VAR TEXT: appends a table row to VAR
IDLE_ROWS="" BULK_ROWS="" LOW_ROWS="" WAL_ROWS=""

background() {
  log "shard-cost background"
  local id; id=$(mark shard-cost "shard cost: background, app stopped")
  counters_start; sleep "$STEADY"; counters_stop
  mark_end "$id"
  row IDLE_ROWS "| фон: приложение остановлено | $(f1 "$XID_S") | $(f0 "$WAL_S") | $(f1 "$COMMIT_S") \
| $(span "$T0" "$T1") |"
}

idle() { # idle SHARDS
  local shards=$1 table=bench_shardcost_s$1_idle id
  log "shard-cost-s$shards-idle"
  drop_all "$table"
  app_start "shard-cost-s$shards-idle" "${DEFAULTS[@]}" CONSUMER_THREADS=0 OUTBOX_TABLE="$table" OUTBOX_SHARDS="$shards"
  id=$(mark shard-cost "shard cost: idle, shards $shards")
  sleep "$WARMUP"
  local l0 l1 before
  if [ -n "$WAL_SPLIT" ]; then before=$(analyzed); l0=$(psql_q "select pg_current_wal_lsn()"); fi
  counters_start; sleep "$STEADY"; counters_stop
  if [ -n "$WAL_SPLIT" ]; then
    l1=$(psql_q "select pg_current_wal_lsn()")
    wal_split "$table" "$l0" "$l1" "$((T1 - T0))" "$before"
    read -r w_own w_cat w_rest <<< "$WAL_PARTS"
    row WAL_ROWS "| $shards | ${w_own:-?} | ${w_cat:-?} | ${w_rest:-?} | $WAL_AUTO | $(span "$T0" "$T1") |"
  fi
  mark_end "$id"
  app_stop
  drop_all "$table"
  row IDLE_ROWS "| $shards | $(f1 "$XID_S") | $(f0 "$WAL_S") | $(f1 "$COMMIT_S") | $(span "$T0" "$T1") |"
}

bulk() { # bulk SHARDS
  local shards=$1 table=bench_shardcost_s$1_bulk id d0 d1 stored parts rest
  log "shard-cost-s$shards-bulk"
  drop_all "$table"
  app_start "shard-cost-s$shards-bulk" "${DEFAULTS[@]}" CONSUMER_THREADS=0 BENCH_HANDLER=noop OUTBOX_TABLE="$table" \
    OUTBOX_SHARDS="$shards"
  id=$(mark shard-cost "shard cost: bulk, shards $shards")
  load 0 2 10000 100
  sleep "$WARMUP"
  d0=$(metric outbox_messages_delivered_total); counters_start; cpu_start
  sleep "$STEADY"
  counters_stop; d1=$(metric outbox_messages_delivered_total); cpu_stop
  # every committed message is still stored: a partition holds 16M xids, so maintenance drops none during the run
  stored=$(metric bench_messages_sent_total)
  read -r parts rest <<< "$(sizes "$table")"
  load_stop
  mark_end "$id"
  window "$T0" "$T1"
  local batches; batches=$(per_s outbox_batch_duration_seconds_count "$T0" "$T1")
  app_stop
  drop_all "$table"
  row BULK_ROWS "| $shards | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") | $(f3 "$P50") / $(f3 "$P99") s | $(f0 "$GROWTH") \
| $CPU % | $(f0 "$(calc "$WAL_B / max($d1 - $d0, 1)")") | $(f1 "$batches") | $(mb "$parts") \
| $(f0 "$(calc "$parts / max($stored, 1)")") | $(mb "$rest") | $(span "$T0" "$T1") |"
}

low() { # low SHARDS RATE
  local shards=$1 rate=$2 table=bench_shardcost_s$1_r$2 id
  log "shard-cost-s$shards-r$rate"
  drop_all "$table"
  kafka_reset
  app_start "shard-cost-s$shards-r$rate" "${DEFAULTS[@]}" DB_POOL_SIZE=48 CONSUMER_THREADS=12 OUTBOX_TABLE="$table" \
    OUTBOX_SHARDS="$shards"
  id=$(mark shard-cost "shard cost: low load $rate msg/s, shards $shards")
  load "$rate" 8
  sleep "$WARMUP"
  counters_start; cpu_start
  sleep "$STEADY"
  counters_stop; cpu_stop
  load_stop
  mark_end "$id"
  window "$T0" "$T1"
  row LOW_ROWS "| $rate | $shards | $(f0 "$DELIVERED") \
| $(f3 "$P50") / $(f3 "$(q_over 0.95 outbox_delivery_delay_seconds "$T0" "$T1")") \
/ $(f3 "$P99") s | $(f1 "$(per_s outbox_batch_duration_seconds_count "$T0" "$T1")") | $CPU % | $(f0 "$WAL_S") \
| $(span "$T0" "$T1") |"
  app_stop
  drop_all "$table"
}

started=$(date -u +'%Y-%m-%d %H:%M')
commit=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet HEAD -- || commit="$commit + незакоммиченные изменения"
phase() { [[ " $PHASES " == *" $1 "* ]]; }
if phase background; then background; fi
if phase idle; then for s in $RUNS; do idle "$s"; done; fi
if phase load; then
  for s in $RUNS; do
    bulk "$s"
    low "$s" 100
    low "$s" 1000
  done
fi

out << EOF

## Стоимость числа шардов

Запуск $started UTC, коммит \`$commit\`, шарды: $RUNS.

Библиотека на настройках по умолчанию (relay workers 4, \`batchSize = 1000\`, \`minTickInterval = 50ms\`,
\`idlePollInterval = 200ms\`, партиции по умолчанию), меняется только \`outbox.shards\`. Каждый прогон: новая
таблица outbox, ${WARMUP} с прогрева, затем окно ${STEADY} с. Сначала тихие прогоны: фон, затем простой для всех
чисел шардов, чтобы окно простоя не шло сразу после нагрузки (autovacuum, очистка после удаления таблиц и full-page
writes нагрузочного прогона попали бы в него); затем для каждого числа шардов bulk и малая нагрузка 100 и 1000 msg/s.
xid/с — прирост следующего xid кластера (транзакции с записью), WAL B/с — прирост \`pg_current_wal_lsn()\`,
коммитов/с — прирост \`pg_stat_database.xact_commit\` базы (вместе с читающими транзакциями, postgres-exporter
включён). CPU БД — среднее по \`docker stats\` (200 % = оба CPU).

### Простой

Приложение запущено без нагрузки и без консьюмера: relay делает тики и шаги шардов вхолостую. Первая строка — фон стенда
при остановленном приложении (postgres-exporter и сам Postgres), измерена один раз.

| шарды | xid/с | WAL B/с | коммитов/с | окно (UTC) |
|---|---|---|---|---|
$IDLE_ROWS${WAL_ROWS:+
WAL окон простоя по \`pg_waldump\` (\`WAL_SPLIT=1\`): outbox — записи на relfilenode объектов таблицы (таблицы,
партиции, индексы, последовательности, их TOAST) и коммиты их транзакций; каталог — записи на relfilenode \`pg_catalog\`;
autovacuum — сколько раз за окно он проанализировал / почистил каждую таблицу (\`pg_stat_user_tables\`).

| шарды | WAL outbox B/с | WAL каталога B/с | прочий WAL B/с | autovacuum (+analyze/+vacuum) | окно (UTC) |
|---|---|---|---|---|---|
$WAL_ROWS}
### bulk

2 писателя × 10 000 сообщений на транзакцию, payload 100 байт, обработчик noop, без консьюмера. WAL на сообщение —
прирост WAL за окно, делённый на прирост \`outbox_messages_delivered_total\` за то же окно. Размеры — в конце окна,
\`pg_total_relation_size\`: партиции таблицы и служебные таблицы (tick, cursor, retry, dead). Размер партиций растёт с
числом записанных сообщений, поэтому рядом он поделён на число хранимых сообщений (\`bench_messages_sent_total\` в
конце окна: обслуживание за прогон не удаляет ни одной партиции, в каждой 16M xid).

| шарды | записано msg/s | доставлено msg/s | задержка p50 / p99 | Δ очереди | CPU БД | WAL B/сообщение | пакетов relay/с | партиции МБ | партиции B/сообщение | служебные МБ | окно (UTC) |
|---|---|---|---|---|---|---|---|---|---|---|---|
$BULK_ROWS
### Малая нагрузка

Режим заказов (одно сообщение на транзакцию), 8 писателей, обработчик Kafka, консьюмер бенча (12 потоков), как в
\`low-load.sh\`. Задержка — от вставки строки до ack Kafka (\`outbox.delivery.delay\`).

| msg/s | шарды | доставлено msg/s | задержка p50 / p95 / p99 | пакетов relay/с | CPU БД | WAL B/с | окно (UTC) |
|---|---|---|---|---|---|---|---|
$LOW_ROWS
EOF
log "done: $RESULTS"
