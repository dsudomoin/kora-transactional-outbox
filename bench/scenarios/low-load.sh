#!/usr/bin/env bash
# low-load: delivery latency at low rates against the shard count. Order mode, Kafka handler and the bench consumer,
# one instance, the relay at the library defaults (workers 4, batchSize 1000, minTickInterval 50 ms, idlePollInterval
# 200 ms) except outbox.shards. Each run: fresh outbox table and topic, WARMUP s, then a STEADY s window with
# pg_stat_statements reset at its start. RUNS="RATE:SHARDS ..." picks the runs. Appends its section (in Russian) to
# RESULTS.
source "$(dirname "$0")/lib.sh"
WARMUP=${WARMUP:-30}
STEADY=${STEADY:-180}
RUNS=${RUNS:-100:1 100:32 100:128 1000:1 1000:32}

# relay_calls TABLE SECONDS: top-level statements per second since the reset: "read tick cursor all", where read is
# the batch read (`from TABLE where shard`), tick and cursor every statement on TABLE_tick and TABLE_cursor, and all
# every statement in the database (writers, consumer, maintenance and postgres-exporter included).
relay_calls() {
  psql_q "select round(coalesce(sum(calls) filter (where query ~ 'from ${1} where shard'), 0) / $2.0, 1),
      round(coalesce(sum(calls) filter (where query ~ '${1}_tick'), 0) / $2.0, 1),
      round(coalesce(sum(calls) filter (where query ~ '${1}_cursor'), 0) / $2.0, 1),
      round(coalesce(sum(calls), 0) / $2.0, 1)
    from pg_stat_statements where toplevel and dbid = (select oid from pg_database where datname = current_database())" |
    tr '|' ' '
}
xid_now() { psql_q "select pg_snapshot_xmax(pg_current_snapshot())::text"; } # the next xid, consuming none

out << EOF

## Задержка при малой нагрузке и число шардов

Режим заказов (одно сообщение на транзакцию), обработчик Kafka и консьюмер бенча (12 потоков), один экземпляр. Relay на
настройках библиотеки по умолчанию (workers 4, \`batchSize = 1000\`, \`minTickInterval = 50ms\`,
\`idlePollInterval = 200ms\`), меняется только \`outbox.shards\`. На каждый прогон: новая таблица outbox и пустой топик,
${WARMUP} с прогрева, затем окно ${STEADY} с. Задержка: от вставки строки до ack Kafka (гистограмма
\`outbox.delivery.delay\`). Пакет relay: один вызов обработчика с чтением и записью курсора
(\`outbox.batch.duration\`). Запросы relay в секунду: \`pg_stat_statements\` за окно; чтение пакета, тики и курсоры
отдельно, «все» — все запросы БД, включая писателей, консьюмер и postgres-exporter. xid/с: прирост счётчика транзакций
кластера. CPU БД: среднее по \`docker stats\` (200 % = оба CPU).

| msg/s | шарды | доставлено msg/s | задержка p50 / p95 / p99 | пакет relay p50 / p99, пакетов/с | CPU БД | чтение пакета /с | тики /с | курсоры /с | все запросы /с | xid/с | окно (UTC) |
|---|---|---|---|---|---|---|---|---|---|---|---|
EOF

run() { # run RATE SHARDS
  local rate=$1 shards=$2 table=bench_lowload_r$1_s$2 name=low-load-r$1-s$2 id t0 t1 x0 x1 calls rd tk cu all
  log "$name"
  drop_outbox "$table"
  kafka_reset
  app_start "$name" DB_POOL_SIZE=48 CONSUMER_THREADS=12 OUTBOX_TABLE="$table" OUTBOX_SHARDS="$shards" \
    OUTBOX_RELAY_WORKERS=4 OUTBOX_RELAY_BATCH_SIZE=1000
  id=$(mark low-load "low load: $rate msg/s, shards $shards")
  load "$rate" 8
  sleep "$WARMUP"
  psql_q "select pg_stat_statements_reset()" > /dev/null
  x0=$(xid_now); t0=$(now); cpu_start
  sleep "$STEADY"
  t1=$(now); x1=$(xid_now); cpu_stop
  calls=$(relay_calls "$table" $((t1 - t0)))
  load_stop
  mark_end "$id"
  window "$t0" "$t1"
  read -r rd tk cu all <<< "$calls"
  echo "| $rate | $shards | $(f0 "$DELIVERED") | $(f3 "$P50") / $(f3 "$(q_over 0.95 outbox_delivery_delay_seconds "$t0" "$t1")") \
/ $(f3 "$P99") s | $(f3 "$(q_over 0.5 outbox_batch_duration_seconds "$t0" "$t1")") / \
$(f3 "$(q_over 0.99 outbox_batch_duration_seconds "$t0" "$t1")") s, $(f1 "$(per_s outbox_batch_duration_seconds_count "$t0" "$t1")") \
| $CPU % | $rd | $tk | $cu | $all | $(((x1 - x0) / (t1 - t0))) | $(utc "$t0" | cut -c12-) – $(utc "$t1" | cut -c12-) |" | out
  app_stop
  drop_outbox "$table"
}

for r in $RUNS; do run "${r%%:*}" "${r##*:}"; done
log "done: $RESULTS"
