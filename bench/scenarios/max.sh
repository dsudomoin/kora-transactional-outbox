#!/usr/bin/env bash
# max: unthrottled ceilings on this machine, each WARMUP s + a DURATION s measured window.
#  single: order mode (1 message per transaction), WRITERS writers, full path (Kafka + inbox consumer): the DB's
#          transaction ceiling with synchronous commit on 2 CPUs.
#  bulk:   sendAll of 100-byte messages, noop handler, no consumer, batch 20 000: 4 writers × 10 000 on 4 shards
#          (relay workers 4) and 16 writers × 1000 on 32 shards (relay workers 8); the outbox's own enqueue and relay
#          ceilings. After each window the writers stop and the relay drains whatever backlog built up.
source "$(dirname "$0")/lib.sh"
WARMUP=${WARMUP:-30}
DURATION=${DURATION:-180}

measure() { # warm-up, window; sets T0 T1 CPU and the window() values
  sleep "$WARMUP"
  T0=$(now); cpu_start
  sleep "$DURATION"
  T1=$(now); cpu_stop
}

log max-single
drop_outbox bench_max_single
kafka_reset
app_start max-single "${SINGLE_ENV[@]}" OUTBOX_TABLE=bench_max_single
id=$(mark max "max single: $WRITERS writers unthrottled, 1 message per transaction")
load 0 "$WRITERS"
measure
load_stop
table=bench_max_single
stopped=$(now); single_rc=0; reconcile "$table" 900 "$(metric bench_messages_sent_total)" || single_rc=$?
if [ "$single_rc" = 1 ]; then single_drain="не за 900 с"; else single_drain="за $(($(now) - stopped)) с"; fi
single_recon=$RECON
mark_end "$id"
window "$T0" "$T1"
single="| 1 сообщение на транзакцию, писателей $WRITERS, kafka + консьюмер | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") \
| $(f0 "$CONSUMED") | $(f3 "$P50") s | $(f3 "$P99") s | $(f3 "$TX99") s | $CPU% | $(f0 "$GROWTH") \
| $(utc "$T0") – $(utc "$T1" | cut -c12-) |"
single_totals=$(totals)
f0 "$CONSUMED" > "$LOGS/max-single.consumed"
app_stop
if [ "$single_rc" != 0 ]; then # before the bulk runs reset the topic and the consumer group: keep the evidence
  out << EOF

## max (без ограничения скорости)

Только прогон с одиночными сообщениями: его сверка не прошла, поэтому прогоны bulk пропущены, а его таблица, топик и
consumer group сохранены.

| нагрузка | закоммичено msg/s | доставлено msg/s | потреблено msg/s | задержка p50 | задержка p99 | транзакция писателя p99 | CPU БД (из 200%) | Δ очереди | окно (UTC) |
|---|---|---|---|---|---|---|---|---|---|
$single

Консьюмер всё выбрал $single_drain после остановки писателей; $single_totals. Сверка: $single_recon.
EOF
  verdict bench_max_single "$single_rc"
fi
drop_outbox bench_max_single

# bulk WRITERS MESSAGES_PER_TX SHARDS RELAY_WORKERS: one unthrottled sendAll run; appends a row to $bulk and the
# drain after the writers stop to $drains.
bulk="" drains=""
bulk() {
  local table=bench_max_bulk
  log "max-bulk $1 x $2, shards $3"
  drop_outbox "$table"
  kafka_reset
  app_start "max-bulk-$1x$2" CONSUMER_THREADS=0 BENCH_HANDLER=noop OUTBOX_TABLE="$table" OUTBOX_SHARDS="$3" \
    OUTBOX_RELAY_WORKERS="$4" OUTBOX_RELAY_BATCH_SIZE=20000 OUTBOX_RELAY_IDLE_POLL=100ms
  local id; id=$(mark max "max bulk: $1 writers x $2 messages per transaction, shards $3, noop")
  load 0 "$1" "$2" 100
  measure
  load_stop
  local stopped backlog drain; stopped=$(now)
  backlog=$(($(metric bench_messages_sent_total) - $(metric outbox_messages_delivered_total)))
  if wait_delivered 600; then drain="за $(($(now) - stopped)) с"; else drain="не за 600 с"; fi
  mark_end "$id"
  window "$T0" "$T1"
  bulk="$bulk| по $2 сообщений на транзакцию, писателей $1, шардов $3, relay workers $4, noop, без консьюмера | $(f0 "$COMMITTED") \
| $(f0 "$DELIVERED") | – | $(f3 "$P50") s | $(f3 "$P99") s | $(f3 "$TX99") s | $CPU% | $(f0 "$GROWTH") \
| $(utc "$T0") – $(utc "$T1" | cut -c12-) |
"
  drains="$drains $1 × $2: в пути $backlog, выбрано $drain;"
  app_stop
  drop_outbox "$table"
}

# the best shape found (4 × 10 000 on 4 shards; more writers only add contention once the DB is saturated) and 16
# writers, which must use smaller transactions: 16 × 10 000 advisory locks exceed the shared lock table
bulk 4 10000 4 4
bulk 16 1000 32 8

out << EOF

## max (без ограничения скорости)

${WARMUP} с прогрева, затем окно ${DURATION} с. Два разных потолка:

- **Потолок транзакций БД** (1 сообщение на транзакцию: вставка заказа + upsert seq пользователя + \`outbox.send\` +
  вставка в inbox на каждое потреблённое сообщение, синхронный коммит, 2 CPU). Это стоимость коммита на каждое
  сообщение на этой БД, а не предел outbox: на каждое сообщение БД также выполняет бизнес-вставку, upsert seq и вставку
  консьюмера в inbox.
- **Пропускная способность outbox** (массовый \`sendAll\`, обработчик noop, без консьюмера): потолки постановки в
  очередь и relay самой библиотеки.

| нагрузка | закоммичено msg/s | доставлено msg/s | потреблено msg/s | задержка p50 | задержка p99 | транзакция писателя p99 | CPU БД (из 200%) | Δ очереди | окно (UTC) |
|---|---|---|---|---|---|---|---|---|---|
$single
$bulk
Одиночные сообщения: консьюмер всё выбрал $single_drain после остановки писателей; $single_totals. Сверка (SQL, после
выборки): закоммиченных строк, не потреблённых и не dead: $single_recon. Bulk, очередь при остановке
писателей:$drains очередь в пределах нескольких транзакций означает, что relay успевал за писателями.

$(consumer_note)
EOF
