#!/usr/bin/env bash
# poison: order mode at RATE msg/s plus COUNT messages over Kafka's max.request.size; they must land in the dead table
# without slowing the rest or breaking per-key order.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-2000}
COUNT=${COUNT:-100}
table=bench_poison

log poison
drop_outbox "$table"
kafka_reset
app_start poison "${SINGLE_ENV[@]}" OUTBOX_TABLE="$table"
id=$(mark poison "poison: $RATE msg/s + $COUNT oversized messages")
load "$RATE" "$WRITERS"
sleep 30
t0=$(now)
sleep 60
t1=$(now)
curl -sf -XPOST "$BENCH/poison?count=$COUNT" > /dev/null
t2=$(now)
sleep 90
t3=$(now)
load_stop
stopped=$(now); rc=0; reconcile "$table" 900 "$(metric bench_messages_sent_total)" || rc=$?
if [ "$rc" = 1 ]; then drain="не за 900 с"; else drain="за $(($(now) - stopped)) с"; fi
sleep 35 # the dead-size gauge comes from maintenance, every 30 s
mark_end "$id"
dead=$(psql_q "select count(*) from ${table}_dead")
dead_gauge=$(metric outbox_dead_size)
retry=$(psql_q "select count(*) from ${table}_retry")
end=$(totals)
app_stop

row() { # row PHASE T0 T1
  window "$2" "$3"
  echo "| $1 | $(utc "$2" | cut -c12-) – $(utc "$3" | cut -c12-) | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") \
| $(f0 "$CONSUMED") | $(f3 "$P50") s | $(f3 "$P99") s |"
}

{
  cat << EOF

## poison

Режим заказов (1 сообщение на транзакцию, писателей $WRITERS), $RATE msg/s, обработчик Kafka, консьюмер с inbox;
шардов 32, relay workers 8. В $(utc "$t1") UTC \`POST /poison?count=$COUNT\` закоммитил $COUNT сообщений размером
\`max.request.size\` продюсера + 1 КБ (~1 МБ каждое, различные ключи, $((t2 - t1)) с на все). Kafka отвергает их на
стороне клиента как постоянные сбои.

| фаза | окно (UTC) | закоммичено msg/s | доставлено msg/s | потреблено msg/s | задержка p50 | задержка p99 |
|---|---|---|---|---|---|---|
EOF
  row до "$t0" "$t1"
  row после "$t2" "$t3"
  cat << EOF

dead-таблица: $dead строк (gauge \`outbox.dead.size\` $dead_gauge), retry-таблица: $retry строк. Консьюмер всё
выбрал $drain после остановки писателей. Итого: $end. Сверка (SQL, после выборки): закоммиченных строк, не потреблённых
и не dead: $RECON (в строках есть $COUNT poison-сообщений, а в счётчике писателей их нет).

$(consumer_note)
EOF
} | out
verdict "$table" "$rc"
