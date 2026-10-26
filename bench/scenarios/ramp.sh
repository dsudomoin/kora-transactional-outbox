#!/usr/bin/env bash
# ramp: order mode (1 message per transaction: business row + row-locked per-user seq + outbox.send), full path to
# Kafka and the inbox consumer, at RATES msg/s for STEP s each; each step is measured over its last STEP - 30 s.
source "$(dirname "$0")/lib.sh"
RATES=${RATES:-1000 5000 10000 20000 40000}
STEP=${STEP:-120}
table=bench_ramp

log ramp
drop_outbox "$table"
kafka_reset
app_start ramp "${SINGLE_ENV[@]}" OUTBOX_TABLE="$table"
id=$(mark ramp "ramp $RATES msg/s, 1 message per transaction")
rows=""
for rate in $RATES; do
  load "$rate" "$WRITERS"
  sleep 30
  t0=$(now); cpu_start
  sleep $((STEP - 30))
  t1=$(now); cpu_stop; cpu=$CPU
  window "$t0" "$t1"
  rows="$rows| $rate | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") | $(f0 "$CONSUMED") | $(f3 "$P50") s | $(f3 "$P99") s \
| $(f3 "$TX99") s | $cpu% | $(utc "$t0" | cut -c12-) – $(utc "$t1" | cut -c12-) |
"
  first=${first:-$t0}
done
load_stop
stopped=$(now); rc=0; reconcile "$table" 900 "$(metric bench_messages_sent_total)" || rc=$?
if [ "$rc" = 1 ]; then drain="не за 900 с"; else drain="за $(($(now) - stopped)) с"; fi
mark_end "$id"
end=$(totals)
app_stop

out << EOF

## ramp (1 сообщение на транзакцию)

Режим заказов: каждая транзакция вставляет заказ, берёт следующий \`seq\` пользователя из счётчика под блокировкой
строки и вызывает \`outbox.send\` (10 000 пользователей, payload 300 байт), писателей $WRITERS; обработчик Kafka;
консьюмер в $CONSUMER_THREADS потоков дедуплицирует через inbox (одна вставка на сообщение, та же БД) и проверяет
порядок \`seq\` по пользователю; шардов 32, relay workers 8, batch 2000. Каждая ступень идёт около $((STEP + 8)) с
($STEP с, затем запросы окна до старта следующей ступени) и измеряется с 30-й по $STEP-ю секунду от её начала. Дата
$(utc "$first" | cut -c1-10) (UTC).

| цель msg/s | закоммичено msg/s | доставлено msg/s | потреблено msg/s | задержка p50 | задержка p99 | транзакция писателя p99 | CPU БД (из 200%) | окно (UTC) |
|---|---|---|---|---|---|---|---|---|
${rows}
После ramp консьюмер всё выбрал $drain после остановки писателей. Итого: $end. Сверка (SQL, после выборки):
закоммиченных строк, не потреблённых и не dead: $RECON.

$(consumer_note)
EOF
verdict "$table" "$rc"
