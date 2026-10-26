#!/usr/bin/env bash
# kafka-outage: order mode at RATE msg/s, Kafka stopped for OUTAGE s, then started; runs until the backlog is drained.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-4000}
OUTAGE=${OUTAGE:-60}
table=bench_kafka_outage

backlog() { echo $(($(metric bench_messages_sent_total) - $(metric outbox_messages_delivered_total))); }
sample() { B=$(backlog); if [ "$B" -gt "$peak" ]; then peak=$B; fi; } # sets B, raises peak

log kafka-outage
drop_outbox "$table"
kafka_reset
app_start kafka-outage "${SINGLE_ENV[@]}" OUTBOX_TABLE="$table"
id=$(mark kafka-outage "kafka-outage: $RATE msg/s, Kafka stopped ${OUTAGE} s")
load "$RATE" "$WRITERS"
sleep 30
t0=$(now)
sleep 60
t1=$(now)
peak=0
kafka_stop
down=$(now)
while [ "$(now)" -lt $((down + OUTAGE)) ]; do sample; sleep 2; done
kafka_start
up=$(now)
drained=""
while [ "$(now)" -lt $((up + 1200)) ]; do
  sample
  if [ "$B" -le "$RATE" ]; then drained=$(now); break; fi # at most 1 s of traffic in flight
  sleep 2
done
sleep 30
load_stop
stopped=$(now); rc=0; reconcile "$table" 900 "$(metric bench_messages_sent_total)" || rc=$?
if [ "$rc" = 1 ]; then drain="не за 900 с"; else drain="за $(($(now) - stopped)) с"; fi
mark_end "$id"
window "$t0" "$t1"
base_p99=$P99
if [ -n "$drained" ]; then
  relay_drain="через $((drained - up)) с после запуска Kafka"
  drain_p99=$(f1 "$(q_over 0.99 outbox_delivery_delay_seconds "$down" $((drained + 30)))")
else
  relay_drain="не за 1200 с"; drain_p99=NaN
fi
end=$(totals)
dead=$(psql_q "select count(*) from ${table}_dead")
app_stop

out << EOF

## kafka-outage

Режим заказов (1 сообщение на транзакцию, писателей $WRITERS), $RATE msg/s, обработчик Kafka, консьюмер с inbox;
шардов 32, relay workers 8. Kafka остановили (\`docker stop $KAFKA\`) в $(utc "$down") UTC на $OUTAGE с и снова
запустили в $(utc "$up" | cut -c12-); писатели всё это время продолжали коммитить. $(rate_note "$RATE")

| базовый p99 задержки | пик очереди | выбрано (очередь ≤ 1 с трафика) | p99 задержки от начала сбоя до выборки + 30 с | dead |
|---|---|---|---|---|
| $(f3 "$base_p99") s | $peak сообщений | $relay_drain | $drain_p99 s | $dead |

Консьюмер всё выбрал $drain после остановки писателей. Итого: $end. Сверка (SQL, после выборки): закоммиченных строк,
не потреблённых и не dead: $RECON. Дубликат здесь — запись, которую Kafka сохранила, но ack на которую relay не получил
до сбоя; relay отправляет её повторно, и inbox отбрасывает вторую копию.

$(consumer_note)
EOF
verdict "$table" "$rc"
