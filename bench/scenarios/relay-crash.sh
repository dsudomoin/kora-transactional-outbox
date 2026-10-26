#!/usr/bin/env bash
# relay-crash: order mode at RATE msg/s, the bench app (writers, relay and consumer in one JVM) killed with kill -9,
# restarted after 10 s on the same outbox table with the same load. Recovery is timed from the relaunch:
#  relay:      every shard's cursor reached the first tick made after the kill (all pre-crash commits handed to Kafka);
#  end to end: every message committed but not consumed at the crash is in the inbox (includes the Kafka consumer
#              group dropping the killed members after session.timeout.ms).
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-4000}
table=bench_relay_crash
env_=("${SINGLE_ENV[@]}" OUTBOX_TABLE="$table")

log relay-crash
drop_outbox "$table"
kafka_reset
app_start relay-crash "${env_[@]}"
id=$(mark relay-crash "relay-crash: $RATE msg/s, kill -9, restart after 10 s")
load "$RATE" "$WRITERS"
sleep 30
t0=$(now)
sleep 60
t1=$(now)
before=$(totals)
pre=$(metric bench_messages_sent_total)
pre_consumed=$(metric bench_consumer_received_total) # a record consumed after this read only lowers the sum
app_kill9
killed=$(now)
sleep 10
# every writer session is gone by now: the rows committed before the crash (at least the counter read just before the
# kill; more if a commit landed in between) and those not consumed yet
pre_rows=$(psql_q "select count(*) from $table")
pre=$((pre_rows > pre ? pre_rows : pre))
psql_q "drop table if exists bench_crash_pending;
  create unlogged table bench_crash_pending as select message_id from $table o
  where not exists (select 1 from inbox i where i.message_id = o.message_id)" > /dev/null
pending=$(psql_q "select count(*) from bench_crash_pending")
last_tick=$(psql_q "select max(id) from ${table}_tick")
launched=$(now)
app_start relay-crash "${env_[@]}"
ready=$(now)
load "$RATE" "$WRITERS"
relay="" e2e=""
while [ "$(now)" -lt $((launched + 600)) ] && { [ -z "$relay" ] || [ -z "$e2e" ]; }; do
  if [ -z "$relay" ] && [ "$(psql_q "select coalesce(bool_and(c.tick_id >= t.id), false)
      from (select distinct on (shard) tick_id from ${table}_cursor order by shard, seq desc) c,
      (select min(id) as id from ${table}_tick where id > $last_tick) t")" = t ]; then
    relay=$(($(now) - launched))
  fi
  if [ -z "$e2e" ] && [ "$(psql_q "select count(*) from bench_crash_pending p
      where not exists (select 1 from inbox i where i.message_id = p.message_id)")" = 0 ]; then
    e2e=$(($(now) - launched))
  fi
  sleep 1
done
sleep 30
load_stop
post=$(metric bench_messages_sent_total) || post=""
stopped=$(now); rc=0; reconcile "$table" 900 "${post:+$((pre + post))}" "$pre_consumed" || rc=$?
if [ "$rc" = 1 ]; then drain="не за 900 с"; else drain="за $(($(now) - stopped)) с"; fi
mark_end "$id"
window "$t0" "$t1"
after=$(totals)
psql_q "drop table bench_crash_pending" > /dev/null
app_stop

out << EOF

## relay-crash

Режим заказов (1 сообщение на транзакцию, писателей $WRITERS), $RATE msg/s, обработчик Kafka, консьюмер с inbox;
шардов 32, relay workers 8. JVM бенчмарка (писатели, relay и консьюмер) убили через \`kill -9\` в $(utc "$killed") UTC
и через 10 с перезапустили на той же таблице outbox, с той же нагрузкой. Базовый p99 задержки до падения: $(f3 "$P99") с.
$(rate_note "$RATE")

| закоммичено, но не потреблено к падению | перезапуск → готовность | перезапуск → relay догнал | перезапуск → всё это потреблено |
|---|---|---|---|
| $pending | $((ready - launched)) s | ${relay:-не за 600} s | ${e2e:-не за 600} s |

До падения: $before. После перезапуска (счётчики начинаются с 0): $after. Счётчики до падения прочитаны прямо перед
убийством, поэтому строки, закоммиченные в промежутке (и COMMIT-ы, шедшие в момент убийства), делают число сверенных
строк немного больше суммы двух закоммиченных счётчиков; сверка ожидает $pre строк от периода до падения (большее из
этого счётчика и числа строк в таблице после убийства). Её счётчик потреблённых — сумма по обоим процессам
($pre_consumed до убийства): запись, которую убитый процесс потребил после этого чтения, только уменьшает сумму, поэтому
проверка с партициями, удалёнными обслуживанием, может пройти, и только если потреблено каждое закоммиченное сообщение.
Консьюмер всё выбрал $drain после остановки писателей. Сверка (SQL, после выборки), по обоим прогонам: закоммиченных
строк, не потреблённых и не dead: $RECON. «Relay догнал»: курсор каждого шарда дошёл до первого тика, сделанного после
падения, то есть все коммиты до падения переданы в Kafka. Сквозное время включает и ожидание, пока consumer group Kafka
исключит убитых консьюмеров (\`session.timeout.ms\`, по умолчанию 45 с). Дубликат — запись, которую убитый relay
отправил, не закоммитив курсор (или которую убитый консьюмер обработал, не закоммитив offset); inbox её отбрасывает.
Проверка порядка у консьюмера работает внутри одного процесса, поэтому покрывает перезапущенный прогон.

$(consumer_note)
EOF
verdict "$table" "$rc"
