#!/usr/bin/env bash
# consumer: the bench consumer's own ceiling, so end-to-end figures can be read as outbox time vs consumer time.
# A backlog of FILL s of traffic is built with the consumer off (Kafka handler), then the app restarts
# with only the consumer (CONSUMER_THREADS) and drains it; measured over DRAIN s that end before the backlog does.
# FILL_WRITERS x FILL_PER_TX messages per transaction build it (1 = order mode); bulk fills a backlog large enough for
# a fast consumer in a minute.
source "$(dirname "$0")/lib.sh"
FILL=${FILL:-300}
DRAIN=${DRAIN:-40}
FILL_WRITERS=${FILL_WRITERS:-$WRITERS}
FILL_PER_TX=${FILL_PER_TX:-1}
table=bench_consumer

log consumer
drop_outbox "$table"
kafka_reset
app_start consumer-fill "${SINGLE_ENV[@]}" CONSUMER_THREADS=0 OUTBOX_TABLE="$table"
id=$(mark consumer "consumer ceiling: $FILL s backlog, then drain with $CONSUMER_THREADS consumer threads")
load 0 "$FILL_WRITERS" "$FILL_PER_TX" 300
sleep 30
f0=$(now); cpu_start
sleep $((FILL - 30))
f1=$(now); cpu_stop; fill_cpu=$CPU
load_stop
sleep 7
fill_rate=$(f0 "$(per_s bench_messages_sent_total "$f0" "$f1")")
fill_p99=$(f3 "$(q_over 0.99 bench_tx_seconds "$f0" "$f1")")
wait_delivered 300 || { log "relay did not deliver the backlog"; exit 1; }
backlog=$(metric bench_messages_sent_total)
app_stop

app_start consumer-drain "${SINGLE_ENV[@]}" OUTBOX_TABLE="$table"
sleep 10
psql_q "select pg_stat_statements_reset()" > /dev/null
c0=$(metric bench_consumer_received_total); t0=$(now); cpu_start
sleep "$DRAIN"
c1=$(metric bench_consumer_received_total); t1=$(now); cpu_stop
lag=$(group_lag)
statements=$(psql_q "select calls, round(total_exec_time)::bigint, round(mean_exec_time::numeric, 3)
  from pg_stat_statements where query like 'insert into inbox %'" | tr '|' ' ')
read -r calls total mean <<< "$statements"
rate=$(((c1 - c0) / (t1 - t0)))
[ "${lag:-0}" -gt 0 ] || log "the backlog ran out inside the window: the rate is a lower bound"
echo "$rate" > "$LOGS/consumer.rate"
rc=0; reconcile "$table" 600 "$backlog" || rc=$?
mark_end "$id"
app_stop

if [ "$FILL_PER_TX" = 1 ]; then
  fill_note="Пока строилась очередь, $FILL_WRITERS неограниченных писателей сами по себе (режим заказов, relay в Kafka, \
без консьюмера) закоммитили **$fill_rate msg/s** при $fill_cpu% CPU БД, p99 транзакции писателя $fill_p99 с \
($(utc "$f0" | cut -c12-) – $(utc "$f1" | cut -c12-) UTC): потолок транзакций БД для 1 сообщения на транзакцию на этом \
стенде, без вставок консьюмера."
else
  fill_note="Очередь построили писатели × сообщения на транзакцию: $FILL_WRITERS × $FILL_PER_TX (payload 300 байт, различные \
случайные ключи, \`seq = 0\`, поэтому проверка порядка у консьюмера их пропускает), закоммичено $fill_rate msg/s."
fi

out << EOF

## consumer (потолок консьюмера бенча, а не outbox)

Сценарии с одиночными сообщениями измеряют путь целиком, через консьюмер бенча: $CONSUMER_THREADS потоков (по одному на
партицию \`orders\`), \`max.poll.records = 2000\`, одна транзакция на poll, один INSERT \`inbox.tryAcceptAll\` на poll
для всех его записей. Его потолок на БД без другой нагрузки: очередь из $backlog сообщений построена за $FILL с при
выключенном консьюмере, затем её выбрал один консьюмер. Окно $(utc "$t0") – $(utc "$t1" | cut -c12-) UTC, очередь,
оставшаяся после него: $lag.

| потреблено msg/s | CPU БД (из 200%) | вызовов INSERT в inbox | серверное время на INSERT |
|---|---|---|---|
| **$rate** | $CPU% | $calls | $mean ms |

$fill_note

При параллельных писателях консьюмер делит с ними 2 CPU, поэтому там его скорость ниже: это видно в столбцах
«потреблено» ниже. Сверка этого прогона: $RECON.
EOF
verdict "$table" "$rc"
