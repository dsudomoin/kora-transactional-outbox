#!/usr/bin/env bash
# bulk: bulk sends. 2 writers each loop inTx { sendAll(10 000 messages, distinct random keys, 100-byte payload) }
# unthrottled; relay workers 4, batchSize 20 000, idlePollInterval 100 ms; shards 1, 2 and 4; handler noop (pure outbox)
# and kafka; no Kafka consumer (the producer side and the outbox handler only). Fresh outbox table per run, WARMUP s,
# then a STEADY s measured window; the headline run (shards 4, noop) measures HEADLINE s, so checkpoints and autovacuum
# of the growing table fall inside the window.
source "$(dirname "$0")/lib.sh"
# fdatasync of one 8 kB write on the Postgres data volume, measured before any load. The shell inside the container
# removes the 16 MB test file however pg_test_fsync ends, and outlives this script if it is interrupted.
fsync=$(docker exec "$PG" sh -c 'f=/var/lib/postgresql/data/pg_test_fsync.tmp; trap "rm -f $f" EXIT
  /usr/lib/postgresql/16/bin/pg_test_fsync -s 2 -f "$f"' |
  awk '$1 == "fdatasync" && !v { v = $2 } END { printf "%.0f", v }')
WARMUP=${WARMUP:-60}
STEADY=${STEADY:-180}
HEADLINE=${HEADLINE:-600}

out << EOF

## bulk

2 потока-писателя × 10 000 сообщений на транзакцию (\`sendAll\`, различные случайные ключи), payload 100 байт
(\`payloadBytes = 100\`: JSON-значение, дополненное до 100 байт), 4 relay-воркера, \`batchSize = 20000\`,
\`idlePollInterval = 100ms\`, шардов 1, 2 и 4. Обработчик noop (считает и выбрасывает) или kafka (поверх добавляется
настоящий брокер). Консьюмер Kafka не запущен. Каждый прогон: новая таблица outbox, ${WARMUP} с прогрева, затем окно
измерения ${STEADY} с (${HEADLINE} с для основного прогона с шардами 4 + noop, чтобы checkpoint-ы и autovacuum растущей
таблицы попали в окно). Записано = закоммиченные сообщения; доставлено = переданные
обработчику (noop: подсчитаны и выброшены; kafka: подтверждены брокером, acks=all); задержка = от вставки на стороне
коммита до outcome обработчика. Δ очереди = изменение (закоммичено − доставлено) за окно; выбрано за = время от остановки
писателей до момента, когда доставленные догнали закоммиченные. Последняя строка оставляет relay с настройками
библиотеки по умолчанию (workers 4, \`batchSize = 1000\`, \`idlePollInterval = 200ms\`) и шардами 4, как в основном
прогоне (по умолчанию в библиотеке 32): то, что получит пользователь без настройки relay.

| шарды | обработчик | записано msg/s | доставлено msg/s | задержка p50 | задержка p99 | транзакция писателя p50 | CPU БД (из 200%) | Δ очереди | выбрано за | окно (UTC) |
|---|---|---|---|---|---|---|---|---|---|---|
EOF

# run SHARDS HANDLER [defaults]: "defaults" leaves the relay at the library defaults (batchSize 1000, idle poll 200 ms).
run() {
  local shards=$1 handler=$2 table=bench_bulk_s$1_$2${3:+_$3} name=bulk-s$1-$2${3:+-$3} label=$2
  local relay=(OUTBOX_RELAY_WORKERS=4 OUTBOX_RELAY_BATCH_SIZE=20000 OUTBOX_RELAY_IDLE_POLL=100ms)
  if [ "${3:-}" = defaults ]; then relay=(); label="$2, relay по умолчанию"; fi
  log "$name"
  drop_outbox "$table"
  kafka_reset
  app_start "$name" CONSUMER_THREADS=0 BENCH_HANDLER="$handler" OUTBOX_TABLE="$table" OUTBOX_SHARDS="$shards" ${relay[@]+"${relay[@]}"}
  local id; id=$(mark bulk "bulk shards=$shards handler=$label")
  load 0 2 10000 100
  sleep "$WARMUP"
  psql_q "select pg_stat_statements_reset()" > /dev/null
  local t0 t1 cpu steady=$STEADY
  if [ "$shards-$handler-${3:-}" = 4-noop- ]; then steady=$HEADLINE; fi
  t0=$(now); cpu_start
  sleep "$steady"
  t1=$(now); cpu_stop; cpu=$CPU
  top_statements > "$LOGS/$name-statements.txt" # evidence for the notes, not in RESULTS.md
  load_stop
  local stopped drained; stopped=$(now)
  if wait_delivered 300; then drained="$(($(now) - stopped)) s"; else drained="нет (300 s)"; fi
  mark_end "$id"
  window "$t0" "$t1"
  app_stop
  drop_outbox "$table"
  echo "$DELIVERED" > "$LOGS/$name.rate"
  echo "| $shards | $label | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") \
| $(f3 "$P50") s | $(f3 "$P99") s | $(f3 "$TX50") s | $cpu% | $(f0 "$GROWTH") | $drained \
| $(utc "$t0") – $(utc "$t1" | cut -c12-) |" | out
}

# top_statements: the five statements with the most execution time since the last reset, with mean ms per call.
top_statements() {
  psql_q "select calls, round(total_exec_time)::bigint, round(mean_exec_time::numeric, 2),
    left(regexp_replace(query, '\s+', ' ', 'g'), 140) from pg_stat_statements order by total_exec_time desc limit 5"
}

if [ -n "${RUNS:-}" ]; then # e.g. RUNS="4:noop 4:kafka 1:kafka 4:noop:defaults": exactly these runs, in this order
  for r in $RUNS; do IFS=: read -r s h d <<< "$r"; run "$s" "$h" ${d:+"$d"}; done
else
  for handler in ${HANDLERS:-noop kafka}; do
    for shards in ${SHARDS:-1 2 4}; do run "$shards" "$handler"; done
  done
  [ -n "${HANDLERS:-}${SHARDS:-}" ] || run 4 noop defaults # what an untuned user gets
fi


out << EOF

### Условия измерения

- Postgres — штатный \`postgres:16\` с настройками из раздела «Стенд»; БД ограничена 2 CPU и 4 ГБ без swap.
- \`cpus: 2\` — это квота CFS: процессы Postgres могут выполняться на любом из $(docker info --format '{{.NCPU}}') vCPU
  ВМ Docker Desktop (ядра $(sysctl -n machdep.cpu.brand_string)), но вместе получают не больше 2 CPU времени за период;
  это не машина с 2 vCPU. Современное ядро Apple вполне может быть быстрее облачного vCPU, поэтому на другом железе
  числа будут другими.
- Kafka здесь — один брокер с replication factor 1, поэтому \`acks=all\` не ждёт ни одного follower: строки kafka
  платят за надёжность меньше, чем на реплицированном кластере.
- Бенчмарк-приложение и БД делят один хост через Docker Desktop: сетевая задержка почти нулевая, а диск ВМ Docker
  Desktop на SSD Mac делает fsync дешёвым: \`pg_test_fsync\` на томе данных даёт $fsync fdatasync/s (одна запись
  8 кБ). Сетевой том обычно намного медленнее на каждый fsync. При 10 000 сообщений на коммит в bulk это почти не
  важно; сценарии с одним сообщением на транзакцию коммитят каждое сообщение и почувствуют это первыми.
- Relay также просыпается по коммиту (особенность дизайна: пост-коммит-хук приближает следующий тик); idle-опрос здесь
  100 мс.
- У каждого сообщения свой случайный ключ — худший случай для advisory-блокировок по ключам (10 000 блокировок на
  транзакцию).
EOF
