#!/usr/bin/env bash
# Runs every scenario in order and writes bench/RESULTS.md from scratch. Needs the stack up
# (docker compose -f observability/docker-compose.yml up -d) and the bench built (./gradlew :bench:installDist).
# About 100 minutes. The stack and its volumes stay in place; each scenario drops its own outbox tables when done.
source "$(dirname "$0")/lib.sh"
dir=$(dirname "$0")

show() { psql_q "select string_agg(name || ' = ' || current_setting(name), ', ' order by name) from pg_settings
  where name in ($1)"; }

cat > "$RESULTS" << EOF
# Результаты бенчмарков

Сгенерировано \`bench/scenarios/run-all.sh\`, запуск $(date -u +'%Y-%m-%d %H:%M') UTC. Каждое число ниже взято из
Prometheus (опрос бенчмарк-приложения и postgres-exporter каждые 5 с) за указанное окно либо из \`docker stats\` и
SQL; вручную ничего не правилось, кроме заметок в конце.

## Стенд

- Машина: $(sysctl -n machdep.cpu.brand_string), ядер: $(sysctl -n hw.ncpu) ($(sysctl -n hw.perflevel0.physicalcpu) + $(sysctl -n hw.perflevel1.physicalcpu)), $(($(sysctl -n hw.memsize) / 1073741824)) ГБ RAM, macOS $(sw_vers -productVersion).
- Docker Desktop $(docker info --format '{{.ServerVersion}}'): ВМ с $(docker info --format '{{.NCPU}}') CPU и $(docker info --format '{{.MemTotal}}' | awk '{ printf "%.1f", $1 / 1073741824 }') ГБ RAM.
- Postgres $(psql_q "show server_version" | cut -d' ' -f1) (\`postgres:16\`), ограниченный **2 CPU и 4 ГБ** (\`cpus: 2\`, \`mem_limit\` = \`memswap_limit\` = 4g, без swap).
  Настройки: $(show "'max_connections', 'shared_buffers', 'max_locks_per_transaction', 'synchronous_commit', 'fsync', 'full_page_writes', 'max_wal_size', 'checkpoint_timeout', 'wal_buffers', 'effective_io_concurrency'").
  Всё остальное — значения образа по умолчанию; те из настроек выше, что от них отличаются, объяснены ниже.
- Kafka $(docker inspect "$KAFKA" --format '{{.Config.Image}}' | cut -d: -f2), один узел KRaft; топик \`orders\` с 12 партициями, пересоздаётся пустым для каждого прогона.
- Бенчмарк-приложение (\`bench/\`, Kotlin, блокирующий код, без корутин) на JVM хоста: $("$JAVA_HOME/bin/java" -version 2>&1 | sed -n 2p | sed 's/.*Environment //; s/ (build.*//'), Kora $(grep koraVersion "$ROOT/gradle.properties" | cut -d= -f2), $(ls "$ROOT/bench/build/install/bench/lib" | grep -E '^(kafka-clients|postgresql|HikariCP)-' | sed 's/\.jar$//; s/-\([0-9]\)/ \1/' | tr '\n' ',' | sed 's/,$//; s/,/, /g'). Сэмплирование трасс 1 %
  (\`BENCH_TRACE_RATIO=0.01\`). Продюсер Kafka: \`linger.ms = 5\`, \`batch.size = 262144\`, \`compression.type = lz4\`, acks=all, идемпотентный.
- Каждый прогон использует новую таблицу outbox (\`OUTBOX_TABLE\`), которая удаляется после прогона (и сохраняется, если сверка не прошла).

Настройки, отличающиеся от значений по умолчанию, и почему их задал бы и продакшен-деплой:

- Postgres: \`shared_buffers = 1GB\` (в образе по умолчанию 128 МБ; четверть RAM — обычный размер);
  \`max_connections = 300\` (по умолчанию 100; запас для пулов приложений); \`max_locks_per_transaction = 256\` (по
  умолчанию 64; нужно этой библиотеке для массовой отправки: \`sendAll\` из 10 000 различных ключей берёт 10 000
  advisory-блокировок, а общая таблица блокировок вмещает \`max_locks_per_transaction × max_connections\`); загружен
  \`pg_stat_statements\` (только для наблюдения, с \`pg_stat_statements.track = all\`, что засекает и запросы, вложенные
  в функции; большинство управляемых сервисов Postgres его загружают); \`shm_size: 1g\` (только для Docker: место под
  разделяемую память). Надёжность штатная: \`synchronous_commit\`, \`fsync\`, \`full_page_writes\` и autovacuum
  включены, таблицы журналируемые, \`max_wal_size\` и \`checkpoint_timeout\` по умолчанию.
- Библиотека (\`outbox.*\`): bulk задаёт шарды 1/2/4, \`relay.batchSize = 20000\` и \`relay.idlePollInterval = 100ms\`
  под транзакции по 10 000 сообщений (relay workers 4 — значение по умолчанию), а одна строка bulk запускает relay с
  настройками по умолчанию. Сценарии с одиночными сообщениями используют шарды 32 (по умолчанию), relay workers 8 (по
  умолчанию 4) и \`batchSize = 2000\` (по умолчанию 1000): больше шардов обрабатывается параллельно и меньше чтений на
  тик при тысячах сообщений в секунду. Всё остальное — значения библиотеки по умолчанию.
- Продюсер Kafka: \`linger.ms = 5\`, \`batch.size = 262144\`, \`compression.type = lz4\` — обычные настройки для
  пропускной способности; acks=all и идемпотентность навязывает библиотека. Сама Kafka — один брокер с replication
  factor 1, чего не бывает в продакшен-кластерах: \`acks=all\` здесь не ждёт ни одного follower.
- Бенчмарк-приложение: пул Hikari 48 (bulk) или писатели + 32 (одиночные сообщения), сэмплирование трасс 1 %; консьюмер
  бенча работает в $CONSUMER_THREADS потоков (по одному на партицию) с \`max.poll.records = 2000\`.

Столбцы: закоммичено/записано = сообщения закоммиченных транзакций писателей; доставлено = записи, которые relay передал
обработчику с outcome «доставлено» (Kafka: подтверждено брокером); потреблено = впервые полученные записи, которые
консьюмер принял через inbox; задержка = от вставки строки (\`created_at\`) до outcome «доставлено»
(outbox.delivery.delay); CPU БД = среднее замеров \`docker stats\` контейнера Postgres, примерно раз в 6 с (200% = оба
CPU заняты). Скорости — это \`increase()\` Prometheus за окно, который экстраполирует к его границам; Δ очереди —
разность снятых счётчиков на границах окна, поэтому эти два значения могут расходиться на несколько процентов.
«Консьюмер всё выбрал за N с» — верхняя граница: сверка, которая засекает это время, опрашивает каждые 10 с и перед тем,
как принять результат, один раз его перепроверяет.

Графики: Grafana http://localhost:3000, дашборд **Outbox** (домашний). Каждый прогон — аннотация-интервал с тегом
\`scenario\` (слой «Scenarios»); чтобы его увидеть, выставьте диапазон времени на окно ниже (UTC).
EOF

rm -f "$LOGS"/*.rate "$LOGS"/*.consumed # rates handed between scenarios: only this run's
for s in bulk bulk-long-tx consumer max ramp kafka-outage relay-crash long-transaction poison; do
  "$dir/$s.sh"
done

out << 'EOF2'

## Найденные узкие места (история: измерено 2026-10-25, до этого прогона)

Этот раздел — фиксированный текст в `run-all.sh`, а не результат этого прогона. **Охват пакета relay** (исправлено в
коммите 12ad7d9, «perf: bound relay batches by data volume»). Отставший relay объединял до `maxTicksPerBatch` (100)
тиков в один пакет — около 1.35M строк при скоростях bulk. Каждое чтение чанка этого пакета по 20 000 строк заново
сканировало множество кандидатов, поэтому чтения замедлялись по мере роста очереди, а растущая очередь делала следующий
пакет ещё больше: спираль смерти. После исправления целевой тик выбирается по верхней отметке id, которую записывает
каждый тик (`max_id`), поэтому один пакет охватывает около `batchSize × shards` id. Конфигурация bulk, шардов 1, noop,
2026-10-25 (UTC); «до» — прерванный прогон на старом коде (аннотация «BEFORE relay fix»), «после» — 240-секундный
проверочный прогон с исправлением.

| | до (23:06–23:11) | после (23:22:43–23:26:45) |
|---|---|---|
| записано msg/s, по минутам | 270k, 273k, 266k, 235k | 254k, 259k, 269k, 265k |
| доставлено msg/s, по минутам | 101k (первые 20 s), затем 44k, 15k, 4.7k, 3.6k | 254k, 259k, 267k, 266k |
| p99 задержки доставки, по минутам | 40 s, 90 s, 159 s, 206 s, 274 s | 0.26 s, 0.21 s, 0.54 s, 0.26 s |
| очередь при остановке писателей | 58.5M сообщений | выбрана через 1 s после остановки писателей |
| чтение чанка пакета (pg_stat_statements) | в среднем 1 785 ms, 95 вызовов за 180 s | в среднем 8.8 ms, 6 416 вызовов за 240 s |

Источники: Prometheus (`rate(...[1m])`, `histogram_quantile` за 60 с) и `pg_stat_statements` (до: 180 с начиная с
23:07:17; после: весь прогон). Строки bulk выше получены на текущем коде.
EOF2
log "done: $RESULTS"
