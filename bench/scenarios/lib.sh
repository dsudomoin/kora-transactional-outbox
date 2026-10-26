# Helpers for the scenario scripts; sourced, not run. Needs the stack up (observability/docker-compose.yml), the
# bench built (`./gradlew :bench:installDist`), curl, python3 and docker on the host.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RESULTS=${RESULTS:-$ROOT/bench/RESULTS.md}
LOGS=$ROOT/bench/build/scenarios
APP=$ROOT/bench/build/install/bench/bin/bench
PG=kora-outbox-bench-postgres-1
KAFKA=kora-outbox-bench-kafka-1
GRAFANA=http://localhost:3000
PROM=http://localhost:9090
BENCH=http://localhost:8080
METRICS=http://localhost:8085/metrics
export JAVA_HOME=${JAVA_HOME:-$(/usr/libexec/java_home -v 21)}
mkdir -p "$LOGS"

# Safety: a process is only ever signalled by the PID this script recorded when it started it, and only while
# `ours` confirms that PID is still a live direct child of this shell, so a PID the OS has reused cannot be hit. The
# PIDs are also written to $LOGS/*.pid; to stop a run by hand see bench/README.md (scenario.pid holds this shell's PID
# and its start time, its identity). Docker actions name the kora-outbox-bench-* containers explicitly.
printf '%s\n%s\n' $$ "$(ps -o lstart= -p $$)" > "$LOGS/scenario.pid"
APP_PIDS=() # bench instances this shell started, by instance number (see inst_start)

# ours PID: PID is alive (not a zombie) and its parent is this shell. Every child signalled here was started by this
# shell, so a reused PID (another parent) fails the check.
ours() {
  local s
  s=$(ps -o ppid=,stat= -p "$1" 2> /dev/null) || return 1
  set -- $s
  [ "${1:-}" = "$$" ] && [ "${2#Z}" = "${2:-}" ]
}

# On any exit: Postgres and Kafka running and unpaused (first, so the apps can stop gracefully), no writers, app,
# sampler or long transaction left behind (a stray unthrottled writer fills the disk), open annotations closed.
cleanup() {
  container_up "$PG"
  container_up "$KAFKA"
  if [ -n "${CPU_PID:-}" ]; then cpu_stop; fi
  long_tx_end
  local i open name
  for i in "${!APP_PIDS[@]}"; do
    if [ -n "${APP_PIDS[i]}" ]; then
      if ours "${APP_PIDS[i]}"; then on "$i" curl -sf --max-time 40 -XDELETE "$BENCH/load" > /dev/null || true; fi
      inst_stop "$i"
    fi
  done
  while read -r open name; do
    curl -sf -XPATCH -H 'Content-Type: application/json' "$GRAFANA/api/annotations/$open" \
      -d "{\"timeEnd\": $(($(now) * 1000)), \"tags\": [\"scenario\", \"$name\", \"aborted\"]}" > /dev/null || true
  done < <(cat "$LOGS/marks.$$" 2> /dev/null)
  rm -f "$LOGS/marks.$$" "$LOGS/scenario.pid"
  return 0
}

# container_up NAME: a paused bench container is unpaused, a stopped one started (after a restart in flight ends).
container_up() {
  local state
  for _ in $(seq 1 30); do
    state=$(docker inspect -f '{{.State.Status}}' "$1" 2> /dev/null) || return 0
    [ "$state" = restarting ] || [ "$state" = removing ] || break
    command sleep 1
  done
  case "$state" in
    paused) log "cleanup: unpausing $1"; docker unpause "$1" > /dev/null || true ;;
    exited | created | dead) log "cleanup: starting $1"; docker start "$1" > /dev/null || true ;;
  esac
}
trap cleanup EXIT
trap 'exit 143' TERM INT HUP
sleep() { command sleep "$@" & wait $!; } # interruptible, so a TERM runs cleanup at once

log() { echo "$(date -u +%H:%M:%S) $*" >&2; }
now() { date +%s; }
utc() { date -u -r "$1" +'%Y-%m-%d %H:%M:%S'; }
out() { cat >> "$RESULTS"; } # appends stdin to RESULTS.md
f0() { printf '%.0f' "$1"; }
f1() { printf '%.1f' "$1"; }
f2() { printf '%.2f' "$1"; }
f3() { printf '%.3f' "$1"; }
calc() { python3 -c "nan = NaN = float('nan'); print($1)"; } # calc EXPR: arithmetic on Prometheus values (NaN-safe)

# --- bench app ---------------------------------------------------------------------------------------------------

# Instance I listens on 8079+I (public) and 8084+I (/metrics, readiness) and is named bench-I (Postgres
# application_name, trace service.instance.id). Instance 1 is the single-instance scenarios' app on 8080/8085.
# APP_PIDS[I] (declared at the top) is its JVM's PID while it runs (also in $LOGS/app-I.pid), empty once it is reaped.

# on I CMD...: runs CMD with BENCH and METRICS pointing at instance I (load, load_stop, metric, totals...).
on() { local BENCH=http://localhost:$((8079 + $1)) METRICS=http://localhost:$((8084 + $1))/metrics; shift; "$@"; }

# inst_start I LOGNAME [ENV=VALUE...]: instance I with BENCH_TRACE_RATIO=0.01 plus the given env; waits for readiness.
# Refuses to start while anything else answers on its port, so readiness can only come from the JVM started here.
inst_start() {
  local i=$1 name=$2 ready=localhost:$((8084 + $1))/system/readiness pid; shift 2
  if curl -sf "$ready" > /dev/null; then log "something already answers on ${ready%%/*}; stop it first"; exit 1; fi
  env BENCH_TRACE_RATIO=0.01 HTTP_PORT=$((8079 + i)) METRICS_PORT=$((8084 + i)) INSTANCE_NAME="bench-$i" "$@" "$APP" \
    >> "$LOGS/$name.log" 2>&1 & # env and bin/bench exec: $! is the JVM
  pid=$!
  APP_PIDS[i]=$pid
  echo "$pid" > "$LOGS/app-$i.pid"
  for _ in $(seq 1 90); do
    ours "$pid" || { log "bench app $i died, see $LOGS/$name.log"; exit 1; }
    curl -sf "$ready" > /dev/null && return 0
    sleep 1
  done
  log "bench app $i not ready after 90 s"; exit 1
}

# inst_stop I: SIGTERM (graceful: the relay finishes its batch), SIGKILL after 60 s only if it is still ours and alive.
inst_stop() {
  local i=$1 pid=${APP_PIDS[$1]:-}
  [ -n "$pid" ] || return 0
  if ours "$pid"; then
    kill "$pid" 2> /dev/null || true
    for _ in $(seq 1 60); do ours "$pid" || break; command sleep 1; done
    if ours "$pid"; then kill -9 "$pid" 2> /dev/null || true; fi
  fi
  wait "$pid" 2> /dev/null || true
  APP_PIDS[i]=""
  rm -f "$LOGS/app-$i.pid"
}

# inst_kill9 I: kill -9 of instance I, reaped at once so the PID is never signalled again.
inst_kill9() {
  local i=$1 pid=${APP_PIDS[$1]:-}
  [ -n "$pid" ] && ours "$pid" || { log "bench app $i is not running"; exit 1; }
  kill -9 "$pid"
  wait "$pid" 2> /dev/null || true
  APP_PIDS[i]=""
  rm -f "$LOGS/app-$i.pid"
}

# The single-instance scenarios' app: instance 1.
app_start() { inst_start 1 "$@"; }
app_stop() { inst_stop 1; }
app_kill9() { inst_kill9 1; }

# load RATE WRITERS [MESSAGES_PER_TX [PAYLOAD_BYTES]]: rate in msg/s, 0 = unlimited.
load() {
  curl -sf -XPOST "$BENCH/load?rate=$1&writers=$2&messagesPerTx=${3:-1}&payloadBytes=${4:-300}" > /dev/null
}
load_stop() { curl -sf -XDELETE "$BENCH/load" > /dev/null; }

# metric NAME: sum of the series of NAME on the app's /metrics right now (no scrape delay); fails, printing nothing,
# when /metrics is unreachable.
metric() {
  local m
  m=$(curl -sf "$METRICS") || return 1
  echo "$m" | awk -v m="$1" '$1 == m || index($1, m "{") == 1 { s += $2 } END { printf "%.0f", s }'
}

# wait_delivered TIMEOUT_S: until this process's delivered counter reaches its committed counter; 1 on timeout. An
# unreachable /metrics never counts as delivered.
wait_delivered() {
  local deadline=$(($(now) + $1)) d s
  while [ "$(now)" -lt "$deadline" ]; do
    d=$(metric outbox_messages_delivered_total) && s=$(metric bench_messages_sent_total) && [ "$d" -ge "$s" ] &&
      return 0
    sleep 1
  done
  return 1
}

# --- Postgres and Kafka ------------------------------------------------------------------------------------------

# group_lag: summed lag of the bench consumer group on `orders`; empty while a partition has no committed offset.
group_lag() {
  docker exec "$KAFKA" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe \
    --group outbox-bench 2> /dev/null |
    awk '$2 == "orders" { n++; if ($6 ~ /^[0-9]+$/) s += $6; else bad = 1 } END { if (n && !bad) print s }'
}

psql_q() { docker exec -e PGOPTIONS='-c client_min_messages=warning' "$PG" psql -U outbox -d outbox -Atq -c "$1"; }

# drop_outbox BASE: the outbox tables of one run (partitions go with the parent).
drop_outbox() {
  psql_q "drop table if exists $1, ${1}_tick, ${1}_cursor, ${1}_retry, ${1}_dead, ${1}_halt, ${1}_maintenance,
    ${1}_dropping cascade" > /dev/null
}

# reconcile BASE TIMEOUT EXPECTED [CONSUMED_BEFORE]: call after the writers stopped, with the app (relay and consumer)
# still running. EXPECTED is the number of messages the writers committed into BASE, from their counters (relay-crash:
# both runs); CONSUMED_BEFORE is added to the consumed counter (relay-crash: the killed process's, read before the kill).
# Waits until the consumer has drained: Kafka group lag 0, no retry rows, no paused shard, and both the consumed
# counter and the SQL count of rows of BASE neither consumed (no inbox row) nor dead unchanged over a 10 s re-check.
# Sets RECON and returns:
#   0  drained, nothing missing, and the rows checked cover EXPECTED; or fewer rows (maintenance drops relayed
#      partitions, taking their rows out of the check) while the consumed counter equals EXPECTED
#   1  not drained within TIMEOUT s: nothing is known about loss
#   2  drained with rows missing: a library defect
#   3  inconclusive: EXPECTED unknown or no rows to check
#   4  partial: fewer rows than EXPECTED and the counters do not prove the rest was consumed
# Every non-zero code keeps BASE (verdict).
reconcile() {
  local deadline=$(($(now) + $2)) expected=${3:-} before=${4:-0} prev="" cur="" lag="" retry="" paused="" missing="?" rows=""
  local consumed
  case "$expected" in '' | *[!0-9]*)
    RECON="INCONCLUSIVE — ожидаемое число закоммиченных неизвестно"; return 3 ;;
  esac
  while [ "$(now)" -lt "$deadline" ]; do
    lag=$(group_lag)
    read -r retry paused missing rows <<< "$(psql_q "select (select count(*) from ${1}_retry),
      (select count(*) from (select distinct on (shard) paused_until from ${1}_cursor order by shard, seq desc) c
        where paused_until > now()),
      count(*) filter (where not exists (select 1 from inbox i where i.message_id = o.message_id)
        and not exists (select 1 from ${1}_dead d where d.id = o.id)), count(*) from $1 o" | tr '|' ' ')"
    consumed=$(metric bench_consumer_received_total) && consumed=$((consumed + before)) || consumed=""
    cur="$consumed $missing"
    if [ "$lag" = 0 ] && [ "$retry" = 0 ] && [ "$paused" = 0 ] && [ -n "$consumed" ] && [ "$cur" = "$prev" ]; then
      RECON="$missing из $rows строк"
      if [ "$missing" != 0 ]; then
        RECON="$RECON — DRAINED WITH ROWS MISSING (дефект библиотеки; таблица сохранена)"; return 2
      fi
      if [ "$rows" = 0 ]; then
        RECON="INCONCLUSIVE — нет строк для проверки, ожидалось $expected (таблица сохранена)"; return 3
      fi
      [ "$rows" -ge "$expected" ] && return 0
      RECON="$RECON ещё в таблице: $((expected - rows)) из $expected закоммиченных строк были в партициях, \
которые обслуживание удалило после relay"
      if [ "$consumed" = "$expected" ]; then
        RECON="$RECON; счётчик потреблённых ($consumed) равен числу закоммиченных, значит, потреблены и они"
        return 0
      fi
      RECON="$RECON — PARTIAL, счётчик потреблённых $consumed не доказывает остальное (таблица сохранена)"
      return 4
    fi
    prev=$cur
    sleep 10
  done
  RECON="NOT DRAINED — сверка без результата, в пути $missing (лаг группы ${lag:-неизвестен}, строк retry $retry, \
шардов на паузе $paused, через ${2} с)"
  return 1
}

# verdict BASE RC: call last, after the section is written. A clean reconciliation (RC 0) drops BASE; otherwise BASE
# is kept as evidence and the scenario exits non-zero, which stops run-all.
verdict() {
  if [ "$2" = 0 ]; then drop_outbox "$1"; return 0; fi
  log "reconciliation of $1 failed (rc $2): $RECON; table kept"
  exit 1
}

# kafka_reset: deletes the topic and the consumer group, so a scenario starts on an empty topic (app stopped).
kafka_reset() {
  docker exec "$KAFKA" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --delete --group outbox-bench > /dev/null 2>&1 || true
  docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic orders > /dev/null 2>&1 || true
  while docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list |
    grep -qx orders; do sleep 1; done
}

# long_tx SECONDS: a session holding an xid open for SECONDS, in the background (LONG_TX_PID, the docker client);
# sets LONG_TX_XID and LONG_TX_BACKEND (its server PID, recorded so cleanup can end exactly that session).
# Only a session that started after the launch counts, and exactly one must: a stale `bench-long-tx` session left by an
# aborted run is neither measured nor terminated.
long_tx() {
  local since rows
  since=$(psql_q "select clock_timestamp()")
  docker exec -e PGAPPNAME=bench-long-tx "$PG" psql -U outbox -d outbox -Atq \
    -c "begin; select pg_current_xact_id(); select pg_sleep($1); commit;" > /dev/null 2>&1 &
  LONG_TX_PID=$!
  echo "$LONG_TX_PID" > "$LOGS/long-tx.pid"
  for _ in $(seq 1 30); do
    rows=$(psql_q "select pid || ' ' || backend_xid from pg_stat_activity where application_name = 'bench-long-tx'
      and backend_start >= '$since' and backend_xid is not null")
    if [ -n "$rows" ]; then
      [ "$(echo "$rows" | wc -l | tr -d ' ')" = 1 ] || { log "more than one new bench-long-tx session: $rows"; exit 1; }
      read -r LONG_TX_BACKEND LONG_TX_XID <<< "$rows"
      break
    fi
    command sleep 1
  done
  [ -n "${LONG_TX_XID:-}" ] || { log "long transaction did not start"; exit 1; }
  echo "$LONG_TX_BACKEND" > "$LOGS/long-tx.backend"
}
long_tx_wait() { # waits for the long transaction to commit, then forgets its PIDs (reaped: never signalled again)
  wait "$LONG_TX_PID" || true
  LONG_TX_PID=""
  long_tx_end
}
long_tx_end() { # ends the recorded session if it is still open
  if [ -n "${LONG_TX_BACKEND:-}" ]; then
    psql_q "select pg_terminate_backend($LONG_TX_BACKEND) from pg_stat_activity
      where pid = $LONG_TX_BACKEND and application_name = 'bench-long-tx'" > /dev/null 2>&1 || true
  fi
  if [ -n "${LONG_TX_PID:-}" ]; then
    if ours "$LONG_TX_PID"; then kill "$LONG_TX_PID" 2> /dev/null || true; fi
    wait "$LONG_TX_PID" 2> /dev/null || true
  fi
  LONG_TX_BACKEND="" LONG_TX_PID=""
  rm -f "$LOGS/long-tx.pid" "$LOGS/long-tx.backend"
  return 0
}

# xmin_pinned BASE XID: every 10 s until the long transaction ends, whether the newest tick's snapshot xmin is XID
# (maintenance deletes old ticks, so they cannot be counted afterwards); prints "N из M замеров".
xmin_pinned() {
  local n=0 m=0
  while ours "$LONG_TX_PID"; do
    m=$((m + 1))
    [ "$(psql_q "select pg_snapshot_xmin(snapshot) = '$2'::xid8 from ${1}_tick order by id desc limit 1")" = t ] &&
      n=$((n + 1))
    sleep 10
  done
  echo "$n из $m замеров"
}

# --- DB CPU ------------------------------------------------------------------------------------------------------

# cpu_start / cpu_stop: `docker stats` of the Postgres container every ~6 s (5 s sleep + the call itself); cpu_stop
# sets CPU to the mean CPU % (200 = 2 CPUs), "?" without samples. Not in $(...): the sampler PID must be cleared in
# this shell. The sampler ignores a failed `docker stats` call instead of dying on it.
cpu_start() {
  CPU_FILE=$LOGS/cpu.$$.$(now)
  (
    set +e
    while :; do docker stats --no-stream --format '{{.CPUPerc}}' "$PG" | tr -d '%'; command sleep 5; done
  ) > "$CPU_FILE" &
  CPU_PID=$!
  echo "$CPU_PID" > "$LOGS/cpu-sampler.pid"
}
cpu_stop() {
  if ours "$CPU_PID"; then kill "$CPU_PID" 2> /dev/null || true; fi
  wait "$CPU_PID" 2> /dev/null || true
  CPU_PID=""; rm -f "$LOGS/cpu-sampler.pid"
  CPU=$(awk '$1 + 0 == $1 { s += $1; n++ } END { if (n) printf "%.0f", s / n; else printf "?" }' "$CPU_FILE")
}

# --- Prometheus --------------------------------------------------------------------------------------------------

# prom EXPR [TIME]: the first sample of an instant query, NaN when empty.
prom() {
  curl -sf "$PROM/api/v1/query" --data-urlencode "query=$1" ${2:+--data-urlencode "time=$2"} |
    python3 -c 'import json, sys; r = json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "NaN")'
}

q_over() { # q_over Q METRIC_BUCKET T0 T1: quantile of a histogram over [T0, T1]
  prom "histogram_quantile($1, sum by (le) (increase(${2}_bucket[$(($4 - $3))s])))" "$4"
}
per_s() { # per_s COUNTER T0 T1: mean rate over [T0, T1]
  prom "sum(increase(${1}[$(($3 - $2))s])) / $(($3 - $2))" "$3"
}
backlog_at() { # committed minus delivered at T, from the scraped counters
  prom "sum(bench_messages_sent_total) - sum(outbox_messages_delivered_total)" "$1"
}

# window T0 T1: sets COMMITTED DELIVERED CONSUMED (msg/s), P50 P99 (delivery delay, s), TX50 TX99 (writer tx, s),
# LAG (max shard lag, s), GROWTH (backlog change over the window, messages). Waits for the scrape after T1.
window() {
  local t0=$1 t1=$2
  sleep 7
  COMMITTED=$(per_s bench_messages_sent_total "$t0" "$t1")
  DELIVERED=$(per_s outbox_messages_delivered_total "$t0" "$t1")
  CONSUMED=$(per_s bench_consumer_received_total "$t0" "$t1")
  P50=$(q_over 0.5 outbox_delivery_delay_seconds "$t0" "$t1")
  P99=$(q_over 0.99 outbox_delivery_delay_seconds "$t0" "$t1")
  TX50=$(q_over 0.5 bench_tx_seconds "$t0" "$t1")
  TX99=$(q_over 0.99 bench_tx_seconds "$t0" "$t1")
  LAG=$(prom "max_over_time(max(outbox_shard_lag)[$(($t1 - $t0))s:5s])" "$t1")
  GROWTH=$(calc "$(backlog_at "$t1") - $(backlog_at "$t0")")
}

# p99_by_minute T0 T1: "p99 per minute" list over [T0, T1], e.g. "0.08 0.09 0.08".
p99_by_minute() {
  local t=$(($1 + 60)) s=""
  while [ "$t" -le "$2" ]; do s="$s $(f3 "$(q_over 0.99 outbox_delivery_delay_seconds $((t - 60)) "$t")")"; t=$((t + 60)); done
  echo "${s# }"
}

# --- Grafana annotations -----------------------------------------------------------------------------------------

# mark NAME TEXT: a region annotation (tags scenario, NAME) starting now; prints its id and fails, so `id=$(mark ...)`
# ends the scenario at its start, when Grafana does not answer. mark_end ID closes it. Open ids are listed with their
# names in $LOGS/marks.<pid> (mark runs in $(...)), so cleanup closes them, tagged NAME and "aborted", if the run fails.
mark() {
  local id
  id=$(curl -sf -XPOST -H 'Content-Type: application/json' "$GRAFANA/api/annotations" \
    -d "{\"time\": $(($(now) * 1000)), \"tags\": [\"scenario\", \"$1\"], \"text\": \"$2\"}" |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])')
  [ -n "$id" ] || { log "Grafana annotation for $1 failed: is Grafana up at $GRAFANA?"; return 1; }
  echo "$id $1" >> "$LOGS/marks.$$"
  echo "$id"
}
mark_end() {
  curl -sf -XPATCH -H 'Content-Type: application/json' "$GRAFANA/api/annotations/$1" \
    -d "{\"timeEnd\": $(($(now) * 1000))}" > /dev/null
  if [ -f "$LOGS/marks.$$" ]; then
    grep -v "^$1 " "$LOGS/marks.$$" > "$LOGS/marks.$$.tmp" || true
    mv "$LOGS/marks.$$.tmp" "$LOGS/marks.$$"
  fi
}

# kafka_stop / kafka_start: the broker container; cleanup starts it again if a run fails in between (container_up).
kafka_stop() { docker stop "$KAFKA" > /dev/null; }
kafka_start() { docker start "$KAFKA" > /dev/null; }

# --- single-message scenarios ------------------------------------------------------------------------------------

# Order mode (1 message per transaction with a business row and a row-locked per-user seq): enough writers that the
# DB, not the writer count, caps the commit rate, and a pool for them plus relay workers, consumer and maintenance.
# The consumer gets one thread per partition of `orders` (12), one inbox INSERT per poll (see consumer.sh).
WRITERS=${WRITERS:-64}
CONSUMER_THREADS=${CONSUMER_THREADS:-12}
SINGLE_ENV=(DB_POOL_SIZE=$((WRITERS + 32)) CONSUMER_THREADS="$CONSUMER_THREADS") # + 8 relay, 12 consumer, maintenance

# consumer_note: the bench consumer's measured ceilings, printed next to every end-to-end figure (from consumer.sh
# and max.sh of this run), so consumer time is not read as outbox time.
consumer_note() {
  local alone full
  alone=$(cat "$LOGS/consumer.rate" 2> /dev/null || echo "?")
  full=$(cat "$LOGS/max-single.consumed" 2> /dev/null || echo "?")
  echo "Потолок консьюмера: консьюмер бенча ($CONSUMER_THREADS потоков, один INSERT в inbox на poll) один на базе \
выбирает $alone msg/s (раздел consumer); при неограниченных писателях на тех же 2 CPU весь путь заказа потребил \
$full msg/s (раздел max). Скорость потребления и сквозное время ограничены этими значениями, а не relay."
}

# rate_note RATE: why the end-to-end scenarios run at RATE and not faster.
rate_note() {
  echo "$1 msg/s ниже ~5.1k msg/s, которые полный путь заказа выдержал в пробном прогоне 2026-10-26 (см. заметки; \
консьюмер тогда делал один round trip вставки в inbox на запись и делил 2 CPU с транзакциями писателей), \
но Postgres уже показывает ~200 % CPU при такой скорости: эти сценарии идут на загруженной базе. \
Скорость выбрана ниже этого потолка: 5k msg/s здесь уже на его уровне, а при 6k очередь консьюмера растёт."
}

# totals: "закоммичено X, доставлено Y, потреблено Z, дубликатов D, нарушений порядка V, неудавшихся транзакций
# писателей F" (this process).
totals() {
  local m; m=$(curl -sf "$METRICS")
  sum() { echo "$m" | awk -v n="$1" '$1 == n || index($1, n "{") == 1 { s += $2 } END { printf "%.0f", s }'; }
  echo "закоммичено $(sum bench_messages_sent_total), доставлено $(sum outbox_messages_delivered_total)," \
    "потреблено $(sum bench_consumer_received_total), дубликатов $(sum bench_consumer_duplicates_total)," \
    "нарушений порядка $(sum bench_consumer_order_violations_total)," \
    "неудавшихся транзакций писателей $(sum bench_tx_failed_total)"
}
