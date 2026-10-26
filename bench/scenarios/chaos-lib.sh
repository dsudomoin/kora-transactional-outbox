# Helpers for the chaos-*.sh scenarios; sourced, not run (it sources lib.sh). A chaos run puts N bench instances on one
# outbox table, one Kafka topic and one consumer group, injects faults, samples every 30 s into
# $LOGS/chaos-NAME.csv, and reconciles the business tables against the inbox in SQL once the consumer has drained:
#   orders.message_id     the id the send returned in the order's committed transaction
#   bench_rolled_back     ids of messages sent in a transaction that then rolled back (ROLLBACK_PERCENT of the orders)
#   bench_consumed        first-time order messages, appended in the consumer's transaction, in commit order
# Safety as in lib.sh: instances are signalled only by their recorded PIDs after `ours`; Docker acts only on $PG and
# $KAFKA; cleanup unpauses/starts both, stops every instance and closes the annotation.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ROLLBACK=${ROLLBACK_PERCENT:-1}
SUMMARY=$LOGS/chaos-summary.txt
CHAOS_WRITERS=${CHAOS_WRITERS:-16} # per instance: 3 × (16 writers + 8 relay + 4 consumer) fits max_connections 300

rnd() { echo $(($1 + RANDOM % ($2 - $1 + 1))); } # rnd LO HI: a random integer in [LO, HI]
hms() { date -u -r "$1" +%H:%M:%S; }

# chaos_begin NAME TABLE INSTANCES TEXT [ENV=VALUE...]: fresh outbox table and topic, empty business tables, INSTANCES
# instances (consumer threads split 12 / INSTANCES), the run's annotation and CSV. TEXT describes the setup.
chaos_begin() {
  NAME=$1 TABLE=$2 N=$3 TEXT=$4
  shift 4
  RUN_ENV=(DB_POOL_SIZE=48 CONSUMER_THREADS=$((12 / N)) ROLLBACK_PERCENT="$ROLLBACK" OUTBOX_TABLE="$TABLE" "$@")
  LOADS=() KILLED=(0 0 0 0 0 0) STARTED=()
  FAULTS="" CHECKS="" CHECK_FAILED="" MAX_LAG=0 MAX_LAG_AT="" MAX_PARTS=0 NEXT_LAG=0 NEXT_CSV=0
  log "chaos $NAME: $TEXT"
  drop_outbox "$TABLE"
  local t i # ranges a previous run left detached but not dropped are standalone: drop_outbox does not reach them
  for t in $(psql_q "select relname from pg_class where relkind = 'r' and relname ~ '^${TABLE}_(x[0-9]+|tail)$'"); do
    psql_q "drop table $t" > /dev/null
  done
  kafka_reset
  for i in $(seq 1 "$N"); do : > "$LOGS/chaos-$NAME-$i.log"; done # this run's logs only (restarts append)
  chaos_start 1
  psql_q "truncate orders, bench_rolled_back, bench_consumed" > /dev/null # only this run's rows are reconciled
  INBOX_FILTER="" # a scoped inbox (consumer column): the bench consumer is unscoped, consumer ''
  if [ "$(psql_q "select count(*) from information_schema.columns where table_schema = current_schema()
      and table_name = 'inbox' and column_name = 'consumer'")" = 1 ]; then INBOX_FILTER="and i.consumer = ''"; fi
  for ((i = 2; i <= N; i++)); do chaos_start "$i"; done # not seq: BSD `seq 2 1` counts down
  MARK=$(mark "chaos-$NAME" "chaos $NAME: $TEXT")
  T0=$(now)
  # UUIDv7 ids start with their unix ms: inbox rows >= this id are this run's messages
  RUN_UUID=$(python3 -c 'import time; h = "%012x" % int(time.time() * 1000); print(h[:8] + "-" + h[8:] + "-0000-0000-000000000000")')
  CSV=$LOGS/chaos-$NAME.csv
  local heap=""
  for i in $(seq 1 "$N"); do heap="$heap,old_gen_$i,live_data_$i"; done
  echo "utc,elapsed_s,instances_up,committed,consumed,duplicates,order_violations,failed_tx,rolled_back,relay_lag_s,\
group_lag,retry_rows,dead_rows,tick_rows,cursor_rows,tick_bytes,cursor_bytes,retry_bytes,outbox_bytes,partitions,\
pending_detach,db_connections,delay_p99_s$heap" > "$CSV"
}

# chaos_start I: instance I with the run's env; STARTED[I] is the database time just before, so sessions of a killed
# predecessor (backend_start earlier) can be told apart from its own.
chaos_start() {
  STARTED[$1]=$(psql_q "select clock_timestamp()")
  inst_start "$1" "chaos-$NAME-$1" "${RUN_ENV[@]}"
}

# chaos_load I RATE WRITERS [MESSAGES_PER_TX [PAYLOAD_BYTES]]: starts instance I's load, re-applied after a restart.
chaos_load() { local i=$1; shift; LOADS[i]="$*"; on "$i" load "$@"; }

# inst_up I: (re)starts instance I with the run's env and its load.
inst_up() {
  chaos_start "$1"
  if [ -n "${LOADS[$1]:-}" ]; then on "$1" load ${LOADS[$1]}; fi
}

up() { [ -n "${APP_PIDS[$1]:-}" ]; } # up I: instance I runs (started here, not killed or stopped)

# fault TEXT: logged with its UTC time for the section, and a point annotation (best effort).
fault() {
  FAULTS="$FAULTS
- $(hms "$(now)") $1"
  log "fault: $1"
  curl -sf -XPOST -H 'Content-Type: application/json' "$GRAFANA/api/annotations" \
    -d "{\"time\": $(($(now) * 1000)), \"tags\": [\"scenario\", \"chaos-$NAME\", \"fault\"], \"text\": \"$1\"}" \
    > /dev/null || true
}

# counters I: "sent consumed duplicates violations failed rolled_back old_gen live_data" of instance I's /metrics.
counters() {
  curl -sf --max-time 5 "http://localhost:$((8084 + $1))/metrics" | awk '
    BEGIN {
      n = split("bench_messages_sent_total|bench_consumer_received_total|bench_consumer_duplicates_total|" \
        "bench_consumer_order_violations_total|bench_tx_failed_total|bench_tx_rolled_back_total|" \
        "jvm_memory_used_bytes{area=\"heap\",id=\"G1 Old Gen\"|jvm_gc_live_data_size_bytes", w, "|")
    }
    { for (j = 1; j <= n; j++) if (index($0, w[j]) == 1 && substr($0, length(w[j]) + 1, 1) ~ /[ {,]/) s[j] += $NF }
    END { for (j = 1; j <= n; j++) printf "%.0f%s", s[j], (j < n ? " " : "\n") }'
}

# chaos_kill I [NOTE]: kill -9 of instance I; its counters, read just before, are kept (KILLED) for the totals.
chaos_kill() {
  local c j=0 v pid=${APP_PIDS[$1]:-}
  if c=$(counters "$1"); then
    for v in $c; do [ "$j" -lt 6 ] && KILLED[j]=$((KILLED[j] + v)); j=$((j + 1)); done
  fi
  inst_kill9 "$1"
  fault "kill -9 bench-$1 (pid $pid)${2:+: $2}"
}

# totals_all: committed consumed duplicates violations failed rolled_back over every process of the run (live ones now,
# killed ones as read before the kill).
totals_all() {
  local t=("${KILLED[@]}") i c j v
  for i in $(seq 1 "$N"); do
    up "$i" && c=$(counters "$i") || continue
    j=0
    for v in $c; do [ "$j" -lt 6 ] && t[j]=$((t[j] + v)); j=$((j + 1)); done
  done
  echo "${t[*]}"
}

# relay_lag: seconds since the tick that the slowest shard's cursor sits on was made (a shard stuck at its position
# keeps growing it), empty when Postgres does not answer. An idle shard appends its cursor only every 5 s, so a quiet
# shard alone reads up to about 5 s without any delay in delivery.
relay_lag() {
  psql_q "select round(extract(epoch from clock_timestamp() - min(t.created_at))::numeric, 1)
    from (select distinct on (shard) tick_id from ${TABLE}_cursor order by shard, seq desc) c
    join ${TABLE}_tick t on t.id = c.tick_id" 2> /dev/null
}

# sample: every 5 s the relay lag (MAX_LAG), every 30 s a CSV row. Never fails the run (Postgres or Kafka may be down).
sample() {
  local t l
  t=$(now)
  if [ "$t" -ge "$NEXT_LAG" ]; then
    NEXT_LAG=$((t + 5))
    l=$(relay_lag) || l=""
    if [ -n "$l" ] && [ "$(calc "$l > $MAX_LAG")" = True ]; then MAX_LAG=$l MAX_LAG_AT=$(hms "$t"); fi
  fi
  if [ "$t" -ge "$NEXT_CSV" ]; then NEXT_CSV=$((t + 30)); csv_row || true; fi
}

csv_row() {
  local t i c db p99 heap="" ups=0 tot
  t=$(now)
  for i in $(seq 1 "$N"); do
    if up "$i" && c=$(counters "$i"); then
      ups=$((ups + 1))
      set -- $c
      heap="$heap,$7,$8"
    else
      heap="$heap,,"
    fi
  done
  tot=$(totals_all | tr ' ' ',')
  db=$(psql_q "select (select count(*) from ${TABLE}_retry), (select count(*) from ${TABLE}_dead),
      (select count(*) from ${TABLE}_tick), (select count(*) from ${TABLE}_cursor),
      pg_total_relation_size('${TABLE}_tick'), pg_total_relation_size('${TABLE}_cursor'),
      pg_total_relation_size('${TABLE}_retry'),
      (select coalesce(sum(pg_total_relation_size(inhrelid)), 0) from pg_inherits where inhparent = '$TABLE'::regclass),
      (select count(*) from pg_inherits where inhparent = '$TABLE'::regclass),
      (select count(*) from pg_inherits where inhparent = '$TABLE'::regclass and inhdetachpending),
      (select count(*) from pg_stat_activity where datname = current_database())" 2> /dev/null | tr '|' ',') ||
    db=",,,,,,,,,,"
  i=$(echo "$db" | cut -d, -f9)
  if [ -n "$i" ] && [ "$i" -gt "$MAX_PARTS" ]; then MAX_PARTS=$i; fi
  p99=$(prom 'histogram_quantile(0.99, sum by (le) (increase(outbox_delivery_delay_seconds_bucket[30s])))' 2> /dev/null) ||
    p99=""
  echo "$(date -u -r "$t" +'%Y-%m-%d %H:%M:%S'),$((t - T0)),$ups,$tot,$(relay_lag || true),$(group_lag || true),$db,\
${p99/NaN/}$heap" >> "$CSV"
}

# chaos_for SECONDS [HOOK]: SECONDS of the run, calling HOOK (fault scheduling) and sample about every second.
chaos_for() {
  local end=$(($(now) + $1))
  while [ "$(now)" -lt "$end" ]; do
    if [ -n "${2:-}" ]; then "$2"; fi
    sample || true
    sleep 1
  done
}

# check OK TEXT: a pass condition of the scenario besides the reconciliation; OK is 0 (passed) or anything else.
check() {
  if [ "$1" = 0 ]; then CHECKS="$CHECKS
- пройдена: $2"; else CHECKS="$CHECKS
- **НЕ ПРОЙДЕНА**: $2"; CHECK_FAILED=1; fi
}

# chaos_progress: every running instance commits and the cluster relays and consumes, over the next 20 s (after a
# Postgres or Kafka fault: the instances recovered by themselves).
chaos_progress() {
  local i before=() after=() d0 d1 delivered0=0 delivered1=0 ok=0 c
  for i in $(seq 1 "$N"); do
    up "$i" || continue
    before[i]=$(on "$i" metric bench_messages_sent_total || echo 0)
    delivered0=$((delivered0 + $(on "$i" metric outbox_messages_delivered_total || echo 0)))
  done
  c=$(totals_all); d0=$(echo "$c" | cut -d' ' -f2)
  chaos_for 20
  for i in $(seq 1 "$N"); do
    up "$i" || continue
    after[i]=$(on "$i" metric bench_messages_sent_total || echo 0)
    delivered1=$((delivered1 + $(on "$i" metric outbox_messages_delivered_total || echo 0)))
    if ! ours "${APP_PIDS[i]}" || [ "${after[i]}" -le "${before[i]}" ]; then ok=1; fi
  done
  c=$(totals_all); d1=$(echo "$c" | cut -d' ' -f2)
  [ "$delivered1" -gt "$delivered0" ] && [ "$d1" -gt "$d0" ] || ok=1
  check "$ok" "каждый экземпляр — тот же процесс, что был запущен в начале, и коммитил в последние 20 с нагрузки; \
кластер за это время доставил $((delivered1 - delivered0)) и потребил $((d1 - d0)) сообщений"
}

# reconcile_orders TIMEOUT [BULK]: after every writer stopped (STOPPED_AT, a database timestamp), instances running.
# Drained = Kafka group lag 0, no retry rows, no paused shard, every shard's cursor at a tick made after STOPPED_AT (the
# relay has passed every commit), and (lost, phantom, inbox rows of the run) unchanged over a 10 s re-check. Then:
#   lost       committed orders whose message_id has no inbox row                               must be 0
#   phantom    bench_rolled_back ids (their transaction rolled back) that have an inbox row     must be 0
#   order      bench_consumed rows whose seq does not exceed the user's previous one; counters  must be 0
#   unexplained  inbox rows of this run (UUIDv7 >= run start) - consumed orders - BULK          must be 0
#              (BULK: bulk messages committed, from the counters; > 0 means an unknown delivery, < 0 lost bulk)
#   dead       rows in the dead table                                                           must be DEAD (0)
# duplicates are counted from the consumer counters. Sets RECON and R_* and returns 0 clean, 1 NOT DRAINED (nothing is
# known about loss), 2 a defect, 3 inconclusive (no orders, or no rolled-back ids although ROLLBACK > 0).
reconcile_orders() {
  local deadline=$(($(now) + $1)) bulk=${2:-0} lag retry paused passed cur prev="" heavy
  R_DRAINED=""
  while [ "$(now)" -lt "$deadline" ]; do
    lag=$(group_lag) || lag=""
    read -r retry paused passed <<< "$(psql_q "select (select count(*) from ${TABLE}_retry),
      (select count(*) from (select distinct on (shard) paused_until from ${TABLE}_cursor order by shard, seq desc) c
        where paused_until > now()),
      (select coalesce(bool_and(c.tick_id >= t.id), false)
        from (select distinct on (shard) tick_id from ${TABLE}_cursor order by shard, seq desc) c,
        (select min(id) as id from ${TABLE}_tick where created_at > '$STOPPED_AT') t)" 2> /dev/null | tr '|' ' ')" ||
      true
    cur=""
    if [ "$lag" = 0 ] && [ "${retry:-}" = 0 ] && [ "${paused:-}" = 0 ] && [ "${passed:-}" = t ]; then
      heavy=$(psql_q "set work_mem = '256MB';
        select (select count(*) from orders o where not exists
            (select 1 from inbox i where i.message_id = o.message_id $INBOX_FILTER)),
          (select count(*) from bench_rolled_back r where exists
            (select 1 from inbox i where i.message_id = r.message_id $INBOX_FILTER)),
          (select count(*) from inbox i where i.message_id >= '$RUN_UUID' $INBOX_FILTER)" | tr '|' ' ') || heavy=""
      cur="$heavy"
    fi
    if [ -n "$cur" ] && [ "$cur" = "$prev" ]; then R_DRAINED=$(now); break; fi
    prev=$cur
    sample || true
    sleep 10
  done
  if [ -z "$R_DRAINED" ]; then
    RECON="NOT DRAINED за $1 с (лаг группы ${lag:-неизвестен}, строк retry ${retry:-?}, шардов на паузе ${paused:-?}, \
relay прошёл момент остановки: ${passed:-?}): сверка без результата, таблицы сохранены"
    return 1
  fi
  read -r R_LOST R_PHANTOM R_INBOX <<< "$cur"
  read -r R_ORDERS R_ROLLED R_CONSUMED R_VIOL_SQL R_DEAD <<< "$(psql_q "set work_mem = '256MB';
    select (select count(*) from orders), (select count(*) from bench_rolled_back),
      (select count(*) from bench_consumed),
      (select count(*) from (select seq <= lag(seq) over (partition by user_id order by id) as bad
        from bench_consumed) c where bad),
      (select count(*) from ${TABLE}_dead)" | tr '|' ' ')"
  read -r _ R_RECEIVED R_DUPS R_VIOL _ _ <<< "$(totals_all)"
  R_UNEXPLAINED=$((R_INBOX - (R_ORDERS - R_LOST) - bulk))
  RECON="закоммичено заказов $R_ORDERS, **потеряно $R_LOST**; откачено $R_ROLLED, **фантомов $R_PHANTOM**; нарушений \
порядка **$R_VIOL_SQL** по SQL на $R_CONSUMED впервые полученных сообщениях заказов, **$R_VIOL** по счётчикам \
консьюмера; строк inbox этого прогона $R_INBOX, закоммичено bulk-сообщений $bulk, **необъяснённых $R_UNEXPLAINED**; \
dead **$R_DEAD** (ожидалось ${DEAD:-0}); дубликатов $R_DUPS ($(f2 "$(calc "100 * $R_DUPS / max($R_RECEIVED, 1)")") % \
от $R_RECEIVED потреблённых, по счётчикам)"
  if [ "$R_LOST" != 0 ] || [ "$R_PHANTOM" != 0 ] || [ "$R_VIOL_SQL" != 0 ] || [ "$R_VIOL" != 0 ] ||
    [ "$R_UNEXPLAINED" != 0 ] || [ "$R_DEAD" != "${DEAD:-0}" ]; then
    RECON="$RECON — DEFECT (таблицы сохранены)"
    return 2
  fi
  if [ "$R_ORDERS" = 0 ] || { [ "$ROLLBACK" != 0 ] && [ "$R_ROLLED" = 0 ]; }; then
    RECON="$RECON — INCONCLUSIVE: нечего проверять (таблицы сохранены)"
    return 3
  fi
  return 0
}

# chaos_end DRAIN_TIMEOUT: stops every load, reconciles, runs the scenario's `chaos_checks` if it defines one (and
# appends the output of its `chaos_extra`, if any, to the section),
# closes the annotation, appends the section (setup, faults, reconciliation, checks, CSV summary) to RESULTS and a line
# to the summary, stops the instances. Clean and every check passed: the outbox table is dropped. Otherwise it is kept
# and the scenario exits non-zero (chaos-all stops).
chaos_end() {
  local timeout=$1 i rc=0 bulk=0 stopped drain
  for i in $(seq 1 "$N"); do
    if up "$i"; then on "$i" load_stop; fi
  done
  for i in $(seq 1 "$N"); do # bulk instances are never killed: their counter is the bulk committed
    set -- ${LOADS[i]:-}
    if up "$i" && [ "${3:-1}" -gt 1 ]; then bulk=$((bulk + $(on "$i" metric bench_messages_sent_total))); fi
  done
  STOPPED_AT=$(psql_q "select clock_timestamp()")
  stopped=$(now)
  reconcile_orders "$timeout" "$bulk" || rc=$?
  if [ "$rc" = 1 ]; then drain="не за $timeout с"; else drain="за $((R_DRAINED - stopped)) с"; fi
  csv_row || true
  if [ -n "$R_DRAINED" ] && declare -F chaos_checks > /dev/null; then chaos_checks; fi
  mark_end "$MARK"
  [ "$rc" != 0 ] || [ -z "${CHECK_FAILED:-}" ] || rc=5
  out << EOF

## chaos: $NAME

$TEXT. Экземпляров: $N, на таблице outbox \`$TABLE\` (у каждого шардов 32, relay workers 8), один топик Kafka
\`orders\` (12 партиций) и одна consumer group (потоков консьюмера на экземпляр: $((12 / N))); $ROLLBACK % заказов
откатываются после отправки. Прогон $(utc "$T0") – $(hms "$stopped") UTC, затем выборка. Замеры каждые 30 с:
\`bench/build/scenarios/chaos-$NAME.csv\`.

Сбои (UTC):${FAULTS:-
- нет}

Сверка (SQL по бизнес-таблицам; консьюмер всё выбрал $drain после остановки): $RECON.
Наибольший лаг relay (возраст тика под курсором самого медленного шарда, замер каждые 5 с; на простаивающем шарде до ~5 с): ${MAX_LAG} с${MAX_LAG_AT:+ в $MAX_LAG_AT}.

Проверки:${CHECKS:-
- только сверка}

$(csv_summary "$CSV")
$(if declare -F chaos_extra > /dev/null; then echo; chaos_extra; fi)
EOF
  echo "chaos-$NAME: $([ "$rc" = 0 ] && echo PASS || echo "FAIL (rc $rc)") — lost ${R_LOST:-?}, phantom \
${R_PHANTOM:-?} of ${R_ROLLED:-?}, order ${R_VIOL_SQL:-?}/${R_VIOL:-?}, unexplained ${R_UNEXPLAINED:-?}, dead \
${R_DEAD:-?}, duplicates ${R_DUPS:-?}, orders ${R_ORDERS:-?}, max relay lag ${MAX_LAG} s, drain $drain" >> "$SUMMARY"
  for i in $(seq 1 "$N"); do inst_stop "$i"; done
  if [ "$rc" = 5 ]; then RECON="$RECON; a check failed"; fi
  verdict "$TABLE" "$rc"
}

# csv_summary CSV: min, max, first, last and least-squares slope per hour of every numeric column after the first minute.
csv_summary() {
  python3 - "$1" << 'PY'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
rows = [r for r in rows if float(r["elapsed_s"]) >= 60] or rows
print("| показатель (после первой минуты) | мин | макс | первое | последнее | наклон в час |")
print("|---|---|---|---|---|---|")
for col in rows[0].keys() if rows else []:
    if col in ("utc", "elapsed_s"):
        continue
    pts = [(float(r["elapsed_s"]) / 3600, float(r[col])) for r in rows if r[col] not in ("", "NaN")]
    if not pts:
        continue
    xs, ys = zip(*pts)
    n = len(pts); mx = sum(xs) / n; my = sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in pts) / den if den else 0
    fmt = lambda v: f"{v:.3g}" if abs(v) < 1000 else f"{v:,.0f}".replace(",", " ")
    print(f"| {col} | {fmt(min(ys))} | {fmt(max(ys))} | {fmt(ys[0])} | {fmt(ys[-1])} | {fmt(slope)} |")
PY
}

# --- kill schedule -------------------------------------------------------------------------------------------------

# kill_cycle: a chaos_for hook. While every instance runs, at NEXT it kills the one `victim` prints ("I [NOTE]"; by
# default a random instance), restarts it DOWN_MIN–DOWN_MAX s later, and sets NEXT KILL_MIN–KILL_MAX s after that.
# kill_cycle_end restarts an instance still down when the load phase ends.
KILL_MIN=${KILL_MIN:-20} KILL_MAX=${KILL_MAX:-60} DOWN_MIN=${DOWN_MIN:-5} DOWN_MAX=${DOWN_MAX:-15}
DOWN="" UP_AT=0 NEXT=""
victim() { rnd 1 "$N"; }
kill_cycle() {
  local note
  if [ -z "$NEXT" ]; then NEXT=$(($(now) + $(rnd "$KILL_MIN" "$KILL_MAX"))); fi
  if [ -n "$DOWN" ]; then
    if [ "$(now)" -ge "$UP_AT" ]; then
      inst_up "$DOWN"
      fault "bench-$DOWN перезапущен"
      DOWN="" NEXT=$(($(now) + $(rnd "$KILL_MIN" "$KILL_MAX")))
    fi
  elif [ "$(now)" -ge "$NEXT" ]; then
    read -r DOWN note <<< "$(victim)"
    chaos_kill "$DOWN" "$note"
    UP_AT=$(($(now) + $(rnd "$DOWN_MIN" "$DOWN_MAX")))
  fi
}
kill_cycle_end() {
  if [ -n "$DOWN" ]; then inst_up "$DOWN"; fault "bench-$DOWN перезапущен"; DOWN=""; fi
}
