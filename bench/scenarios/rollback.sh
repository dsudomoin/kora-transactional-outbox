#!/usr/bin/env bash
# rollback: the previous release (OLD_REV, feature/requeue) beside and after this one on one outbox table, order mode
# at RATE msg/s per loaded instance. Instance 1 runs this version, instance 2 the previous one:
#   1. new under load LOAD_S s, then an idle tail of TAIL_S s (no load) with cursorAnchorInterval ANCHOR, so
#      maintenance anchors the idle shards and cleans ticks up to the anchors;
#   2. new under load again, old started beside it (a rolling deploy back: WINDOW s together), new stopped;
#   3. old alone under load LOAD_S s, from the cursors and ticks the new one left;
#   4. new started beside old (WINDOW s together), old stopped, new alone LOAD_S s; then the reconciliation.
# Pass: clean reconciliation of the business tables against the inbox (no lost message, every shard's effective tick
# past the stop: no stuck shard), the idle tail anchored every shard, and no ERROR in either version's log.
# The previous release's bench app is built once from a `git archive OLD_REV` export in OLD_ROOT.
source "$(dirname "$0")/chaos-lib.sh"
RATE=${RATE:-1000}
LOAD_S=${LOAD_S:-120}
WINDOW=${WINDOW:-30}
ANCHOR_S=${ANCHOR_S:-120}
TAIL_S=${TAIL_S:-210} # past ANCHOR_S + 2 maintenance intervals (30 s): every idle shard anchored at least once
BOUND=$((ANCHOR_S + 60)) # an anchored cursor's tick, and the oldest tick kept, is at most about this old
OLD_REV=${OLD_REV:-5fc60dd}
OLD_ROOT=${OLD_ROOT:-$ROOT/bench/build/rollback-$OLD_REV}
OLD_APP=$OLD_ROOT/bench/build/install/bench/bin/bench
NEW_APP=$APP

if [ ! -x "$OLD_APP" ]; then
  log "building the bench app of $OLD_REV in $OLD_ROOT"
  mkdir -p "$OLD_ROOT"
  git -C "$ROOT" archive "$OLD_REV" | tar -x -C "$OLD_ROOT"
  (cd "$OLD_ROOT" && ./gradlew :bench:installDist -q)
fi

# instance 2 is the previous release, and starts only when the scenario asks (chaos_begin starts every instance)
OLD_UP=""
chaos_start() {
  if [ "$1" = 2 ] && [ -z "$OLD_UP" ]; then return 0; fi
  STARTED[$1]=$(psql_q "select clock_timestamp()")
  if [ "$1" = 2 ]; then APP=$OLD_APP; else APP=$NEW_APP; fi
  inst_start "$1" "chaos-$NAME-$1" "${RUN_ENV[@]}"
  APP=$NEW_APP
}
# stop_inst I: graceful stop of instance I, its counters kept for the totals as chaos_kill keeps them
stop_inst() {
  local c j=0 v
  on "$1" load_stop || true
  if c=$(counters "$1"); then
    for v in $c; do [ "$j" -lt 6 ] && KILLED[j]=$((KILLED[j] + v)); j=$((j + 1)); done
  fi
  LOADS[$1]=""
  inst_stop "$1"
  fault "bench-$1 stopped ($([ "$1" = 2 ] && echo "$OLD_REV" || echo new))"
}
start_old() { OLD_UP=1; chaos_start 2; fault "bench-2 started ($OLD_REV)"; chaos_load 2 "$RATE" "$CHAOS_WRITERS"; }
start_new() { chaos_start 1; fault "bench-1 started (new)"; chaos_load 1 "$RATE" "$CHAOS_WRITERS"; }

chaos_begin rollback bench_rollback 2 "Откат на предыдущий релиз ($OLD_REV) и обратно: режим заказов, $RATE msg/s на \
нагруженный экземпляр; новая версия $LOAD_S с под нагрузкой и $TAIL_S с простоя (cursorAnchorInterval ${ANCHOR_S} с), затем \
старая рядом с новой $WINDOW с (rolling deploy), старая одна $LOAD_S с, новая рядом со старой $WINDOW с, новая одна \
$LOAD_S с" OUTBOX_CURSOR_ANCHOR_INTERVAL="${ANCHOR_S}s"
chaos_load 1 "$RATE" "$CHAOS_WRITERS"
chaos_for "$LOAD_S"
on 1 load_stop; LOADS[1]=""; fault "bench-1: нагрузка снята, простой"
chaos_for "$TAIL_S"
# the idle shards' latest cursor rows: anchored within the interval, so tick cleanup keeps about one interval
read -r SHARDS_N CURSOR_AGE TICKS_KEPT TICK_AGE <<< "$(psql_q "select count(*),
    round(extract(epoch from clock_timestamp() - min(t.created_at)))::int, (select count(*) from ${TABLE}_tick),
    (select round(extract(epoch from clock_timestamp() - min(created_at)))::int from ${TABLE}_tick)
  from (select distinct on (shard) shard, tick_id from ${TABLE}_cursor order by shard, seq desc) c
  left join ${TABLE}_tick t on t.id = c.tick_id" | tr '|' ' ')"
chaos_load 1 "$RATE" "$CHAOS_WRITERS"; fault "bench-1: нагрузка снова"
chaos_for 20
start_old
chaos_for "$WINDOW"
stop_inst 1
chaos_for "$LOAD_S"
start_new
chaos_for "$WINDOW"
stop_inst 2
chaos_for "$LOAD_S"

errors() { grep -h ' ERROR ' "$LOGS"/chaos-rollback-"$1".log 2> /dev/null | sed 's/^.* ERROR //' | cut -c1-160 | sort | uniq -c; }
chaos_checks() {
  check "$([ "${CURSOR_AGE:-x}" -le "$BOUND" ] 2> /dev/null && [ "$TICK_AGE" -le "$BOUND" ] && echo 0 || echo 1)" \
    "через $TAIL_S с простоя тик последней строки курсора каждого из $SHARDS_N шардов не старше $BOUND с (самый старый \
${CURSOR_AGE:-удалён} с: якоря); тиков осталось $TICKS_KEPT, самому старому $TICK_AGE с"
  local new old
  new=$(errors 1) old=$(errors 2)
  check "$([ -z "$new$old" ] && echo 0 || echo 1)" "ни одного ERROR в логах обеих версий${new:+; новая: $new}\
${old:+; $OLD_REV: $old}"
}
chaos_end 900
