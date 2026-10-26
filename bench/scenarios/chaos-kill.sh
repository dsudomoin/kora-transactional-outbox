#!/usr/bin/env bash
# chaos kill: 3 instances at RATE msg/s each (order mode), DURATION s; every KILL_MIN–KILL_MAX s one
# instance at random gets kill -9 (by its recorded PID) and restarts after DOWN_MIN–DOWN_MAX s.
# Pass: clean reconciliation; no shard's relay stuck 60 s or more (cursor tick age from SQL, every 5 s).
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-900}
RATE=${RATE:-1000}

chaos_begin kill bench_chaos_kill 3 "Режим заказов, 3 экземпляра × $RATE msg/s в течение $DURATION с; каждые \
${KILL_MIN}–$KILL_MAX с \`kill -9\` случайного экземпляра, перезапуск через ${DOWN_MIN}–$DOWN_MAX с"
for i in 1 2 3; do chaos_load "$i" "$RATE" "$CHAOS_WRITERS"; done
chaos_for "$DURATION" kill_cycle
kill_cycle_end
chaos_checks() {
  check "$([ "$(calc "$MAX_LAG < 60")" = True ] && echo 0 || echo 1)" \
    "ни один шард relay не стоял 60 с и дольше: наибольший лаг relay $MAX_LAG с"
  check "$([ "$(grep -c 'kill -9' <<< "$FAULTS")" -gt 0 ] && echo 0 || echo 1)" \
    "\`kill -9\`: $(grep -c 'kill -9' <<< "$FAULTS") раз"
}
chaos_end 900
