#!/usr/bin/env bash
# chaos pg-pause: 3 instances at RATE msg/s each (order mode), DURATION s; FAULT_AT s in, the Postgres
# container is frozen with `docker pause` for PAUSE s, then `docker unpause`d (cleanup unpauses it if the run is
# interrupted meanwhile). Pass: clean; the instances recover by themselves.
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-300}
RATE=${RATE:-1000}
FAULT_AT=${FAULT_AT:-120}
PAUSE=${PAUSE:-30}

chaos_begin pg-pause bench_chaos_pg_pause 3 "Режим заказов, 3 экземпляра × $RATE msg/s в течение $DURATION с; \
\`docker pause $PG\` на $PAUSE с, начиная с $FAULT_AT-й секунды"
for i in 1 2 3; do chaos_load "$i" "$RATE" "$CHAOS_WRITERS"; done
chaos_for "$FAULT_AT"
fault "docker pause $PG"
docker pause "$PG" > /dev/null
sleep "$PAUSE"
docker unpause "$PG" > /dev/null
fault "docker unpause $PG"
chaos_for $((DURATION - FAULT_AT - PAUSE - 20))
chaos_progress
chaos_end 900
