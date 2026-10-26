#!/usr/bin/env bash
# chaos pg-restart: 3 instances at RATE msg/s each (order mode), DURATION s; FAULT_AT s in, `docker
# restart` of the Postgres container under load. Pass: clean; the instances recover by themselves (the same processes
# commit, relay and consume again).
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-300}
RATE=${RATE:-1000}
FAULT_AT=${FAULT_AT:-120}

chaos_begin pg-restart bench_chaos_pg_restart 3 "Режим заказов, 3 экземпляра × $RATE msg/s в течение $DURATION с; \
\`docker restart $PG\` на $FAULT_AT-й секунде"
for i in 1 2 3; do chaos_load "$i" "$RATE" "$CHAOS_WRITERS"; done
chaos_for "$FAULT_AT"
fault "docker restart $PG"
docker restart "$PG" > /dev/null
fault "$PG перезапущен"
chaos_for $((DURATION - FAULT_AT - 20))
chaos_progress
chaos_end 900
