#!/usr/bin/env bash
# chaos kafka-restart: 3 instances at RATE msg/s each (order mode), DURATION s; FAULT_AT s in, `docker
# restart` of the Kafka container under load. Pass: clean (the backlog drains); the instances recover by themselves.
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-300}
RATE=${RATE:-1000}
FAULT_AT=${FAULT_AT:-120}

chaos_begin kafka-restart bench_chaos_kafka_restart 3 "Режим заказов, 3 экземпляра × $RATE msg/s в течение $DURATION с; \
\`docker restart $KAFKA\` на $FAULT_AT-й секунде"
for i in 1 2 3; do chaos_load "$i" "$RATE" "$CHAOS_WRITERS"; done
chaos_for "$FAULT_AT"
fault "docker restart $KAFKA"
docker restart "$KAFKA" > /dev/null
fault "$KAFKA перезапущен"
chaos_for $((DURATION - FAULT_AT - 20))
chaos_progress
chaos_end 900
