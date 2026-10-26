#!/usr/bin/env bash
# chaos slow-handler: 1 instance whose Kafka handler sleeps HANDLER_DELAY_MS per record before each
# handleBatch (DelayedHandler), with the library's default batchSize (1000) and batchTimeout (30 s), at RATE msg/s
# (order mode) for DURATION s. 8 relay workers × 1000 / (1000 × 20 ms) ≈ 400 msg/s of handler capacity, so RATE above it
# builds a backlog that must drain after the load stops.
# Pass: clean; duplicates at most 5 % of the consumed messages (no duplicate storm).
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-300}
RATE=${RATE:-600}
DELAY=${HANDLER_DELAY_MS:-20}

chaos_begin slow-handler bench_chaos_slow_handler 1 "Режим заказов, 1 экземпляр, $RATE msg/s в течение $DURATION с; \
обработчик Kafka спит $DELAY мс на каждую запись перед каждым пакетом, \`batchSize = 1000\`, \`batchTimeout = 30 s\` \
(значения по умолчанию)" \
  HANDLER_DELAY_MS="$DELAY" OUTBOX_RELAY_BATCH_SIZE=1000
chaos_load 1 "$RATE" "$CHAOS_WRITERS"
chaos_for "$DURATION"
chaos_checks() {
  check "$([ "$(calc "$R_DUPS <= 0.05 * $R_RECEIVED")" = True ] && echo 0 || echo 1)" \
    "дубликатов $R_DUPS из $R_RECEIVED потреблённых (допустимо 5 %)"
}
chaos_end 2400
