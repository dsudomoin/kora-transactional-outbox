#!/usr/bin/env bash
# chaos small-pool: 1 instance with a Hikari pool of POOL connections, fewer than its POOL_WRITERS writers plus
# 8 relay workers (and 12 consumer threads), at RATE msg/s (order mode) for DURATION s.
# Pass: clean; no deadlock: committed and consumed grow in every 30 s sample of the load; the rates are reported.
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-300}
RATE=${RATE:-3000}
WRITERS=${POOL_WRITERS:-32} # its own name: lib.sh already defaults WRITERS to 64
POOL=${POOL:-12}

chaos_begin small-pool bench_chaos_small_pool 1 "Режим заказов, 1 экземпляр, писатели ($WRITERS) запрашивают $RATE msg/s в \
течение $DURATION с при \`DB_POOL_SIZE=$POOL\` (писателям, 8 relay-воркерам и 12 потокам консьюмера нужно $((WRITERS + 20)))" \
  DB_POOL_SIZE="$POOL"
chaos_load 1 "$RATE" "$WRITERS"
chaos_for "$DURATION"
chaos_checks() {
  local stalls rate
  read -r stalls rate <<< "$(python3 - "$CSV" "$DURATION" << 'PY'
import csv, sys
rows = [r for r in csv.DictReader(open(sys.argv[1])) if 30 <= float(r["elapsed_s"]) <= float(sys.argv[2])]
stalls = sum(1 for a, b in zip(rows, rows[1:]) for c in ("committed", "consumed") if float(b[c] or 0) <= float(a[c] or 0))
span = float(rows[-1]["elapsed_s"]) - float(rows[0]["elapsed_s"]) if len(rows) > 1 else 0
rate = (float(rows[-1]["committed"]) - float(rows[0]["committed"])) / span if span else 0
print(stalls, round(rate))
PY
)"
  check "$([ "$stalls" = 0 ] && echo 0 || echo 1)" "ни одного 30-секундного замера без коммитов или потребления \
(остановок: $stalls); закоммичено $rate msg/s из запрошенных $RATE, неудавшихся транзакций писателей \
$(totals_all | cut -d' ' -f5)"
}
chaos_end 900
