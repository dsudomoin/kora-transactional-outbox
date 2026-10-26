#!/usr/bin/env bash
# chaos soak: DURATION s (60 min) of two instances on one outbox table: bench-1 in order mode at RATE
# msg/s, bench-2 bulk at BULK_RATE msg/s (BULK_WRITERS writers × BULK_PER_TX messages per transaction). Ranges of
# XIDS xids (1M by default instead of 16.7M, so partitions are created and dropped within the hour).
# Pass: clean (bulk messages by count: inbox rows of the run = orders + bulk committed); flat: over the last third of
# the run no sampled column rises above 1.25 × its mean over the middle third plus a small absolute slack; for the
# JVM heap columns (a sawtooth) the floor (minimum) of the last third against 1.25 × the floor of the middle third.
# The outbox's own size is reported, not checked: it is a sawtooth of whole xid ranges by design.
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-3600}
RATE=${RATE:-2000}
BULK_RATE=${BULK_RATE:-20000}
BULK_WRITERS=${BULK_WRITERS:-2}
BULK_PER_TX=${BULK_PER_TX:-1000}
XIDS=${XIDS:-1000000}
DIAG=${DIAG:-0} # 1: soak-diag.sh captures statement, checkpoint, WAL, I/O, size and wait-event data every 10 min
if [ "$DIAG" = 1 ]; then source "$(dirname "$0")/soak-diag.sh"; fi

chaos_begin soak bench_chaos_soak 2 "Soak: bench-1 в режиме заказов, $RATE msg/s; bench-2 в режиме bulk, $BULK_RATE msg/s \
(писатели × сообщения на транзакцию: $BULK_WRITERS × $BULK_PER_TX); $DURATION с, \`xidsPerPartition = $XIDS\`" \
  OUTBOX_XIDS_PER_PARTITION="$XIDS"
chaos_load 1 "$RATE" "$CHAOS_WRITERS"
chaos_load 2 "$BULK_RATE" "$BULK_WRITERS" "$BULK_PER_TX" 300
if [ "$DIAG" = 1 ]; then
  diag_begin
  chaos_for "$DURATION" diag_hook
  diag_final
  chaos_extra() { diag_report; }
else
  chaos_for "$DURATION"
fi
chaos_checks() {
  local verdict
  verdict=$(python3 - "$CSV" "$DURATION" << 'PY'
import csv, sys
dur = float(sys.argv[2])
rows = [r for r in csv.DictReader(open(sys.argv[1])) if float(r["elapsed_s"]) <= dur]
mid = [r for r in rows if dur / 3 <= float(r["elapsed_s"]) < 2 * dur / 3]
last = [r for r in rows if float(r["elapsed_s"]) >= 2 * dur / 3]
slack = {"rows": 1000, "bytes": 8 << 20, "partitions": 2, "pending_detach": 1, "db_connections": 10,
         "old_gen": 256 << 20, "live_data": 256 << 20, "delay_p99_s": 1.0, "relay_lag_s": 5}
bad = []
for col in rows[0].keys():
    key = next((k for k in slack if k in col), None)
    if key is None or col == "dead_rows" or col == "outbox_bytes":
        continue
    m = [float(r[col]) for r in mid if r[col] not in ("", "NaN")]
    l = [float(r[col]) for r in last if r[col] not in ("", "NaN")]
    if not m or not l:
        continue
    heap = key in ("old_gen", "live_data")
    limit = 1.25 * (min(m) if heap else sum(m) / len(m)) + slack[key]
    seen = min(l) if heap else max(l)
    if seen > limit:
        bad.append(f"{col} {seen:.4g} > {limit:.4g}")
print("; ".join(bad))
PY
)
  check "$([ -z "$verdict" ] && echo 0 || echo 1)" "ровно на последней трети прогона относительно средней (строки и \
байты tick/cursor/retry, партиции, pending detach, соединения с БД, old gen и live data по экземплярам, p99 задержки, \
лаг relay)${verdict:+: $verdict}"
}
chaos_end 1800
