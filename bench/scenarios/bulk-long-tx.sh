#!/usr/bin/env bash
# bulk-long-tx: the bulk setup (shards 4, noop) throttled to half its measured max, with a transaction holding an
# xid open for LONG_TX s in the middle. Delivery delay must stay flat while the snapshot xmin is pinned.
source "$(dirname "$0")/lib.sh"
LONG_TX=${LONG_TX:-300}
max=$(cat "$LOGS/bulk-s4-noop.rate" 2> /dev/null || echo 0)
RATE=${BULK_LONG_TX_RATE:-$(python3 -c "print(int($max / 2 // 10000 * 10000))")}
[ "$RATE" -gt 0 ] || { log "no bulk-s4-noop rate: run bulk.sh first or set BULK_LONG_TX_RATE"; exit 1; }
table=bench_bulk_long_tx

log "bulk-long-tx at $RATE msg/s"
drop_outbox "$table"
kafka_reset
app_start bulk-long-tx CONSUMER_THREADS=0 BENCH_HANDLER=noop OUTBOX_TABLE="$table" OUTBOX_SHARDS=4 \
  OUTBOX_RELAY_WORKERS=4 OUTBOX_RELAY_BATCH_SIZE=20000 OUTBOX_RELAY_IDLE_POLL=100ms
id=$(mark bulk-long-tx "bulk-long-tx: $RATE msg/s, ${LONG_TX} s open transaction")
load "$RATE" 2 10000 100
sleep 60
t0=$(now)
sleep 60
t1=$(now)
long_tx "$LONG_TX"
xid=$LONG_TX_XID
cpu_start
pinned=$(xmin_pinned "$table" "$xid")
long_tx_wait
t2=$(now)
cpu_stop; cpu=$CPU
sleep 60
t3=$(now)
load_stop
wait_delivered 120 || true
mark_end "$id"
minutes=$(p99_by_minute "$t1" "$t2")
app_stop

row() { # row PHASE T0 T1
  window "$2" "$3"
  echo "| $1 | $(utc "$2" | cut -c12-) – $(utc "$3" | cut -c12-) | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") \
| $(f3 "$P50") s | $(f3 "$P99") s | $(f1 "$LAG") s |"
}

{
  cat << EOF

## bulk-long-tx

Конфигурация bulk (шардов 4, noop), ограниченная до **$RATE msg/s** (половина максимума bulk при шардах 4 и noop,
округлённая вниз до 10 000), с сессией, которая держит xid открытым $LONG_TX с (\`begin; select pg_current_xact_id();
select pg_sleep($LONG_TX)\`, xid $xid). snapshot xmin самого нового тика был равен этому xid в $pinned, снятых каждые
10 с, пока транзакция была открыта. CPU БД, пока она была открыта: $cpu% из 200%. Дата $(utc "$t0" | cut -c1-10) (UTC).

| фаза | окно (UTC) | записано msg/s | доставлено msg/s | задержка p50 | задержка p99 | наибольший лаг шарда |
|---|---|---|---|---|---|---|
EOF
  row до "$t0" "$t1"
  row "транзакция открыта" "$t1" "$t2"
  row после "$t2" "$t3"
  cat << EOF

p99 задержки по минутам, пока транзакция была открыта: $minutes с.

Relay читает пакеты по снапшотам тиков, а не только строки ниже snapshot xmin, поэтому открытая транзакция, которая
удерживает xmin, не останавливает доставку того, что закоммичено после её начала.
EOF
} | out
drop_outbox "$table"
