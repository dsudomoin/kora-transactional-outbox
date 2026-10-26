#!/usr/bin/env bash
# long-transaction: order mode at RATE msg/s with a session holding an xid open for LONG_TX s in the middle; delivery
# delay p99 must stay flat while the snapshot xmin is pinned.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-4000}
LONG_TX=${LONG_TX:-600}
table=bench_long_tx

log long-transaction
drop_outbox "$table"
kafka_reset
app_start long-transaction "${SINGLE_ENV[@]}" OUTBOX_TABLE="$table"
id=$(mark long-transaction "long-transaction: $RATE msg/s, ${LONG_TX} s open transaction")
load "$RATE" "$WRITERS"
sleep 30
t0=$(now)
sleep 120
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
stopped=$(now); rc=0; reconcile "$table" 900 "$(metric bench_messages_sent_total)" || rc=$?
if [ "$rc" = 1 ]; then drain="не за 900 с"; else drain="за $(($(now) - stopped)) с"; fi
mark_end "$id"
minutes=$(p99_by_minute "$t1" "$t2")
end=$(totals)
app_stop

row() { # row PHASE T0 T1
  window "$2" "$3"
  echo "| $1 | $(utc "$2" | cut -c12-) – $(utc "$3" | cut -c12-) | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") \
| $(f0 "$CONSUMED") | $(f3 "$P50") s | $(f3 "$P99") s | $(f1 "$LAG") s |"
}

{
  cat << EOF

## long-transaction

Режим заказов (1 сообщение на транзакцию, писателей $WRITERS), $RATE msg/s, обработчик Kafka, консьюмер с inbox;
шардов 32, relay workers 8. Сессия держала xid $xid открытым $LONG_TX с (\`begin; select pg_current_xact_id(); select
pg_sleep($LONG_TX)\`); snapshot xmin самого нового тика был равен этому xid в $pinned, снятых каждые 10 с, пока она
была открыта. CPU БД, пока она была открыта: $cpu% из 200%. Дата $(utc "$t0" | cut -c1-10) (UTC). $(rate_note "$RATE")

| фаза | окно (UTC) | закоммичено msg/s | доставлено msg/s | потреблено msg/s | задержка p50 | задержка p99 | наибольший лаг шарда |
|---|---|---|---|---|---|---|---|
EOF
  row до "$t0" "$t1"
  row "транзакция открыта" "$t1" "$t2"
  row после "$t2" "$t3"
  cat << EOF

p99 задержки по минутам, пока транзакция была открыта: $minutes с. Консьюмер всё выбрал $drain после остановки
писателей. Итого: $end. Сверка (SQL, после выборки): закоммиченных строк, не потреблённых и не dead: $RECON.

$(consumer_note)
EOF
} | out
verdict "$table" "$rc"
