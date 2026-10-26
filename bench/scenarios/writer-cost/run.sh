#!/usr/bin/env bash
# run.sh DIR NAME CLIENTS SECONDS: pgbench of DIR/NAME.sql (DIR inside the bench Postgres container); prints one line:
# NAME clients tps latency_ms failed db_cpu% window_utc. DB CPU is the container's cgroup counter over the run, so it
# includes pgbench itself (same container, same CPUs). A failing pgbench (e.g. a statement error) prints its output
# and exits with pgbench's status.
set -euo pipefail
PG=kora-outbox-bench-postgres-1
dir=$1 name=$2 clients=$3 secs=$4
usage() { docker exec "$PG" awk '$1 == "usage_usec" { print $2 }' /sys/fs/cgroup/cpu.stat; }
t0=$(python3 -c 'import time; print(time.time())'); u0=$(usage)
rc=0
out=$(docker exec "$PG" pgbench -U outbox -d outbox -n -M prepared -c "$clients" -j "$clients" -T "$secs" \
  -f "$dir/$name.sql" 2>&1) || rc=$?
if [ "$rc" != 0 ]; then
  echo "$name: pgbench failed (exit $rc):" >&2
  echo "$out" | tail -20 >&2
  exit "$rc"
fi
u1=$(usage); t1=$(python3 -c 'import time; print(time.time())')
tps=$(echo "$out" | awk '/^tps = / { print $3 }')
lat=$(echo "$out" | awk '/^latency average = / { print $4 }')
fail=$(echo "$out" | awk '/number of failed/ { print $5 }')
cpu=$(python3 -c "print(round(($u1 - $u0) / 1e6 / ($t1 - $t0) * 100))")
w0=$(date -u -r "${t0%.*}" +%H:%M:%S); w1=$(date -u -r "${t1%.*}" +%H:%M:%S)
echo "$name clients=$clients tps=$tps latency_ms=$lat failed=${fail:-?} db_cpu=$cpu% window=$w0-$w1"
echo "$out" | grep -E 'error|ERROR' | head -3 || true
