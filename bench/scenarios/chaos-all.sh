#!/usr/bin/env bash
# chaos-all: every chaos and soak scenario in order (about 2 h 20 min: 110 min of load plus setup and drains), each
# appending its section to bench/RESULTS.md and a line to bench/build/scenarios/chaos-summary.txt; stops at the first
# failure, keeping that run's tables. Prints the summary. Needs the stack up and the bench built, like run-all.sh.
set -uo pipefail
dir=$(cd "$(dirname "$0")" && pwd)
summary=$dir/../build/scenarios/chaos-summary.txt
mkdir -p "$(dirname "$summary")"
: > "$summary"
rc=0
for s in kill pg-restart pg-pause kafka-restart partition-churn small-pool slow-handler soak; do
  "$dir/chaos-$s.sh" || { rc=$?; echo "chaos-all: stopped after chaos-$s (exit $rc)" >> "$summary"; break; }
done
cat "$summary"
exit "$rc"
