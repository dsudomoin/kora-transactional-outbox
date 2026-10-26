#!/usr/bin/env bash
# chaos partition-churn: 3 instances at RATE msg/s each (order mode), DURATION s, with tiny xid partitions
# (XIDS per range) and maintenance every MAINT, so dozens of partitions are created and dropped. Kills as in chaos-kill,
# but each kill first waits up to 30 s for partition DDL of this table in flight (pg_stat_activity, a session of a live
# instance) and kills that instance; without one in 30 s it kills a random instance.
# Pass: clean; at least 5 kills during maintenance DDL; partition count bounded (at most PARTS_MAX); no detach pending
# and no detached range left over at the end; the gauge outbox.partitions.orphaned (ranges the library cannot sweep)
# is 0 on every instance that has run the orphan step.
source "$(dirname "$0")/chaos-lib.sh"
DURATION=${DURATION:-600}
RATE=${RATE:-1000}
XIDS=${XIDS:-20000}
MAINT=${MAINT:-2s}
PARTS_MAX=${PARTS_MAX:-128} # 2 × premake (64): steady state is premake ahead + the current range + the tail

# maint_ddl SECONDS: waits up to SECONDS for partition DDL on $TABLE (create, attach, detach, drop) in flight from a
# live instance's session (started after that instance: a killed predecessor's orphaned backend does not count); prints
# "I идёт DDL обслуживания: STATEMENT" or nothing. A procedure that commits after every 10 ms poll: one long
# statement would hold its snapshot for the whole wait, pinning the xmin that partition drops and DETACH CONCURRENTLY
# wait for, so the DDL it hunts would never come.
maint_ddl() {
  local starts="" i
  for i in $(seq 1 "$N"); do if up "$i"; then starts="$starts${starts:+, }('bench-$i', '${STARTED[i]}'::timestamptz)"; fi; done
  docker exec -e PGOPTIONS='-c client_min_messages=warning' "$PG" psql -U outbox -d outbox -Atq \
    -c "create procedure pg_temp.maint_ddl(secs float8, inout hit text) language plpgsql as \$f\$
    declare t0 timestamptz := clock_timestamp();
    begin
      loop
        select substring(a.application_name from 7) || ' идёт DDL обслуживания: '
            || left(regexp_replace(a.query, '\s+', ' ', 'g'), 120) into hit
          from pg_stat_activity a join (values $starts) s(app, started) on a.application_name = s.app
          where a.state = 'active' and a.backend_start >= s.started
            and a.query ~* '^\s*(alter|drop|create) table' and a.query ~* '\m${TABLE}(_x|_tail|\M)'
          limit 1;
        exit when hit is not null or clock_timestamp() - t0 > secs * interval '1 second';
        commit; -- also drops the cached pg_stat_activity snapshot
        perform pg_sleep(0.01);
      end loop;
    end \$f\$" -c "call pg_temp.maint_ddl($1, null)"
}
victim() {
  local hit
  hit=$(maint_ddl 30) || hit=""
  if [ -n "$hit" ]; then echo "$hit"; else echo "$(rnd 1 "$N") DDL партиций за 30 с не замечен"; fi
}

chaos_begin partition-churn bench_chaos_churn 3 "Режим заказов, 3 экземпляра × $RATE msg/s в течение $DURATION с при \
\`xidsPerPartition = $XIDS\` и \`maintenanceInterval = $MAINT\`; каждые ${KILL_MIN}–$KILL_MAX с \`kill -9\` \
экземпляра, выполняющего DDL партиций (такого ждут до 30 с, иначе берут случайный экземпляр), перезапуск через \
${DOWN_MIN}–$DOWN_MAX с" OUTBOX_XIDS_PER_PARTITION="$XIDS" OUTBOX_MAINTENANCE_INTERVAL="$MAINT"
for i in 1 2 3; do chaos_load "$i" "$RATE" "$CHAOS_WRITERS"; done
chaos_for "$DURATION" kill_cycle
kill_cycle_end
chaos_checks() {
  local maint created dropped swept pending leftover orphaned="" i v
  maint=$(grep -c 'идёт DDL обслуживания' <<< "$FAULTS" || true)
  created=$(cat "$LOGS/chaos-$NAME-"*.log | grep -c "created partition ${TABLE}_x" || true)
  dropped=$(cat "$LOGS/chaos-$NAME-"*.log | grep -c "dropped partition ${TABLE}_x" || true)
  swept=$(cat "$LOGS/chaos-$NAME-"*.log | grep -c "dropped partition ${TABLE}_x.*detached earlier but never dropped" || true)
  for i in $(seq 1 "$N"); do # NaN on an instance that has not completed the orphan step (it lacks the maintenance lock)
    v=$(curl -sf --max-time 5 "http://localhost:$((8084 + i))/metrics" | awk '$1 == "outbox_partitions_orphaned" { print $2 }') || v=""
    orphaned="$orphaned${orphaned:+, }bench-$i ${v:-нет}"
  done
  read -r pending leftover <<< "$(psql_q "select
      (select count(*) from pg_inherits where inhparent = '$TABLE'::regclass and inhdetachpending),
      (select count(*) from pg_class c where c.relkind = 'r' and c.relname ~ '^${TABLE}_(x[0-9]+|tail)$'
        and not exists (select 1 from pg_inherits i where i.inhrelid = c.oid))" | tr '|' ' ')"
  check "$([ "$maint" -ge 5 ] && echo 0 || echo 1)" "убийств во время DDL партиций этой таблицы: $maint (нужно 5)"
  check "$([ "$MAX_PARTS" -le "$PARTS_MAX" ] && [ "$created" -gt 0 ] && [ "$dropped" -gt 0 ] && echo 0 || echo 1)" \
    "партиций создано $created, удалено $dropped (логи экземпляров); в замерах присоединено не больше $MAX_PARTS (граница \
$PARTS_MAX)"
  check "$([ "$pending" = 0 ] && [ "$leftover" = 0 ] && echo 0 || echo 1)" \
    "после выборки: незавершённых detach $pending, оставшихся отсоединённых диапазонов и хвостов $leftover; \
подобрано диапазонов, осиротевших из-за убийства между detach и drop: $swept (логи экземпляров)"
  # passes when no instance reports a non-zero count and at least one reports 0
  check "$(! grep -qE ' [0-9.]*[1-9]' <<< "$orphaned" && grep -qE ' 0(\.0)?(,|$)' <<< "$orphaned" && echo 0 || echo 1)" \
    "gauge outbox.partitions.orphaned после выборки: $orphaned"
}
chaos_end 900
