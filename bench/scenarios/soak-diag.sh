# Diagnostics for chaos-soak.sh with DIAG=1; sourced after chaos-lib.sh. Changes no Postgres setting. Into bench_diag_*
# tables in the bench DB:
#  - every DIAG_EVERY s (snapshot N): pg_stat_statements, pg_stat_bgwriter (Postgres 16 keeps the checkpointer's
#    counters there), pg_stat_wal, pg_stat_io and the size of every table over 8 MB (partitions summed per parent);
#  - every WAIT_EVERY s: the wait events of active sessions (client backends with their statement's first 60
#    characters) and of busy background processes (not idling in an Activity wait).
# diag_report prints per-window markdown tables: environment (checkpoints, buffers, WAL, sizes), relay and writer
# timings from Prometheus, wait events, and the per-window mean of the statements with the most time.
DIAG_EVERY=${DIAG_EVERY:-600}
WAIT_EVERY=${WAIT_EVERY:-3}
DIAG_N=-1 NEXT_SNAP=0 NEXT_WAIT=0

diag_begin() {
  psql_q "drop table if exists bench_diag_pss, bench_diag_bgw, bench_diag_wal, bench_diag_io, bench_diag_sizes,
      bench_diag_waits;
    create table bench_diag_pss as select 0 as snap, clock_timestamp() as at, toplevel, queryid,
      left(regexp_replace(query, '\\s+', ' ', 'g'), 160) as query, calls, total_exec_time, rows, shared_blks_read,
      shared_blks_written, shared_blks_dirtied, wal_bytes from pg_stat_statements with no data;
    create table bench_diag_bgw as select 0 as snap, clock_timestamp() as at, * from pg_stat_bgwriter with no data;
    create table bench_diag_wal as select 0 as snap, clock_timestamp() as at, * from pg_stat_wal with no data;
    create table bench_diag_io as select 0 as snap, clock_timestamp() as at, * from pg_stat_io with no data;
    create table bench_diag_sizes (snap int, at timestamptz, name text, bytes bigint);
    create table bench_diag_waits (at timestamptz, backend_type text, wait text, statement text, n int)" > /dev/null
}

diag_snap() {
  psql_q "insert into bench_diag_pss select $1, clock_timestamp(), toplevel, queryid,
      left(regexp_replace(query, '\\s+', ' ', 'g'), 160), calls, total_exec_time, rows, shared_blks_read,
      shared_blks_written, shared_blks_dirtied, wal_bytes
      from pg_stat_statements where dbid = (select oid from pg_database where datname = current_database());
    insert into bench_diag_bgw select $1, clock_timestamp(), * from pg_stat_bgwriter;
    insert into bench_diag_wal select $1, clock_timestamp(), * from pg_stat_wal;
    insert into bench_diag_io select $1, clock_timestamp(), * from pg_stat_io;
    insert into bench_diag_sizes select $1, clock_timestamp(), coalesce(p.relname, c.relname),
        sum(pg_total_relation_size(c.oid))
      from pg_class c left join pg_inherits i on i.inhrelid = c.oid left join pg_class p on p.oid = i.inhparent
      where c.relkind = 'r' and c.relnamespace = 'public'::regnamespace
      group by 3 having sum(pg_total_relation_size(c.oid)) > 8 << 20
      union all select $1, clock_timestamp(), '(database)', pg_database_size(current_database())" > /dev/null
}

diag_waits() {
  psql_q "insert into bench_diag_waits select clock_timestamp(), backend_type,
      coalesce(wait_event_type || ':' || wait_event, 'CPU'),
      case when backend_type = 'client backend' then left(regexp_replace(query, '\\s+', ' ', 'g'), 60) else '' end,
      count(*)
    from pg_stat_activity
    where pid <> pg_backend_pid() and (state = 'active'
      or (backend_type <> 'client backend' and wait_event_type is distinct from 'Activity'))
    group by 1, 2, 3, 4" > /dev/null
}

# diag_hook: a chaos_for hook (wait sample every WAIT_EVERY s, snapshot every DIAG_EVERY s); never fails the run.
diag_hook() {
  local t
  t=$(now)
  if [ "$t" -ge "$NEXT_WAIT" ]; then NEXT_WAIT=$((t + WAIT_EVERY)); diag_waits || true; fi
  if [ "$t" -ge "$NEXT_SNAP" ]; then
    NEXT_SNAP=$((t + DIAG_EVERY)) DIAG_N=$((DIAG_N + 1))
    diag_snap "$DIAG_N" || true
  fi
}

# diag_final: the last snapshot, at the end of the load (before the writers stop).
diag_final() { DIAG_N=$((DIAG_N + 1)); diag_snap "$DIAG_N"; }

# diag_report: markdown tables over the windows between consecutive snapshots.
diag_report() {
  local w from to
  echo "Диагностика (\`soak-diag.sh\`, таблицы \`bench_diag_*\`): окно — промежуток между соседними снимками, в минутах нагрузки."
  echo
  echo "| окно | checkpoints timed / requested | буферы от checkpoint / bgwriter / backends | checkpoint write / sync, с | WAL, МБ | WAL full-page images | WAL buffers full | БД в конце, ГБ | inbox, ГБ | outbox, ГБ | orders + bench_consumed, ГБ |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|"
  psql_q "select format('| %s–%s | %s / %s | %s / %s / %s | %s / %s | %s | %s | %s | %s | %s | %s | %s |',
      round(extract(epoch from a.at - f.at) / 60), round(extract(epoch from b.at - f.at) / 60),
      b.checkpoints_timed - a.checkpoints_timed, b.checkpoints_req - a.checkpoints_req,
      b.buffers_checkpoint - a.buffers_checkpoint, b.buffers_clean - a.buffers_clean,
      b.buffers_backend - a.buffers_backend,
      round(((b.checkpoint_write_time - a.checkpoint_write_time) / 1000)::numeric),
      round(((b.checkpoint_sync_time - a.checkpoint_sync_time) / 1000)::numeric, 1),
      round((wb.wal_bytes - wa.wal_bytes) / 1048576), wb.wal_fpi - wa.wal_fpi, wb.wal_buffers_full - wa.wal_buffers_full,
      round((select bytes from bench_diag_sizes where snap = b.snap and name = '(database)') / 1e9, 1),
      round(coalesce((select bytes from bench_diag_sizes where snap = b.snap and name = 'inbox'), 0) / 1e9, 1),
      round(coalesce((select bytes from bench_diag_sizes where snap = b.snap and name = '$TABLE'), 0) / 1e9, 1),
      round(coalesce((select sum(bytes) from bench_diag_sizes where snap = b.snap
        and name in ('orders', 'bench_consumed')), 0) / 1e9, 1))
    from bench_diag_bgw a join bench_diag_bgw b on b.snap = a.snap + 1
      join bench_diag_wal wa on wa.snap = a.snap join bench_diag_wal wb on wb.snap = b.snap,
      (select at from bench_diag_bgw where snap = 0) f
    order by a.snap"
  echo
  echo "Чтения и записи по типу backend (pg_stat_io, блоки relation, по окнам; времени нет: track_io_timing выключен):"
  echo
  echo "| окно | client backends: reads / writes / extends | checkpointer writes | bgwriter writes | autovacuum reads / writes |"
  echo "|---|---|---|---|---|"
  psql_q "with d as (select b.snap, b.backend_type, sum(b.reads - a.reads) r, sum(b.writes - a.writes) w,
        sum(b.extends - a.extends) e
      from bench_diag_io a join bench_diag_io b on b.snap = a.snap + 1 and b.backend_type = a.backend_type
        and b.object = a.object and b.context = a.context
      where b.object = 'relation' group by 1, 2),
    f as (select min(at) at from bench_diag_bgw), t as (select snap, at from bench_diag_bgw)
    select format('| %s–%s | %s / %s / %s | %s | %s | %s / %s |',
      round(extract(epoch from (select at from t where snap = s.snap - 1) - f.at) / 60),
      round(extract(epoch from (select at from t where snap = s.snap) - f.at) / 60),
      max(r) filter (where backend_type = 'client backend'), max(w) filter (where backend_type = 'client backend'),
      max(e) filter (where backend_type = 'client backend'), max(w) filter (where backend_type = 'checkpointer'),
      max(w) filter (where backend_type = 'background writer'),
      max(r) filter (where backend_type = 'autovacuum worker'), max(w) filter (where backend_type = 'autovacuum worker'))
    from d s, f group by s.snap, f.at order by s.snap"
  echo
  echo "Relay и писатели по окнам (Prometheus):"
  echo
  echo "| окно | закоммичено, msg/s | задержка p50 / p99 / max, с | пакет relay p99 / max, с | лаг шарда max, с | транзакция писателя p99 / max, с | транзакций писателя > 0.5 с |"
  echo "|---|---|---|---|---|---|---|"
  local f0 i
  f0=$(psql_q "select extract(epoch from at)::bigint from bench_diag_bgw where snap = 0")
  for i in $(seq 1 "$DIAG_N"); do
    from=$(psql_q "select extract(epoch from at)::bigint from bench_diag_bgw where snap = $((i - 1))")
    to=$(psql_q "select extract(epoch from at)::bigint from bench_diag_bgw where snap = $i")
    w=$((to - from))
    echo "| $(((from - f0) / 60))–$(((to - f0) / 60)) | $(f0 "$(per_s bench_messages_sent_total "$from" "$to")") \
| $(f3 "$(q_over 0.5 outbox_delivery_delay_seconds "$from" "$to")") / $(f3 "$(q_over 0.99 outbox_delivery_delay_seconds "$from" "$to")") \
/ $(f3 "$(prom "max(max_over_time(outbox_delivery_delay_seconds_max[${w}s]))" "$to")") \
| $(f3 "$(q_over 0.99 outbox_batch_duration_seconds "$from" "$to")") / $(f3 "$(prom "max(max_over_time(outbox_batch_duration_seconds_max[${w}s]))" "$to")") \
| $(f1 "$(prom "max(max_over_time(outbox_shard_lag[${w}s]))" "$to")") \
| $(f3 "$(q_over 0.99 bench_tx_seconds "$from" "$to")") / $(f3 "$(prom "max(max_over_time(bench_tx_seconds_max[${w}s]))" "$to")") \
| $(f0 "$(prom "sum(increase(bench_tx_seconds_count[${w}s])) - sum(increase(bench_tx_seconds_bucket{le=\"0.536870911\"}[${w}s]))" "$to")") |"
  done
  echo
  echo "Wait events по окнам, доля замеров активных сессий (замер каждые ${WAIT_EVERY} с; CPU — без ожидания):"
  echo
  psql_q "with t as (select snap, at from bench_diag_bgw), f as (select min(at) at from t),
      w as (select (select max(snap) from t where t.at <= x.at) as snap, x.backend_type, x.wait, x.n from bench_diag_waits x),
      s as (select snap, backend_type || ' ' || wait as k, sum(n) n from w where snap < (select max(snap) from t)
        group by 1, 2),
      top as (select k from s group by k order by sum(n) desc limit 12)
    select '| ожидание | ' || (select string_agg(format('%s–%s', round(extract(epoch from a.at - f.at) / 60),
        round(extract(epoch from b.at - f.at) / 60)), ' | ' order by a.snap)
      from t a join t b on b.snap = a.snap + 1, f) || ' |'
    union all select '|---|' || (select string_agg('---|', '') from t where snap > 0)
    union all select * from (select format('| %s | %s |', k, (select string_agg(coalesce(round(100.0 * s2.n /
        (select sum(n) from s s3 where s3.snap = t.snap), 1)::text, '0'), ' | ' order by t.snap)
      from (select snap from t where snap < (select max(snap) from t)) t left join s s2 on s2.snap = t.snap and s2.k = top.k))
      from top order by (select sum(n) from s where s.k = top.k) desc) x"
  echo
  echo "Ожидания client backend по запросам (весь прогон, 12 самых частых пар запрос / ожидание, доля замеров client backend):"
  echo
  echo "| запрос (первые 60 символов) | ожидание | доля |"
  echo "|---|---|---|"
  psql_q "select format('| %s | %s | %s %% |', replace(statement, '|', '/'), wait,
      round(100.0 * sum(n) / (select sum(n) from bench_diag_waits where backend_type = 'client backend'), 1))
    from bench_diag_waits where backend_type = 'client backend' group by statement, wait order by sum(n) desc limit 12"
  echo
  echo "Среднее время выполнения по окнам (мс на вызов, по приращениям pg_stat_statements) для 14 запросов с наибольшим"
  echo "суммарным временем за прогон (вложенные запросы учтены: \`pg_stat_statements.track = all\`):"
  echo
  psql_q "with d as (select b.snap, b.toplevel, b.queryid, b.query, b.calls - a.calls dc,
        b.total_exec_time - a.total_exec_time dt
      from bench_diag_pss a join bench_diag_pss b on b.snap = a.snap + 1 and b.queryid = a.queryid
        and b.toplevel = a.toplevel where b.calls >= a.calls),
      t as (select snap, at from bench_diag_bgw), f as (select min(at) at from t),
      top as (select queryid, toplevel, sum(dt) tot from d group by 1, 2 order by 3 desc limit 14),
      wins as (select snap from t where snap > 0)
    select '| запрос | всего, с | ' || (select string_agg(format('%s–%s', round(extract(epoch from a.at - f.at) / 60),
        round(extract(epoch from b.at - f.at) / 60)), ' | ' order by a.snap) from t a join t b on b.snap = a.snap + 1, f)
      || ' |'
    union all select '|---|---|' || (select string_agg('---|', '') from t where snap > 0)
    union all select * from (select format('| %s | %s | %s |',
        replace(left((select min(query) from d where d.queryid = top.queryid and d.toplevel = top.toplevel), 90), '|', '/'),
        round(top.tot::numeric / 1000), (select string_agg(coalesce(round((d.dt / nullif(d.dc, 0))::numeric, 3)::text,
          '–'), ' | ' order by wins.snap)
          from wins left join d on d.snap = wins.snap and d.queryid = top.queryid and d.toplevel = top.toplevel))
      from top order by top.tot desc) x"
}
