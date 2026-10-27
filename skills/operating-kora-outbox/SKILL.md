---
name: operating-kora-outbox
description: Use when monitoring, alerting on, sizing or troubleshooting kora-transactional-outbox in a running Kora service, such as outbox.* metrics and dashboards, growing outbox.shard.lag, dead letters and the dead-letter hook, the admin API (OutboxAdmin, kora-outbox-admin, /kora-outbox/admin/v1), redrive or discard, tail overflow, orphaned partitions, stranded shards, «outbox relay halted» or «xid regression detected», outbox.shards and the emergency reshard, «refuses this write», rolling deploys, upgrades and rollback to the previous release, pg_dump/restore or failover. Also «алерты outbox», «dead letters», «лаг аутбокса», «эксплуатация outbox», «админка outbox».
---

# Operating kora-outbox

## Overview

Procedures: the library README (https://github.com/dsudomoin/kora-transactional-outbox#readme). Act through the admin
API ([admin-api.md](admin-api.md)). **Never improvise SQL against the library's tables**; run only SQL the README
prints, on a staging copy first.

## Wake someone: delivery stopped or soon stops

| Signal | Meaning | README section |
|---|---|---|
| `outbox.shard.lag` growing | Target down, slow handler, too few workers; ERROR «outbox shards [...] start from ticks [...] that no longer exist»: stranded | «Обработка сбоев», «Застрявшие шарды» |
| `outbox.maintenance.since_success` > 5 × `maintenanceInterval` | No maintenance anywhere; `transaction_pooling` = 1: a pooler | «Обслуживание работает там же, где relay» |
| `outbox.partitions.xids_left` not NaN | Tail detached: inserts fail at 0 | «Партиции» |
| `outbox.dead.pending.oldest` > X (X > `relay.retry.maxDelay`) and `outbox.dead.hook.failures` growing | Hook failing: letters wait, no alerts sent | «Dead letters» |
| ERROR «outbox xid regression detected» | «recovered»: done; «relay halted»: reason at line end, maybe `POST /recovery/xid-regression` | «Восстановление после регрессии xid (xid regression recovery)» |
| ERROR «... holds cursors for N shards, but this instance runs with outbox.shards = M ...», «... refuses this write ...» | Old-count instance after a reshard: restart with the new count | «Решардинг» |

## Morning: nothing lost, waits for a human

| Signal | README section |
|---|---|
| `outbox.dead.size` grows, `outbox.dead.pending` does not (kept letters) | «Dead letters» |
| `outbox.partitions.orphaned` or `outbox.inbox.partitions.orphaned` > 0 | «Осиротевшие партиции» |
| `outbox.partitions.overflow` = 1 past one extension cycle, `premade` < 2, ERROR «... over outbox.partitions.maxTailScan» | «Переполнение хвоста (tail overflow)» |
| ERROR «outbox partition tail swap of ... has not run on N due runs» (names the holder) | «Партиции» |
| `outbox.parked.size` not back to its level | «Алерты» |

Meters and logs: [metrics.md](metrics.md); dashboard `observability/grafana/dashboards/outbox.json`.

## Never

- Change library tables' rows by hand; use the admin API.
- Detach outbox partitions by hand, or drop or truncate library tables while instances run.
- Disable the relay everywhere: maintenance and dead-letter passes need it.

## Shards and pool

- **`outbox.shards`: leave it unset** (256 for a new outbox). A differing value is the **emergency reshard**: stop
  writers, wait for `outbox.shard.lag` 0, stop all instances (old release too), start one with the new value, then the
  rest («Решардинг», [config-reference.md](../using-kora-outbox/config-reference.md)).
- **Pool:** `db.maxPoolSize` ≥ own peak + `relay.workers` + 3, + 1 while `drain()` runs. Profiles:
  [sizing-examples.md](../using-kora-outbox/sizing-examples.md).

## Deploys and incidents

- «Rolling-деплои»: the same schema-qualified table names and inbox `retention` / `partitionInterval` everywhere.
- **Rollback to 5fc60dd**: «Rolling-деплои», «Обновление с предыдущего релиза». The old release writes dead letters
  `kept`: list the window's kept letters afterwards. No tail step: any tail state rolls back and heals on re-upgrade.
  The old release starts only with `outbox.shards` equal to the cursors' count: set it (256 for an outbox this
  release created, or the new count after a reshard).
- The night tail conversion stalls writers ≤ `tailConversion.maxStall`: keep it below their
  `lock_timeout`/`statement_timeout`.
- «Репликация, failover и PITR»: async failover may deliver events whose data is gone; PITR redelivers.
- `relay.retry.maxAttempts = 0` (default): a key stuck on a broken target shows as lag only.
