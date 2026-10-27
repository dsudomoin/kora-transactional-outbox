---
name: operating-kora-outbox
description: Use when monitoring, alerting on, sizing or troubleshooting kora-transactional-outbox in a running Kora service, such as outbox.* metrics and dashboards, growing outbox.shard.lag, dead letters and the dead-letter hook, the admin API (OutboxAdmin, kora-outbox-admin, /kora-outbox/admin/v1), redrive or discard, tail overflow, orphaned partitions, stranded shards, «outbox relay halted» or «xid regression detected», outbox.shards and the emergency reshard, «refuses this write», rolling deploys, upgrades and rollback to the previous release, pg_dump/restore or failover. Also «алерты outbox», «dead letters», «лаг аутбокса», «эксплуатация outbox», «админка outbox».
---

# Operating kora-outbox

## Overview

Procedures: the library README (https://github.com/dsudomoin/kora-transactional-outbox#readme). Act through the admin
API: `OutboxAdmin` in code or `kora-outbox-admin` over HTTP ([admin-api.md](admin-api.md)). **Never improvise SQL
against the library's tables**; run only SQL the README prints, on a staging copy first.

## Wake someone: delivery stopped or soon stops

| Signal | Meaning | README section |
|---|---|---|
| `outbox.shard.lag` growing | Target down, slow handler, too few workers; with ERROR «outbox shards [...] start from ticks [...] that no longer exist»: stranded shards | «Обработка сбоев», «Застрявшие шарды» |
| `outbox.maintenance.since_success` > 5 × `maintenanceInterval` | No maintenance anywhere; `transaction_pooling` = 1: a pooler | «Обслуживание работает там же, где relay» |
| `outbox.partitions.xids_left` not NaN | Tail detached: inserts fail at 0 | «Партиции» |
| `outbox.dead.pending.oldest` > X (X > `relay.retry.maxDelay`) and `outbox.dead.hook.failures` growing | Hook failing: letters wait, no alerts sent | «Dead letters» |
| ERROR «outbox xid regression detected» | «recovered»: done; «relay halted»: reason at line end, maybe `POST /recovery/xid-regression` | «Восстановление после регрессии xid (xid regression recovery)» |
| ERROR «... holds cursors for N shards, but this instance runs with outbox.shards = M ...» or «... refuses this write ...» | Old-count instance after a reshard: restart with the new count or unset | «Решардинг» |

## Morning: nothing lost, waits for a human

| Signal | README section |
|---|---|
| `outbox.dead.size` grows, `outbox.dead.pending` does not (kept letters) | «Dead letters» |
| `outbox.partitions.orphaned` or `outbox.inbox.partitions.orphaned` > 0 | «Осиротевшие партиции» |
| `outbox.partitions.overflow` = 1 past one extension cycle, `outbox.partitions.premade` < 2 | «Переполнение хвоста (tail overflow)» |
| ERROR «outbox partition tail swap of ... timed out waiting for writers» | «Партиции», «Транзакции, которые держат outbox после `send`» |
| `outbox.parked.size` not back to its level | «Алерты» |

Meters and logs: [metrics.md](metrics.md); dashboard `observability/grafana/dashboards/outbox.json`.

## Never

- Change rows of `<table>_retry`, `_parked`, `_dead`, `_cursor`, `_instance` or `_halt` by hand; use the admin API.
- Detach outbox partitions by hand, or drop or truncate library tables while instances run.
- Disable the relay everywhere: maintenance and the dead-letter pass run only with it.

## Shards and pool

- **`outbox.shards`: leave it unset** (256 for a new outbox; an existing one keeps its count). A differing set value
  is the **emergency reshard**: stop writers, wait for `outbox.shard.lag` 0, stop all instances (old release too), start
  one with the new value, then the rest; startup refuses while another instance with a different count runs
  («Решардинг», [config-reference.md](../using-kora-outbox/config-reference.md)).
- **Pool:** `db.maxPoolSize` ≥ own peak + `relay.workers` + 3, + 1 while `drain()` runs. Profiles:
  [sizing-examples.md](../using-kora-outbox/sizing-examples.md).

## Deploys and incidents

- «Rolling-деплои»: same schema-qualified `outbox.table` and `outbox.inbox.table` everywhere; inbox `retention` /
  `partitionInterval` on all instances at once.
- **Rollback to the previous release** (5fc60dd): «Rolling-деплои», item «Обновление с предыдущего релиза (5fc60dd),
  смешанный парк и откат». The old release writes dead letters `kept`: list the window's kept letters afterwards.
  Once no new instance runs, drop the hot tail's bound check the README gives (`<table>_tail_bounded`).
- «Репликация, failover и PITR»: async failover can deliver events whose data is gone; PITR redelivers.
- `relay.retry.maxAttempts = 0` (default): a key stuck on a broken target shows only as lag.
