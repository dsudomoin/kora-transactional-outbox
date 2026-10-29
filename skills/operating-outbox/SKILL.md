---
name: operating-outbox
description: Use when monitoring, alerting on, sizing or troubleshooting dsudomoin/outbox in a running Kora service, such as outbox.* metrics and dashboards, growing outbox.shard.lag, dead letters and the dead-letter hook, the admin API (OutboxAdmin, outbox-admin-kora, /kora-outbox/admin/v1), redrive or discard, tail overflow, orphaned partitions, stranded shards, «outbox relay halted» or «xid regression detected», outbox.shards and the emergency reshard, «refuses this write», rolling deploys, upgrades and rollback to the previous release, pg_dump/restore or failover, Kafka delivery slow after RecordTooLargeException or an expired batch. Also «алерты outbox», «dead letters», «лаг аутбокса», «эксплуатация outbox», «админка outbox».
---

# Operating outbox

## Overview

Procedures: the library README (https://github.com/dsudomoin/outbox#readme). Act through the admin
API: `OutboxAdmin` in code or `outbox-admin-kora` over HTTP ([admin-api.md](admin-api.md)). **Never improvise SQL
against the library's tables**; run only the README's SQL, on a staging copy first.

## Wake someone: delivery stops

| Signal | Meaning | README section |
|---|---|---|
| `outbox.shard.lag` growing | Target down, slow handler, too few workers; ERROR «outbox shards [...] start from ticks [...] that no longer exist»: stranded shards | «Обработка сбоев», «Застрявшие шарды» |
| `outbox.maintenance.since_success` > 5 × `maintenanceInterval` | No maintenance anywhere; `transaction_pooling` = 1: a pooler | «Обслуживание работает там же, где relay» |
| `outbox.partitions.xids_left` not NaN | Tail detached: inserts fail at 0 | «Партиции» |
| `outbox.dead.pending.oldest` > X (X > `relay.retry.maxDelay`) and `outbox.dead.hook.failures` growing | Hook failing: letters wait | «Dead letters» |
| ERROR «outbox xid regression detected» | «recovered»: done; «relay halted»: its reason, maybe `POST /recovery/xid-regression` | «Восстановление после регрессии xid» |
| ERROR «... holds cursors for N shards, but this instance runs with outbox.shards = M ...» or «... refuses this write ...» | Old-count instance after a reshard: restart with the new count or unset | «Решардинг» |

## Morning: nothing lost, waits for a human

| Signal | README section |
|---|---|
| `outbox.dead.size` grows, `outbox.dead.pending` does not | «Dead letters» |
| `outbox.partitions.orphaned` or `outbox.inbox.partitions.orphaned` > 0 | «Осиротевшие партиции» |
| `outbox.partitions.overflow` = 1 past an extension, `premade` < 2 | «Переполнение хвоста (tail overflow)» |
| ERROR «... has not run», «... over outbox.partitions.maxTailScan» | «Партиции» |
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
  («Решардинг», [config-reference.md](../using-outbox/config-reference.md)).
- **Pool:** `db.maxPoolSize` ≥ own peak + `relay.workers` + 3, + 1 while `drain()` runs. Profiles:
  [sizing-examples.md](../using-outbox/sizing-examples.md).

## Deploys and incidents

- «Rolling-деплои»: identical schema-qualified `outbox.table`, `outbox.inbox.table`, inbox `retention` and
  `partitionInterval`.
- **Rollback to 5fc60dd** («Обновление с предыдущего релиза»): set its `outbox.shards` to the cursors' count (256 if
  this release created it); then list the window's `kept` dead letters.
- «Репликация, failover и PITR»: async failover may deliver events whose data is gone; PITR redelivers.
- `relay.retry.maxAttempts = 0` (default): a key stuck on a broken target shows as lag only.
- Kafka records slower for up to 10 min after a call expired or was cut, often with a `RecordTooLargeException` dead
  letter («Требования к Kafka»): on kafka-clients ≤ 4.1 an oversized record shared a producer batch, and the sender
  resends its batch-mates unbatched. Nothing is lost; fix the cause with `batch.size` ≤ the topic's
  `max.message.bytes`, or kafka-clients 4.2+.
- Upgrades: Postgres 14+ (13 fails at startup), Kora 1.2.23+ (using-outbox).
- Writers' timeouts: ≥ 300 ms by day, ≥ `tailConversion.maxStall` + 300 ms at night.
