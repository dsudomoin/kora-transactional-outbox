---
name: operating-kora-outbox
description: Use when monitoring, alerting on, sizing or troubleshooting kora-transactional-outbox in a running Kora service, such as outbox.* metrics and dashboards, growing outbox.shard.lag, rows in outbox_retry or outbox_dead, redrive, tail overflow, orphaned partitions, «outbox relay halted» or «xid counter went backwards», changing outbox.shards, rolling deploys, pg_dump/restore or failover. Also «алерты outbox», «dead letters», «лаг аутбокса», «эксплуатация outbox».
---

# Operating kora-outbox

## Overview

Recovery procedures live in the library README (https://github.com/dsudomoin/kora-transactional-outbox#readme),
under the Russian section names quoted below. Follow them as written, on a staging copy first (the README marks them
as not rehearsed on a live database). **Never improvise SQL against the library's tables.**

## Alerts (need a Micrometer `MeterRegistry` in the graph)

| Signal | Meaning | README section |
|---|---|---|
| `outbox.shard.lag` growing | Target down, slow handler, too few workers | «Обработка сбоев» |
| `outbox.dead.size` > 0 | Dead letters to triage | «Dead letters» |
| `outbox.maintenance.since_success` > 5 × `maintenanceInterval` | No maintenance on any instance | «Обслуживание работает там же, где relay» |
| `outbox.partitions.premade` < 2 or `outbox.partitions.overflow` = 1 | Rows go, or soon go, to the tail | «Переполнение хвоста (tail overflow)» |
| `outbox.partitions.xids_left` not NaN | Tail detached: inserts fail at 0 | «Партиции» |
| `outbox.partitions.orphaned` or `outbox.inbox.partitions.orphaned` > 0 | A detached range holds rows | «Осиротевшие партиции» |
| `outbox.maintenance.transaction_pooling` = 1 | Maintenance skipped behind a transaction pooler | «Соединения: без transaction pooling» |
| ERROR «outbox relay halted» | xid regression after a logical restore: stop writers | «Восстановление после регрессии xid (xid regression recovery)» |

Also watch `outbox.delivery.delay` p99 and `outbox.messages.failed{outcome}`. Every meter and its Prometheus name:
[metrics.md](metrics.md). Dashboard: `observability/grafana/dashboards/outbox.json` in the library repo.

## Never

- Touch `<table>_retry` rows by hand. Redrive dead letters by moving rows from `<table>_dead` back into `<table>` in
  one transaction («Dead letters»): `message_id` is kept, so inbox consumers deduplicate within their window.
- Detach outbox partitions by hand, or drop or truncate library tables while instances run.
- Delete the `<table>_halt` row without the repair steps of the xid procedure.
- Disable the relay on every instance: maintenance runs only where the relay runs.

## Sizing

- **Shards** (default 32): one worker per shard at a time, sequentially; a hot shard caps throughput. Changing
  `outbox.shards` needs an empty outbox («Решардинг»); startup fails on a mismatch.
- **Workers** (default 4): each holds a connection while working; `db.maxPoolSize` += workers + 2.
- **Idle cost** («Стоимость простоя»): about `1 / idlePollInterval + shards / 5` write transactions per second per
  cluster (11.4 with defaults). A larger `idlePollInterval` idles cheaper, picks up other instances' commits slower.
- **premake** («Партиции»): `premake / 2 × xidsPerPartition ≥ peak xid/s × longest snapshot seconds`.

## Deploys and incidents

- «Rolling-деплои»: same schema-qualified `outbox.table` and `outbox.inbox.table` everywhere; inbox `retention` /
  `partitionInterval` changes on all instances at once; shutdown redelivers unfinished batches (duplicates, no loss).
- Restart after changing `outbox.*` under hot config reload: gauges keep reading the first components.
- «Репликация, failover и PITR»: async-replica failover can deliver events whose data is gone; PITR redelivers.
- `relay.retry.maxAttempts = 0` (default): a key stuck on a broken target never reaches dead letters; only lag shows it.
