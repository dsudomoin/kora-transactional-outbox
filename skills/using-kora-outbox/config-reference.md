# kora-outbox configuration reference

HOCON path `outbox` (`OutboxConfig`); every key is optional except `outbox.kafka.driverProperties."bootstrap.servers"`
when `KafkaOutboxModule` is in the graph. Values are validated at startup, before any DDL; a key that does not exist
here is silently ignored.

```hocon
outbox {
  table = "app.outbox"               # the schema of the service's own tables, identical on every instance
  kafka.driverProperties {
    "bootstrap.servers" = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
  }
}
```

## `outbox`

| Key | Default | Rule and meaning |
|---|---|---|
| `table` | `"outbox"` | `[schema.]name`, each part `[A-Za-z_][A-Za-z0-9_]{0,40}`, lowercased. Base name of `<table>`, `_retry`, `_parked`, `_dead`, `_redriven`, `_tick`, `_checkpoint`, `_cursor`, `_halt`, `_maintenance`, `_dropping`, `_instance`. Unqualified = `current_schema()` of each connection. |
| `shards` | unset: `256` for a new outbox | 1..32767. A shard is processed by one worker at a time across all instances; a key always maps to one shard. **Leave it unset**: an existing outbox then keeps its cursors' count (INFO at startup), so an upgrade needs no setting. Shards are chosen once; a set value that differs from the cursors' count is the emergency reshard (below), never a routine change. |

## `outbox.relay`

| Key | Default | Rule and meaning |
|---|---|---|
| `enabled` | `true` | `false`: no relay workers, no partition maintenance and no dead-letter pass on this instance (the hook then gets letters only from the pass at the end of `drain()`); `OutboxRelay.drain()` still works; the heartbeat still runs. Keep it on for at least one production instance. |
| `workers` | `4` | ≥ 1. Relay threads per instance; each holds a DB connection while working. |
| `batchSize` | `1000` | ≥ 1. Rows per chunk read and per handler call, for every handler. A call may also carry up to the chunk limit of parked (requeued) records of the chunk's keys: up to 2 × `batchSize` records. |
| `maxTicksPerBatch` | `100` | ≥ 1. |
| `minTickInterval` | `50ms` | ≥ 0. Under load, at most one tick per interval. |
| `idlePollInterval` | `200ms` | > 0. Idle tick rate; how fast a commit on another instance is noticed. |
| `batchTimeout` | `30s` | > 0. Bound of one handler call: wind-down at 80 %, abandoned (all records retried) at 100 %. With Kafka ≥ `delivery.timeout.ms + 5 s + max.block.ms`. `drain()` gives up after 2 × this without progress. |
| `shutdownTimeout` | `30s` | ≥ 0. Grace for in-flight batches at shutdown; then they are interrupted and redelivered later. |
| `headFailuresBeforeDivert` | `3` | ≥ 1. Consecutive failures before a shard isolates a single failing key. |
| `cursorAnchorInterval` | `1h` | > 0 and ≥ `partitions.maintenanceInterval`. Maintenance writes an idle shard one cursor row this often; ticks are kept about this long, so a rollback to the previous release resumes from these anchors. |

## `outbox.relay.retry`

| Key | Default | Rule and meaning |
|---|---|---|
| `initialDelay` | `1s` | > 0. First backoff of a failed key and of a paused shard; doubles per failure. |
| `maxDelay` | `5m` | ≥ `initialDelay`. Backoff ceiling. |
| `maxAttempts` | `0` | ≥ 0. After this many failed attempts a record moves to `<table>_dead`. `0` = retry forever. `NotAttempted` does not count. Global, Kafka included: a dead-lettered record lets the later records of its key go on, so with ordered topics keep 0 and bound a slow route in its handler (`record.attempts()`, writing-kora-outbox-handlers). |

## `outbox.relay.requeue`

Records a `handleBatch` override set aside with `Outcome.requeue()` (writing-kora-outbox-handlers, `requeue.md`).

| Key | Default | Rule and meaning |
|---|---|---|
| `delay` | `1s` | > 0. A requeued record comes back after this unless a new record of its key brings it back first; `requeue(notBefore)` not before `notBefore` (capped at 24 h). A parked record handed back and not attempted waits at least this too. |
| `maxRequeues` | `0` | ≥ 0. The requeue past this moves the record to `<table>_dead` with «requeue limit reached», attempts unchanged. Every hand-back answered with `requeue` counts, merges ahead of a new record of the key included. `0` = unlimited. Global; a handler can bound per route with `record.requeues()`, or by time, since merges spend requeues at an unpredictable rate: `Duration.between(record.createdAt(), now)` past a limit → `permanent`. |

## `outbox.partitions`

| Key | Default | Rule and meaning |
|---|---|---|
| `xidsPerPartition` | `16777216` | ≥ 1000. Size of one xid range partition. Tests: `1000000`. |
| `premake` | `64` | ≥ 2. Ranges kept ahead of the current xid; must cover the longest snapshot: `premake / 2 × xidsPerPartition ≥ peak xid/s × longest snapshot s`. Tests: `4`. |
| `maintenanceInterval` | `30s` | > 0. Outbox and inbox maintenance period; also the heartbeat period of each instance in `<table>_instance`. |
| `swapLockTimeout` | `200ms` | > 0, whole ms. How long the tail swap waits for `ACCESS EXCLUSIVE` on the outbox, i.e. the writers' pause per attempt; also each lock wait when the library moves rows back (orphans, dead letters). |
| `restoreOrphans` | `true` | Rows of a range the library detached as relayed, at or above the slowest shard's horizon, are moved back by maintenance; `false`: ERROR only. |

## `outbox.inbox` (`InboxModule`)

| Key | Default | Rule and meaning |
|---|---|---|
| `table` | `"inbox"` | Same naming rules as `outbox.table`. Created only when a component uses `Inbox`. |
| `partitionInterval` | `1h` | ≥ 1m. |
| `premake` | `24h` | ≥ 2 × `partitionInterval`; `premake − partitionInterval` > `outbox.partitions.maintenanceInterval`. |
| `retention` | `24h` | ≥ 2 × `partitionInterval`; `(retention + premake) / partitionInterval` ≤ 1000. Ids younger than `retention − partitionInterval` (23 h) are always deduplicated. Change on all instances at once. |
| `attachOrphans` | `true` | A range detached inside the dedup window is attached back by inbox maintenance; `false`: ERROR only. |

An inbox-only service also reads `outbox.partitions.maintenanceInterval` and `outbox.relay.shutdownTimeout`.

## `outbox.recovery`

What to do when the xid counter is found below a tick's xmax (README «Восстановление после регрессии xid (xid
regression recovery)»).

| Key | Default | Rule and meaning |
|---|---|---|
| `xidRegression` | `"auto"` | `auto` or `halt`. `auto`: the instance holding the maintenance lock recovers by itself when at most `maxRows` rows must be re-sent. `halt`: never by itself; the relay waits for `recoverXidRegression` (admin API). |
| `maxRows` | `1000000` | ≥ 0. Rows the automatic recovery re-sends while writers wait for it; more leave the relay halted. |
| `clockMargin` | `10m` | ≥ 0. Rows written since the newest tick before the regression minus this margin are re-sent. |

## `outbox.deadLetters`

The dead-letter hook and the dead table (writing-kora-outbox-handlers, `dead-letter-hook.md`).

| Key | Default | Rule and meaning |
|---|---|---|
| `batchSize` | `100` | 1..1000. Letters per hook call; also key locks per redrive transaction. |
| `timeout` | `30s` | > 0. Bound of one hook call; a call past it leaves its letters pending with backoff `relay.retry.initialDelay` doubling to `maxDelay`. With the Kafka DLQ ≥ `delivery.timeout.ms + 5 s + max.block.ms`, else a WARN at startup. |
| `retention` | `0` | 0..3650 days. Maintenance deletes `kept` letters older than this by `failed_at`; `0` keeps them forever. `pending` and `scheduled` letters are never deleted. |
| `redriveHistory` | `30d` | > 0, ≤ 3650 days. How long `<table>_redriven` remembers a message's redrive count, which a letter that dies again carries in `redrives`. |

## Shard count and the emergency reshard

- Rough sizing: 256 suits typical services. Go lower only for tiny installs. More than 256 is rarely needed; 1024
  works but costs bulk throughput at saturation. Many shards are cheap: idle shards write nothing
  ([sizing-examples.md](sizing-examples.md)).
- Every instance heartbeats into `<table>_instance` (id `<host>/<pid>/<8 hex>`, shard count, whether its dead-letter
  hook is active) every `maintenanceInterval`; `OutboxAdmin.status()` lists them as `instances`.
- An explicit `shards` that differs from the cursors' count takes the emergency reshard path at bootstrap. It refuses
  while another instance with a different count has a live heartbeat, and while any relay ticks: the newest tick must be
  at least 1 min old (more for slow tick rates), which it waits up to 2 min for, so a relay of a release without the
  heartbeat counts too. An instance of the previous release (5fc60dd) with its relay disabled is invisible to both
  checks: stop every old instance before an explicit reshard. Under locks it then requires a drained outbox: empty retry
  and parked tables, no batch in progress, no committed row unrelayed, no standalone range, no xid regression. Each
  failed check is named with its count, and startup fails. Procedure: stop writers, wait for `outbox.shard.lag` 0, stop
  all instances, start one instance with the new value, wait for its INFO «outbox resharded ... from N to M shards»,
  then start the rest with the same value or unset (README «Решардинг»).
- After a reshard, an instance still running with the old count halts: ERROR «... holds cursors for N shards, but
  this instance runs with outbox.shards = M ...», its relay takes no shards, its maintenance runs no step, `drain()`
  throws, and `send` and redrives are refused within one heartbeat (`IllegalStateException` «... refuses this write
  ...»): restart it with the new count or with `shards` unset. Dead
  letters keep their old shard; a redrive computes it again.

## `outbox.kafka` (`KafkaOutboxModule`)

`deadLetterTopic` (default unset: Kafka DLQ off, letters stay `kept` in the dead table): topic template, `{topic}` is
the letter's topic, e.g. `"{topic}.dlq"`. Validated at startup: a Kafka topic name (up to 249 of `[a-zA-Z0-9._-]`),
not `{topic}` alone. Behaviour and duplicates: writing-kora-outbox-handlers, `dead-letter-hook.md`.

`driverProperties`: Kafka producer properties, validated at startup.

| | Properties |
|---|---|
| Forced | `enable.idempotence=true`, `acks=all`, byte-array key and value serializers |
| Rejected | `transactional.id`; `partitioner.class`; `partitioner.ignore.keys=true`; `acks` other than `all`/`-1`; `enable.idempotence=false`; `max.in.flight.requests.per.connection` > 5; `delivery.timeout.ms` < `linger.ms + request.timeout.ms`; anything Kafka's `ProducerConfig` rejects |
| Defaults when unset | `delivery.timeout.ms=20000`, then (only if that was unset) `request.timeout.ms=15000`; `max.block.ms=5000`; `enable.metrics.push=false` |

Producer client metrics are not bound to Micrometer; they are visible through JMX only.
