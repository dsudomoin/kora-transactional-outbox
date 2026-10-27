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
| `table` | `"outbox"` | `[schema.]name`, each part `[A-Za-z_][A-Za-z0-9_]{0,40}`, lowercased. Base name of `<table>`, `_retry`, `_parked`, `_dead`, `_tick`, `_cursor`, `_halt`, `_maintenance`, `_dropping`. Unqualified = `current_schema()` of each connection. |
| `shards` | `32` | 1..32767. A shard is processed by one worker at a time across all instances; a key always maps to one shard. Changing it needs an empty outbox (README «Решардинг»); startup fails on a mismatch. |

## `outbox.relay`

| Key | Default | Rule and meaning |
|---|---|---|
| `enabled` | `true` | `false`: no relay workers and no partition maintenance on this instance; `OutboxRelay.drain()` still works. Keep it on for at least one production instance. |
| `workers` | `4` | ≥ 1. Relay threads per instance; each holds a DB connection while working. |
| `batchSize` | `1000` | ≥ 1. Rows per chunk read and per handler call, for every handler. A call may also carry up to the chunk limit of parked (requeued) records of the chunk's keys: up to 2 × `batchSize` records. |
| `maxTicksPerBatch` | `100` | ≥ 1. |
| `minTickInterval` | `50ms` | ≥ 0. Under load, at most one tick per interval. |
| `idlePollInterval` | `200ms` | > 0. Idle tick rate; how fast a commit on another instance is noticed. |
| `batchTimeout` | `30s` | > 0. Bound of one handler call: wind-down at 80 %, abandoned (all records retried) at 100 %. With Kafka ≥ `delivery.timeout.ms + 5 s + max.block.ms`. `drain()` gives up after 2 × this without progress. |
| `shutdownTimeout` | `30s` | ≥ 0. Grace for in-flight batches at shutdown; then they are interrupted and redelivered later. |
| `headFailuresBeforeDivert` | `3` | ≥ 1. Consecutive failures before a shard isolates a single failing key. |

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
| `maintenanceInterval` | `30s` | > 0. Outbox and inbox maintenance period. |

## `outbox.inbox` (`InboxModule`)

| Key | Default | Rule and meaning |
|---|---|---|
| `table` | `"inbox"` | Same naming rules as `outbox.table`. Created only when a component uses `Inbox`. |
| `partitionInterval` | `1h` | ≥ 1m. |
| `premake` | `24h` | ≥ 2 × `partitionInterval`; `premake − partitionInterval` > `outbox.partitions.maintenanceInterval`. |
| `retention` | `24h` | ≥ 2 × `partitionInterval`; `(retention + premake) / partitionInterval` ≤ 1000. Ids younger than `retention − partitionInterval` (23 h) are always deduplicated. Change on all instances at once. |

An inbox-only service also reads `outbox.partitions.maintenanceInterval` and `outbox.relay.shutdownTimeout`.

## `outbox.kafka.driverProperties` (`KafkaOutboxModule`)

Kafka producer properties, validated at startup.

| | Properties |
|---|---|
| Forced | `enable.idempotence=true`, `acks=all`, byte-array key and value serializers |
| Rejected | `transactional.id`; `partitioner.class`; `partitioner.ignore.keys=true`; `acks` other than `all`/`-1`; `enable.idempotence=false`; `max.in.flight.requests.per.connection` > 5; `delivery.timeout.ms` < `linger.ms + request.timeout.ms`; anything Kafka's `ProducerConfig` rejects |
| Defaults when unset | `delivery.timeout.ms=20000`, then (only if that was unset) `request.timeout.ms=15000`; `max.block.ms=5000`; `enable.metrics.push=false` |

Producer client metrics are not bound to Micrometer; they are visible through JMX only.
