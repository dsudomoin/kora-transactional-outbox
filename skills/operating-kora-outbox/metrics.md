# kora-outbox metrics

Micrometer meters, registered only when the Kora graph has a `MeterRegistry` (for example Kora's `MetricsModule` from
`ru.tinkoff.kora:micrometer-module`). Prometheus adds suffixes: counters `_total`, timers `_seconds` with
`_bucket` series (`outbox_messages_delivered_total`, `outbox_delivery_delay_seconds_bucket`); gauges keep the plain
name (`outbox_shard_lag`, `outbox_dead_size`).

| Meter | Type | Tags | Meaning |
|---|---|---|---|
| `outbox.messages.enqueued` | counter | `topic` | Counted in `send`, including sends whose transaction rolled back. Do not compute backlog as enqueued − delivered; use `outbox.shard.lag`. |
| `outbox.messages.delivered` | counter | `topic` | Records the handler reported delivered. |
| `outbox.messages.failed` | counter | `topic`, `outcome` = `retry` / `dead` | Records diverted to (or failing again in) the retry table, and records moved to dead letters. |
| `outbox.messages.requeued` | counter | `topic` | Records the handler set aside with `Outcome.requeue()`, every requeue counted. Not a failure; the requeue past `relay.requeue.maxRequeues` counts as `failed{outcome="dead"}` instead. |
| `outbox.delivery.delay` | timer, percentile histogram | `topic` | Row insert to delivered outcome; buckets 1 ms to 6 h (about 110 bucket series per topic). |
| `outbox.batch.duration` | timer, percentile histogram | | One shard step that wrote a cursor row. |
| `outbox.shard.paused` | counter | | Shard pauses: target down, failing head, no free handler thread. |
| `outbox.shard.lag` | gauge, seconds | | How far the slowest shard's tick is behind the latest tick. |
| `outbox.retry.size`, `outbox.parked.size`, `outbox.dead.size` | gauges | | Rows in `<table>_retry`, `<table>_parked` (requeued records waiting) and `<table>_dead` (every status). |
| `outbox.dead.pending` | gauge | | Dead letters waiting for the hook's decision (`pending`); 0 without an active hook. |
| `outbox.dead.pending.oldest` | gauge, seconds | | Since `failed_at` of the oldest `pending` letter; 0 when there is none. |
| `outbox.dead.decisions` | counter | `decision` = `keep` / `discard` / `redrive` / `redrive_at` | Committed hook decisions. Admin API calls are not counted. |
| `outbox.dead.hook.failures` | counter | | Failed hook calls (exception, invalid answer, `deadLetters.timeout`): one per call, not per letter. |
| `outbox.partitions.premade` | gauge | | xid ranges above the current xid. |
| `outbox.partitions.overflow` | gauge | | 1 while the tail partition is in use. |
| `outbox.partitions.xids_left` | gauge | | xids left before inserts fail while the tail is detached; NaN while attached. |
| `outbox.partitions.orphaned` | gauge | | Detached ranges kept because they hold rows. |
| `outbox.maintenance.transaction_pooling` | gauge | | 1 when the last maintenance run detected a transaction pooler. |
| `outbox.maintenance.since_success` | gauge, seconds | | Since the last finished maintenance run on any instance. |
| `outbox.inbox.outside_window` | counter | | Ids accepted without dedup (outside the window, or not UUIDv7). |
| `outbox.inbox.maintenance.transaction_pooling`, `outbox.inbox.partitions.orphaned` | gauges | | The same for the inbox. |

Outbox gauges are refreshed by maintenance every `outbox.partitions.maintenanceInterval` on the instance that runs
it; another instance keeps its previous values (NaN before its first run). `since_success` is read by every instance.
Gauges register once per name: after a hot config reload that rebuilds outbox components they freeze, so restart.

Dead letters: `outbox.dead.size` growing while `outbox.dead.pending` does not means kept letters pile up (no hook,
the hook kept them, or the Kafka DLQ refused them): morning work. `outbox.dead.pending.oldest` above X while
`outbox.dead.hook.failures` grows means the hook fails: wake someone. Take X above `relay.retry.maxDelay` (5 min),
the hook's backoff ceiling.

Alert on `outbox.parked.size` that does not return to its usual level or keeps growing. `outbox.shard.lag` does not
see parked rows, so a record requeued forever (a blocking process that never ends, `relay.requeue.maxRequeues` 0)
shows only here; `outbox.messages.requeued{topic}` shows which topic requeues. Bound it in the handler or with
`maxRequeues` ([requeue.md](../writing-kora-outbox-handlers/requeue.md)).

Kafka producer client metrics of the outbox are not bound to Micrometer (JMX only).

Logs: logger `io.github.dsudomoin.kora.outbox`, repeated lines rate-limited. INFO for start, stop, partition DDL and
each changing admin call; WARN for diverted keys, shard pauses, deferred partition extensions, tail overflow, stale
maintenance, dead-letter hook failures and letters the Kafka DLQ kept; ERROR for dead letters, handler errors,
stranded shards, the xid regression, `premade < 2`, a detached tail, orphans and transaction pooling. WARN «outbox tick
made without its shard set» (at most once a minute): a partition is held by an `ACCESS EXCLUSIVE` lock (`ALTER TABLE`,
`VACUUM FULL`, `TRUNCATE`, a manual `LOCK`), so ticks step every shard and lag grows while it lasts.

Tracing (with an OpenTelemetry `Tracer` in the graph): one `PRODUCER` span per delivery attempt,
`outbox deliver <topic>`, child of the writer's trace, with attributes `messaging.destination.name`,
`outbox.message_id`, `outbox.attempt`, `outbox.delay_ms`, `outbox.outcome` (`delivered`, `retry`, `permanent`,
`not_attempted`, `requeued`).
