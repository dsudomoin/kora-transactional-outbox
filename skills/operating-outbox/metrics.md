# outbox metrics

Micrometer meters, registered only when the Kora graph has a `MeterRegistry` (for example Kora's `MetricsModule` from
`ru.tinkoff.kora:micrometer-module`). Prometheus adds suffixes: counters `_total`, timers `_seconds` with
`_bucket` series (`outbox_messages_delivered_total`, `outbox_delivery_delay_seconds_bucket`); gauges keep the plain
name (`outbox_shard_lag`, `outbox_dead_size`).

Without Kora the same meters come from `OutboxMetrics` (`outbox-micrometer`) passed to
`OutboxRuntime.builder(...).telemetry(...)`. `ObservationOutboxTelemetry` adds three Micrometer observations on top:
`outbox.enqueue` per send call, `outbox.relay.step` per relay step (span `outbox relay step`, a trace root;
high-cardinality `outbox.shards`) and `outbox.deliver` per record (span kind CONSUMER, parented by the stored trace;
tags `messaging.destination.name`, `outbox.outcome`; high-cardinality `outbox.shard`). Rename them with an
`Outbox*ObservationConvention`, rename meters with a `MeterFilter`; README «Наблюдаемость» → «Контракт по этапам».

| Meter | Type | Tags | Meaning |
|---|---|---|---|
| `outbox.messages.enqueued` | counter | `topic` | Counted in `send`, including sends whose transaction rolled back. Do not compute backlog as enqueued − delivered; use `outbox.shard.lag`. |
| `outbox.messages.delivered` | counter | `topic` | Records the handler reported delivered. |
| `outbox.messages.failed` | counter | `topic`, `outcome` = `retry` / `dead` | Records diverted to (or failing again in) the retry table, and records moved to dead letters. |
| `outbox.messages.requeued` | counter | `topic` | Records the handler set aside with `Outcome.requeue()`, every requeue counted. Not a failure; the requeue past `relay.requeue.maxRequeues` counts as `failed{outcome="dead"}` instead. |
| `outbox.delivery.delay` | timer, percentile histogram | `topic` | Row insert to delivered outcome; buckets 1 ms to 6 h (about 110 bucket series per topic). |
| `outbox.batch.duration` | timer, percentile histogram | | One shard step that wrote a cursor row. |
| `outbox.shard.paused` | counter | | Shard pauses: target down, failing head, no free handler thread. |
| `outbox.relay.failures` | counter | | Relay failures it logs at WARN: a pass failing outside a step (tick, cursor read, connection, a lost one included) and a rolled-back single-shard step. Not a multi-shard step redone shard by shard, not maintenance, heartbeat, the dead-letter pass or failures while stopping. |
| `outbox.shard.lag` | gauge, seconds | | How far the slowest shard's tick is behind the latest tick. |
| `outbox.retry.size`, `outbox.parked.size`, `outbox.dead.size` | gauges | | Rows in `<table>_retry`, `<table>_parked` (requeued records waiting) and `<table>_dead` (every status). |
| `outbox.dead.pending` | gauge | | Dead letters waiting for the hook's decision (`pending`); 0 without an active hook. |
| `outbox.dead.pending.oldest` | gauge, seconds | | Since `failed_at` of the oldest `pending` letter; 0 when there is none. |
| `outbox.dead.decisions` | counter | `decision` = `keep` / `discard` / `redrive` / `redrive_at` | Committed hook decisions. Admin API calls are not counted. |
| `outbox.dead.hook.failures` | counter | | Failed hook calls (exception, invalid answer, `deadLetters.timeout`): one per call, not per letter. |
| `outbox.partitions.premade` | gauge | | xid ranges above the current xid. |
| `outbox.partitions.overflow` | gauge | | 1 while the tail partition holds rows: writes go on into it; the next swap turns it into a range (over `maxTailScan`: inside `tailConversion.window`). |
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
`maxRequeues` ([requeue.md](../writing-outbox-handlers/requeue.md)).

Kafka producer client metrics of the outbox are not bound to Micrometer (JMX only).

Logs: logger `io.github.dsudomoin.outbox`, repeated lines rate-limited. INFO for start, stop, partition DDL and
each changing admin call; WARN for diverted keys, shard pauses, deferred partition extensions, tail swaps without
their lock (55P03), a window tail conversion that did not fit `maxStall`, tail overflow, stale
maintenance, dead-letter hook failures, letters the Kafka DLQ kept and failed heartbeats («outbox heartbeat of instance
... failed»); ERROR for dead letters, handler errors, stranded shards, the shard-count mismatch after a reshard («...
holds cursors for N shards, but this instance runs with outbox.shards = M ...»), the xid regression, `premade < 2`, a
detached tail, a swap not run on 10 due runs or with ranges running out (hourly, names the cause), a hot tail over
`maxTailScan` outside the window (hourly), a window that ended without converting it («... ended without a
conversion», once), orphans and transaction pooling. WARN «outbox tick made without its shard set» (at most once a minute): a
partition is held by an `ACCESS EXCLUSIVE` lock (`ALTER TABLE`, `VACUUM FULL`, `TRUNCATE`, a manual `LOCK`), so ticks
step every shard and lag grows while it lasts.

Tracing (with an OpenTelemetry `Tracer` in the graph): one `PRODUCER` span per delivery attempt,
`outbox deliver <topic>`, child of the writer's trace, with attributes `messaging.destination.name`,
`outbox.message_id`, `outbox.attempt`, `outbox.delay_ms`, `outbox.outcome` (`delivered`, `retry`, `permanent`,
`not_attempted`, `requeued`).
