# Requeue: set a record aside without blocking its key

`Outcome.requeue()` means «not now: let the rest of my key go, hand me back later». Use it when the record is not
ready but the target is fine, typically a handler that runs a per-entity job queue (key = account id) and finds
another process of the same account running, a process that ends on its own.

| | `Outcome.retry(e)` | `Outcome.requeue()` |
|---|---|---|
| Means | the target failed | the record is not ready; the target is fine |
| Its key | waits: later records queue behind it in `<table>_retry` | goes on: later records are delivered while it waits |
| Comes back | after backoff (1 s doubling to 5 min), or at `notBefore` if later | whichever comes first: right before the next new record of its key (in the same call), or its due time `now + outbox.relay.requeue.delay` (1 s); with `requeue(notBefore)`, `max(now + delay, notBefore)`, at most 24 h ahead |
| Counts | `record.attempts()`, `relay.retry.maxAttempts` | `record.requeues()`, `relay.requeue.maxRequeues` |
| Shard | pauses and backs off when nothing was delivered | no pause, no backoff, chunk size unchanged |

Never re-send a copy with `Outbox.send` from the handler instead: `handleBatch` runs outside the relay step's
transaction, so a crash between the two leaves an extra copy. `requeue` moves the record into `<table>_parked` in the
step's own transaction (same `message_id`, payload, headers, `created_at`); a rolled-back step moves nothing.

## The coordinator

Only a `handleBatch` override can requeue: the default `handleBatch` (calling `handle` per record) never returns it.
`OutboxHandler.byTopic` passes a route's `requeue` through like any outcome.

```java
/** Per-account job queue: one topic, key = account id. Not a @Component if byTopic wraps it. */
public final class AccountJobsHandler implements OutboxHandler {

    private static final int GIVE_UP_AFTER = 1000; // requeues, then dead letters

    private final AccountProcesses processes; // which processes of which accounts run now
    private final AccountJobs jobs;

    public AccountJobsHandler(AccountProcesses processes, AccountJobs jobs) {
        this.processes = processes;
        this.jobs = jobs;
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        jobs.start(record);
    }

    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        var outcomes = new ArrayList<Outcome>(records.size());
        var failedKeys = new HashSet<ByteBuffer>();
        for (var record : records) {
            // an unkeyed record: its failure holds back nothing
            var key = record.key() == null ? null : ByteBuffer.wrap(record.key());
            if (key != null && failedKeys.contains(key) || OutboxHandler.windDownRequested()) {
                outcomes.add(Outcome.notAttempted());
            } else if (processes.blocks(record)) { // e.g. a running WITHDRAW of the account blocks its FEE_RELEASE
                outcomes.add(record.requeues() < GIVE_UP_AFTER
                        ? Outcome.requeue()
                        : Outcome.permanent(new IllegalStateException("blocked " + record.requeues() + " times")));
            } else {
                try {
                    handle(record);
                    outcomes.add(Outcome.delivered());
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    outcomes.add(Outcome.retry(e));
                    if (key != null) {
                        failedKeys.add(key);
                    }
                }
            }
        }
        return outcomes;
    }
}
```

- The `handleBatch` contract still holds: once a key fails in the call, its later records are `notAttempted()`. A
  `requeue` there is not applied: the record follows the failed one into `<table>_retry`.
- Use `requeue(notBefore)` when the handler knows when the blocking process ends.
- Give up either in the handler (`Outcome.permanent(e)` past a `record.requeues()` bound, per route) or globally
  with `outbox.relay.requeue.maxRequeues` (default 0 = unlimited): the requeue past it moves the record to
  `<table>_dead` with the error «requeue limit reached», attempts unchanged. Every hand-back answered with `requeue`
  counts, a merge ahead of a new record of the key included, so a busy key spends requeues faster than one per
  `delay`: bound by count, not by expected time.

## Order

Per key, first delivery follows commit order, except that a requeued record is delivered after the records of its key
that were delivered while it was parked. Parked records of a key come back in their original order, ahead of the
newer record that brings them: FEE_RELEASE#1 requeued, then FEE_RELEASE#2 arrives → one call [#1, #2]. An unkeyed
record comes back only at its due time, and so does a record whose key waits in `<table>_retry`, while it waits.

## When a parked record is handed back

| Outcome | Effect |
|---|---|
| `delivered()` | Leaves `<table>_parked`. |
| `requeue()` | Stays parked, `requeues() + 1`, new due time. |
| `notAttempted()` | Stays parked, due no earlier than `now + delay`; no attempt spent. |
| `retry(e)` | Becomes the retry head of its key: the key is blocked from then on, and its newer parked records follow it. `maxAttempts` applies as to any record. |
| `permanent(e)` | `<table>_dead`. |

A `requeue` of a retry-table head parks it and lets the next record of its key go on.

## Call size and wind-down

- A call carries the chunk plus up to the chunk limit (≤ `batchSize`) of parked records of the chunk's keys, oldest
  first: up to 2 × `batchSize` records. Keep the blocked check cheap, or size the batch budget in SKILL.md for 2 ×.
- A call in which the handler only requeued (or left not attempted) the merged parked records and attempted none of
  the new ones fails as a whole, like a call with nothing attempted: shard pause, halved chunk, probe, divert. A
  handler that winds down right after requeuing costs a pause, never a spinning shard.
- A call that pauses the shard applies none of its requeues: those records are handed again with the chunk.

## Observing it

Counter `outbox.messages.requeued{topic}` (every requeue), gauge `outbox.parked.size`, span outcome `requeued`
(operating-kora-outbox, [metrics.md](../operating-kora-outbox/metrics.md)). In tests `OutboxRelay.drain()` does not
wait for a parked record that is not due yet: assert `select count(*) from <table>_parked` = 0 next to `_retry`.
