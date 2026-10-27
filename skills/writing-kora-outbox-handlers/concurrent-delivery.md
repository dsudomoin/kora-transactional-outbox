# Concurrent delivery inside handleBatch

A sequential handler delivers at most min(shards, instances × workers) / mean seconds per record (2 × 4 / 0.3 s ≈ 26
msg/s), whatever the `batchSize`. Above that, deliver the records of one call concurrently.
[ConcurrentAuditHandler.java](ConcurrentAuditHandler.java) follows every rule below; start from it. The `handleBatch`
contract in SKILL.md still holds: one outcome per record in input order, no nulls, never throw.

## Rules

- **Cap requests in flight per call** with a `Semaphore` of P, independent of `batchSize`. The target sees up to
  min(shards, instances × workers) × P at once (a retry pass hands up to `batchSize` heads in one call): give its
  owner that cap, not the Little's-law average. Over HTTP/1.1 every request in flight is its own connection, and the
  JDK `HttpClient` has no per-host limit of its own.
- **Throughput.** A call waits for its slowest request, so rows/s ≤ min(shards, instances × workers) × `batchSize` /
  (call time + step overhead), where call time ≈ ceil(route records / P) × p99. Size it with one instance down, with
  ≥ 2× headroom. The deadline still applies: ceil(route records / P) × p99 ≤ 0.8 × `batchTimeout` − per-record
  timeout, and `byTopic` runs the other routes (often `kafka.handler()`) after this one in the same call.
- **Down-target breaker.** While the target looks down, start at most 3 requests and send the rest only if one of them
  is answered; otherwise return `notAttempted()` for the rest (no attempt spent). Without it every record of every call
  hits a down target and spends an attempt. The default `handleBatch` stops after 3 failing pairs while nothing
  succeeded. The example remembers whether the previous call got any answer and probes only then, so a healthy target
  pays no extra round per call.
- **One (topic, key) in order.** At most one request per key in flight: start a key's next record only after the
  previous one was delivered or dead-lettered; after it fails, the key's later records are `notAttempted()`. Unkeyed
  records never block each other. Keep events that need no order unkeyed so they all go out at once.
- **Wind-down.** `windDownRequested()` is per thread, and a non-blocking submit loop never sees it: poll it on the
  calling thread while waiting (`future.get(100, MILLISECONDS)`, `semaphore.tryAcquire(100, MILLISECONDS)` in a loop).
  On wind-down, cancel unfinished requests, keep the real outcome of each finished one, and return `notAttempted()` for
  the rest. Restore the interrupt flag after `InterruptedException`.
- **Never throw.** Catch what building or starting a request throws and return `Outcome.retry(e)` for that record.
- **Retry bound for this route only.** `outbox.relay.retry.maxAttempts` is global, so it stays 0 while an ordered Kafka
  topic shares the relay. Return `Outcome.permanent(e)` instead of `Outcome.retry(e)` once
  `record.attempts() + 1 >= N` (`attempts` counts earlier failed deliveries).
- **Tracing.** The default `handleBatch` runs each `handle` in the record's trace context; an override does not.

## Worked numbers

Profile 4 in [sizing-examples.md](../using-kora-outbox/sizing-examples.md): 3 instances, default 256 shards, 2 000
orders/s with one audit event each, audit p99 300 ms. With `workers` 4, `batchSize` 400 and P = 200: about 9 100
rows/s against 4 000 needed with one instance down (2.3×), and at most 12 × 200 = 2 400 requests in flight at the audit
service.
