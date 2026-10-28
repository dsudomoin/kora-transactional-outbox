---
name: writing-outbox-handlers
description: Use when writing or reviewing an OutboxHandler or an OutboxDeadLetterHandler (dead-letter hook) for dsudomoin/outbox (delivering outbox records to an HTTP API, another database or several targets, OutboxHandler.byTopic next to kafka.handler(), a wrapper around the Kafka handler), or when choosing outbox.relay.batchSize, batchTimeout or retry.maxAttempts for a slow target, or delivering concurrently inside handleBatch, or setting a record aside with Outcome.requeue so the rest of its key goes on (a running process of the same account blocks it). Also «свой обработчик outbox», «доставка из аутбокса в HTTP», «dead letters», «Retry-After», «отложить событие», «requeue», «хук dead letters», «DLQ», outbox.kafka.deadLetterTopic.
---

# Writing outbox handlers

A `handleBatch` call gets `outbox.relay.batchTimeout` (default 30 s): at 80 % the relay asks it to wind down and
interrupts the thread, at 100 % it abandons the call and retries every record, delivered ones included.
**A handler is correct only if one call normally finishes well inside 80 % of `batchTimeout`**, with a per-request
timeout well below it (Kora's default: 2 min).

## The batch budget

```
batchSize × p99(one record) ≤ 0.8 × batchTimeout − per-record timeout
```

p99 2 s, request timeout 5 s, defaults: (24 − 5) / 2 → `batchSize` ≤ 9, Kafka included.

A sequential handler delivers at most min(shards, instances × workers) / mean seconds per record (2 × 4 / 0.3 s ≈
26 msg/s), whatever the `batchSize`. If too low, follow every rule in [concurrent-delivery.md](concurrent-delivery.md).
The soft deadline is a safety net, not flow control.

## Outcomes

| Target answer | `handle` | `handleBatch` |
|---|---|---|
| Success | return | `Outcome.delivered()` |
| Never succeeds (4xx except 408/429) | throw `OutboxPermanentException` | `Outcome.permanent(e)`: to `<table>_dead`, key continues |
| Transient (5xx, 408, IO) | throw anything else | `Outcome.retry(e)`: key blocked, backoff 1 s → 5 min |
| 429 / Retry-After | throw: plain backoff only | `Outcome.retry(e, notBefore)`, capped at 24 h |
| Not tried | | `Outcome.notAttempted()`, spends no attempt |
| Not now, let the key go (a running process of the same entity blocks it) | | [`Outcome.requeue()`](requeue.md) |

- `relay.retry.maxAttempts` = 0 (default) retries forever: alert on `outbox.shard.lag`. It is global, and a dead
  record lets later records of its key overtake it: with ordered Kafka topics keep 0 and bound only the slow route
  (`Outcome.permanent(e)` once `record.attempts() + 1 >= N`).
- Idempotency key = `record.messageId()` (stable across attempts and redrive), not `record.id()`.

## Overriding handleBatch

`handle` suffices for status mapping. Retry-After: start from [AuditHandler.java](AuditHandler.java). Contract:

- one `Outcome` per record, input order, no nulls;
- never throw: a thrown call retries all its records;
- after a (topic, key) fails, its later records in the call are `notAttempted()`;
- check `OutboxHandler.windDownRequested()` before each record and, if concurrent, while waiting (Kora's JDK
  HTTP client clears the interrupt flag); restore the flag after `InterruptedException`;
- run each record in `OutboxHandler.inRecordScope(record, () -> ...)` to keep its trace, like the default.

## Dead letters

Without a hook dead records are `kept` for humans (operating-outbox). An `OutboxDeadLetterHandler` component
decides instead: `keep()`, `discard()`, `redrive()` or `redriveAt(at)` per letter. First read
[dead-letter-hook.md](dead-letter-hook.md): calls are all-or-nothing and at-least-once (idempotent by `messageId`);
bound redrive loops with `letter.redrives()`; `active()` false (as `NONE`) keeps all, uncalled. For Kafka, set
`outbox.kafka.deadLetterTopic` instead, minding its duplicates. Alert on `outbox.dead.size`.

## Wiring

- One untagged handler: a factory returning exactly `OutboxHandler`, e.g.
  `OutboxHandler.byTopic(Map.of("audit", new AuditHandler(url)), kafka.handler())`, injecting `KafkaOutboxSender`,
  never `KafkaOutboxHandler` (Kora 1.2.23 then fails: «More than one component matches dependency type:
  OutboxHandler (no tags)»).
- A wrapper around `kafka.handler()` delegates `handleBatch` too (else one acks=all round trip per record).
