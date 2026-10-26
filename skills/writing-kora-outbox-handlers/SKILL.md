---
name: writing-kora-outbox-handlers
description: Use when writing or reviewing an OutboxHandler for kora-transactional-outbox (delivering outbox records to an HTTP API, another database or several targets, OutboxHandler.byTopic next to kafka.handler(), a wrapper around the Kafka handler), or when choosing outbox.relay.batchSize, batchTimeout or retry.maxAttempts for a slow target. Also «свой обработчик outbox», «доставка из аутбокса в HTTP», «dead letters», «Retry-After».
---

# Writing kora-outbox handlers

## Overview

One `handleBatch` call gets `outbox.relay.batchTimeout` (default 30 s). At 80 % (24 s) the relay asks it to wind
down and interrupts the thread; at 100 % it abandons the call and retries every record, delivered ones included.
**A handler is correct only if one call normally finishes well inside 80 % of `batchTimeout`.**

## The batch budget

```
batchSize × p99(one record) ≤ 0.8 × batchTimeout − per-record timeout
```

Target p99 2 s, request timeout 5 s, defaults: (24 − 5) / 2 → `batchSize` ≤ 9. Put the arithmetic in a config
comment. `batchSize` applies to every handler, Kafka included. If that costs too much throughput:

- raise `batchTimeout` to fit, keeping it ≥ `delivery.timeout.ms + 5 s + max.block.ms`;
- or override `handleBatch` to deliver keys concurrently, one (topic, key) in order. `windDownRequested()` is
  per thread: poll it on the calling thread.

| Rationalization | Reality |
|---|---|
| "The soft deadline returns NotAttempted for the rest anyway" | A safety net, not flow control. It interrupts a live request (`Retry`: an attempt spent, maybe a duplicate). `byTopic` routes run in turn, so routes behind the slow one, often `kafka.handler()`, wait. |
| "`batchSize` only limits memory" | It is how many records one call must finish before the deadline. |
| "The HTTP client has a timeout" | Kora's default read timeout is 2 min. Set a per-request timeout well below `batchTimeout`. |

## Outcomes

| Target answer | `handle` | `handleBatch` |
|---|---|---|
| Success | return | `Outcome.delivered()` |
| Never succeeds (4xx except 408/429) | throw `OutboxPermanentException` | `Outcome.permanent(e)`: to `<table>_dead`, key continues |
| Transient (5xx, 408, IO) | throw anything else | `Outcome.retry(e)`: key blocked, backoff 1 s → 5 min |
| 429 / Retry-After | throw: plain backoff only | `Outcome.retry(e, notBefore)`, capped at 24 h |
| Not tried | | `Outcome.notAttempted()`, spends no attempt |

- `relay.retry.maxAttempts` defaults to 0: retry forever. A target failing forever blocks its keys while
  `outbox.shard.lag` grows. Alert on lag, or set `maxAttempts` knowing it applies to Kafka too.
- A dead-letter path needs an alert on `outbox.dead.size > 0` and a redrive plan (operating-kora-outbox); say so.
- Idempotency key = `record.messageId()`, stable across attempts and redrive. Not `record.id()`.

## Overriding handleBatch

`handle` alone suffices for status mapping; the default `handleBatch` keeps the contract. For Retry-After or
concurrency, start from [AuditHandler.java](AuditHandler.java). Contract:

- one `Outcome` per record, input order, no nulls;
- never throw: a thrown call retries all its records, delivered ones included;
- after a (topic, key) fails, its later records in the call are `notAttempted()`;
- check `OutboxHandler.windDownRequested()` before each record (Kora's JDK-transport HTTP client clears the
  interrupt flag); restore the flag after `InterruptedException`.

## Wiring

- One untagged handler: a factory returning exactly `OutboxHandler`, e.g.
  `OutboxHandler.byTopic(Map.of("audit", new AuditHandler(url)), kafka.handler())` with `KafkaOutboxSender kafka`.
- A wrapper around `kafka.handler()` delegates `handleBatch` too, or each record becomes its own acks=all round trip.
