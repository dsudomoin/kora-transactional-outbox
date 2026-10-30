---
name: writing-outbox-handlers
description: Use when writing or reviewing an OutboxTopicHandler, TypedOutboxTopicHandler (with OutboxDeserializer), OutboxHandler or an OutboxDeadLetterHandler (dead-letter hook) for dsudomoin/outbox (delivering outbox records of some topics to an HTTP API, another database or several targets next to Kafka, decoding payloads to typed values, OutboxHandler.byTopic next to kafka.handler(), a wrapper around the Kafka handler), or when choosing outbox.relay.batchSize, batchTimeout or retry.maxAttempts for a slow target, or delivering concurrently inside handleBatch, or setting a record aside with Outcome.requeue so the rest of its key goes on (a running process of the same account blocks it), or making a record wait for an instance of another release (Outcome.waitFor, OutboxWaitException, Outcome.Waiting), or reading contract versions (@OutboxContract, record.contractVersion()). Also «свой обработчик outbox», «доставка из аутбокса в HTTP», «dead letters», «Retry-After», «отложить событие», «requeue», «хук dead letters», «DLQ», «версия контракта», outbox.kafka.deadLetterTopic.
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
| Not on this instance: another release handles it (a contract version or sub-type this release does not know) | throw `OutboxWaitException(reason)` | `Outcome.waitFor(reason)`, `waitFor(reason, notBefore)`: key blocked, no attempt spent, never dead by itself |

- `relay.retry.maxAttempts` = 0 (default) retries forever: alert on `outbox.shard.lag`. It is global, and a dead
  record lets later records of its key overtake it: with ordered Kafka topics keep 0 and bound only the slow route
  (`Outcome.permanent(e)` once `record.attempts() + 1 >= N`).
- Idempotency key = `record.messageId()` (stable across attempts and redrive), not `record.id()`.

## Overriding handleBatch

`handle` suffices for status mapping. Retry-After: start from [AuditHandler.java](AuditHandler.java). Contract:

- one `Outcome` per record, input order, no nulls;
- never throw: a thrown call retries all its records;
- after a (topic, key) fails or waits, its later records in the call are `notAttempted()`;
- catch `OutboxWaitException` before any catch-all: `new Outcome.Waiting(e.kind(), e.getMessage(), null)` keeps its
  reason and kind (`decode(record)` and `record.contractVersion()` throw it with kind `contract`); block the key, but
  do not count it toward a «target down» stop, since a wait says nothing about the target. The relay also turns a
  `Retry` or `Permanent` carrying one into a wait, after your loop has counted it as a failure;
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

- A target for some topics is an `OutboxTopicHandler`: `topics()` (read once at build) plus `handle`, and
  `handleBatch` as above. In Kora it is a plain `@Component`: `OutboxModule` collects every untagged one, no
  factory, no `Map`, no tag. Without Kora: `OutboxRuntime.builder(config, tx, kafka.handler()).route(new
  AuditHandler(url))`. Every other topic goes to the graph's one `OutboxHandler` (Kafka by default). Two handlers
  claiming one topic, or one claiming none, fail the start naming the classes. Different topics are different
  (topic, key) pairs: no order between them.
- Do not make a per-topic target an `OutboxHandler` `@Component`: it replaces the Kafka handler, and next to another
  one Kora 1.2.23 fails with «More than one component matches dependency type: OutboxHandler (no tags)».
- Typed values: extend `TypedOutboxTopicHandler<T>`, constructor `(OutboxDeserializer<T> deserializer, ...)` calling
  `super(Set.of("audit"), deserializer)`, override `handle(T value, OutboxRecord record)`. `value` is never null:
  a null payload (tombstone) goes to `handleTombstone(record)`; override it if the topic carries them, else a
  tombstone goes to the dead table. Kotlin declares `value: T` non-null. A `handleBatch` override decodes with
  `decode(record)` (null for a tombstone). Defaults:
  - a payload that does not decode, or a null from the deserializer, at a version the handler reads → dead table at
    once (so a deserializer does no network I/O: its failure is never retried);
  - a record of a newer contract version than `T` → waits for an instance that reads it;
  - a topic no route claims while no `OutboxHandler` stands behind the routes (no `KafkaOutboxModule`) → waits for an
    instance with the route; `send` of such a topic in that app throws `IllegalArgumentException` at once.
- Deserializers: with `OutboxJsonModule`, every `@Json` class gets one from its `JsonReader`; an app's own
  `OutboxDeserializer<T>` component wins for its `T`. Protobuf has no generic factory: declare
  `OutboxDeserializer<Msg>` as `(topic, payload) -> Msg.parseFrom(payload)`. A String key: `record.keyAsString()`
  (UTF-8, null if keyless).
- Assembling by hand still works: a factory returning exactly `OutboxHandler`, e.g.
  `OutboxHandler.byTopic(Map.of("audit", toAudit), kafka.handler())` with `toAudit` an `OutboxHandler`, injecting
  `KafkaOutboxSender`, never `KafkaOutboxHandler`; keep it the only untagged `OutboxHandler` (same Kora error).
- A wrapper around `kafka.handler()` delegates `handleBatch` too (else one acks=all round trip per record).

## Waiting and contract versions

- `Outcome.Waiting` is neither a delivery nor a failure. The record becomes its key's head in `<table>_retry`, later
  records of the key queue behind it, `attempts` stays, `maxAttempts` never applies, the shard does not pause. It is
  handed again alone after a quarter of its wait so far, within `outbox.relay.waiting.delay` (1 s) and `maxDelay`
  (5 min), until an instance that can handle it takes it. `kind`: `contract`, `no_route`, `handler` (yours).
- Use it only for «another release handles this»; a failing target is `Retry`, a blocked entity is `requeue`.
- `@OutboxContract(n)` on an event class is its payload version (none = 1, not inherited). `TypedOutbox<T>` and
  `KafkaOutboxPublisher` stamp `T`'s version in the `outbox-contract` header (none for 1); `record.contractVersion()`
  reads it (1 without; an invalid value throws `OutboxWaitException`).
- `TypedOutboxTopicHandler<T>` reads versions up to `T`'s; a newer record waits undecoded, an older one arrives and
  the handler branches on `record.contractVersion()`. `T` is resolved from the subclass declaration (generic
  intermediates, Kora AOP proxies, Kotlin); else pass it: `super(topics, deserializer, AuditEvent.class)`.
- A plain `OutboxTopicHandler`, or your own `OutboxHandler` behind the routes, declares no version and is not covered
  by the writer guard: read `record.contractVersion()` yourself and throw `OutboxWaitException` for a version this
  release does not know ([AuditHandler.java](AuditHandler.java)).
- A breaking change of an event goes out in two releases with two classes (`OrderCreatedV1` written,
  `OrderCreated` read): the release protocol is in using-outbox.
