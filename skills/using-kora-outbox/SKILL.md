---
name: using-kora-outbox
description: Use when adding kora-transactional-outbox (io.github.dsudomoin:kora-outbox, kora-outbox-kafka) to a Kora 1.x service, sending events with Outbox, TypedOutbox, ProtobufOutbox or KafkaOutboxPublisher, editing outbox.* or outbox.kafka.driverProperties, or preparing the Postgres role and Kafka topics for it. Also «аутбокс», «outbox», «transactional outbox», «отправить событие в Kafka в одной транзакции с БД».
---

# Using kora-outbox

## Overview

What the javadocs omit and services break on: database, Kafka and config prerequisites.

## Quick reference

| Situation | Do |
|---|---|
| Any send | Inside the same `db.inTx(...)` as the business writes; exceptions propagate. |
| Ordered / unordered | key = ordering unit (userId), order per (topic, key) in commit order. Unkeyed: `OutboxMessage.of(topic, payload)`, `typed.send(topic, null, value)`. |
| `null` value in `TypedOutbox` | Kafka tombstone: deletes the key on a compacted topic. |
| Kotlin | `db.inTx<Unit> { _: Connection -> ... }`; blocking, no coroutines. |
| Keyed messages for **2+ distinct (topic, key)** in one transaction | One `outbox.sendAll(...)` for all of them. |
| Sizing shards, workers, pool, partitions | Profiles: [sizing-examples.md](sizing-examples.md). |
| Existing tests that boot the graph | `outbox.relay.enabled = false` unless they assert delivery (testing-kora-outbox). |

## Why one sendAll

Every keyed send (`Outbox`, `TypedOutbox`, `ProtobufOutbox`, `KafkaOutboxPublisher`) locks its (topic, key) until
commit, and transactions taking overlapping keys in different orders can deadlock (40P01). One keyed message per
transaction cannot deadlock on these locks, and unkeyed sends take none: the rule is for 2+ keyed ones. `sendAll`
locks its keys at once in a fixed order. `TypedOutbox` has no `sendAll`; serialize with Kora's `JsonWriter<T>`:

```java
outbox.sendAll(List.of(
        OutboxMessage.of("orders", order.userId(), orderJson.toByteArrayUnchecked(created)),
        OutboxMessage.of("payments", order.userId(), paymentJson.toByteArrayUnchecked(payment))));
```

## Config rules

- `outbox.relay.batchTimeout ≥ delivery.timeout.ms + 5 s + max.block.ms` (defaults 20 + 5 + 5 = 30 s).
- Metrics need a Micrometer `MeterRegistry`. Add Kora's `MetricsModule` only if the service exposes it: Kora serves
  `/metrics` from its private HTTP server (`httpServer.privateApiHttpPort`, default 8085; needs an HTTP server module).
  Otherwise say in the notes that outbox metrics are off.
- Keys and producer rules: [config-reference.md](config-reference.md).

## Before deploy: required in your final notes

After adding or changing the outbox, end your answer with a «Before deploy» list that covers every clause below,
each marked done, not applicable (why) or for ops:

1. **DB role**: `CREATE` on the schema (bootstrap runs on every start); owns the tables or is a member of the owner
   role; `SELECT` on `<table>_id_seq`; `pg_read_all_stats` recommended; no role `statement_timeout` of seconds.
2. **Connections**: direct or session-mode pooler, never transaction pooling. `db.maxPoolSize` ≥ the service's own
   peak + `relay.workers` + 2.
3. **Table**: `outbox.table = "<schema>.outbox"` in the schema the service's own tables use, the same on every
   instance. Schema unknown: leave it unqualified and ask. Never guess `public` (on PG 15+ only the database owner may
   create there).
4. **Bulk**: `sendAll` with thousands of distinct keys needs `max_locks_per_transaction` sized for the peak (SQLState
   53200; restart).
5. **Topics**: created with final partition count and ACLs before writers deploy; RF ≥ 3, `min.insync.replicas` ≥ 2,
   `unclean.leader.election.enable=false`; no auto-create; never add partitions later (keys move).
6. **Relay** enabled on at least one instance: partition maintenance runs only there.
7. **Alerts**: `outbox.shard.lag` growing, `outbox.dead.size > 0`, `outbox.delivery.delay` p99 (more in
   operating-kora-outbox). Redrive dead letters by moving rows from `<table>_dead` back into `<table>` in one
   transaction, never via `<table>_retry`.
