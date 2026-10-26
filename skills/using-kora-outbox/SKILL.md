---
name: using-kora-outbox
description: Use when adding kora-transactional-outbox (io.github.dsudomoin:kora-outbox, kora-outbox-kafka) to a Kora 1.x service, sending events with Outbox, TypedOutbox, ProtobufOutbox or KafkaOutboxPublisher, editing outbox.* or outbox.kafka.driverProperties, or preparing the Postgres role and Kafka topics for it. Also «аутбокс», «outbox», «transactional outbox», «отправить событие в Kafka в одной транзакции с БД».
---

# Using kora-outbox

## Overview

The javadocs cover wiring and sending. Services break on what they omit: database, Kafka and config prerequisites.

## Quick reference

| Situation | Do |
|---|---|
| Any send | Inside the same `db.inTx(...)` as the business writes; exceptions propagate. |
| Ordered events | key = ordering unit (userId). Order holds per (topic, non-null key), in commit order. |
| Unordered | `OutboxMessage.of(topic, payload)`, `typed.send(topic, null, value)`; 4-argument form: `(String) null`. |
| `null` value in `TypedOutbox` | Kafka tombstone: deletes the key on a compacted topic. |
| Kotlin | `db.inTx<Unit> { _: Connection -> ... }`; blocking, no `suspend` or dispatcher hops. |
| Keyed sends for **2+ distinct (topic, key)** in one transaction | One `outbox.sendAll(...)` for all of them. |
| Choosing shards, workers, pool, partitions | Five worked profiles: [sizing-examples.md](sizing-examples.md). |
| Existing tests that boot the graph | `outbox.relay.enabled = false` unless they assert delivery (testing-kora-outbox). |

## Why one sendAll

Every keyed send (`Outbox`, `TypedOutbox`, `ProtobufOutbox`, `KafkaOutboxPublisher`) locks its (topic, key) until
commit. Transactions taking overlapping keys in different orders can deadlock (SQLState 40P01). `sendAll` locks its
keys at once in a fixed order. Unkeyed sends take no lock. `TypedOutbox` has no `sendAll`; serialize yourself:

```java
db.inTx(() -> {
    orders.insert(order);
    outbox.sendAll(List.of(   // orderJson, auditJson: Kora JsonWriter<T> components
            OutboxMessage.of("orders", order.userId(), orderJson.toByteArrayUnchecked(created)),
            OutboxMessage.of("audit", order.userId(), auditJson.toByteArrayUnchecked(audit))));
});
```

## Config rules

- `outbox.relay.batchTimeout ≥ delivery.timeout.ms + 5 s + max.block.ms` (defaults 20 + 5 + 5 = 30 s). Raising
  producer timeouts or lowering `batchTimeout` turns slow sends into retries and duplicates.
- Metrics need a Micrometer `MeterRegistry` in the graph: add Kora's `MetricsModule` (`micrometer-module`) if absent.
- Keys, defaults and rejected producer properties: [config-reference.md](config-reference.md).

## Before deploy: required in your final notes

When you add or change the outbox, end your answer with a «Before deploy» list stating for this service:

1. **DB role**: `CREATE` on the schema (bootstrap runs on every start); owns the tables or is a member of the owner
   role; `SELECT` on `<table>_id_seq`; `pg_read_all_stats` recommended; no role `statement_timeout` of seconds. No
   migrations for library tables.
2. **Connections**: direct or session-mode pooler, never transaction pooling. `db.maxPoolSize` += `relay.workers` + 2.
3. **Table**: `outbox.table = "<schema>.outbox"`, identical on every instance.
4. **Bulk**: `sendAll` with thousands of distinct keys needs `max_locks_per_transaction` sized for the peak (else
   SQLState 53200; restart required).
5. **Topics**: created with final partition count and ACLs before writers deploy; RF ≥ 3, `min.insync.replicas` ≥ 2,
   `unclean.leader.election.enable=false`. Not broker auto-create.
6. **Relay** enabled on at least one instance: partition maintenance runs only there.
7. **Alerts**: see operating-kora-outbox.

## Common mistakes

| Mistake | Effect |
|---|---|
| Two keyed `TypedOutbox.send` in one transaction | Occasional 40P01 aborts under concurrency |
| Unqualified `outbox.table` with differing `db.schema` | Instances use different tables |
| Adding partitions to a live topic | Keys move: consumers see them out of order |
