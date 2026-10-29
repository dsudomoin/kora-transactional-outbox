---
name: using-outbox
description: Use when adding dsudomoin/outbox (io.github.dsudomoin:outbox-kora, outbox-kafka-kora, outbox-admin-kora) to a Kora 1.x service, sending events with Outbox, TypedOutbox, ProtobufOutbox or KafkaOutboxPublisher, editing outbox.* or outbox.kafka.driverProperties, or preparing the Postgres role, the schema migration (outbox.schema.bootstrap, OutboxSql, OutboxOwner) and Kafka topics for it. Also «аутбокс», «outbox», «transactional outbox», «отправить событие в Kafka в одной транзакции с БД».
---

# Using outbox

## Quick reference

- **Versions**: Java 17+; Postgres 14+ (13 fails at startup), one primary; kafka-clients 3.9.2 or 4.x; Kora 1.2.23+:
  `outbox-kora` brings `platform("ru.tinkoff.kora:kora-parent:1.2.23")`, which raises every Kora module and HikariCP
  (7.1.0) on the runtime classpath, so set the service's own `kora-parent` and its annotation or symbol processors to
  1.2.23 too.
- **Any send**: inside the same `db.inTx(...)` as the business writes; exceptions propagate.
- **`null` `TypedOutbox` value** = Kafka tombstone: deletes the key on compacted topics.
- **Kotlin**: `db.inTx<Unit> { _: Connection -> ... }`; blocking, no coroutines.
- **Sizing**: [sizing-examples.md](sizing-examples.md); every key and producer rule:
  [config-reference.md](config-reference.md).
- **Tests that boot the graph**: `outbox.relay.enabled = false` unless they assert delivery (testing-outbox).
- **Admin API**: `outbox-admin-kora` (`OutboxAdminHttpModule`) needs one `OutboxAdminAuthorizer` (operating-outbox).

## Keys and sendAll

Every keyed send (`Outbox`, `TypedOutbox`, `ProtobufOutbox`, `KafkaOutboxPublisher`) locks its (topic, key) until
commit, and transactions taking overlapping keys in different orders deadlock (40P01). So a transaction with keyed
messages for **2+ distinct (topic, key)** sends them all in one `outbox.sendAll(...)`, which locks them at once in a
fixed order (one keyed message cannot deadlock). `TypedOutbox` has no `sendAll`; serialize with Kora's
`JsonWriter<T>`:

```java
outbox.sendAll(List.of(
        OutboxMessage.of("orders", order.userId(), orderJson.toByteArrayUnchecked(created)),
        OutboxMessage.of("payments", order.userId(), paymentJson.toByteArrayUnchecked(payment))));
```

Key a message only by its own ordering unit (userId: order holds per (topic, key) in commit order); an event that
needs no order (audit) stays unkeyed: `OutboxMessage.of(topic, payload)` or `typed.send(topic, null, value)`: no lock,
a failure blocks nothing.

## Config rules

- `outbox.relay.batchTimeout ≥ delivery.timeout.ms + 5 s + max.block.ms` (defaults 20 + 5 + 5 = 30 s).
- With Kafka, `outbox.relay.retry.maxDelay` < 10 min (default 5 min): a record kept for an unbatched resend is
  forgotten after 10 min ([config-reference.md](config-reference.md), «Oversized records»).
- Metrics need a Micrometer `MeterRegistry`: Kora's `MetricsModule`, only with an HTTP server module (`/metrics` on
  `httpServer.privateApiHttpPort`, 8085); else say outbox metrics are off.

## Before deploy

End your answer with a «Before deploy» list covering every clause below, each marked done, not applicable (why) or
for ops:

1. **Versions**: Postgres 14+, kafka-clients 3.9.2 or 4.x, Kora 1.2.23+ with the service's `kora-parent` and
   processors aligned to it (Quick reference).
2. **DB role and schema**, one of two models ([config-reference.md](config-reference.md), `outbox.schema`):
   - *one role owns everything* (default, `outbox.schema.bootstrap = true`): `CREATE` on the schema; owner of the
     tables or member of the owner role; `SELECT` on `<table>_id_seq`; `DO` blocks in PL/pgSQL allowed (the tail swap
     is one; PUBLIC has it by default);
   - *migrations own the base tables* (`bootstrap = false`): the output of `OutboxSql.outbox(config, "<app role>")`
     (and `OutboxSql.inbox`) pasted into a Flyway or Liquibase migration, run with the app's `search_path` or a
     schema-qualified `outbox.table`; the app role DML-only plus `USAGE` on the schema; a second `JdbcDatabase` tagged
     `@Tag(OutboxOwner.class)` on the same database as the owner (owns the tables, `CREATE` on the schema, `DO`
     allowed).

   Either way: `pg_read_all_stats` recommended for the role that runs maintenance; no role `statement_timeout` of
   seconds.
3. **Connections**: direct or session-mode pooler, never transaction pooling, the owner database included.
   `db.maxPoolSize` ≥ the service's own peak + `relay.workers` + 3 (+ 1 while `drain()` runs). Leave `outbox.shards`
   unset (256 for a new outbox).
4. **Table**: `outbox.table = "<schema>.outbox"` in the service's own schema, the same on every instance. Schema
   unknown: leave it unqualified and ask; never guess `public`.
5. **Bulk**: `sendAll` with thousands of distinct keys needs `max_locks_per_transaction × (max_connections +
   max_prepared_transactions)` ≥ concurrent bulk transactions × distinct keys each + other locks (SQLState 53200);
   state the numbers. It needs a restart; hot standbys need at least the primary's value.
6. **Topics** the Kafka handler writes (other `byTopic` routes need none), and DLQ topics with
   `outbox.kafka.deadLetterTopic`: created before writers deploy, broker
   auto-create off, ACLs (WRITE, DESCRIBE) for the service principal, final partition count (added partitions move
   keys), the producer's `batch.size` ≤ the topic's `max.message.bytes` (the default 16 384 is; else kafka-clients
   ≤ 4.1 stall a batch holding an oversized record until `delivery.timeout.ms`). One line per topic:
   `kafka-topics.sh --bootstrap-server "$KAFKA" --create --topic <topic> --partitions <N> --replication-factor 3 --config min.insync.replicas=2 --config unclean.leader.election.enable=false`
7. **Relay** enabled on at least one instance: maintenance and the dead-letter pass run only there.
8. **Alerts**: `outbox.shard.lag` growing, `outbox.dead.size > 0`, `outbox.maintenance.since_success > 5 ×
   maintenanceInterval`, `outbox.maintenance.transaction_pooling = 1`, `outbox.partitions.premade < 2`,
   `overflow = 1`, `xids_left` not NaN, `orphaned > 0`; with a dead-letter hook, `outbox.dead.pending.oldest`. Dead
   letters: admin API, never SQL.
