---
name: using-outbox
description: Use when adding dsudomoin/outbox (io.github.dsudomoin:outbox-kora, outbox-kafka-kora, outbox-admin-kora) to a Kora 1.x service, sending events with Outbox, TypedOutbox, ProtobufOutbox or KafkaOutboxPublisher, changing an event's shape or contract version (@OutboxContract, the two-release protocol, OutboxIncompatibleContractException), adding, removing or moving an OutboxTopicHandler route's topic (the outbox-local marker, OutboxMissingRouteException), editing outbox.* or outbox.kafka.driverProperties, or preparing the Postgres role, the schema migration (outbox.schema.bootstrap, OutboxSql, OutboxOwner) and Kafka topics for it. Also «аутбокс», «outbox», «transactional outbox», «отправить событие в Kafka в одной транзакции с БД».
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
fixed order (one keyed message cannot deadlock). Build typed messages with `TypedOutbox.message(...)`, which
serializes and stamps the contract version exactly as `send` would; never hand-serialize into `OutboxMessage.of`, which
drops the version:

```java
outbox.sendAll(List.of(
        orderEvents.message("orders", order.userId(), created),       // TypedOutbox<OrderCreated>
        paymentEvents.message("payments", order.userId(), payment))); // TypedOutbox<PaymentTaken>
```

Key a message only by its own ordering unit (userId: order holds per (topic, key) in commit order); an event that
needs no order (audit) stays unkeyed: `OutboxMessage.of(topic, payload)` or `typed.send(topic, null, value)`: no lock,
a failure blocks nothing.

## Contract versions and the release protocol

Each release must read everything the next one writes, so rolling back to it is safe for availability, not only for
data (a record no running release reads waits; it is never lost or dead-lettered by itself).

- `@OutboxContract(n)` on an event class is its payload version (none = 1). `TypedOutbox<T>` (Kora injects it; by
  hand `new TypedOutbox<>(outbox, serializer, OrderCreated.class)`) and `KafkaOutboxPublisher` (last argument the
  value class) stamp it in the `outbox-contract` header; `ProtobufOutbox` and `Outbox.send` send headers as given.
- Additive change: a new field nullable or with a default, old readers skipping unknown fields; no version bump.
- **Breaking change** (a field the handler needs, a changed meaning): mandatory two releases.
  1. Release N writes v1 with the old class and reads v1 and v2 with the new one:

     ```java
     @Json public record OrderCreatedV1(long id, String userId) {}                       // written by N, version 1
     @OutboxContract(2) @Json public record OrderCreated(long id, String userId, @Nullable String currency) {}

     TypedOutbox<OrderCreatedV1> events;                                                 // N still writes v1
     final class OrderCreatedHandler extends TypedOutboxTopicHandler<OrderCreated> {     // N reads v1 and v2
         protected void handle(OrderCreated event, OutboxRecord record) {
             var currency = record.contractVersion() == 1 ? "RUB" : event.currency();
             // ...
         }
     }
     ```

  2. Release N+1 writes `TypedOutbox<OrderCreated>` (v2) and drops `OrderCreatedV1`; keep the v1 branch while v1
     records may still arrive (outbox, retry table, dead letters to redrive).
- A field is removed in two releases (stop requiring it when reading, then stop writing it). One event contract per
  topic.
- The writer guard enforces it: a send of version v > 1 to a topic some live relay instance reads below v throws
  `OutboxIncompatibleContractException` before writing (topic, type, versions, instance ids), counted in
  `outbox.messages.contract_rejected`; deploy the reading release everywhere first. Limits: reports are one heartbeat
  (30 s) stale, a crashed old instance blocks up to about 90 s, an old instance started after the check still gets v2
  (it waits), plain handlers and Kafka-bound topics are not version-checked, no live relay instance (on a fresh read)
  means nothing is refused.
- A writer whose heartbeat has not read the reports for over 3 × `maintenanceInterval` decides both the version and
  the route check by its last read: refused if there was none, if it found no live relay instance, or if a live relay
  instance in it had no report (`contracts` or `routes` null) or an incompatible one; otherwise let through, counted in
  `outbox.messages.route_unchecked`. Such a refusal names no instance.

**Routes and their releases.** A record of an `OutboxTopicHandler` route's topic must never reach Kafka on an instance
that lacks the route (an older release mid-rollout, or after a rollback). Kafka-bound topics are unaffected: no marker,
no route check.

- Every send to a topic a route of this instance claims, typed, raw or through `sendAll`, carries the header
  `outbox-local` with a null value, stamped by the library. `outbox-local` is reserved like `outbox-contract`: never
  set it; a caller's header of that name is stripped, whatever its value. Route handlers see it in
  `record.headers()`; Kafka, dead-letter topics included, never gets it.
- A relay instance without the route makes such a record wait (`no_route`, no attempt spent, its key held) instead of
  handing it to the handler behind its routes; unmarked records still go there, batched.
- The route guard: a send to a topic routed here throws `OutboxMissingRouteException` before writing (`topic()`,
  `instances()`) while a live relay instance does not report the topic in its `routes`, whatever the version (v1
  included), counted in `outbox.messages.route_rejected`. A v1 send to a topic no route here claims (raw bytes
  included) is never checked.
- **Add a route in two steps**: ship the handler to every instance and wait until admin status shows the topic in
  `routes` of every live relay instance; then turn writing on with a feature flag (or the next release). Writing
  earlier throws `OutboxMissingRouteException`: that is the expected behaviour, there is no warn-only switch.

  ```java
  db.inTx(() -> {
      orders.insert(order);
      if (flags.auditEvents()) { // on once every live relay instance reports "audit" in its routes
          auditEvents.send("audit", order.userId(), new AuditEvent(order.id())); // TypedOutbox<AuditEvent>
      }
  });
  ```

- **Remove a route in two releases**: stop writing its topic, then drop the route once its records are delivered.
  Both in one release: the leftovers wait `no_route`; return the route or abandon them (operating-outbox).
- **Move a topic between the fallback and a route** in two releases, through a new topic. The fallback is the handler
  behind the routes: Kafka, or the app's own `OutboxHandler`, a `byTopic(...)` inside it included. Release N adds the
  new topic on the receiving side (a route, a `byTopic` entry or a Kafka topic) while writers keep the old one; once N
  runs everywhere (a new route shows in every `routes`), a flag switches writers to the new topic; N+1 removes the old
  topic once its records are delivered. In one release the side with the route is refused on every write while the
  other release runs, and after a rollback its marked records wait even where the older `byTopic` would deliver them.
- Rolling back past the release that added a route loses nothing: the marked records wait; roll forward or abandon
  them. Rolling back to a release built with a library version that lacks `outbox-local` (5fc60dd) is unsupported once
  route records exist: it hands them to its handler (Kafka). The guard protects against release mistakes, not every
  rollback.
- Every instance that writes a route's topic, relay on or off, registers that route: an unmarked write to a topic
  some live relay instance routes logs the WARN «... wrote to topic ... unmarked ...».
- An app without an `OutboxHandler` behind its routes (no `KafkaOutboxModule`) refuses a send to a topic no route
  claims with `IllegalArgumentException` (a typo fails in the transaction).
- README «Версии контрактов и откат» has the rollback table and every limit, the route rules included.

## Config rules

- `outbox.relay.batchTimeout ≥ delivery.timeout.ms + 5 s + max.block.ms` (defaults 20 + 5 + 5 = 30 s).
- `outbox.relay.waiting { delay = 1s, maxDelay = 5m, alertAfter = 15m }`: how often a record that waits for another
  release is handed again (a quarter of its wait, within `delay`..`maxDelay`) and when its WARN becomes an ERROR
  ([config-reference.md](config-reference.md)). Defaults fit; alert on `outbox.waiting.oldest > alertAfter`.
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
     allowed). The app role must not have `CREATE` on any schema of the owner's `search_path` (PUBLIC has it on
     `public` on PG 14 and on databases moved from it): what it creates there runs as the owner. Startup WARNs with
     the `REVOKE`.

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
6. **Topics** the Kafka handler writes (topics an `OutboxTopicHandler` claims need none, but writing to a new route's
   topic starts only once every live relay instance reports the route, «Routes and their releases» above), and DLQ
   topics with `outbox.kafka.deadLetterTopic`: created before writers deploy, broker auto-create off, ACLs (WRITE,
   DESCRIBE) for the service principal, final partition count (added partitions move keys), the producer's `batch.size`
   ≤ the topic's `max.message.bytes` (the default 16 384 is; else kafka-clients ≤ 4.1 stall a batch holding an oversized
   record until `delivery.timeout.ms`). One line per topic:
   `kafka-topics.sh --bootstrap-server "$KAFKA" --create --topic <topic> --partitions <N> --replication-factor 3 --config min.insync.replicas=2 --config unclean.leader.election.enable=false`
7. **Relay** enabled on at least one instance: maintenance and the dead-letter pass run only there.
8. **Alerts**: `outbox.shard.lag` growing, `outbox.dead.size > 0`, `outbox.maintenance.since_success > 5 ×
   maintenanceInterval`, `outbox.maintenance.transaction_pooling = 1`, `outbox.partitions.premade < 2`, `overflow = 1`,
   `xids_left` not NaN, `orphaned > 0`, `outbox.waiting.oldest > 900` (`relay.waiting.alertAfter`),
   `outbox.messages.contract_rejected` or `outbox.messages.route_rejected` growing; with a dead-letter hook,
   `outbox.dead.pending.oldest`. Dead letters: admin API, never SQL.
