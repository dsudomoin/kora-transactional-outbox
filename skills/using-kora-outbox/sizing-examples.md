# kora-outbox sizing examples

Five service profiles with a configuration to copy and adapt. Every number comes from a formula below or from a
measurement in the library's `bench/RESULTS.md` (section names quoted). Those measurements ran on Postgres 16 with 2 CPU
on Apple M5 Pro cores and a single Kafka broker with replication factor 1. Your hardware will differ, so re-check with
`outbox.shard.lag`, `outbox.delivery.delay` and database CPU after deploy. Schema names in the examples (`orders.`,
`billing.`, `imports.`) are placeholders: use the schema the service's own tables live in.

## How shards, workers and Kafka partitions relate

- **Shard** = `hash(topic, key) mod outbox.shards`. It decides which relay worker may deliver a message. Each shard is
  processed by at most one worker at a time across all instances (a transaction advisory lock per step), sequentially.
  A worker steps the shards that got rows since the last tick as a group, in one transaction, so many shards do not
  mean many steps.
- **Kafka partition** = Kafka's default partitioner over the key bytes. It decides consumer parallelism: at most one
  consumer thread per partition in a consumer group.
- **They are independent.** Shards need not match partitions. Per-key order holds with any combination: a key always
  maps to one shard (one worker at a time, commit order) and to one Kafka partition (while the partition count stays
  fixed). Shards control relay parallelism; partitions control consumer parallelism.
- **Relay parallelism = min(shards, instances × relay.workers).** With the default 256 shards that is
  instances × workers: workers are the knob. When an instance dies, the remaining workers pick up its shards on their
  next step.
- Each working worker holds one DB connection; each instance has `max(4, 2 × workers)` handler threads.
- **Shards are chosen once.** Leave `outbox.shards` unset: a new outbox gets 256, an existing one keeps its count, so
  an upgrade needs no setting. Changing the count later is the emergency reshard (stop writers, drain, stop all
  instances; [config-reference.md](config-reference.md)), never a routine change. Topic partitions can only be added,
  and adding them moves keys: choose them for the target load from the start.

## Formulas used below

| Quantity | Formula | Source |
|---|---|---|
| Pool per instance | `db.maxPoolSize` = own need + `relay.workers` + 3 (+1 while `drain()` runs); the 3 are outbox maintenance, inbox maintenance and the dead-letter thread | README «Размер пула соединений» |
| Postgres connections | Σ over services and instances of `maxPoolSize` + other clients + `superuser_reserved_connections` (3) ≤ `max_connections` (Postgres default 100) | Postgres |
| Advisory lock slots | `max_locks_per_transaction × (max_connections + max_prepared_transactions)` ≥ concurrent writer transactions × distinct keys each + other locks | README «max_locks_per_transaction для массовой отправки» |
| Kafka call budget | `batchTimeout ≥ delivery.timeout.ms + 5 s + max.block.ms` (defaults 20 + 5 + 5 = 30 s) | `KafkaOutboxSender` |
| Dead-letter hook budget | `deadLetters.timeout` ≥ the same sum with the Kafka DLQ | README «Kafka DLQ» |
| Handler call budget | `batchSize × p99(record) ≤ 0.8 × batchTimeout − per-record timeout` | `OutboxHandler`, `OutboxConfig` |
| Sequential handler throughput | ≤ min(shards, instances × workers) / mean seconds per record | Little's law; RESULTS «chaos: slow-handler» |
| Concurrent handler throughput | rows/s ≤ min(shards, instances × workers) × `batchSize` / (call time + step overhead), call time ≈ ceil(route records / P) × p99; check with one instance down, aim for ≥ 2× | One call per worker step; P = requests in flight per call |
| Requests in flight at a target | ≤ min(shards, instances × workers) × P | One call per running worker |
| Idle relay cost | ≈ `1 / idlePollInterval` write transactions per second per cluster plus one maintenance run per `maintenanceInterval`, at any shard count; idle shards write nothing but one anchor cursor row per `relay.cursorAnchorInterval` (1 h) | README «Стоимость простоя» |
| Time one xid range covers | `xidsPerPartition / cluster xid/s`; the outbox holds about 1–2 ranges of rows | README «Партиции» |
| Long snapshots | `premake / 2 × xidsPerPartition ≥ peak xid/s × longest snapshot s` | README «Партиции» |
| Inbox dedup window | `retention − partitionInterval` (default 23 h) | `Inbox` javadoc |

## Choosing the shard count

Rough sizing: **256 suits typical services.** Go lower only for tiny installs. More than 256 is rarely needed; 1024
works but costs bulk throughput at saturation. Many shards are cheap: idle shards write nothing.

Measured with library defaults (one instance, workers 4, `batchSize` 1000), RESULTS «Под нагрузкой после исправлений
ревью (fix round 1)» and «Простой после чекпоинтов»:

| shards | 1 000 msg/s, Kafka: p50 / p99, DB CPU (of 200 %) | bulk, noop handler: delivered msg/s, p50 | idle xid/s |
|---|---|---|---|
| 32 | 0.048–0.049 / 0.110 s, 26 % | 293–296k, 0.33–3.1 s | 5.1 |
| 256 | 0.053 / 0.110 s, 27–28 % | 288–289k, 0.40 s | |
| 1024 | 0.057 / 0.111 s, 31 % | 261k, 1.6 s | 5.1 |

- The previous release at 32 shards: 0.074 / 0.146 s and 68 % DB CPU at 1 000 msg/s, bulk 299–308k msg/s; 256 shards
  on this release are within the spread of its bulk runs.
- Relay steps per second stay about 40 at 1 000 msg/s for any shard count: one step per tick and group, not per
  shard.
- Bulk at 32 shards delivers as much but its queue grew in most windows (writers got faster, relay capacity did not),
  hence the wide p50. 1024 shards: about 10 % less bulk throughput and a 1.6 s p50 at saturation.

## 1. Small service

**Assumptions:** 10–100 msg/s, 2 instances, 1 message per transaction, topic `orders` with 6 partitions, the
service's own peak need 8 connections.

```hocon
db {
  maxPoolSize = 13              # own 8 + relay.workers 2 + 3
}
outbox {
  table = "orders.outbox"       # outbox.shards unset: 256 for a new outbox
  relay.workers = 2             # 2 instances × 2 = 4 workers, 2 with one instance down
  kafka.driverProperties {
    "bootstrap.servers" = ${KAFKA_BOOTSTRAP}
  }
}
```

```bash
kafka-topics.sh --bootstrap-server "$KAFKA" --create --topic orders --partitions 6 \
  --replication-factor 3 --config min.insync.replicas=2 --config unclean.leader.election.enable=false
```

**Postgres:** 2 × 13 = 26 connections. Each transaction holds 1 key lock, so the default `max_locks_per_transaction`
(64) is enough.

**Why these numbers:** the default shard count costs nothing extra here: idle cost is about 1 / 0.2 s = 5 write
transactions per second per cluster at any shard count (5.1 xid/s measured), and one instance with 4 workers carried
1 000 msg/s at 26–31 % DB CPU. Two workers per instance keep a stalled handler call from holding all delivery.
Producer timeouts stay at the defaults, so `batchTimeout` 30 s meets 20 + 5 + 5.

## 2. Typical service with an inbox consumer

**Assumptions:**

- 1–5k msg/s, 3 instances (6 at the top), 1–3 messages per transaction.
- Topic `orders` with 12 partitions, read by a consumer service of 3 instances × 4 threads.
- Writer's own need 20 connections; consumer's own need 4.

Writer service:

```hocon
db {
  maxPoolSize = 26              # own 20 + relay.workers 3 + 3
}
outbox {
  table = "orders.outbox"       # outbox.shards unset: 256
  relay.workers = 3             # 3 instances × 3 = 9 workers, 6 with one instance down
  kafka.driverProperties {
    "bootstrap.servers" = ${KAFKA_BOOTSTRAP}
    "linger.ms" = 5               # throughput settings from the bench stand
    "batch.size" = 262144
    "compression.type" = "lz4"
  }
}
```

Consumer service (`InboxModule` alone):

```hocon
db {
  maxPoolSize = 9               # 4 listener threads (one transaction per poll) + 1 inbox maintenance + own 4
}
outbox.inbox {
  table = "billing.inbox"       # defaults: partitionInterval 1h, premake 24h, retention 24h
}
kafka.consumer.orders {
  topics = ["orders"]
  threads = 4                   # 3 instances × 4 = 12 = partitions
  driverProperties {
    "bootstrap.servers" = ${KAFKA_BOOTSTRAP}
    "group.id" = "billing-orders"
    "max.poll.records" = 2000
    "allow.auto.create.topics" = false
  }
}
```

```bash
kafka-topics.sh --bootstrap-server "$KAFKA" --create --topic orders --partitions 12 \
  --replication-factor 3 --config min.insync.replicas=2 --config unclean.leader.election.enable=false
```

**Postgres:**

- Connections: 3 × 26 + 3 × 9 = 105. That is above the default `max_connections` (100) before any other client, so
  raise it (the bench used 300).
- Locks: ≤ 3 keys per transaction, so the default `max_locks_per_transaction` is enough. Send several keyed messages
  of one transaction with one `sendAll`.
- Inbox disk: rows ≈ consumed msg/s × retention. The bench soak held 9.0 GB of inbox after 1 h at ~22k msg/s, about
  114 B per id, so 5k msg/s × 24 h needs ~49 GB. Lower `retention` (≥ 2 × `partitionInterval`) if your largest
  consumer lag or replay allows.

**Why these numbers:**

- Relay parallelism min(256, 3 × 3) = 9. One instance with 4 workers delivered 1 000 msg/s with about 40 relay steps
  per second at 0.110 s p99, so 6 workers with one instance down leave room.
- 12 partitions = consumer threads. The tool's 5k msg/s ceiling in RESULTS «max» / «ramp» was the database commit
  rate (2 CPU), not the relay.
- `linger.ms` 5 + the default `request.timeout.ms` 15 000 ≤ `delivery.timeout.ms` 20 000, as Kafka requires.

## 3. Bulk / high throughput

**Assumptions:**

- 50–250k msg/s from `sendAll` of 1–10k distinct keys per transaction (imports, fan-out).
- 2 instances with 2 writer threads each; own need 6 connections.

```hocon
db {
  maxPoolSize = 15              # own 6 + 2 writers + relay.workers 4 + 3
}
outbox {
  table = "imports.outbox"      # outbox.shards unset: 256
  relay.workers = 4             # one instance can carry the load alone, as in the bench
  partitions {
    xidsPerPartition = 60000    # EXAMPLE for 200 xid/s measured: ≈ 5 min per range; see below
    premake = 128               # 64 × 60000 / 200 xid/s ≈ 5.3 h of snapshot cover
  }
  kafka.driverProperties {
    "bootstrap.servers" = ${KAFKA_BOOTSTRAP}
    "linger.ms" = 5
    "batch.size" = 262144
    "compression.type" = "lz4"
  }
}
```

```bash
kafka-topics.sh --bootstrap-server "$KAFKA" --create --topic imports --partitions 12 \
  --replication-factor 3 --config min.insync.replicas=2 --config unclean.leader.election.enable=false
```

**Postgres:**

- Locks: 2 instances × 2 writers × 10 000 keys = 40 000 advisory locks at peak. With `max_connections = 300`,
  `max_locks_per_transaction` ≥ 40 000 / 300 ≈ 134 plus other locks; the bench used 256 (76 800 slots). Changing it
  needs a restart, and a hot standby needs at least the primary's value.
- Connections: 2 × 15 = 30.

**Why these numbers:**

- 256 shards with relay defaults (`batchSize` 1000) delivered 288–289k msg/s at p50 0.40 s, p99 1.0–1.1 s and 196 %
  DB CPU (2 writers × 10 000 per transaction, payload 100 B, noop handler; RESULTS «Под нагрузкой после исправлений
  ревью (fix round 1)»). The Kafka handler adds producer work: re-check with `outbox.shard.lag`.
- A relay batch covers about `batchSize × shards` ids.
- Bulk burns few xids per message, so a 16 777 216-xid default range can hold hours of rows. Measure the cluster xid
  rate (`select pg_current_xact_id()` twice, a minute apart), set `xidsPerPartition ≈ xid/s × 300` (≥ 1000), then
  size `premake` with the snapshot formula.

## 4. Slow external target next to Kafka

**Assumptions:**

- 3 instances.
- An HTTP audit target with p99 300 ms (2 s in the worst variant) per record, routed with
  `OutboxHandler.byTopic(Map.of("audit", auditHandler), kafka.handler())`.
- Orders go to Kafka as in profile 2.

```hocon
db {
  maxPoolSize = 26              # own 20 + relay.workers 3 + 3
}
outbox {
  table = "orders.outbox"       # outbox.shards unset: 256
  relay {
    workers = 3
    batchSize = 75              # (0.8 × 30 s − 1 s) / 0.3 s = 76; with p99 2 s and a 5 s timeout: (24 − 5) / 2 → 9
    # retry.maxAttempts stays 0: orders need per-user order; the audit handler dead-letters its own records
  }
  kafka.driverProperties {
    "bootstrap.servers" = ${KAFKA_BOOTSTRAP}
  }
}
httpClient.audit {
  url = "https://audit.internal"
  requestTimeout = 1s           # per record, well below batchTimeout; 5s for the 2 s variant
}
```

**Postgres:** 3 × 26 = 78 connections. The Kafka topic is as in profile 2.

**Why these numbers:**

- The batch budget gives `batchSize` 75; it also caps Kafka records per call, and orders in a chunk wait behind the
  audit route of the same call.
- Sequential throughput is at most min(256, 3 × 3) / 0.3 s = 30 audit msg/s (≈ 4.5 msg/s at 2 s). RESULTS «chaos:
  slow-handler» shows the cap: 20 ms per record with 8 workers (≤ 400 msg/s) moved about 380 msg/s end to end
  (178 080 orders in 300 s of load plus 167 s of drain) against 600 offered, and relay lag reached 204 s.
- Above that rate, deliver concurrently inside `handleBatch`
  ([concurrent-delivery.md](../writing-kora-outbox-handlers/concurrent-delivery.md)), or publish audit to Kafka and call
  HTTP from a consumer.
- Concurrent variant for 2 000 orders/s with one audit event each (4 000 rows/s, about half of them audit): `workers`
  4, `batchSize` 400, P = 200 requests in flight per call, `db.maxPoolSize` = own 20 + 4 + 3 = 27. Estimates, not
  measured:
  - Calls running: min(256, 3 × 4) = 12, and 8 with one instance down (2 × 4).
  - A call carries ≈ 200 audit records, one round of P: ≈ 0.3 s plus ≈ 0.05 s for the database step and the Kafka
    route. With one instance down, 8 × 400 / 0.35 s ≈ 9 100 rows/s, ≈ 2.3× the 4 000 needed.
  - In flight at the audit service: ≈ 600 on average (2 000/s × 0.3 s), at most 12 × 200 = 2 400. Give its owner the
    cap: a retry pass after an outage hands up to 400 due heads per call, all audit, still limited to P.
  - Deadline: at worst ceil(400 / 200) × the 1 s request timeout = 2 s (plus connect time), far inside 24 s.
- `relay.retry.maxAttempts` is global. A record moved to `<table>_dead` lets the later records of its key go on, and
  a redrive delivers it after them, so with a Kafka route that needs per-key order keep `maxAttempts` = 0 and alert
  on `outbox.shard.lag`. Bound only the HTTP route, in its handler: return `Outcome.permanent(e)` instead of
  `Outcome.retry(e)` once `record.attempts() + 1 >= 20` (backoff 1 s doubling to 5 min: ≈ 59 min), and alert on
  `outbox.dead.size > 0` or decide them with a dead-letter hook
  ([dead-letter-hook.md](../writing-kora-outbox-handlers/dead-letter-hook.md)).

## 5. Tests and CI

**Assumptions:** one Postgres 16 Testcontainer and one Kafka container with a single broker.

```hocon
db {
  maxPoolSize = 7               # test code 2 + relay.workers 1 + 3 + 1 for drain()
}
outbox {
  table = "outbox_orders_it"    # unique per test class when classes share one database
  shards = 4                    # a tiny install: set once, at the table's creation
  relay {
    enabled = false             # deliver with OutboxRelay.drain(); also in every test that does not check delivery
    workers = 1
  }
  partitions { xidsPerPartition = 1000000, premake = 4 }
  kafka.driverProperties { "bootstrap.servers" = ${KAFKA_BOOTSTRAP} }
}
```

- Topic: `Admin.createTopics(List.of(new NewTopic("orders", 1, (short) 1)))` before sending, with broker auto-create
  off.
- Tests with many distinct keys per `sendAll`: `.withCommand("postgres", "-c", "max_locks_per_transaction=256")`.
- Tests that keep the relay on: `relay { workers = 2, minTickInterval = 5ms, idlePollInterval = 100ms }`, as the
  library's own Kafka wiring test.

**Why these numbers:**

- `xidsPerPartition` 1 000 000 with `premake` 4 builds 6 partitions instead of 66 at bootstrap.
- `shards` 4 keeps `drain()` short: every shard is stepped up to the tick (the library's own wiring tests use 4). A
  test table is new each run, so the count is never changed on an existing outbox.
- The pool follows the production formula. With the relay disabled, no workers, outbox maintenance or dead-letter
  thread run, so 7 is an upper bound, not a requirement.
