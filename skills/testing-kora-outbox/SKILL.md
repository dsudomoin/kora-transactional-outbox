---
name: testing-kora-outbox
description: Use when writing or fixing tests of a Kora 1.x service that uses kora-transactional-outbox or its inbox, such as a @KoraAppTest with Testcontainers Postgres or Kafka that must prove an event was delivered, OutboxRelay.drain(), or existing tests that log Kafka connection errors, hang or slow down after the outbox was added. Also «тест outbox», «интеграционный тест аутбокса», «проверить доставку события».
---

# Testing a service that uses kora-outbox

## Every test that boots the graph

If the test does not assert delivery, its config contains `outbox.relay.enabled = false`. Otherwise each such test
starts relay workers and Kafka producers against a broker that is not there. Keep
`outbox.kafka.driverProperties."bootstrap.servers"` set (it is validated at startup; with the relay off nothing
connects). Add `outbox.partitions { xidsPerPartition = 1000000, premake = 4 }`: bootstrap then creates 6 partitions
instead of 66.

## Delivery test: required parts

1. Postgres 16+ Testcontainer (no H2). Bulk tests with many distinct keys:
   `.withCommand("postgres", "-c", "max_locks_per_transaction=256")`.
2. Kafka Testcontainer with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, and the topic created with `Admin` **before**
   anything is sent. Auto-create hides a missing deploy step, and a failed send waits in `<table>_retry` for its
   backoff, which `drain()` does not wait for.
3. Config: `relay.enabled = false`, the fast partition settings, `bootstrap.servers` from the container, a unique
   `outbox.table` per test class when classes share one database.
4. Act through the real service method, then `int delivered = relay.drain();` (`@TestComponent OutboxRelay relay`).
   It makes a tick and processes every shard up to it. With the relay left on, poll for the effect with a deadline;
   never sleep and then assert.
5. Assert on the target, not on outbox rows: the Kafka record's value, key, and
   `KafkaOutboxHeaders.messageId(record)` equal to the id `send` returned, or the HTTP stub's body and
   `Idempotency-Key` equal to that id.
6. Assert `select count(*) from <table>_retry` = 0 (and `<table>_dead` = 0 unless the test expects a dead letter).

```java
@KoraAppTest(Application.class)
class OrderDeliveryTest implements KoraAppTestConfigModifier {
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    static { PG.start(); KAFKA.start(); createTopic("orders"); } // Admin.createTopics(...).all().get()

    @TestComponent OrderService orders;
    @TestComponent OutboxRelay relay;

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db { jdbcUrl = "%s", username = "%s", password = "%s", poolName = "test" }
                outbox {
                  relay.enabled = false
                  partitions { xidsPerPartition = 1000000, premake = 4 }
                  kafka.driverProperties { "bootstrap.servers" = "%s" }
                }
                """.formatted(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), KAFKA.getBootstrapServers()));
    }

    @Test
    void orderEventReachesKafka() {
        var messageId = orders.createOrder("u1", 100); // the id send returned, if the service exposes it
        assertThat(relay.drain()).isEqualTo(1);
        var record = consumeOne("orders"); // new group, earliest, poll with a deadline
        assertThat(record.key()).isEqualTo("u1");
        assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(messageId); // else: isNotNull()
        assertThat(count("select count(*) from outbox_retry")).isZero();
    }
}
```

Testcontainers 2.x packages: `org.testcontainers.postgresql`, `org.testcontainers.kafka`.

## Other cases

- **Custom handler without Kafka**: a test `@KoraApp` whose `OutboxHandler` factory returns a handler that records
  what it got. The library's own test fixtures are not published; write the ten lines.
- **Retry path**: `drain()` throws `IllegalStateException` after 2 × `batchTimeout` without progress (a stub
  answering 500). Lower `batchTimeout` only with a non-Kafka handler; with Kafka it must stay ≥
  `delivery.timeout.ms + 5 s + max.block.ms`.
- **Unit tests**: `Outbox` and `Inbox` are interfaces; mock them. `new TypedOutbox<>(outbox, serializer)` wraps a mock.
- **Ambiguous `inTx`**: an expression lambda needs a cast, `db.inTx((SqlFunction0<UUID>) () -> outbox.send(m))`.
