---
name: consuming-with-outbox-inbox
description: Use when a Kora 1.x Kafka consumer reads messages published through dsudomoin/outbox and must apply each one once, or when touching Inbox, tryAccept / tryAcceptAll, inbox.scope, KafkaOutboxHeaders.messageId, the outbox-message-id header, outbox.inbox.* settings, duplicates after redelivery or a replay. Also «inbox», «дедупликация», «идемпотентный консьюмер», «повторы сообщений из Kafka».
---

# Consuming with the outbox inbox

## Overview

Delivery is at-least-once. `Inbox` records message ids in the consumer's own transaction, so "accepted" and
"applied" commit or roll back together. The dedup id is `KafkaOutboxHeaders.messageId(record)`.

## Core pattern: one poll, one transaction, one accept call

```java
@KafkaListener("kafka.consumer.orders")
public void process(ConsumerRecords<String, byte[]> records) {
    db.inTx(() -> {
        var ids = new ArrayList<UUID>();
        for (var record : records) {
            var id = KafkaOutboxHeaders.messageId(record);
            if (id != null) {
                ids.add(id);
            }
        }
        var fresh = new HashSet<>(inbox.tryAcceptAll(ids)); // one statement per poll
        for (var record : records) {
            var id = KafkaOutboxHeaders.messageId(record);
            if (id == null || fresh.remove(id)) { // no header: apply without dedup; repeated id: once
                apply(record);                      // your writes, same transaction
            }
        }
    });
}
```

| Rule | Why |
|---|---|
| Batch listener, one `tryAcceptAll` per `inTx` | Several accept calls in one transaction can deadlock with a concurrent consumer (40P01). Per-record `tryAccept` was ~8× slower in the benchmark; one `inTx` + one `tryAccept` per record is correct, only slow. |
| Accept and writes in one `inTx`, exceptions propagate | A rollback records no id, so the redelivered poll is applied. |
| `inbox.scope("orders-projection")` and its own `group.id` per independent consumer | A shared scope shares ids: the second consumer skips what the first accepted. 1–64 chars of `A-Z a-z 0-9 _ . : -`; never rename it. |
| The consumer also sends outbox messages | All keyed sends of the transaction in one `sendAll` (using-outbox). |

## Missing header: apply without dedup, never throw

`messageId(record)` is `null` when the header is absent or not a UUID: another producer, a replay tool, a hand-made
record. Apply it without dedup; log or count it if you want a trail. This is the library's own rule: `tryAccept`
returns `true` for an id that is not UUIDv7 or is outside the window, and never fails the transaction.

Throwing makes the record a poison pill: Kora backs off and re-polls from the committed offset, the record fails
again, and the partition stops for this consumer group.

| Rationalization | Reality |
|---|---|
| "No silent processing without dedup" | The library accepts undeduplicable ids itself. |
| "Throw, it will be retried" | A retry never adds the header; every later record of the partition waits. |

## Dedup window

Ids younger than `outbox.inbox.retention − partitionInterval` (24 h − 1 h = 23 h) are always deduplicated; older ones
are accepted again (WARN, counter `outbox.inbox.outside_window`). Set `retention` above the largest consumer lag or
replay to cover; change it and `partitionInterval` on all instances at once.

## Wiring and database

Consume-only service: `InboxModule` alone, no `OutboxModule`, no handler. The library creates the inbox table: the
DB role needs `CREATE` and ownership, a direct or session-mode connection (no transaction pooling), and the same
`outbox.inbox.table` on every instance.
