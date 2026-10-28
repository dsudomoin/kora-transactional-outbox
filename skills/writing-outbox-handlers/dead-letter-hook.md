# The dead-letter hook

A record moves to `<table>_dead` when it fails permanently (`OutboxPermanentException`, `Outcome.permanent`, Kafka's
`RecordTooLargeException` and the like), runs out of `relay.retry.maxAttempts`, or is requeued past
`relay.requeue.maxRequeues`. Its key goes on. Without a hook the letter is written `kept` and waits for a human, who
works through the admin API (operating-outbox). With an `OutboxDeadLetterHandler` component in the graph it is
written `pending` and the hook decides.

## When to write one

Write a hook when a rule decides better than a person reading `/status` in the morning: send an alert, redrive once
the target has had time to recover, discard what is known to be garbage, or copy letters to your own store. When the
only wish is «dead letters to a Kafka topic», set `outbox.kafka.deadLetterTopic` instead (below). Otherwise write
none: kept letters plus the `outbox.dead.size` alert are the default.

```java
public interface OutboxDeadLetterHandler {
    List<DeadLetterDecision> handle(List<DeadLetter> letters) throws Exception; // one decision per letter, same order
    default boolean active() { return true; }
}
```

`DeadLetter` carries the dead row: `id`, `messageId`, `topic`, `key`, `payload`, `headers`, `createdAt`, `attempts`,
`failedAt`, `error`, `status`, `redrives`, `handlerAttempts`, `dueAt`. Up to `outbox.deadLetters.batchSize` (100)
letters per call.

| Decision | Effect |
|---|---|
| `DeadLetterDecision.keep()` | Stays for humans (`kept`); the hook never gets it again. |
| `discard()` | Deleted. |
| `redrive()` | Right after the decision commits, back to the outbox as a new message after the later messages of its key: same message id, topic, key, payload and headers, `attempts` from zero, shard computed for the current `outbox.shards`. While a transaction that sends its key is open, it waits `scheduled` and tries again after `relay.retry.initialDelay`. |
| `redriveAt(at)` | Waits `scheduled` and redrives at `at`, without another hook call; `at` is clamped to [now, now + 7 days]. |

A redrive breaks per-key order: the letter is delivered after the key's later records. Inbox consumers deduplicate
it by message id within their window.

## The rules

- **A call is all or nothing.** A call that throws (an `Error` too), returns an invalid list (null, wrong size, a
  null element) or runs past `outbox.deadLetters.timeout` (30 s; it is interrupted and abandoned) leaves every one of
  its letters `pending`, decided letters included. Return a decision for each letter, or throw for all of them.
- **At least once.** A failed call is repeated after a backoff (`relay.retry.initialDelay` doubling to `maxDelay`)
  with `handlerAttempts` one higher; a crash between the call and the commit repeats it too, and a shutdown
  interrupts it and hands the letters to another instance or the next start. So an alert sent from the hook may go
  twice: make every side effect idempotent by `letter.messageId()`.
- **Honour the interrupt.** Use interruptible calls or client timeouts shorter than `deadLetters.timeout`. The hook
  has one thread: a call that ignores the interrupt holds it past the timeout, and the instance decides no letter
  until that call returns (one ERROR «hook ignores interrupts»).
- **Stop redrive loops with `redrives`.** `DeadLetter.redrives()` counts how often the message was redriven before
  this death, by the hook and by admin calls, remembered in `<table>_redriven` for `outbox.deadLetters.redriveHistory`
  (30 d). A hook that always redrives a record that always fails loops forever; bound it:

  ```java
  @Override
  public List<DeadLetterDecision> handle(List<DeadLetter> letters) throws Exception {
      var decisions = new ArrayList<DeadLetterDecision>(letters.size());
      for (var letter : letters) {
          if (letter.redrives() < 3) { // the target may be back: once more in 10 minutes
              decisions.add(DeadLetterDecision.redriveAt(Instant.now().plus(Duration.ofMinutes(10))));
          } else {
              alerts.send(letter.messageId(), letter.topic(), letter.error()); // idempotent by messageId; a throw repeats the call
              decisions.add(DeadLetterDecision.keep());
          }
      }
      return decisions;
  }
  ```

- **`active()`.** Default `true`. `false` means «would keep everything»: letters are written `kept` at once and the
  hook is never called. It is read once at startup, so switch a hook off by config through `active()`, not inside
  `handle`. `OutboxDeadLetterHandler.NONE` is that without a class of its own (`active()` returns `false`).
- **Isolation.** The hook runs on the thread `outbox-dead-letter-hook`, outside every relay thread and transaction:
  a slow or failing hook never delays delivery. The pass thread `outbox-dead-letters` holds one connection for its
  whole run, hook call included (the `+ 3` of the pool budget). Letters are locked `for update skip locked`, so two
  instances never hand one letter to the hook at once; order between letters is not guaranteed. The pass runs only
  where the relay runs; with the relay off, only the pass at the end of `drain()` calls the hook.
- **Wiring.** One component of type `OutboxDeadLetterHandler` (or a subtype) per graph. With `KafkaOutboxModule` it
  replaces the Kafka hook, `deadLetterTopic` set or not. Kora 1.x puts a `@Component` class into every `@KoraApp` of
  its source set: with several apps there (test apps), declare the hook as a factory in the app that needs it.
- **Mixed fleets.** Keep the hook the same on every instance. During a rolling deploy that adds it, instances without
  an active hook leave `pending` letters alone while an instance with an active hook heartbeats. The previous release
  (5fc60dd) writes its dead letters `kept`, and the hook never sees them, nor letters that were dead before the
  upgrade.

## Kafka DLQ instead of code

Off by default. With `outbox.kafka.deadLetterTopic = "{topic}.dlq"` (`{topic}` = the letter's topic) the Kafka
module's hook publishes each letter through the relay's producers:

- The record carries the letter's key, payload and headers, then `outbox-dead-topic` (original topic),
  `outbox-dead-error` (last error, no value when none), `outbox-dead-attempts` (failed deliveries, decimal),
  `outbox-dead-failed-at` (ISO-8601, UTC) and `outbox-message-id`.
- A letter Kafka acknowledges is discarded. Kept, with a WARN at most once a minute per kind: a DLQ topic name that
  comes out invalid (over 249 characters), a DLQ topic equal to the original, and letters Kafka refused for good (too
  large). Refusals are remembered by message id (the last 10 000) until restart: such a letter handed again is kept at
  once, without a send.
- **Duplicates.** Any other failure (no ack in time, topic on cooldown or missing) fails the whole call, so its
  letters come again: already acknowledged ones repeat in the DLQ topic once per failed round while a batch-mate keeps
  failing. A broker refusal after the append closes the producer: batch-mates still in flight are sent again in the
  same call and may land twice. DLQ consumers deduplicate by `outbox-message-id`.
- Create DLQ topics before deploy when the broker does not auto-create them; until then their letters wait
  `pending`. Keep `outbox.deadLetters.timeout` ≥ `delivery.timeout.ms + 5 s + max.block.ms` (30 s by default), else
  a WARN at startup: a cut call repeats acknowledged letters too.

## Alerts

`outbox.dead.pending.oldest` above X (> `relay.retry.maxDelay`) while `outbox.dead.hook.failures` grows: the hook
fails and its alerts do not go out. `outbox.dead.size` growing while `outbox.dead.pending` does not: kept letters
pile up for humans. `outbox.dead.decisions{decision}` counts committed decisions
([metrics.md](../operating-outbox/metrics.md)).
