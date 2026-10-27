# kora-outbox admin API

What needs a human decision is one call, not hand SQL (README «Админ-API и HTTP»). Any instance answers the same:
they all work on the shared database.

## In code: `OutboxAdmin`

A component of `OutboxModule`. Each call runs its own transactions; inside `inTx` it throws `IllegalStateException`.

| Method | Does |
|---|---|
| `status()` | Relay halt, lag, retry and parked sizes, dead letters by topic and status, orphans, tail overflow, `premade`, `xidsLeft`, seconds since maintenance, shards, and `instances`: every heartbeat row, expired ones included (id, shard count, `lastSeen`, whether its dead-letter hook is active). A `lastSeen` older than 3 × `maintenanceInterval` means the instance is not running; a crashed instance's row stays about an hour after its heartbeat expires, 3 × `maintenanceInterval` + 1 h after its last beat. |
| `findDeadLetters(filter, afterId, limit)`, `countDeadLetters(filter)` | Dead letters by `DeadLetterFilter` (topic, key, error substring, `failedFrom` inclusive, `failedTo` exclusive, status), id order, up to 1 000 per page. |
| `redriveDeadLetters(ids)` / `(filter)` | Back to the outbox as the hook's `redrive`: a new message after the later messages of its key, same message id. Returns the count. |
| `discardDeadLetters(ids)` / `(filter)` | Delete; returns the count. |
| `restoreOrphan(table, fromXid)` | Rows at `fromXid` and above back to the outbox, the rest dropped with the table. |
| `recoverXidRegression()` | The xid regression recovery now, in any mode and for any `maxRows`. |

- Bulk calls take letters dead when the call starts, in chunks of 1 000 by id, one transaction per chunk. Letters
  held by a running hook call, and (redrive) letters whose key an open transaction sends, are skipped without
  waiting and not counted: call again, then check the count with the same filter. An empty filter is rejected.
- Every changing call logs one INFO line with its arguments and result.

## Over HTTP: `kora-outbox-admin`

`OutboxAdminHttpModule` serves the same calls on the service's public HTTP server under `/kora-outbox/admin/v1`
(OpenAPI 3 contract `openapi/kora-outbox-admin.yaml` in the jar). It needs Kora's HTTP server, a JSON module and
exactly one `OutboxAdminAuthorizer` component: without one the service does not build. Deny with
`OutboxAdminAuthorizer.unauthorized` (401) or `forbidden` (403), never by throwing (any exception is a 500
«authorization failed»). Expose it only on an internal network or behind TLS: answers carry payloads. The
`X-Api-Key` header below is the README's example authorizer; yours may differ.

```bash
API=http://app:8080/kora-outbox/admin/v1
KEY="X-Api-Key: $OUTBOX_ADMIN_KEY"
JSON="Content-Type: application/json"

curl -s -H "$KEY" $API/status                                                  # first thing in the morning
curl -s -H "$KEY" "$API/dead-letters?topic=orders&status=kept&limit=100"       # next page: &afterId=<nextAfterId>
curl -s -H "$KEY" "$API/dead-letters/count?topic=orders&error=timeout"
curl -s -H "$KEY" -H "$JSON" -d '{"ids":[1001,1002]}' $API/dead-letters/redrive
curl -s -H "$KEY" -H "$JSON" -d '{"filter":{"topic":"orders","error":"timeout"}}' $API/dead-letters/redrive
curl -s -H "$KEY" -H "$JSON" -d '{"ids":[1003]}' $API/dead-letters/discard
curl -s -X POST -H "$KEY" "$API/orphans/outbox_x83886080/restore?fromXid=83900000"
curl -s -X POST -H "$KEY" $API/recovery/xid-regression
```

- 200 with JSON (changing calls: `{"count": N}`); 400 rejected input (unknown field, over 1 000 `ids`, empty filter,
  not an orphan); 401 / 403 from the authorizer; 409 not now (maintenance running: retry; no xid regression); 500
  otherwise. Every error body is `{"message": "..."}`.
- A body holds exactly one of `ids` (up to 1 000) and `filter`. `status` is `pending`, `kept` or `scheduled`. A filter
  key is UTF-8 text: for a binary key use `ids` or `OutboxAdmin`.
- Times come back in milliseconds while Postgres keeps microseconds: a `failedAt` copied into `failedFrom`/`failedTo`
  can miss its own letter. Use `ids` for an exact set.
- Adding the contract to `openapi.management.file` moves the service's own spec from `/openapi` to
  `/openapi/<file name>`: tell whoever downloads it.

## Procedures

- **Dead letters** («Dead letters»): `/status`, then list, redrive or discard. A redrive keeps the message id, so
  inbox consumers deduplicate it within `retention − partitionInterval` (23 h by default).
- **Orphans** («Осиротевшие партиции»): compare the orphan's rows with what consumers got (the README's `select`),
  then one `restore` call with the cut-off `fromXid` (`0` returns all).
- **xid halt** («Восстановление после регрессии xid (xid regression recovery)»): one `POST /recovery/xid-regression`;
  writers wait about 2.5–3 s per million rows re-sent. On a large outbox, or after the ERROR «failed after it locked
  the outbox», first raise `outbox.recovery.timeout` on the calling instance and restart it. Never delete the
  `<table>_halt` row.
- **Stranded shards** («Застрявшие шарды»): no admin call. Stop all instances and run the README's single
  transaction exactly as printed, shard numbers from the ERROR, on a copy first; consumers get the shards' rows again.
- **After a rollback window** («Rolling-деплои», «Обновление с предыдущего релиза (5fc60dd), смешанный парк и
  откат»): the previous release writes dead letters `kept` and the hook never sees them:
  `curl -s -H "$KEY" "$API/dead-letters?status=kept&failedFrom=<start>&failedTo=<end>"`.
