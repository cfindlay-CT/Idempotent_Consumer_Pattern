# Idempotent Consumer Pattern — Azure Functions (Isolated Worker)

A reference implementation of the **Idempotent Consumer** pattern for Azure Service Bus,
built on the isolated-worker .NET Functions model with Redis as the distributed
deduplication store. This is not a toy retry-wrapper — it is a concurrency-correct answer
to a problem that every at-least-once message system eventually forces on you.

## Architectural Overview

Azure Service Bus, like every broker built on at-least-once delivery, makes one guarantee:
your handler will see a message at least once. It does **not** guarantee exactly once.
Redeliveries happen routinely — a lock-renewal timeout during a slow downstream call, a
client retry on an ambiguous network timeout, a competing consumer claiming the message
when the function app scales out to multiple instances. Any of these can hand the same
`MessageId` to two execution contexts within milliseconds of each other.

The naive fix — "check the database for this ID, and if it's not there, insert it and
process" — works fine in a single-threaded demo and fails in production. A
read-then-write check is two separate operations, and the gap between them is a race
window. Under concurrent load, two consumers can both pass the "not found" check before
either has written its row, and both proceed to process the same business event. The bug
doesn't show up in testing because testing is rarely concurrent enough to land in that
window — it shows up at 2 a.m. during a traffic spike, which is exactly when you can least
afford it.

The Idempotent Consumer pattern closes that window by making the "have I seen this
already?" check and the "claim it" write a single atomic operation, backed by a store that
supports that atomicity natively. This repository uses Redis's `SET key value NX` for
that purpose: one round-trip, one outcome — either you now hold the claim, or you don't.

## Component Breakdown

**Tracking store — [`RedisCacheService`](services/RedisCacheService.cs)**
A single Redis key per `MessageId` (`idempotency:lock:{messageId}`) carries the entire
lifecycle of a message's processing state as its value: `processing` while a claim is
held, `completed` once work has finished. One key, one value, no separate "lock table" and
"processed table" to keep in sync — there is no second write that can fail and leave the
two stores disagreeing about a message's state.

**Lock acquisition strategy**
`AcquireLockAsync` uses `SET ... NX EX` — set if-not-exists, with an expiry — in a single
Redis command. This is the atomic primitive the entire pattern rests on: existence-check
and claim happen as one indivisible operation, so two concurrent consumers racing on the
same `MessageId` cannot both win. Exactly one `SET NX` succeeds; the loser is told,
unambiguously, "this is already being handled."

**Message processing flow**
1. Reject messages with no `MessageId` outright — there is no key to dedupe on, so the
   message is dead-lettered rather than looped forever.
2. Attempt to claim the lock. Failure to claim means a duplicate: complete the message
   without processing it (this is a *success* path, not an error).
3. On successful claim, process the message, then overwrite the same key with a
   `completed` marker and a longer retention TTL.
4. On a processing exception, explicitly release the lock and re-throw, letting Service
   Bus redeliver immediately rather than waiting out the full lock TTL.

## Failure Modes Handled

- **Distributed race conditions** — `SET NX` collapses the check-and-claim sequence into
  one atomic Redis command, eliminating the read-then-write gap that lets two concurrent
  consumers both believe they're first.
- **Lock TTL expiration on instance crash** — if the owning instance dies mid-processing
  without releasing or completing the lock, the lock's own TTL (default 5 minutes) expires
  it automatically. The message becomes reprocessable instead of being stuck behind a
  claim nobody will ever release.
- **Transient processing errors** — a failure inside the processing block releases the
  lock immediately and re-throws, so Service Bus can redeliver and retry right away instead
  of being blocked for the remainder of the lock TTL.
- **Poison / unidentifiable messages** — a missing `MessageId` is dead-lettered on receipt.
  There is no key to dedupe on, so there is no safe way to evaluate the message at all;
  letting it loop through retries would only burn through delivery attempts for nothing.
- **Late duplicate redelivery after success** — the completion record's retention TTL is
  deliberately longer than the lock's claim TTL, so it outlives the broker's redelivery
  window. A duplicate arriving after the original has already completed still finds the
  key occupied and is correctly recognized as a duplicate.
- **No gap between "done processing" and "marked done"** — completion reuses the same key
  rather than deleting the lock and writing a new key, so there is never a moment where the
  key is briefly absent and a redelivered duplicate could slip through.

## Stack

- .NET, Azure Functions Isolated Worker model
- Azure Service Bus trigger
- Redis (`StackExchange.Redis`) as the distributed lock / dedup store
- OpenTelemetry export to Azure Monitor (optional, enabled via `APPLICATIONINSIGHTS_CONNECTION_STRING`)
