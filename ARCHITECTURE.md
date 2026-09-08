# Architecture

LedgerMesh is three Spring Boot services that complete an order through a choreographed saga
over Kafka. The design goal is simple to state: killing any one service at any moment must not
change the outcome of any order. This document explains the mechanisms that make that true.

## Services and topics

```
             POST /orders                          GET /stock/{sku}
                  |                                      ^
                  v                                      | (HTTP, breaker + time limiter,
+-----------------------------+                          |  cache fallback)
|        order-service        |--------------------------+
|  orders, order_item         |
|  outbox_event, processed_event
+--------------+--------------+
   order.created |  ^ inventory.reserved / inventory.rejected
   order.cancelled|  ^ payment.completed / payment.failed
                  v  |
+-----------------------------+        inventory.reserved        +-----------------------------+
|      inventory-service      | ------------------------------> |       payment-service       |
|  stock_item (Postgres)      |                                 |  payment (deferred queue)   |
|  stock:{sku} (Redis, TTL)   |                                 |  synthetic processor        |
|  outbox_event, processed_event                                |  outbox_event, processed_event
+-----------------------------+                                 +-----------------------------+
```

| Topic | Producer | Consumers | Meaning |
|---|---|---|---|
| `order.created` | order | inventory | reserve stock for these lines |
| `inventory.reserved` | inventory | order, payment | stock is held; order moves to RESERVED, payment authorizes |
| `inventory.rejected` | inventory | order | out of stock or unknown sku; order is CANCELLED |
| `payment.completed` | payment | order | order is CONFIRMED |
| `payment.failed` | payment | order | order is CANCELLED and compensation is emitted |
| `order.cancelled` | order | inventory | release the reservation |
| `order.payment_requested` | order | payment | the payment deadline passed; answer with the outcome on file or attempt now |

Every topic has three partitions and every message is keyed by order id, so all events for one
order are processed in sequence within a topic.

## Transactional outbox

No service ever calls the Kafka producer from business code. A state change and the event that
announces it are written to the service's own database in one transaction: the business rows and
an `outbox_event` row (`OutboxWriter`, propagation `MANDATORY`). A relay (`OutboxRelay`) polls
unpublished rows in id order every 200 ms, sends each one with `acks=all` and an idempotent
producer, waits for the broker acknowledgement and only then stamps `published_at`.

Failure cases:

* crash after commit, before the relay ran: the row is still unpublished and is sent on restart
* crash after the send, before the stamp: the row is sent again; consumers ignore the duplicate
* broker unavailable: the relay stops at the first failure to keep order and retries next tick

Nothing is ever lost, at the cost of at-least-once delivery, which the next section absorbs.

## Idempotent consumers

Each event carries a globally unique `eventId`. Consumers run their work through
`IdempotentConsumer.once(consumer, eventId, work)`, which inside a single transaction checks a
`processed_event` marker, runs the work, and inserts the marker. Offsets are committed per record
after the listener returns (`ack-mode: record`, `enable.auto.commit=false`).

* process killed before the transaction commits: nothing was written, the offset was not
  committed, the record is redelivered and processed from scratch
* process killed after commit but before the offset commit: the record is redelivered, the marker
  exists, the work is skipped and the offset is committed

The order state machine adds a second layer: a transition is only valid from specific states and
terminal orders never move, so even a duplicate that slips through by a different event id (for
example a re-run reservation) cannot change an outcome.

## Saga and compensation

`OrderStateMachine` is a pure transition table:

```
PENDING  + INVENTORY_RESERVED  -> RESERVED
PENDING  + INVENTORY_REJECTED  -> CANCELLED (OUT_OF_STOCK)
PENDING  + RESERVATION_TIMEOUT -> CANCELLED (RESERVATION_TIMEOUT), emit order.cancelled
RESERVED + PAYMENT_COMPLETED   -> CONFIRMED
RESERVED + PAYMENT_FAILED      -> CANCELLED (PAYMENT_DECLINED), emit order.cancelled
RESERVED + PAYMENT_TIMEOUT     -> CANCELLED (PAYMENT_TIMEOUT), emit order.cancelled
```

Payment outcomes are also accepted from PENDING because the two inbound topics are independent
and a payment can only exist for a reserved order. When a payment is declined the order service
writes `order.cancelled` to its outbox in the same transaction as the cancellation; the inventory
service releases the reserved units, again idempotently. A legitimate CANCELLED (out of stock or
declined card) is a correct business outcome and is reported separately from failures.

## Deadlines, the reaper and the timeline

The outbox and idempotent consumers guarantee that nothing is lost while every service eventually
comes back. They do not bound how long "eventually" is, and they cannot help when a message is
structurally unprocessable or a downstream row was lost outside the system. Deadlines close that
gap. Each order carries `deadlineAt` for its current step: creation sets it to now plus the
reservation timeout, the move to RESERVED resets it to now plus the payment timeout, and a terminal
state clears it.

`StuckOrderReaper` polls `status in (PENDING, RESERVED) and deadlineAt <= now` on a fixed delay.
A silent reservation is cancelled through the state machine, which also emits `order.cancelled`:
if inventory did reserve but its notice never applied, the release makes the stock whole; if it
never reserved, the release is a no-op. A silent payment is first re-driven: `order.payment_requested`
goes to the payment service, which either re-emits `payment.completed` / `payment.failed` under a
fresh event id (the first copy evidently never applied) or attempts the payment now (recording it
from the request when it was never seen). Only a second expiry cancels with `PAYMENT_TIMEOUT`. A
payment outcome that arrives after that is recorded in the timeline as ignored and left for
reconciliation; the terminal rule of the state machine still holds.

Every step, including ignored and late events, is appended to `order_event` in the same
transaction as the change it describes and served by `GET /orders/{id}/timeline`. Because the row
and the state change commit together, the timeline is exact across restarts and redeliveries.

## Inventory: atomic reservations and the Redis cache

A multi-line order is reserved all or nothing: the rows are locked in sku order (`PESSIMISTIC_WRITE`,
sorted to avoid deadlocks), every line is checked, then every line is decremented. Concurrent
reservations for the last unit serialize on the lock and exactly one wins.

Redis holds `stock:{sku}` with a TTL. Writes are write-through but deferred to `afterCommit`, so
the cache never shows a value that was rolled back; the TTL bounds the damage of a missed write.
`GET /stock/{sku}` reads the cache first and falls back to the database. Cache reads sit behind a
circuit breaker (`redis`): if Redis is slow or gone the breaker opens and reads go straight to
Postgres. Cache write failures are logged and swallowed; the reservation is already committed.

## Payment: retry, breaker, time limiter, deferred queue

The processor call is decorated as `Retry(CircuitBreaker(TimeLimiter(call)))`:

* the time limiter cuts a hung call at 1 s
* the breaker counts failures and timeouts and opens after 60% failures in a 20 call window,
  moving to half open after 5 s
* retry re-invokes on transient faults and timeouts up to three times with a short pause; a
  `CallNotPermittedException` from an open breaker is not retried
* the fallback on the outermost decorator converts exhaustion into `Deferred`

`Deferred` is not a failure. The payment row stays open with a `next_attempt_at`, and
`DeferredPaymentSweeper` retries it with linear backoff. The same sweeper picks up payments that
were recorded but never attempted, which is exactly what a kill between "record" and "attempt"
leaves behind. Declines are business results, not exceptions: they are final and never retried.

The synthetic processor is deterministic: outcomes depend only on the order id and the attempt
number, so a run is reproducible and the retry and breaker paths are exercised on every run.

## Read-your-writes stock check

`GET /stock/{sku}` on the order service is the only synchronous cross-service call, used for UI
freshness after placing an order. It is decorated with `@CircuitBreaker` and `@TimeLimiter`
(800 ms). Failures and timeouts fall back to the last value seen for that sku from a local
Caffeine cache, marked `source: cache`, or `unknown` when nothing was ever fetched. The saga never
depends on this path.

## Why a kill never fails an order

Take any moment in an order's life and kill inventory or payment:

1. Before the consumer read the event: the event is still on the topic, the restarted consumer
   resumes from the committed offset.
2. During processing, before commit: the transaction is rolled back, the offset is not committed,
   the record is redelivered.
3. After commit, before the offset commit: redelivered, recognized as processed, skipped.
4. After commit, before the outbox relay published: the outbox row survives in Postgres and is
   published after restart.
5. During publish: at worst the event is sent twice; the downstream consumer ignores the
   duplicate by event id.
6. Payment specifically, between recording and authorizing: the sweeper finds the open payment.

Every step is either durable or replayable, and every replay is idempotent. The order service
keeps accepting orders throughout because it only needs its own database to commit, which is why
the chaos run reports `failed / stuck 0` while one service is down.

## Observability

Every service exposes `/actuator/health/{liveness,readiness}`, `/actuator/prometheus` and
`/actuator/circuitbreakers`. Custom meters:

| Meter | Service | Purpose |
|---|---|---|
| `ledgermesh.orders.by_state{state}` | order | orders per state |
| `ledgermesh.orders.transitions{to}` | order | transition counts |
| `ledgermesh.saga.latency` | order | creation to terminal state, p50/p95/p99 |
| `ledgermesh.saga.redrives` | order | payments asked for again by the reaper |
| `ledgermesh.payments.redriven{state}` | payment | re-drives answered for open or settled payments |
| `ledgermesh.outbox.backlog`, `.published`, `.send.failures` | all | relay health |
| `ledgermesh.consumer.duplicates` | all | redeliveries ignored |
| `ledgermesh.breaker.transitions{name,from,to}` | all | breaker history |
| `ledgermesh.payments.by_state`, `.deferred`, `.outcomes` | payment | deferred queue |
| `ledgermesh.cache.reads{result}` | inventory | cache hit ratio |
| `resilience4j.retry.calls{kind}` | payment | retries |

A correlation id is minted per HTTP request, carried in Kafka headers and event payloads, and
printed in every log line.
