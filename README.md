# LedgerMesh

Fault tolerant order processing across three Spring Boot microservices on Docker: Kafka
messaging (Redpanda), Redis caching, Resilience4j circuit breakers, time limiters and retries,
a GitLab CI pipeline, and a chaos test that kills one service under load and proves that zero
orders failed.

`make chaos` starts the stack, submits orders at 20/s for 60 s, kills the inventory or payment
service three times at random moments (restarting each after 5 s), waits for the saga backlog to
drain and asserts every order reached CONFIRMED or a legitimate out of stock CANCELLED.

## Architecture

```
  client --POST /orders--> order-service --order.created--> inventory-service
                               ^   |                              |
                               |   +--GET /stock (breaker,        | inventory.reserved / rejected
                               |      time limiter, cache) -------+       |
                               |                                          v
                               +------ payment.completed / failed ---- payment-service
                               |                                      (retry + breaker + time
                               +------ order.cancelled --> inventory   limiter + deferred queue)

  each service: Postgres database with orders/stock/payments + outbox_event + processed_event
  inventory: Redis stock:{sku} cache (write-through after commit, TTL)
```

* **order-service** (8081): accepts orders, writes the order and an `order.created` outbox row in
  one transaction, drives the saga `PENDING -> RESERVED -> CONFIRMED | CANCELLED` and emits
  `order.cancelled` as compensation when a payment is declined. Every step has a deadline; a
  reaper cancels reservations that never answer and re-drives silent payments once before
  cancelling. Each step is appended to a per order timeline. `GET /stock/{sku}` is a
  read-your-writes lookup wrapped in `@CircuitBreaker` + `@TimeLimiter` with a cache fallback.
* **inventory-service** (8082): reserves stock atomically for all lines of an order, releases it
  on cancellation, serves stock levels from Redis with write-through and TTL.
* **payment-service** (8083): authorizes against a deterministic synthetic processor behind
  `Retry(CircuitBreaker(TimeLimiter(call)))`; exhaustion defers the payment to a retry queue,
  never drops it. A re-drive from the order service is answered with the outcome on file or a
  fresh attempt.
* **common**: event contracts, topic names, transactional outbox relay, idempotent consumer,
  correlation ids, breaker transition metrics.

Kill any one service at any moment and the outcome of every order is unchanged: the outbox makes
every event durable before it is sent, idempotent consumers make every redelivery harmless, and
the deferred queue makes a lost payment attempt resumable. [ARCHITECTURE.md](ARCHITECTURE.md)
walks through each failure point.

## Quick start

```bash
make up                 # build the three images and start Redpanda, Redis, Postgres, services
curl -s -X POST localhost:8081/orders -H 'content-type: application/json' \
  -d '{"customerId":"cust-1","items":[{"sku":"SKU-ALPHA","quantity":2,"unitPrice":"9.99"}]}'
curl -s localhost:8081/orders/<id>          # PENDING -> RESERVED -> CONFIRMED
curl -s localhost:8081/orders/<id>/timeline # every step with a timestamp
curl -s localhost:8082/stock/SKU-ALPHA      # served from Redis
curl -s localhost:8081/stock/SKU-ALPHA      # via order-service, "source": "live" | "cache"
make down
```

Requirements: JDK 21, Maven 3.9, Docker with Compose, Python 3 (chaos harness).

## Chaos test

```bash
make chaos    # alias: make demo
```

Measured output of the run recorded in this repository (macOS host, Docker via Colima, three
kills, restart after 5 s):

```
LedgerMesh chaos summary
  load                 60s at 20 orders/s
  orders submitted     1200
  confirmed            1162
  cancelled (stock)    38
  failed / stuck       0
  kills                3  (inventory-service @22s, payment-service @40s, inventory-service @56s)
  saga latency         p50 19955 ms   p95 33136 ms   max 35397 ms
  breaker transitions  order-service/inventory CLOSED->OPEN x2; order-service/inventory HALF_OPEN->CLOSED x1; order-service/inventory HALF_OPEN->OPEN x2; order-service/inventory OPEN->HALF_OPEN x4
  retries              with retry 71 ok / 0 exhausted, without retry 1088 ok / 0 failed
  deferred payments    0
  duplicate events     1 ignored by idempotent consumers
  stock probes         {'live': 58, 'cache': 40, 'unknown': 0, 'error': 0}
```

`failed / stuck` counts orders that did not reach a terminal state, orders cancelled for any
reason other than stock, and rejected submissions. `cancelled (stock)` are orders for
`SKU-SCARCE`, which is seeded with 40 units so the out of stock branch is exercised on every run.
Retries, deferred payments and breaker transitions come from the synthetic processor's
deterministic transient faults and from the kills themselves. Counters are snapshotted right
before each kill because a killed JVM loses its in-memory meters.

Knobs: `CHAOS_DURATION`, `CHAOS_RATE`, `CHAOS_KILLS`, `CHAOS_RESTART_AFTER`, `CHAOS_KEEP_STACK=1`.
Output lands in `chaos/out/` (orders, kill timeline, metric snapshots, summary).

## Tests

```bash
make lint     # spotless (google-java-format)
make test     # mvn verify: 77 unit tests + 7 integration tests
```

Unit tests (H2, no Docker): saga state machine transitions and compensation, deadline reaper
(silent reservation cancelled with release, silent payment re-driven once then cancelled), the
timeline endpoint, outbox relay ordering and re-send behaviour, idempotent consumer, breaker and
time limiter fallbacks, cache write-through after commit, atomic and concurrent reservations,
retry / breaker / deferred queue, re-drive answers, deterministic processor, the replay cap that
turns a record into a parked poison message.

Integration tests (`e2e-tests`, Testcontainers Redpanda + Postgres + Redis, all three services
booted in one JVM): an order flows to CONFIRMED end to end, out of stock and declined payment
paths with stock release, triple delivery of the same event reserves stock once, a payment
listener stopped mid-flight confirms after restart, an inventory service restarted with twelve
orders in flight confirms all of them, and a record that can never be processed reaches the dead
letter topic after the configured attempts without holding up the record behind it, is replayed
once, and is parked on the second replay.

## API

| Method | Path | Service | Notes |
|---|---|---|---|
| POST | `/orders` | order | body `{customerId, items:[{sku, quantity, unitPrice}]}`; 202 with the order, `Location` header |
| GET | `/orders/{id}` | order | `status` PENDING, RESERVED, CONFIRMED, CANCELLED; `reason` OUT_OF_STOCK, PAYMENT_DECLINED, RESERVATION_TIMEOUT or PAYMENT_TIMEOUT; `deadlineAt` for the current step, `redrives` |
| GET | `/orders/{id}/timeline` | order | ordered list of `{type, from, to, reason, correlationId, at}`; ignored and late events appear with `to: null` |
| GET | `/stock/{sku}` | order | live value from inventory, or `source: cache` / `unknown` under the breaker |
| GET | `/stock/{sku}` | inventory | cache first, database on miss |
| PUT | `/stock/{sku}` | inventory | body `{available}`; creates or restocks |
| GET | `/admin/dlq` | all | dead letters per source topic that this service has not replayed or parked |
| POST | `/admin/dlq/{topic}/replay` | all | puts dead letters back on `{topic}`; `max` caps the batch, answer is `{replayed, parked}` |
| GET | `/actuator/health/readiness`, `/actuator/prometheus`, `/actuator/circuitbreakers` | all | probes and metrics |

Customers whose id ends with `-declined` and amounts above 10000 are declined by the processor.

## Topics

| Topic | Producer | Consumers |
|---|---|---|
| `order.created` | order | inventory |
| `inventory.reserved` | inventory | order, payment |
| `inventory.rejected` | inventory | order |
| `payment.completed` | payment | order |
| `payment.failed` | payment | order |
| `order.cancelled` | order | inventory |
| `order.payment_requested` | order | payment |

Three partitions each, keyed by order id, JSON payloads, `x-correlation-id` and `event-id`
headers. Every topic has a single partition `<topic>.dlq` shadow. Topics are created on startup by
the first service up.

## CI

`.gitlab-ci.yml` defines three stages:

1. **test**: `maven:3.9-eclipse-temurin-21` with a docker-in-docker service so Testcontainers can
   run the integration tests; JUnit reports and the executable jars are kept as artifacts.
2. **build**: a matrix job per service builds the multi-stage Dockerfile and pushes
   `$CI_REGISTRY_IMAGE/<service>:<short sha>` (and `latest` on the default branch).
3. **chaos**: runs `make chaos` against the compose stack on merge requests and the default
   branch and keeps `chaos/out/` as an artifact; the job fails if any order failed.

`.github/workflows/ci.yml` mirrors the same three jobs.

## Saga deadlines

Every saga step carries a deadline on the order row (`deadlineAt`): `ledgermesh.saga.reservation`
(default 60 s) for the inventory answer and `ledgermesh.saga.payment` (default 120 s) for the
payment answer. `StuckOrderReaper` runs every 5 s and handles expired orders through the same
state machine and outbox as any inbound event:

* PENDING past its deadline: `CANCELLED (RESERVATION_TIMEOUT)` and `order.cancelled` is emitted so
  a reservation that was made but never reported is released.
* RESERVED past its deadline, first time: `order.payment_requested` is emitted, the deadline is
  extended and `redrives` becomes 1. The payment service re-emits the outcome it already has or
  attempts the payment now.
* RESERVED past its deadline again: `CANCELLED (PAYMENT_TIMEOUT)` with release.

Timeout cancellations count as failures in the chaos summary, so a deadline that is too tight for
the stack shows up as a red run rather than a quietly cancelled order.

## Dead letters and lag

Every business topic has a `<topic>.dlq` shadow. A record whose handler keeps failing is retried in
place with exponential backoff (`ledgermesh.dlq.max-attempts`, default 8), which holds the partition
while a database or broker recovers; when the attempts run out the record is published to the dead
letter topic with its original coordinates and the exception in headers, and the partition moves on.
A payload that cannot be decoded skips the retries. Failed deliveries are counted per topic, so the
attempts a record burned stay visible after it has left the topic.

`POST /admin/dlq/{topic}/replay?max=100` reads dead letters with a dedicated consumer group,
republishes each one on its source topic with the original key, value and headers, and only then
commits, so a crash during a replay repeats a record rather than dropping it. Consumers that already
applied the event ignore the replay by event id. Each replay stamps a `dlq-replay-count` header;
once a record has used up `ledgermesh.dlq.max-replays` (default 3) the replayer parks it instead of
republishing, which is what ends the loop between a topic and its dead letter shadow.
`GET /admin/dlq` reports what is still waiting.

Two gauges are recomputed from broker offsets every five seconds:
`ledgermesh.consumer.lag{group,topic}` (end offset minus committed offset over all partitions) and
`ledgermesh.dlq.depth{topic}` (the same difference for the replay group on the dead letter topic).

## Releases

* **v3.0.0**: a `<topic>.dlq` shadow for every topic with bounded in place retries,
  `POST /admin/dlq/{topic}/replay`, a poison message cap that parks a record once its replays are
  used up, and `ledgermesh.consumer.lag` / `ledgermesh.dlq.depth` gauges read from broker offsets.
* **v2.0.0**: per step saga deadlines with a stuck order reaper (cancel silent reservations, re-drive
  silent payments once, then cancel), `order.payment_requested` re-drive topic answered by the
  payment service, `GET /orders/{id}/timeline`, `deadlineAt` and `redrives` on the order response.
* **v1.0.0**: three services on Redpanda with a transactional outbox, idempotent consumers, a
  compensating saga, Redis write-through caching, Resilience4j breakers / retries / time limiters,
  GitLab CI and the chaos harness (1200 orders, 3 kills, 0 failed or stuck).

## Layout

```
common/             events, outbox, idempotency, correlation, metrics
order-service/      saga, orders API, stock check client
inventory-service/  reservations, Redis cache, stock API
payment-service/    authorizer, deferred queue, synthetic processor
e2e-tests/          Testcontainers integration tests
deploy/             docker-compose.yml (Redpanda, Redis, Postgres, three services)
chaos/              run.sh, loadgen.py, report.py
```
