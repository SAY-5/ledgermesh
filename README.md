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
* **inventory-service** (8082): reserves stock atomically for all lines of an order and records
  the hold per order and sku, releases exactly that hold on cancellation, serves stock levels from
  Redis with write-through and TTL.
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
make chaos           # alias: make demo
make chaos-tight     # the same run with six kills and a two second restart
make chaos-baseline  # the same load with no kills, the reference for the latency lines
```

Two runs are recorded in the repository, written by the harness rather than copied into prose:
[chaos/evidence/baseline/summary.txt](chaos/evidence/baseline/summary.txt) with no kills, and
[chaos/evidence/steady/summary.txt](chaos/evidence/steady/summary.txt) with three.
`CHAOS_RECORD=1` refreshes the file for the profile in effect. Every recorded summary opens with the
commit it was taken at, the UTC date, the host, the Docker server version and each `CHAOS_*` knob
that was set, so a reader can repeat it.

| | no kills | three kills |
|---|---|---|
| recorded | 2026-09-27T02:03:18Z at `c1c6fb3` | 2026-09-27T02:06:26Z at `8a0922c` |
| orders submitted | 1200 | 1200 |
| confirmed | 1147 | 1147 |
| cancelled, out of stock | 53 | 53 |
| failed / stuck | **0** | **0** |
| kills | none | inventory-service @17s, order-service @37s, payment-service @52s |
| saga latency p50 / p95 / max | 874 / 4532 / 14900 ms | 17206 / 35109 / 38902 ms |
| retries ok / exhausted | 74 / 2 | 81 / 0 |
| submits retried on their key | 0 over 0 orders | 1628 over 246 orders |
| stock probes live / cache / refused | 99 / 2 / 0 | 46 / 31 / 24 |
| breaker transitions | none | order-service/inventory CLOSED->OPEN x1; order-service/inventory HALF_OPEN->OPEN x2; order-service/inventory OPEN->HALF_OPEN x2 |

The three kill run in full, as the file contains it:

```
LedgerMesh chaos summary
  recorded             2026-09-27T02:06:26Z at commit 8a0922c
  host                 Darwin 25.0.0 arm64, 10 cpu, 16.0 GiB; Docker server 29.2.1 (colima); Python 3.14.4
  knobs                CHAOS_PROFILE=steady CHAOS_DURATION=60 CHAOS_RATE=20 CHAOS_KILLS=3 CHAOS_RESTART_AFTER=5 CHAOS_VICTIMS=inventory-service payment-service order-service CHAOS_SEED=3 CHAOS_DRAIN_TIMEOUT=180 CHAOS_DRAIN_CAP=600 CHAOS_MAX_P95=90000
  load                 60s at 20 orders/s, seed 3
  orders submitted     1200
  confirmed            1147
  cancelled (stock)    53
  failed / stuck       0
  kills                3  (inventory-service @17s, order-service @37s, payment-service @52s)
  saga latency         p50 17206 ms   p95 35109 ms   max 38902 ms
  p95 ceiling          90000 ms (CHAOS_MAX_P95) held
  breaker transitions  order-service/inventory CLOSED->OPEN x1; order-service/inventory HALF_OPEN->OPEN x2; order-service/inventory OPEN->HALF_OPEN x2
  retries              with retry 81 ok / 0 exhausted, without retry 1066 ok / 0 failed
  deferred payments    0
  duplicate events     0 ignored by idempotent consumers
  compensations        0 reservations released, 0 releases for orders that held nothing
  resubmits            1628 retried submits over 246 orders, 0 answered from the idempotency store (0 replays counted by the service), 0 gave up, 0 duplicate orders
  stock probes         {'live': 46, 'cache': 31, 'unknown': 1, 'error': 24}
  services             order-service UP, inventory-service UP, payment-service UP
  consumer lag         worst 0 on order-service|inventory.rejected
  dead letter depth    worst 0 on order-service inventory.rejected
  parked records       worst 0 on order-service inventory.rejected
  breaker states       inventory-service/redis CLOSED; order-service/inventory CLOSED; payment-service/processor CLOSED
  in flight sagas      0
  stuck orders         0
```

`failed / stuck` counts orders that did not reach a terminal state, orders cancelled for any
reason other than stock, and rejected submissions. `cancelled (stock)` are orders for
`SKU-SCARCE`, which is seeded with 40 units so the out of stock branch is exercised on every run.
Retries, deferred payments and breaker transitions come from the synthetic processor's
deterministic transient faults and from the kills themselves. Counters are snapshotted right
before each kill because a killed JVM loses its in-memory meters. The last seven lines are the
`/ops/overview` of each service read after the backlog drained.

The counts are a function of the seed: both runs above used `CHAOS_SEED=3`, and both submitted
1200 orders and cancelled the same 53 for stock. The latency lines
are not, which is why every summary carries its host and its date. Order ids are fresh UUIDs, so
which orders meet a transient processor fault differs from run to run, and the queue that builds
behind a killed service moves with whatever else the machine is doing; a re-recording on a busier
host reports materially different figures at the same seed. Each latency line is one measurement of
one run, not a specification.

Because of that, the gate on latency is a ceiling rather than an expected value: with
`CHAOS_MAX_P95` set, the summary prints the ceiling and whether it held, and the harness exits
non zero when the p95 is above it. The recorded three kill run held a ceiling of 90000 ms, roughly
two and a half times its own p95, which is wide enough to survive a loaded developer machine and
tight enough to catch a change that doubles recovery time. Both CI pipelines set it on the chaos
job.

Knobs: `CHAOS_PROFILE` (`steady` three kills restarting after 5 s, `tight` six kills restarting
after 2 s), `CHAOS_DURATION`, `CHAOS_RATE`, `CHAOS_KILLS`, `CHAOS_RESTART_AFTER`, `CHAOS_VICTIMS`
(default all three services), `CHAOS_SEED` (kill schedule and load mix; a fresh one is drawn and
printed when unset), `CHAOS_MAX_P95`, `CHAOS_DRAIN_TIMEOUT`, `CHAOS_DRAIN_CAP`, `CHAOS_RECORD=1`,
`CHAOS_LABEL`, `CHAOS_KEEP_STACK=1`, `CHAOS_PYTHON`, and `LEDGERMESH_ORDER_PORT` /
`LEDGERMESH_INVENTORY_PORT` / `LEDGERMESH_PAYMENT_PORT` when 8081 to 8083 are taken on the host.
Output lands in `chaos/out/` (orders, kill timeline, metric snapshots, summary); the harness tears
the stack down on every exit path unless `CHAOS_KEEP_STACK=1`.

## Tests

```bash
make lint     # spotless (google-java-format)
make test     # mvn verify: 89 unit tests + 14 integration tests
```

Unit tests (H2, no Docker): saga state machine transitions and compensation, deadline reaper
(silent reservation cancelled with release, silent payment re-driven once then cancelled), the
timeline endpoint, outbox relay ordering and re-send behaviour, idempotent consumer, breaker and
time limiter fallbacks, cache write-through after commit, atomic and concurrent reservations,
retry / breaker / deferred queue, re-drive answers, deterministic processor, the replay cap that
turns a record into a parked poison message, the replayer committing only the offsets it handled
and waiting for its group assignment before it treats silence as an empty topic, the request
deduplication store, the relay counting a send that never confirmed, the open and overdue saga
counts behind the ops overview, and the reservation ledger (a release credits what the order held,
a release before the reservation is a no-op that blocks the late reservation, a repeated
reservation takes stock once).

Integration tests (`e2e-tests`, Testcontainers Redpanda + Postgres + Redis, all three services
booted in one JVM): an order flows to CONFIRMED end to end, out of stock and declined payment
paths with stock release, triple delivery of the same event reserves stock once, a payment
listener stopped mid-flight confirms after restart, an inventory service restarted with twelve
orders in flight confirms all of them, and a record that can never be processed reaches the dead
letter topic after the configured attempts without holding up the record behind it, is replayed
once, is parked on the second replay and is then listed by `GET /admin/dlq/parked` with its replay
count and original coordinates. A release that reaches inventory before the reservation it undoes
leaves stock untouched and the late reservation is rejected. Exactly once at the boundary has a class of its own: the
same idempotency key twice, two calls racing on one key, and the same key retried across a restart
of the order service each place one order and return one body, and an outbox row put back into the
crash window is sent again, ignored by the consumer, and leaves the stock ledger and the payment
unchanged. All three services boot in one JVM and therefore share one classpath, so the order and payment
contexts exclude the Redis auto-configuration that only the inventory service needs; without that
their health endpoints try to reach a Redis on localhost, readiness never turns UP and every saga
assertion times out. The ops overview is checked against a listener that is
stopped and started again: lag rises and falls, the order shows up as in flight and then does not,
and the services that do not own the saga report the rest of the page without it.

## Browser demo

`web/` is a single page that runs a TypeScript model of the v1 mechanisms in the browser, so a
visitor can kill a service and watch the saga settle without a Docker daemon:

```bash
cd web
npm ci
npm run dev             # vite dev server
npm run build           # type check and the production bundle
npm run selfcheck       # the model's invariants, in node
npm run config:check    # the mirrored constants still match the services' application.yml
npm run measured:check  # the measured figures still match chaos/evidence/
```

It models the saga and its compensation, the outbox and its crash window, idempotent consumers, the
stock check behind a breaker and a time limiter, the Redis cache with its TTL, the retry policy and
the deferred payment queue. It models nothing added after v1: saga deadlines and the reaper, the
timeline endpoint, dead letters and replay, the `Idempotency-Key` header and `/ops/overview` exist
only in the services. Both sets of constants the page quotes are generated, one from the three
`application.yml` files and one from the summaries under `chaos/evidence/`, and every figure on the
page is labelled simulated or measured. [web/README.md](web/README.md) has the module map, the
virtual clock and the seeding model.

## API

| Method | Path | Service | Notes |
|---|---|---|---|
| POST | `/orders` | order | body `{customerId, items:[{sku, quantity, unitPrice}]}`; 202 with the order, `Location` header; an `Idempotency-Key` header makes the call repeatable, answered with `Idempotent-Replay` |
| GET | `/orders/{id}` | order | `status` PENDING, RESERVED, CONFIRMED, CANCELLED; `reason` OUT_OF_STOCK, PAYMENT_DECLINED, RESERVATION_TIMEOUT or PAYMENT_TIMEOUT; `deadlineAt` for the current step, `redrives` |
| GET | `/orders/{id}/timeline` | order | ordered list of `{type, from, to, reason, correlationId, at}`; ignored and late events appear with `to: null` |
| GET | `/stock/{sku}` | order | live value from inventory, or `source: cache` / `unknown` under the breaker |
| GET | `/stock/{sku}` | inventory | cache first, database on miss |
| PUT | `/stock/{sku}` | inventory | body `{available}`; creates or restocks |
| GET | `/ops/overview` | all | health, consumer lag, dead letter depth, parked depth, breaker states and, on the order service, in flight and stuck sagas |
| GET | `/admin/dlq` | all | dead letters per source topic that this service has not replayed or parked |
| GET | `/admin/dlq/parked` | all | records parked on `<topic>.parked` per source topic, with their replay count, original coordinates and exception |
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
headers. Every topic has single partition `<topic>.dlq` and `<topic>.parked` shadows. Topics are created on
startup by the first service up.

## CI

`.gitlab-ci.yml` defines four stages:

1. **test**: `maven:3.9-eclipse-temurin-21` with a docker-in-docker service so Testcontainers can
   run the integration tests; JUnit reports and the executable jars are kept as artifacts. The
   suite needs no service containers of its own: every broker, database and cache it uses is one
   Testcontainers starts.
2. **build**: a matrix job per service builds the multi-stage Dockerfile and pushes
   `$CI_REGISTRY_IMAGE/<service>:<short sha>` (and `latest` on the default branch).
3. **chaos**: runs `make chaos` against the compose stack on merge requests and the default
   branch and keeps `chaos/out/` as an artifact; the job fails if any order failed or if the p95
   saga latency is above the `CHAOS_MAX_P95` ceiling it sets.
4. **web**: `npm ci`, the two generator checks, the production bundle and the simulation's
   self-check, with `web/dist` kept as an artifact.

`.github/workflows/ci.yml` mirrors the same four jobs.

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

## Exactly once effects

`POST /orders` takes an optional `Idempotency-Key` header. The first call runs the work and stores
the answer it returned under that key in the same transaction as the order, its outbox row and its
timeline entry, so the key and the effect can never disagree. A repeat is answered from the store,
byte for byte, with `Idempotent-Replay: true`, and places no second order; the current state of the
order is always at `GET /orders/{id}`. Two calls racing on one key both try to store the answer, the
primary key lets one of them through, and the loser rolls back its own order and returns what the
winner wrote.

On the way out, the relay stamps `attemptedAt` on an outbox row before it sends and `publishedAt`
only after the broker acknowledged it. A row that comes back attempted but unpublished is exactly
the crash window between the two: it may or may not have reached the broker, so the relay sends it
again and counts `ledgermesh.outbox.resends`. The redelivery is harmless because the consumer
recognises the event id it already applied, which is why stock, payments and order state are the
same after a repeat as before it.

## Dead letters and lag

Every business topic has a `<topic>.dlq` shadow. A record whose handler keeps failing is retried in
place with exponential backoff (`ledgermesh.dlq.max-attempts`, default 8), which holds the partition
while a database or broker recovers; when the attempts run out the record is published to the dead
letter topic with its original coordinates and the exception in headers, and the partition moves on.
A payload that cannot be decoded skips the retries. Failed deliveries are counted per topic, so the
attempts a record burned stay visible after it has left the topic.

`POST /admin/dlq/{topic}/replay?max=100` reads dead letters with a dedicated consumer group,
republishes each one on its source topic with the original key, value and headers, and commits only
the offsets it handled, so a crash during a replay repeats a record rather than dropping it and a
batch cut short by `max` leaves the rest for the next call. Consumers that already applied the
event ignore the replay by event id. Each replay stamps a `dlq-replay-count` header; once a record
has used up `ledgermesh.dlq.max-replays` (default 3) the replayer parks it instead of republishing:
the record is copied to `<topic>.parked` with all of its headers, which ends the loop between a
topic and its dead letter shadow without losing the record. `GET /admin/dlq` reports what is still
waiting and `GET /admin/dlq/parked` what was parked.

Three gauges are recomputed from broker offsets every five seconds:
`ledgermesh.consumer.lag{group,topic}` (end offset minus committed offset over all partitions),
`ledgermesh.dlq.depth{topic}` (the same difference for the replay group on the dead letter topic)
and `ledgermesh.dlq.parked.depth{topic}` (records retained on the parked topic).

## Ops overview

`GET /ops/overview` is the page to open first during an incident. Every service answers for itself:

```json
{
  "service": "order-service",
  "health": "UP",
  "consumerLag": { "order-service|inventory.reserved": 0 },
  "deadLetterDepth": { "order.created": 0 },
  "parkedDepth": { "order.created": 0 },
  "breakers": { "inventory": "CLOSED" },
  "sagas": { "inFlight": 2, "stuck": 0, "byState": { "PENDING": 1, "RESERVED": 1 } }
}
```

The numbers are the same gauges the Prometheus endpoint publishes, so the page and the dashboards
cannot disagree. `sagas` is null on the services that do not own the saga. `stuck` counts open
orders whose current step has already passed its deadline, which is the number that says the reaper
is behind rather than idle; it is also published as `ledgermesh.saga.stuck`. The chaos summary
prints the overview of all three services at the end of a run.

## Releases

Every entry in full is in [CHANGELOG.md](CHANGELOG.md).

* **v5.0.0**: `GET /ops/overview` per service (health, consumer lag, dead letter depth, breaker
  states, in flight and stuck sagas), a `ledgermesh.saga.stuck` gauge, a `tight` chaos profile with
  twice the kills and a two second restart, and the overview in the chaos summary.
* **v4.0.0**: `Idempotency-Key` on `POST /orders` backed by a request deduplication store written
  in the order's own transaction, and an outbox relay that stamps an attempt before the send so a
  crash between the send and the ack shows up as a counted re-send rather than a second effect.
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
chaos/              run.sh, loadgen.py, report.py, evidence/ (recorded summaries)
web/                browser demo: a TypeScript simulation of the v1 mechanisms (see web/README.md)
```
