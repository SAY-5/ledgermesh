# Changelog

Dates are the commit dates of the release tags. Each numbered release is a demo milestone;
see the versioning note in [CONTRIBUTING.md](CONTRIBUTING.md).

## Unreleased

* An order cancelled at a saga deadline kept the payment authorized for it. The payment service
  authorizes off `inventory.reserved` whatever the order has become since, and only inventory read
  `order.cancelled`, so the stock came back and the money did not. Killing the order service more
  than once can use up an order's 60 s reservation window, since the deadline counts the order
  service's own absences. Seed 1040, run with the final ledger kept, showed it directly: six orders
  cancelled for `RESERVATION_TIMEOUT` with their stock released and their payments still
  `AUTHORIZED`. The chaos job of run 36519954920 (seed 54826) failed with four orders cancelled,
  and those were most likely the same: its artifact keeps no order states or reasons, so this is
  inferred, from its four reservations released and none for orders that held nothing, no stuck
  order, a longest saga of 69721 ms against the 60 s window, and the fact that under a load with no
  declined customers the only cancellations that release stock are the deadline ones, of which
  `PAYMENT_TIMEOUT` takes at least 120 s. The payment service now reads `order.cancelled` and voids
  an authorized or open payment, or inserts a voided marker when the cancellation comes first, so a
  late reservation or re-drive charges nothing.
* A void was only a status on the payment row: the processor kept the authorization, and an approval
  that landed after the void (an attempt that had read the payment open, a reservation that reached
  the payment service after the cancellation, or a call the time limiter had cut off, which runs on
  and approves up to `slow-millis` later) was dropped from the ledger but left on the card; so was
  an approval whose commit lost a race, rolled back or died with the process. Every approval is now
  an authorization with a code of its own, a payment keeps one while it is authorized and none
  otherwise, and a reconciler in the payment service lists the processor's outstanding
  authorizations every 5 s and releases every one its payment does not keep once it is older than
  the grace period, the longest processor call plus a commit allowance (10 s by default). No
  authorization survives more than the grace period plus one interval unless its payment keeps it.
  Voids, authorizations, approvals a payment does not keep and cancellations sent again also have
  the sweeper ask the processor at once, behind its breaker and time limit, to release all but the
  kept code, for settled payments only. The synthetic processor keeps what it granted in a
  `processor_hold` table of its own, one row per authorization, committed apart from the payment
  service's transactions, and it and its table exist only while `ledgermesh.processor.synthetic` is
  on.
* A cancellation the payment service could not apply before its retries ran out went to
  `order.cancelled.dlq`, where only an operator's replay could bring it back, and the charge stayed.
  The payment service now answers every cancellation with `payment.voided`, the order service
  records the answer (`compensatedAt`), and its reaper sends `order.cancelled` again, under a fresh
  event id, for every cancellation still unanswered after `ledgermesh.saga.compensation` (60 s),
  and again after each wait; `ledgermesh.saga.compensations.overdue` counts those past due.
* Upgrading: the consumer group `payment-service` is not new, but its subscription to
  `order.cancelled` is. With no committed offset on that topic, `auto-offset-reset: earliest`
  applies, so its first start reads every cancellation still inside the topic's retention (the
  broker's default, as nothing here sets one) and voids what it finds; older cancellations are not
  read. Orders cancelled before the upgrade are not waiting for `payment.voided` either. After
  upgrading, call `POST /admin/compensations/resend` on the order service, and again until it
  answers `resent: 0`: each call sends `order.cancelled` again for up to `max` (1 to 1000) cancelled
  orders whose cancellation was never answered and that are not waiting for an answer, oldest
  first, and each one sent waits for its answer from then on like a new cancellation, so the next
  call takes the next ones. Inventory treats the copies as the no-op releases they are.
* The chaos harness waits the payment service's reconciliation grace period plus one interval, then
  reads every order, its timeline, its payment, its stock holds and the synthetic processor's
  authorizations from the three databases into `chaos/out/final.json`, and fails a run whose ledger
  breaks a money or stock rule: a confirmed order paid once, holding its stock and its one
  authorization, any other order holding no authorization, a cancelled one charged nothing, holding
  nothing and its cancellation answered. A cancellation for a saga deadline is counted under
  `cancelled (deadline)` with its reason instead of under `failed / stuck`, and every order that was
  not confirmed or cancelled for stock is listed with its payment, card, holds and timeline. The
  GitHub chaos job takes a `chaos_seeds` input when started by hand and runs one job per seed
  listed.
* Two calls racing on one `Idempotency-Key` could both place an order. The answer was saved with a
  merge, which reads the row first: a call that stored its answer after the other had committed
  found that row and overwrote it instead of failing on the primary key, and both orders were
  reserved, charged and confirmed. The answer and the consumers' processed markers are now always
  inserted, and the `repeat` workflow runs `ExactlyOnceIT` 50 times on every pull request that
  changes that code.
* A payment has two creators on two topics, the `inventory.reserved` listener and a re-drive that
  finds no payment on file, and it was saved with a merge as well: a creator that saved after the
  other had committed the same order overwrote that payment with its own copy. A new payment is now
  always inserted and flushed, so the second creator fails on the primary key before it calls the
  processor and takes the payment on file when the error handler delivers it again. A new order is
  inserted without the read a merge made first.
* The order service counts a created or changed order in `ledgermesh.orders.transitions` and logs
  it only once its transaction has committed, so the loser of a race on one idempotency key no
  longer shows up in either.
* The `repeat` workflow fails the runner that had a failed run as well as its summary, and also
  starts for changes to the root `pom.xml`, the saga service, the order service's configuration
  and the test stack. It is not a required check.
* Dead letter replay commits only the offsets it handled and waits for its group assignment before
  treating silence as an empty topic, and a record whose replays are used up is copied to a
  retained `<topic>.parked` topic and listed by `GET /admin/dlq/parked` instead of vanishing.
* Inventory keeps a reservation row per order and sku: a release credits exactly what that order
  held, a release that arrives before its reservation leaves stock untouched and blocks the late
  reservation, and a repeated reservation takes stock once.
* The chaos harness kills the order service as well, sends an `Idempotency-Key` per order and
  retries it while intake is away, seeds the kill schedule from `CHAOS_SEED`, tears the stack down
  on every exit path, stamps commit, host, Docker version and knobs into the summary, records runs
  under `chaos/evidence/` with `CHAOS_RECORD=1`, and fails a run whose p95 saga latency exceeds
  `CHAOS_MAX_P95`.
* Both pipelines run the browser demo's type check, its two generator checks, its bundle and its
  self-check, and set the p95 ceiling on the chaos job.
* The browser demo labels every figure measured or simulated, reads its constants from the
  services' configuration and its measured figures from the recorded runs, and replaces the service
  map with a readable list on narrow viewports.

## v5.0.0 (2026-09-10)

* `GET /ops/overview` on every service: health, consumer lag, dead letter depth, breaker states
  and, on the order service, in flight and stuck sagas with the count per state.
* `ledgermesh.saga.stuck` gauges open orders whose current step has already passed its deadline.
* A `tight` chaos profile runs the existing harness with six kills and a two second restart, and
  the chaos summary now ends with the overview of all three services.

## v4.0.0 (2026-09-10)

* `POST /orders` accepts an `Idempotency-Key`. The first call stores the answer it returned in the
  same transaction as the order, and a repeat is answered from that store with `Idempotent-Replay`
  instead of placing a second order.
* Two calls racing on one key place one order: the store's primary key lets one insert through and
  the loser rolls its own order back and returns the winner's answer.
* The outbox relay stamps an attempt before each send, so a row that comes back attempted but
  unpublished is recognised as the crash window between the send and the ack, sent again and
  counted in `ledgermesh.outbox.resends`.

## v3.0.0 (2026-09-10)

* Every business topic has a `<topic>.dlq` shadow. A record whose handler keeps failing is retried
  in place with exponential backoff and then dead lettered with its original coordinates and the
  exception in headers, so the partition moves on.
* `POST /admin/dlq/{topic}/replay` puts dead letters back on their source topic with a dedicated
  consumer group, committing only after the republish. `GET /admin/dlq` reports what is waiting.
* A replay count travels with the record; once it reaches `ledgermesh.dlq.max-replays` the replayer
  parks the record instead of republishing it, which ends the loop between a topic and its shadow.
* `ledgermesh.consumer.lag{group,topic}` and `ledgermesh.dlq.depth{topic}` gauges are recomputed
  from broker offsets, and `ledgermesh.consumer.delivery.failures{topic}` counts failed deliveries.
* The producer refuses to start with settings that would break the relay's delivery guarantees.

## v2.0.0 (2026-09-08)

* Per step saga deadlines on the order row, with a reaper that cancels a reservation that never
  answered, re-drives a silent payment once and then cancels it.
* `order.payment_requested` re-drive topic, answered by the payment service with the outcome on
  file or a fresh attempt.
* `GET /orders/{id}/timeline` returns every saga step with a timestamp; `deadlineAt` and `redrives`
  appear on the order response.

## v1.0.0 (2026-09-08)

* Order, inventory and payment services on Kafka with a transactional outbox, idempotent consumers
  and a compensating saga.
* Redis write-through stock cache, Resilience4j circuit breakers, retries and time limiters.
* Chaos harness that kills a service three times under load and proves no order failed, plus the
  GitLab and GitHub CI pipelines.
