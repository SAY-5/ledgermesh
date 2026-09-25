# Changelog

Dates are the commit dates of the release tags. Each numbered release is a demo milestone;
see the versioning note in [CONTRIBUTING.md](CONTRIBUTING.md).

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
