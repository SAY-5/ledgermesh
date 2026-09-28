# LedgerMesh browser demo

A single page that runs a TypeScript model of the v1 mechanisms of the three services in the
browser, so a visitor can kill a service and watch the saga settle anyway. It is a model, not the
services: nothing here talks to Kafka, Redis or Postgres.

```bash
cd web
npm ci
npm run dev              # vite dev server
npm run build            # tsc --noEmit and the production bundle
npm run selfcheck        # the simulation's invariants, run in node
npm run config:check     # fails when the mirrored constants drift from the services' yml
npm run measured:check   # fails when the measured figures drift from chaos/evidence/
```

Every pipeline runs `npm ci`, `npm run config:check`, `npm run measured:check`, `npm run build` and
`npm run selfcheck`, so a change to a service's configuration or to a recorded chaos run fails here
rather than drifting. The output in `dist/` is a static bundle with no server
side.

## Layout

```
src/sim/          the model: no React, no DOM, deterministic
  cluster.ts        the three services, the broker and the clock in one object
  service.ts        a killable service: offsets commit on the tick after the poll
  broker.ts         topics, partitions by order id, lag, redeliveries
  events.ts         the event contracts and the topic names, mirrored from the common module
  outbox.ts         the relay: attemptedAt before the send, publishedAt after the ack
  idempotent.ts     the processed-event store the consumers check
  orderService.ts   the saga, the stock check behind a breaker and a time limiter
  inventoryService.ts reservations, releases, the write-through cache
  cache.ts          the Redis stock view: entries with a TTL that outlive a killed service
  paymentService.ts the authorizer, the deferred queue, the synthetic processor
  stateMachine.ts   the transition table
  resilience.ts     breaker, retry and time limiter windows
  prng.ts           mulberry32, the only source of randomness
  loadgen.ts        the order mix and the kill schedule
  config.generated.ts   constants read out of the services' application.yml
  measured.generated.ts the recorded chaos runs read out of chaos/evidence/
  measured.ts       the measured figure each panel is shown against
  selfcheck.ts      assertions over all of the above
src/components/   the page: hero, service map, saga trace, resilience panels, chaos run
src/hooks/        useRunner: steps a cluster from requestAnimationFrame
src/styles/       tokens and one stylesheet per section
scripts/          the two generators
```

## Virtual clock and seeding

`Cluster` owns the clock. Time advances only inside `cluster.step()`, in fixed `tickMs` ticks, and
nothing in the model reads the wall clock. `useRunner` decides how many ticks to take per animation
frame, which is why a panel can run at 1x, 2x or 4x and produce the same result, and why every
elapsed figure on the page is labelled virtual.

Every random choice comes from `Prng` seeded per panel, so a seed fixes the order mix, the transient
processor faults, the redeliveries and the kill schedule. Two runs at one seed produce the same
summary; the self-check asserts exactly that.

## Constants mirrored from the services

`scripts/extract-config.mjs` reads the three `application.yml` files and `chaos/loadgen.py` and
writes `src/sim/config.generated.ts`: breaker windows and thresholds, time limits, the retry policy,
the deferred sweep timings, the Redis TTL, the outbox poll interval, the saga deadlines, the dead
letter caps, the seeded stock and the load mix. The simulation and the prose it renders read those
values, and `npm run config:check` fails when the generated file no longer matches the yml, so one
edit in the services cannot leave the page quietly wrong.

`scripts/extract-measured.mjs` does the same for evidence: it parses the recorded chaos summaries in
`chaos/evidence/` into `src/sim/measured.generated.ts`, including the raw text of each summary. Every
figure the page presents as measured is built from that module, and the self-check asserts each
number it renders appears in the summary it claims to come from. A figure typed by hand fails the
check. The permalink the hero prints beside the figures is read out of the history rather than kept
as a constant, so commit a recorded summary before running `npm run measured`. The generator reads
the commit that introduced the bytes the summary holds, so a later commit that touches the file
without changing them leaves the link where it was. The generator refuses while the file differs
from its committed version, and it needs the full history rather than a shallow clone.

## What this page does not simulate

The model covers the v1 mechanisms: the saga and its compensation, the transactional outbox and its
crash window, idempotent consumers, the stock check behind a breaker and a time limiter, the
write-through Redis cache with its TTL, the retry policy and the deferred payment queue.

It does not model the releases after v1, which exist only in the services and are documented in the
repository README: per step saga deadlines and the stuck order reaper, the order timeline endpoint,
the `order.payment_requested` re-drive, the dead letter shadow topics with replay and parking,
consumer lag and dead letter depth gauges, the `Idempotency-Key` header and its request
deduplication store, and the `/ops/overview` page. The page also never refuses a submission, because
the order service is not killed here, so its failed counter is narrower than the harness's.
