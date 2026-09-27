// Written by web/scripts/extract-measured.mjs from the chaos summaries recorded under
// chaos/evidence/. Do not edit; run `npm run measured` after recording a run. `raw` is the
// summary text the figures were read from, so a hand written figure can be caught.
export const MEASURED = {
  "baseline": {
    "source": "chaos/evidence/baseline/summary.txt",
    "recordedAt": "2026-09-27T02:03:18Z",
    "commit": "c1c6fb3",
    "host": "Darwin 25.0.0 arm64, 10 cpu, 16.0 GiB; Docker server 29.2.1 (colima); Python 3.14.4",
    "knobs": "CHAOS_PROFILE=steady CHAOS_DURATION=60 CHAOS_RATE=20 CHAOS_KILLS=0 CHAOS_RESTART_AFTER=5 CHAOS_VICTIMS=inventory-service payment-service order-service CHAOS_SEED=3 CHAOS_DRAIN_TIMEOUT=180 CHAOS_DRAIN_CAP=600",
    "load": "60s at 20 orders/s, seed 3",
    "submitted": 1200,
    "confirmed": 1147,
    "cancelledStock": 53,
    "failedStuck": 0,
    "kills": [],
    "p50Ms": 874,
    "p95Ms": 4532,
    "maxMs": 14900,
    "p95CeilingMs": null,
    "p95CeilingHeld": null,
    "retriesWithRetry": 74,
    "retriesExhausted": 2,
    "deferred": 2,
    "duplicates": 0,
    "resubmits": 0,
    "retriedOrders": 0,
    "probes": {
      "live": 99,
      "cache": 2,
      "unknown": 0,
      "error": 0
    },
    "breakerTransitions": "none",
    "stuckOrders": 0,
    "raw": "LedgerMesh chaos summary\n  recorded             2026-09-27T02:03:18Z at commit c1c6fb3\n  host                 Darwin 25.0.0 arm64, 10 cpu, 16.0 GiB; Docker server 29.2.1 (colima); Python 3.14.4\n  knobs                CHAOS_PROFILE=steady CHAOS_DURATION=60 CHAOS_RATE=20 CHAOS_KILLS=0 CHAOS_RESTART_AFTER=5 CHAOS_VICTIMS=inventory-service payment-service order-service CHAOS_SEED=3 CHAOS_DRAIN_TIMEOUT=180 CHAOS_DRAIN_CAP=600\n  load                 60s at 20 orders/s, seed 3\n  orders submitted     1200\n  confirmed            1147\n  cancelled (stock)    53\n  failed / stuck       0\n  kills                0  (baseline, no kills)\n  saga latency         p50 874 ms   p95 4532 ms   max 14900 ms\n  breaker transitions  none\n  retries              with retry 74 ok / 2 exhausted, without retry 1073 ok / 0 failed\n  deferred payments    2\n  duplicate events     0 ignored by idempotent consumers\n  compensations        0 reservations released, 0 releases for orders that held nothing\n  resubmits            0 retried submits over 0 orders, 0 answered from the idempotency store (0 replays counted by the service), 0 gave up, 0 duplicate orders\n  stock probes         {'live': 99, 'cache': 2, 'unknown': 0, 'error': 0}\n  services             order-service UP, inventory-service UP, payment-service UP\n  consumer lag         worst 0 on order-service|inventory.rejected\n  dead letter depth    worst 0 on order-service inventory.rejected\n  parked records       worst 0 on order-service inventory.rejected\n  breaker states       inventory-service/redis CLOSED; order-service/inventory CLOSED; payment-service/processor CLOSED\n  in flight sagas      0\n  stuck orders         0\n"
  },
  "chaos": {
    "source": "chaos/evidence/steady/summary.txt",
    "recordedAt": "2026-09-27T02:06:26Z",
    "commit": "8a0922c",
    "host": "Darwin 25.0.0 arm64, 10 cpu, 16.0 GiB; Docker server 29.2.1 (colima); Python 3.14.4",
    "knobs": "CHAOS_PROFILE=steady CHAOS_DURATION=60 CHAOS_RATE=20 CHAOS_KILLS=3 CHAOS_RESTART_AFTER=5 CHAOS_VICTIMS=inventory-service payment-service order-service CHAOS_SEED=3 CHAOS_DRAIN_TIMEOUT=180 CHAOS_DRAIN_CAP=600 CHAOS_MAX_P95=90000",
    "load": "60s at 20 orders/s, seed 3",
    "submitted": 1200,
    "confirmed": 1147,
    "cancelledStock": 53,
    "failedStuck": 0,
    "kills": [
      {
        "service": "inventory-service",
        "atSeconds": 17
      },
      {
        "service": "order-service",
        "atSeconds": 37
      },
      {
        "service": "payment-service",
        "atSeconds": 52
      }
    ],
    "p50Ms": 17206,
    "p95Ms": 35109,
    "maxMs": 38902,
    "p95CeilingMs": 90000,
    "p95CeilingHeld": true,
    "retriesWithRetry": 81,
    "retriesExhausted": 0,
    "deferred": 0,
    "duplicates": 0,
    "resubmits": 1628,
    "retriedOrders": 246,
    "probes": {
      "live": 46,
      "cache": 31,
      "unknown": 1,
      "error": 24
    },
    "breakerTransitions": "order-service/inventory CLOSED->OPEN x1; order-service/inventory HALF_OPEN->OPEN x2; order-service/inventory OPEN->HALF_OPEN x2",
    "stuckOrders": 0,
    "raw": "LedgerMesh chaos summary\n  recorded             2026-09-27T02:06:26Z at commit 8a0922c\n  host                 Darwin 25.0.0 arm64, 10 cpu, 16.0 GiB; Docker server 29.2.1 (colima); Python 3.14.4\n  knobs                CHAOS_PROFILE=steady CHAOS_DURATION=60 CHAOS_RATE=20 CHAOS_KILLS=3 CHAOS_RESTART_AFTER=5 CHAOS_VICTIMS=inventory-service payment-service order-service CHAOS_SEED=3 CHAOS_DRAIN_TIMEOUT=180 CHAOS_DRAIN_CAP=600 CHAOS_MAX_P95=90000\n  load                 60s at 20 orders/s, seed 3\n  orders submitted     1200\n  confirmed            1147\n  cancelled (stock)    53\n  failed / stuck       0\n  kills                3  (inventory-service @17s, order-service @37s, payment-service @52s)\n  saga latency         p50 17206 ms   p95 35109 ms   max 38902 ms\n  p95 ceiling          90000 ms (CHAOS_MAX_P95) held\n  breaker transitions  order-service/inventory CLOSED->OPEN x1; order-service/inventory HALF_OPEN->OPEN x2; order-service/inventory OPEN->HALF_OPEN x2\n  retries              with retry 81 ok / 0 exhausted, without retry 1066 ok / 0 failed\n  deferred payments    0\n  duplicate events     0 ignored by idempotent consumers\n  compensations        0 reservations released, 0 releases for orders that held nothing\n  resubmits            1628 retried submits over 246 orders, 0 answered from the idempotency store (0 replays counted by the service), 0 gave up, 0 duplicate orders\n  stock probes         {'live': 46, 'cache': 31, 'unknown': 1, 'error': 24}\n  services             order-service UP, inventory-service UP, payment-service UP\n  consumer lag         worst 0 on order-service|inventory.rejected\n  dead letter depth    worst 0 on order-service inventory.rejected\n  parked records       worst 0 on order-service inventory.rejected\n  breaker states       inventory-service/redis CLOSED; order-service/inventory CLOSED; payment-service/processor CLOSED\n  in flight sagas      0\n  stuck orders         0\n"
  }
} as const;
