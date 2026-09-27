#!/usr/bin/env python3
"""Submits orders at a fixed rate and records every order id it was given.

Standard library only. Every logical order carries one Idempotency-Key; a
submit that gets no answer (connection refused, reset, timeout, 5xx) is
retried with the same key until the order service answers or the retry
budget is spent, so a kill of the order service costs latency, not orders.
Also polls the order service's stock endpoint so the synchronous inventory
path (breaker + time limiter) is exercised while the inventory service is
being killed.
"""
import argparse
import http.client
import json
import random
import threading
import time
import urllib.error
import urllib.request
import uuid

SKUS = ["SKU-ALPHA", "SKU-BRAVO", "SKU-CHARLIE", "SKU-SCARCE"]
WEIGHTS = [40, 30, 25, 5]
RETRY_STATUSES = {502, 503, 504}


def post_json(url, payload, key, timeout=5.0):
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json", "Idempotency-Key": key}
    req = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read()), resp.headers.get("Idempotent-Replay", "false") == "true"


def get_json(url, timeout=3.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


def retryable(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in RETRY_STATUSES
    return isinstance(error, (urllib.error.URLError, http.client.HTTPException, TimeoutError,
                              ConnectionError, OSError))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost:8081")
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--out", default="chaos/out/orders.json")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--retry-seconds", type=float, default=90.0,
                        help="how long one order keeps retrying its key before it counts as an error")
    parser.add_argument("--retry-interval", type=float, default=1.0)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    submitted, errors = [], []
    lock = threading.Lock()
    stop = threading.Event()
    stock_calls = {"live": 0, "cache": 0, "unknown": 0, "error": 0}
    counters = {"resubmits": 0, "retriedOrders": 0, "replayedAnswers": 0}

    def submit(seq, sku, qty, price):
        key = str(uuid.uuid4())
        payload = {
            "customerId": f"cust-{seq % 250}",
            "items": [{"sku": sku, "quantity": qty, "unitPrice": f"{price}.00"}],
        }
        t0 = time.time()
        attempts = 0
        while True:
            attempts += 1
            try:
                order, replayed = post_json(f"{args.base}/orders", payload, key)
            except Exception as e:  # noqa: BLE001
                if retryable(e) and time.time() - t0 < args.retry_seconds:
                    time.sleep(args.retry_interval)
                    continue
                with lock:
                    errors.append({"seq": seq, "key": key, "attempts": attempts,
                                   "error": str(e), "at": t0})
                return
            with lock:
                submitted.append({"id": order["id"], "key": key, "sku": sku, "quantity": qty,
                                  "submittedAt": t0, "acceptedAt": time.time(),
                                  "attempts": attempts, "replayed": replayed})
                if attempts > 1:
                    counters["resubmits"] += attempts - 1
                    counters["retriedOrders"] += 1
                if replayed:
                    counters["replayedAnswers"] += 1
            return

    def probe_stock():
        while not stop.is_set():
            try:
                view = get_json(f"{args.base}/stock/SKU-ALPHA")
                stock_calls[view.get("source", "unknown")] += 1
            except Exception:  # noqa: BLE001
                stock_calls["error"] += 1
            stop.wait(0.5)

    prober = threading.Thread(target=probe_stock, daemon=True)
    prober.start()

    interval = 1.0 / args.rate
    start = time.time()
    seq = 0
    threads = []
    while time.time() - start < args.duration:
        # the order mix is drawn on this thread so the sequence depends on the seed alone
        sku = rng.choices(SKUS, WEIGHTS)[0]
        qty = rng.randint(1, 3)
        price = rng.randint(5, 80)
        t = threading.Thread(target=submit, args=(seq, sku, qty, price), daemon=True)
        t.start()
        threads.append(t)
        seq += 1
        next_at = start + seq * interval
        delay = next_at - time.time()
        if delay > 0:
            time.sleep(delay)
    deadline = time.time() + args.retry_seconds + 15
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.time()))
    stop.set()
    prober.join(timeout=2)

    result = {
        "rate": args.rate,
        "duration": args.duration,
        "seed": args.seed,
        "retrySeconds": args.retry_seconds,
        "startedAt": start,
        "finishedAt": time.time(),
        "submitted": submitted,
        "submitErrors": errors,
        "stockProbes": stock_calls,
        **counters,
    }
    with open(args.out, "w") as fh:
        json.dump(result, fh)
    print(f"loadgen: {len(submitted)} orders submitted, {len(errors)} submit errors, "
          f"{counters['resubmits']} resubmits over {counters['retriedOrders']} orders, "
          f"{counters['replayedAnswers']} answered from the idempotency store, "
          f"stock probes {stock_calls}")


if __name__ == "__main__":
    main()
