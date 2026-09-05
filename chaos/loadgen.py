#!/usr/bin/env python3
"""Submits orders at a fixed rate and records every order id it was given.

Standard library only. Also polls the order service's stock endpoint so the
synchronous inventory path (breaker + time limiter) is exercised while the
inventory service is being killed.
"""
import argparse
import json
import random
import threading
import time
import urllib.error
import urllib.request

SKUS = ["SKU-ALPHA", "SKU-BRAVO", "SKU-CHARLIE", "SKU-SCARCE"]
WEIGHTS = [40, 30, 25, 5]


def post_json(url, payload, timeout=5.0):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def get_json(url, timeout=3.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost:8081")
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--out", default="chaos/out/orders.json")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    submitted, errors = [], []
    lock = threading.Lock()
    stop = threading.Event()
    stock_calls = {"live": 0, "cache": 0, "unknown": 0, "error": 0}

    def submit(seq):
        sku = rng.choices(SKUS, WEIGHTS)[0]
        qty = rng.randint(1, 3)
        payload = {
            "customerId": f"cust-{seq % 250}",
            "items": [{"sku": sku, "quantity": qty, "unitPrice": f"{rng.randint(5, 80)}.00"}],
        }
        t0 = time.time()
        try:
            order = post_json(f"{args.base}/orders", payload)
            with lock:
                submitted.append({"id": order["id"], "sku": sku, "quantity": qty, "submittedAt": t0})
        except Exception as e:  # noqa: BLE001
            with lock:
                errors.append({"seq": seq, "error": str(e), "at": t0})

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
        t = threading.Thread(target=submit, args=(seq,), daemon=True)
        t.start()
        threads.append(t)
        seq += 1
        next_at = start + seq * interval
        delay = next_at - time.time()
        if delay > 0:
            time.sleep(delay)
    for t in threads:
        t.join(timeout=10)
    stop.set()
    prober.join(timeout=2)

    result = {
        "rate": args.rate,
        "duration": args.duration,
        "startedAt": start,
        "finishedAt": time.time(),
        "submitted": submitted,
        "submitErrors": errors,
        "stockProbes": stock_calls,
    }
    with open(args.out, "w") as fh:
        json.dump(result, fh)
    print(f"loadgen: {len(submitted)} orders submitted, {len(errors)} submit errors, stock probes {stock_calls}")


if __name__ == "__main__":
    main()
