#!/usr/bin/env python3
"""Chaos run bookkeeping: metric snapshots, drain wait and the final summary.

Counters live in the JVM and reset when a container is killed, so the killer
takes a snapshot of the victim right before each kill and the report adds those
to the final scrape. Exit code is non zero when any order failed or is stuck.
"""
import json
import re
import statistics
import sys
import time
import urllib.request

SERVICES = {
    "order-service": "http://localhost:8081",
    "inventory-service": "http://localhost:8082",
    "payment-service": "http://localhost:8083",
}
TERMINAL = {"CONFIRMED", "CANCELLED"}
LINE = re.compile(r'^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+([-+eE.\d]+)$')


def fetch(url, timeout=3.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode()


def scrape(service):
    counters = {}
    try:
        text = fetch(f"{SERVICES[service]}/actuator/prometheus")
    except Exception:  # noqa: BLE001
        return counters
    for line in text.splitlines():
        m = LINE.match(line)
        if not m:
            continue
        name, labels, value = m.group(1), m.group(2) or "", float(m.group(3))
        if name in ("ledgermesh_breaker_transitions_total", "resilience4j_retry_calls_total",
                    "ledgermesh_payments_deferred_total", "ledgermesh_consumer_duplicates_total",
                    "ledgermesh_outbox_published_total", "ledgermesh_inventory_releases_total"):
            counters[name + labels] = value
    return counters


def snapshot(service, path):
    with open(path, "a") as fh:
        fh.write(json.dumps({"service": service, "at": time.time(), "counters": scrape(service)}) + "\n")


def overview(service):
    """The /ops/overview page of one service, or an empty dict when it cannot be reached."""
    try:
        return json.loads(fetch(f"{SERVICES[service]}/ops/overview"))
    except Exception:  # noqa: BLE001
        return {}


def overview_lines():
    """Health, lag, dead letters, breakers and open sagas as the services report them now."""
    pages = {service: overview(service) for service in SERVICES}
    health = ", ".join(f"{s} {p.get('health', 'UNREACHABLE')}" for s, p in pages.items())
    lag = {f"{s}/{k}": v for s, p in pages.items() for k, v in p.get("consumerLag", {}).items()}
    depth = {f"{s}/{k}": v for s, p in pages.items() for k, v in p.get("deadLetterDepth", {}).items()}
    breakers = {f"{s}/{k}": v for s, p in pages.items() for k, v in p.get("breakers", {}).items()}
    sagas = next((p["sagas"] for p in pages.values() if p.get("sagas")), {})
    worst_lag = max(lag.items(), key=lambda kv: kv[1], default=("none", 0))
    return [
        f"  services             {health}",
        f"  consumer lag         {sum(lag.values())} total, worst {worst_lag[1]} on {worst_lag[0]}",
        f"  dead letter depth    {sum(depth.values())} waiting across {len(depth)} topics",
        "  breaker states       " + ("; ".join(f"{k} {v}" for k, v in sorted(breakers.items()))
                                     if breakers else "none"),
        f"  in flight sagas      {sagas.get('inFlight', 0)}",
        f"  stuck orders         {sagas.get('stuck', 0)}",
    ]


def order(order_id):
    return json.loads(fetch(f"{SERVICES['order-service']}/orders/{order_id}"))


def wait_drain(orders_path, stall_window, hard_cap=None):
    """Wait for every submitted order to settle. Gives up only after `stall_window` seconds
    without any order reaching a terminal state, or after `hard_cap` seconds in total."""
    with open(orders_path) as fh:
        ids = [o["id"] for o in json.load(fh)["submitted"]]
    pending = set(ids)
    started = time.time()
    last_progress = started
    hard_deadline = started + (hard_cap if hard_cap else stall_window * 4)
    while pending and time.time() < hard_deadline and time.time() - last_progress < stall_window:
        before = len(pending)
        for oid in list(pending):
            try:
                if order(oid)["status"] in TERMINAL:
                    pending.discard(oid)
            except Exception:  # noqa: BLE001
                pass
        if len(pending) < before:
            last_progress = time.time()
        if pending:
            print(f"drain: {len(pending)} orders still open", flush=True)
            time.sleep(2)
    return len(pending)


def label(labels, key):
    m = re.search(key + r'="([^"]*)"', labels)
    return m.group(1) if m else ""


def summary(orders_path, snapshots_path, kills_path):
    with open(orders_path) as fh:
        run = json.load(fh)
    submitted = run["submitted"]
    states = {}
    latencies = []
    for o in submitted:
        info = order(o["id"])
        states[o["id"]] = (info["status"], info.get("reason"))
        if info["status"] in TERMINAL:
            created = parse_ts(info["createdAt"])
            updated = parse_ts(info["updatedAt"])
            latencies.append((updated - created) * 1000.0)
    confirmed = sum(1 for s, _ in states.values() if s == "CONFIRMED")
    cancelled_stock = sum(1 for s, r in states.values() if s == "CANCELLED" and r == "OUT_OF_STOCK")
    cancelled_other = sum(1 for s, r in states.values() if s == "CANCELLED" and r != "OUT_OF_STOCK")
    stuck = sum(1 for s, _ in states.values() if s not in TERMINAL)
    failed = stuck + cancelled_other + len(run["submitErrors"])

    totals = {}
    for service in SERVICES:
        totals[service] = {}
    if snapshots_path:
        try:
            with open(snapshots_path) as fh:
                for line in fh:
                    snap = json.loads(line)
                    for k, v in snap["counters"].items():
                        totals[snap["service"]][k] = totals[snap["service"]].get(k, 0.0) + v
        except FileNotFoundError:
            pass
    for service in SERVICES:
        for k, v in scrape(service).items():
            totals[service][k] = totals[service].get(k, 0.0) + v

    kills = []
    try:
        with open(kills_path) as fh:
            kills = [json.loads(line) for line in fh if line.strip()]
    except FileNotFoundError:
        pass

    transitions = []
    retries = {"successful_with_retry": 0, "failed_with_retry": 0, "successful_without_retry": 0,
               "failed_without_retry": 0}
    deferred = duplicates = releases = 0
    for service, counters in totals.items():
        for key, value in counters.items():
            name, _, labels = key.partition("{")
            labels = "{" + labels if labels else ""
            if name == "ledgermesh_breaker_transitions_total" and value > 0:
                transitions.append(f"{service}/{label(labels, 'name')} "
                                   f"{label(labels, 'from')}->{label(labels, 'to')} x{int(value)}")
            elif name == "resilience4j_retry_calls_total":
                retries[label(labels, "kind")] = retries.get(label(labels, "kind"), 0) + int(value)
            elif name == "ledgermesh_payments_deferred_total":
                deferred += int(value)
            elif name == "ledgermesh_consumer_duplicates_total":
                duplicates += int(value)
            elif name == "ledgermesh_inventory_releases_total":
                releases += int(value)

    p50 = statistics.median(latencies) if latencies else 0
    p95 = percentile(latencies, 95)
    pmax = max(latencies) if latencies else 0
    start = run["startedAt"]
    timeline = ", ".join(f"{k['service']} @{k['at'] - start:.0f}s" for k in kills)

    lines = [
        "LedgerMesh chaos summary",
        f"  load                 {run['duration']:.0f}s at {run['rate']:.0f} orders/s",
        f"  orders submitted     {len(submitted)}",
        f"  confirmed            {confirmed}",
        f"  cancelled (stock)    {cancelled_stock}",
        f"  failed / stuck       {failed}",
        f"  kills                {len(kills)}  ({timeline})",
        f"  saga latency         p50 {p50:.0f} ms   p95 {p95:.0f} ms   max {pmax:.0f} ms",
        "  breaker transitions  " + ("; ".join(sorted(transitions)) if transitions else "none"),
        f"  retries              with retry {retries['successful_with_retry']} ok / "
        f"{retries['failed_with_retry']} exhausted, without retry {retries['successful_without_retry']} ok / "
        f"{retries['failed_without_retry']} failed",
        f"  deferred payments    {deferred}",
        f"  duplicate events     {duplicates} ignored by idempotent consumers",
        f"  stock probes         {run['stockProbes']}",
    ]
    lines += overview_lines()
    text = "\n".join(lines)
    print(text)
    with open("chaos/out/summary.txt", "w") as fh:
        fh.write(text + "\n")
    return failed


def percentile(values, p):
    if not values:
        return 0
    ordered = sorted(values)
    k = (len(ordered) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def parse_ts(value):
    from datetime import datetime, timezone
    value = value.replace("Z", "+00:00")
    if "." in value:
        head, tail = value.split(".", 1)
        frac, _, tz = tail.partition("+")
        value = f"{head}.{frac[:6].ljust(6, '0')}+{tz}" if tz else f"{head}.{frac[:6].ljust(6, '0')}"
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "snapshot":
        snapshot(sys.argv[2], sys.argv[3])
    elif cmd == "drain":
        sys.exit(1 if wait_drain(sys.argv[2], float(sys.argv[3]), float(sys.argv[4]) if len(sys.argv) > 4 else None) else 0)
    elif cmd == "summary":
        sys.exit(1 if summary(sys.argv[2], sys.argv[3], sys.argv[4]) else 0)
    else:
        sys.exit(f"unknown command {cmd}")
