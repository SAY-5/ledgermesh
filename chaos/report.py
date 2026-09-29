#!/usr/bin/env python3
"""Chaos run bookkeeping: metric snapshots, drain wait, the final ledger and the summary.

Counters live in the JVM and reset when a container is killed, so the killer
takes a snapshot of the victim right before each kill and the report adds those
to the final scrape. Once the backlog has drained, `ledger` reads every order,
its timeline, its payment and its stock holds straight from the three databases
into final.json, so a failed run explains itself. The summary opens with the
provenance of the run (commit, time, host, Docker, profile and every knob in
effect) so a recorded summary can be re-derived, and audits that ledger: money
and stock must follow each order's outcome.

What a run guarantees: every submitted order reaches CONFIRMED or CANCELLED;
a confirmed order is paid once, for its amount, and holds its stock; a cancelled
order is charged nothing and holds nothing. An order is cancelled only for stock
or because its saga outlived a deadline (RESERVATION_TIMEOUT, PAYMENT_TIMEOUT),
and a deadline cancellation is reported as a class of its own. Exit code is non
zero when an order is stuck or cancelled for another reason, when a submission
was refused or placed twice, when the ledger breaks a money or stock rule or
cannot be read, or when CHAOS_MAX_P95 is set and the p95 saga latency exceeds it.
"""
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import time
import urllib.request

SERVICES = {
    "order-service": "http://localhost:" + os.environ.get("LEDGERMESH_ORDER_PORT", "8081"),
    "inventory-service": "http://localhost:" + os.environ.get("LEDGERMESH_INVENTORY_PORT", "8082"),
    "payment-service": "http://localhost:" + os.environ.get("LEDGERMESH_PAYMENT_PORT", "8083"),
}
TERMINAL = {"CONFIRMED", "CANCELLED"}
# the saga deadlines the reaper enforces; cancelling for one of these is compensated, not failed
DEADLINES = ("RESERVATION_TIMEOUT", "PAYMENT_TIMEOUT")
LINE = re.compile(r'^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+([-+eE.\d]+)$')
KNOBS = ["CHAOS_PROFILE", "CHAOS_DURATION", "CHAOS_RATE", "CHAOS_KILLS", "CHAOS_RESTART_AFTER",
         "CHAOS_VICTIMS", "CHAOS_SEED", "CHAOS_DRAIN_TIMEOUT", "CHAOS_DRAIN_CAP", "CHAOS_MAX_P95"]
SUMMARY_PATH = os.environ.get("CHAOS_SUMMARY", "chaos/out/summary.txt")


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
                    "ledgermesh_payments_deferred_total", "ledgermesh_payments_voided_total",
                    "ledgermesh_consumer_duplicates_total",
                    "ledgermesh_outbox_published_total", "ledgermesh_inventory_releases_total",
                    "ledgermesh_requests_replayed_total"):
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
    """Health, lag, dead letters, parked records, breakers and open sagas as reported now."""
    pages = {service: overview(service) for service in SERVICES}
    health = ", ".join(f"{s} {p.get('health', 'UNREACHABLE')}" for s, p in pages.items())
    lag = {k: v for p in pages.values() for k, v in p.get("consumerLag", {}).items()}
    depth = {f"{s} {k}": v for s, p in pages.items() for k, v in p.get("deadLetterDepth", {}).items()}
    parked = {f"{s} {k}": v for s, p in pages.items() for k, v in p.get("parkedDepth", {}).items()}
    breakers = {f"{s}/{k}": v for s, p in pages.items() for k, v in p.get("breakers", {}).items()}
    sagas = next((p["sagas"] for p in pages.values() if p.get("sagas")), {})
    worst_lag = max(lag.items(), key=lambda kv: kv[1], default=("none", 0))
    worst_depth = max(depth.items(), key=lambda kv: kv[1], default=("none", 0))
    worst_parked = max(parked.items(), key=lambda kv: kv[1], default=("none", 0))
    return [
        f"  services             {health}",
        f"  consumer lag         worst {worst_lag[1]} on {worst_lag[0]}",
        f"  dead letter depth    worst {worst_depth[1]} on {worst_depth[0]}",
        f"  parked records       worst {worst_parked[1]} on {worst_parked[0]}",
        "  breaker states       " + ("; ".join(f"{k} {v}" for k, v in sorted(breakers.items()))
                                     if breakers else "none"),
        f"  in flight sagas      {sagas.get('inFlight', 0)}",
        f"  stuck orders         {sagas.get('stuck', 0)}",
    ]


def sh(*cmd):
    try:
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return done.stdout.strip() if done.returncode == 0 else ""
    except Exception:  # noqa: BLE001
        return ""


def host_memory_gib():
    if platform.system() == "Darwin":
        raw = sh("sysctl", "-n", "hw.memsize")
        return int(raw) / 2 ** 30 if raw.isdigit() else None
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) / 2 ** 20
    except OSError:
        return None
    return None


def provenance_lines():
    """Where, when and with what this summary was produced, read from the environment."""
    commit = sh("git", "rev-parse", "--short", "HEAD") or "unknown"
    # Untracked files count as changes: a summary recorded beside a file that is not in the
    # commit cannot be re-derived from the commit the header names.
    dirty = " with uncommitted changes" if sh("git", "status", "--porcelain") else ""
    when = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    mem = host_memory_gib()
    docker = sh("docker", "version", "--format", "{{.Server.Version}}") or "unknown"
    context = sh("docker", "context", "show")
    host = (f"{platform.system()} {platform.release()} {platform.machine()}, "
            f"{os.cpu_count()} cpu, {mem:.1f} GiB" if mem else
            f"{platform.system()} {platform.release()} {platform.machine()}, {os.cpu_count()} cpu")
    knobs = " ".join(f"{k}={os.environ[k]}" for k in KNOBS if os.environ.get(k))
    return [
        f"  recorded             {when} at commit {commit}{dirty}",
        f"  host                 {host}; Docker server {docker}" + (f" ({context})" if context else "")
        + f"; Python {platform.python_version()}",
        f"  knobs                {knobs or 'defaults'}",
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


# the compose project run.sh starts; psql runs inside its postgres container
COMPOSE = ["docker", "compose", "-p", "ledgermesh", "-f", "deploy/docker-compose.yml"]


def rows(database, query):
    """The rows of one query as dicts, read with psql inside the stack's postgres container."""
    wrapped = f"select coalesce(json_agg(t), '[]'::json) from ({query}) t"
    done = subprocess.run(COMPOSE + ["exec", "-T", "postgres", "psql", "-U", "ledgermesh", "-d",
                                     database, "-At", "-v", "ON_ERROR_STOP=1", "-c", wrapped],
                          capture_output=True, text=True, timeout=60)
    if done.returncode != 0:
        raise RuntimeError(f"{database}: {done.stderr.strip() or done.stdout.strip()}")
    return json.loads(done.stdout)


def by_order(records):
    grouped = {}
    for r in records:
        grouped.setdefault(r.pop("order_id"), []).append(r)
    return grouped


def ledger(orders_path, out_path, settle=None):
    """Writes the final state of every submitted order, read from the databases rather than the
    APIs: the order row and its timeline, the payment the payment service holds for it and the
    stock holds the inventory ledger records for it, plus the stock rows and any order, payment or
    hold that belongs to no submitted order.

    A cancellation is terminal before it is compensated: the release and the void travel on
    order.cancelled after the order has turned CANCELLED, which is all the drain waits for. So a
    ledger that breaks a money or stock rule is read again every 2 s, and the one written is the
    first that keeps every rule, or the last read once `settle` seconds pass without the number
    of broken rules falling (or four times that in all)."""
    with open(orders_path) as fh:
        submitted = json.load(fh)["submitted"]
    started = last_progress = time.time()
    fewest = None
    while True:
        final = read_ledger(submitted)
        broken = len(audit(final))
        if fewest is None or broken < fewest:
            fewest, last_progress = broken, time.time()
        now = time.time()
        if not broken or not settle or now - last_progress >= settle or now - started >= settle * 4:
            break
        print(f"ledger: {broken} money or stock rules broken, waiting for compensations", flush=True)
        time.sleep(2)
    with open(out_path, "w") as fh:
        json.dump(final, fh, indent=1)
    print(f"ledger: {len(submitted)} orders written to {out_path}, {broken} money or stock rules "
          f"broken", flush=True)


def read_ledger(submitted):
    orders = {r["id"]: r for r in rows(
        "orders", "select id, status, reason, customer_id, amount::text as amount, created_at, "
                  "updated_at, deadline_at, redrives from orders")}
    items = by_order(rows("orders", "select order_id, sku, quantity from order_item"))
    timeline = by_order(rows(
        "orders", "select order_id, type, from_status, to_status, reason, occurred_at "
                  "from order_event order by id"))
    payments = {}
    for p in rows("payments", "select * from payment"):
        p["amount"] = f"{float(p['amount']):.2f}"
        p.pop("version", None)
        payments[p.pop("order_id")] = p
    holds = by_order(rows(
        "inventory", "select order_id, sku, quantity, state, created_at, released_at "
                     "from reservation order by id"))
    stock = rows("inventory", "select sku, available, reserved from stock_item order by sku")
    ids = [o["id"] for o in submitted]
    known = set(ids)
    final = {
        "orders": [{"id": oid, "order": orders.get(oid), "items": items.get(oid, []),
                    "timeline": timeline.get(oid, []), "payment": payments.get(oid),
                    "holds": holds.get(oid, [])} for oid in ids],
        "stock": stock,
        "strays": {
            "orders": sorted(set(orders) - known),
            "payments": {k: v for k, v in payments.items() if k not in known},
            "holds": {k: v for k, v in holds.items() if k not in known},
        },
    }
    return final


def audit(final):
    """Checks the ledger against the money and stock rules. A confirmed order holds one authorized
    payment for its amount and exactly the stock it asked for. Any other order holds no authorized
    payment, no open one either once it is cancelled (an open payment can still be authorized),
    and no stock. An order cancelled for anything but stock sent order.cancelled, and both of its
    readers must have answered it: the payment voided or declined, and a released row or marker in
    the inventory ledger, since until then a late reservation could still take stock or money. The
    stock rows agree with the holds, and nothing is held or paid for an order that was never
    submitted. Returns (order or sku, problem) pairs."""
    problems = []
    reserved = {}

    def holding(hs):
        held = {}
        for h in hs:
            if h["state"] == "RESERVED":
                held[h["sku"]] = held.get(h["sku"], 0) + h["quantity"]
                reserved[h["sku"]] = reserved.get(h["sku"], 0) + h["quantity"]
        return held

    for rec in final["orders"]:
        o, pay = rec["order"], rec["payment"]
        held = holding(rec["holds"])
        if o is None:
            problems.append((rec["id"], "the order service has no row for it"))
            continue
        paid = pay["status"] if pay else "none"
        if o["status"] == "CONFIRMED":
            wanted = {}
            for i in rec["items"]:
                wanted[i["sku"]] = wanted.get(i["sku"], 0) + i["quantity"]
            if paid != "AUTHORIZED":
                problems.append((rec["id"], f"confirmed with payment {paid}"))
            elif float(pay["amount"]) != float(o["amount"]):
                problems.append((rec["id"], f"confirmed at {o['amount']} but paid {pay['amount']}"))
            if held != wanted:
                problems.append((rec["id"], f"confirmed holding {held or 'nothing'} for {wanted}"))
            continue
        outcome = " ".join(x for x in (o["status"], o["reason"]) if x)
        compensated = o["status"] == "CANCELLED" and o["reason"] != "OUT_OF_STOCK"
        if paid == "AUTHORIZED":
            problems.append((rec["id"], f"{outcome} but charged {pay['amount']}"))
        elif paid in ("NEW", "DEFERRED") and o["status"] == "CANCELLED":
            problems.append((rec["id"], f"{outcome} with its payment still {paid}"))
        elif compensated and paid not in ("VOIDED", "DECLINED"):
            problems.append((rec["id"], f"{outcome} but the payment service never voided it "
                                        f"(payment {paid})"))
        if held and o["status"] == "CANCELLED":
            problems.append((rec["id"], f"{outcome} still holding {held}"))
        elif compensated and not rec["holds"]:
            problems.append((rec["id"], f"{outcome} but inventory never released it"))
    for oid, hs in final["strays"]["holds"].items():
        if holding(hs):
            problems.append((oid, "stock held for an order never submitted"))
    for s in final["stock"]:
        if s["reserved"] != reserved.get(s["sku"], 0):
            problems.append((s["sku"], f"stock row reserves {s['reserved']}, the holds add up to "
                                       f"{reserved.get(s['sku'], 0)}"))
    for oid in final["strays"]["orders"]:
        problems.append((oid, "an order no submission returned"))
    for oid, p in final["strays"]["payments"].items():
        problems.append((oid, f"a {p['status']} payment for an order never submitted"))
    return problems


def story(rec):
    """One order on one line: outcome, payment, holds and its timeline in seconds from creation."""
    o, pay = rec["order"], rec["payment"]
    start = parse_ts(o["created_at"])
    steps = " ".join(
        f"{e['type']}{'->' + e['to_status'] if e['to_status'] else ' ignored'}"
        f"@{parse_ts(e['occurred_at']) - start:.1f}s" for e in rec["timeline"])
    holds = ", ".join(f"{h['sku']} x{h['quantity']} {h['state']}" for h in rec["holds"]) or "none"
    payment = f"{pay['status']} {pay['amount']}" if pay else "none"
    outcome = " ".join(x for x in (o["status"], o["reason"]) if x)
    return f"    {rec['id']} {outcome}; payment {payment}; holds {holds}; {steps}"


def summary(orders_path, snapshots_path, kills_path, final_path=None):
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
    deadline = {}
    for s, r in states.values():
        if s == "CANCELLED" and r in DEADLINES:
            deadline[r] = deadline.get(r, 0) + 1
    cancelled_other = sum(1 for s, r in states.values()
                          if s == "CANCELLED" and r != "OUT_OF_STOCK" and r not in DEADLINES)
    stuck = sum(1 for s, _ in states.values() if s not in TERMINAL)
    # one accepted submit per key: a retried key that placed two orders would show up here
    duplicate_orders = len(submitted) - len({o["id"] for o in submitted})
    failed = stuck + cancelled_other + len(run["submitErrors"]) + duplicate_orders

    final, problems, unreadable = None, [], None
    if final_path:
        try:
            with open(final_path) as fh:
                final = json.load(fh)
            problems = audit(final)
        except (OSError, ValueError, KeyError, TypeError) as e:
            unreadable = f"{final_path}: {e}"

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
    deferred = voided = duplicates = releases = release_noops = replayed = 0
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
            elif name == "ledgermesh_payments_voided_total":
                voided += int(value)
            elif name == "ledgermesh_consumer_duplicates_total":
                duplicates += int(value)
            elif name == "ledgermesh_inventory_releases_total":
                if label(labels, "result") in ("", "released"):
                    releases += int(value)
                else:
                    release_noops += int(value)
            elif name == "ledgermesh_requests_replayed_total":
                replayed += int(value)

    p50 = statistics.median(latencies) if latencies else 0
    p95 = percentile(latencies, 95)
    pmax = max(latencies) if latencies else 0
    start = run["startedAt"]
    timeline = ", ".join(f"{k['service']} @{k['at'] - start:.0f}s" for k in kills)
    ceiling = os.environ.get("CHAOS_MAX_P95")
    breached = bool(ceiling) and p95 > float(ceiling)

    lines = ["LedgerMesh chaos summary"]
    lines += provenance_lines()
    lines += [
        f"  load                 {run['duration']:.0f}s at {run['rate']:.0f} orders/s"
        + (f", seed {run['seed']}" if "seed" in run else ""),
        f"  orders submitted     {len(submitted)}",
        f"  confirmed            {confirmed}",
        f"  cancelled (stock)    {cancelled_stock}",
        f"  cancelled (deadline) {sum(deadline.values())}"
        + (f"  ({deadline_detail(deadline, final)})" if deadline else ""),
        f"  failed / stuck       {failed}",
        f"  kills                {len(kills)}  ({timeline})" if kills else "  kills                0  (baseline, no kills)",
        f"  saga latency         p50 {p50:.0f} ms   p95 {p95:.0f} ms   max {pmax:.0f} ms",
    ]
    if ceiling:
        lines.append(f"  p95 ceiling          {float(ceiling):.0f} ms (CHAOS_MAX_P95) "
                     + ("BREACHED" if breached else "held"))
    lines += [
        "  breaker transitions  " + ("; ".join(sorted(transitions)) if transitions else "none"),
        f"  retries              with retry {retries['successful_with_retry']} ok / "
        f"{retries['failed_with_retry']} exhausted, without retry {retries['successful_without_retry']} ok / "
        f"{retries['failed_without_retry']} failed",
        f"  deferred payments    {deferred}",
        f"  duplicate events     {duplicates} ignored by idempotent consumers",
        f"  compensations        {releases} reservations released, {release_noops} releases for orders "
        f"that held nothing, {voided} payments voided",
        f"  resubmits            {run.get('resubmits', 0)} retried submits over "
        f"{run.get('retriedOrders', 0)} orders, {run.get('replayedAnswers', 0)} answered from the "
        f"idempotency store ({replayed} replays counted by the service), "
        f"{len(run['submitErrors'])} gave up, {duplicate_orders} duplicate orders",
        f"  stock probes         {run['stockProbes']}",
    ]
    lines += overview_lines()
    if final_path:
        lines += ledger_lines(final, problems, unreadable, final_path)
    text = "\n".join(lines)
    print(text)
    os.makedirs(os.path.dirname(SUMMARY_PATH) or ".", exist_ok=True)
    with open(SUMMARY_PATH, "w") as fh:
        fh.write(text + "\n")
    return failed + len(problems) + (1 if unreadable else 0) + (1 if breached else 0)


def deadline_detail(counts, final):
    """The reasons behind the deadline cancellations and, from the ledger, what their payments
    and stock holds ended as."""
    text = ", ".join(f"{reason} {n}" for reason, n in sorted(counts.items()))
    if not final:
        return text
    payments, held = {}, 0
    for rec in final["orders"]:
        o = rec["order"]
        if o and o["status"] == "CANCELLED" and o["reason"] in DEADLINES:
            state = rec["payment"]["status"] if rec["payment"] else "none"
            payments[state] = payments.get(state, 0) + 1
            held += sum(1 for h in rec["holds"] if h["state"] == "RESERVED")
    paid = ", ".join(f"{state} {n}" for state, n in sorted(payments.items()))
    return f"{text}; payments {paid}; holds left {held}"


def ledger_lines(final, problems, unreadable, final_path, shown=20):
    """The audit of the final ledger, then every order that did not end confirmed or cancelled
    for stock and every order the audit flagged, one line each, so a red run names its orders."""
    if unreadable:
        return [f"  ledger               unreadable, so money and stock were not checked ({unreadable})"]
    lines = [f"  ledger               {len(problems)} money or stock problems in {final_path}"]
    lines += [f"    {key}: {text}" for key, text in problems[:shown]]
    flagged = {key for key, _ in problems}
    odd = [rec for rec in final["orders"] if rec["order"] and (
        rec["id"] in flagged or not (rec["order"]["status"] == "CONFIRMED" or (
            rec["order"]["status"] == "CANCELLED" and rec["order"]["reason"] == "OUT_OF_STOCK")))]
    lines.append(f"  orders to read       {len(odd)} neither confirmed nor cancelled for stock, "
                 "or flagged above" + (":" if odd else ""))
    lines += [story(rec) for rec in odd[:shown]]
    if len(odd) > shown:
        lines.append(f"    and {len(odd) - shown} more in {final_path}")
    return lines


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
    elif cmd == "ledger":
        ledger(sys.argv[2], sys.argv[3], float(sys.argv[4]) if len(sys.argv) > 4 else None)
    elif cmd == "summary":
        sys.exit(1 if summary(*sys.argv[2:6]) else 0)
    else:
        sys.exit(f"unknown command {cmd}")
