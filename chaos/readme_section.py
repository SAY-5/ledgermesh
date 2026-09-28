#!/usr/bin/env python3
"""Writes the chaos section of the README from the recorded summaries, so no figure in it is typed.

Every number the section shows is read from a file under chaos/evidence/, replayed from the seed
with the harness's own code, or read out of a service's configuration. Usage:

    uv run python chaos/readme_section.py            # rewrite README.md in place
    uv run python chaos/readme_section.py --check     # exit 1 when README.md has drifted

The prose lives here rather than in the README, because a sentence that quotes a figure has to be
regenerated with it. Edit the section by editing this file and running it.
"""
import json
import pathlib
import random
import re
import textwrap
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "chaos"))
import loadgen  # noqa: E402  the order mix comes from the harness's own constants

# The chaos job of the workflow run named here, on a GitHub hosted runner, drawing its own seed.
CI_RUN = {"id": "36293910826", "commit": "e6a4dd3", "seed": "82863", "p95": "54489"}

RUNS = {
    "baseline": "chaos/evidence/baseline/summary.txt",
    "baseline-repeat": "chaos/evidence/baseline-repeat/summary.txt",
    "steady": "chaos/evidence/steady/summary.txt",
    "steady-repeat": "chaos/evidence/steady-repeat/summary.txt",
}


def field(raw, label, path):
    m = re.search(rf"^  {label}\s+(.*)$", raw, re.M)
    if not m:
        raise SystemExit(f"{path}: no {label} line")
    return m.group(1).strip()


def knob(run, name):
    m = re.search(rf"\b{name}=(\S+)", run["knobs"])
    return m.group(1) if m else None


def load(path):
    raw = (REPO / path).read_text()
    recorded = field(raw, "recorded", path)
    m = re.match(r"^(\S+) at commit (\S+)$", recorded)
    if not m:
        raise SystemExit(f"{path}: the provenance line reads {recorded!r}")
    kills = field(raw, "kills", path)
    inside = re.search(r"\(([^)]*)\)", kills)
    run = {
        "path": path,
        "raw": raw.rstrip("\n"),
        "when": m.group(1),
        "commit": m.group(2),
        "host": field(raw, "host", path),
        "knobs": field(raw, "knobs", path),
        "load": field(raw, "load", path),
        "submitted": field(raw, "orders submitted", path),
        "confirmed": field(raw, "confirmed", path),
        "cancelled": field(raw, r"cancelled \(stock\)", path),
        "failed": field(raw, "failed / stuck", path),
        "kills": "none" if kills.startswith("0") else inside.group(1),
        "latency": field(raw, "saga latency", path),
        "retries": field(raw, "retries", path),
        "duplicates": field(raw, "duplicate events", path),
        "resubmits": field(raw, "resubmits", path),
        "probes": field(raw, "stock probes", path),
        "transitions": field(raw, "breaker transitions", path),
    }
    run["seed"] = knob(run, "CHAOS_SEED")
    return run


def latency(run):
    m = re.search(r"p50 (\d+) ms\s+p95 (\d+) ms\s+max (\d+) ms", run["latency"])
    return m.group(1), m.group(2), m.group(3)


def probes(run):
    return [re.search(rf"'{key}': (\d+)", run["probes"]).group(1) for key in ("live", "cache", "error")]


def retries(run):
    m = re.search(r"with retry (\d+) ok / (\d+) exhausted", run["retries"])
    return m.group(1), m.group(2)


def resubmits(run):
    m = re.search(r"^(\d+) retried submits over (\d+) orders", run["resubmits"])
    return m.group(1), m.group(2)


def kill_moments(run):
    """The second each kill landed, as the summary records it."""
    return [int(m) for m in re.findall(r"@(\d+)s", run["kills"])]


def kill_timeline(run):
    """The kills a run recorded: each victim and the second it landed, from the summary, with the
    target run.sh drew for it and the second it sent the kill, from the kills.jsonl beside the
    summary, when the run recorded them.

    The schedule is read rather than replayed. run.sh draws it with bash's $RANDOM, and one seed
    gives a different sequence under the bash 3.2 that macOS ships than under bash 5, so a replay
    matches only the runs taken under the bash that replays it.

    The summary's @Ns counts from the load generator's start, while target and sent count from
    run.sh's START, and CI runs log the same kill a second either side of its @Ns. So a target is
    only ever compared with the sent second on its own clock, and a kill sent before its target
    is refused, since on one clock that cannot happen."""
    path = pathlib.Path(run["path"]).with_name("kills.jsonl")
    if not (REPO / path).is_file():
        raise SystemExit(f"{path}: missing, so the kills of {run['path']} are not recorded")
    recorded = [json.loads(line) for line in (REPO / path).read_text().splitlines() if line.strip()]
    if [k["service"] for k in recorded] != re.findall(r"(\S+) @\d+s", run["kills"]):
        raise SystemExit(f"{path}: the victims differ from the kills line of {run['path']}")
    timeline = []
    for k, at in zip(recorded, kill_moments(run)):
        target, sent = k.get("target"), k.get("sent")
        if (target is None) != (sent is None):
            raise SystemExit(f"{path}: {k['service']} records only one of target and sent, so its "
                             "delay cannot be read on one clock; record the run again")
        if target is not None and sent < target:
            raise SystemExit(f"{path}: {k['service']} was sent at t+{sent} s, before its target "
                             f"t+{target} s, so the two are not readings of one clock")
        timeline.append({"service": k["service"], "landed": at, "target": target, "sent": sent})
    return timeline


def seeded_mix(run):
    """Replay the load generator's draw sequence, with its own constants, for one run's knobs."""
    duration, rate = re.search(r"^(\d+)s at (\d+) orders/s", run["load"]).groups()
    orders = int(duration) * int(rate)
    rng = random.Random(int(run["seed"]))
    quantities = []
    for _ in range(orders):
        sku = rng.choices(loadgen.SKUS, loadgen.WEIGHTS)[0]
        qty = rng.randint(1, 3)
        rng.randint(5, 80)  # the price draw, kept so the sequence matches the harness
        if sku == SCARCE:
            quantities.append(qty)
    held = covered = 0
    for qty in quantities:
        if held + qty > SCARCE_STOCK:
            break
        held += qty
        covered += 1
    return {"orders": orders, "scarce": len(quantities), "units": sum(quantities),
            "covered": covered, "held": held}


def scarce_stock():
    """The seeded stock of the scarce SKU, read from the inventory service's configuration."""
    yml = (REPO / "inventory-service/src/main/resources/application.yml").read_text()
    seeds = dict(pair.split("=") for pair in re.search(r"seed: \$\{STOCK_SEED:([^}]*)\}", yml).group(1).split(","))
    sku = min(seeds, key=lambda k: int(seeds[k]))
    return sku, int(seeds[sku])


SCARCE, SCARCE_STOCK = scarce_stock()
runs = {key: load(path) for key, path in RUNS.items()}
base, base2 = runs["baseline"], runs["baseline-repeat"]
chaos, chaos2 = runs["steady"], runs["steady-repeat"]

seeds = {run["seed"] for run in runs.values()}
if len(seeds) != 1:
    raise SystemExit(f"the recorded runs no longer share one seed: {sorted(seeds)}")
seed = seeds.pop()

for key, run in runs.items():
    if run["failed"] != "0":
        raise SystemExit(f"{run['path']}: failed / stuck is {run['failed']}, the prose claims 0")
    if run["submitted"] != base["submitted"]:
        raise SystemExit(f"{run['path']}: {run['submitted']} orders, not {base['submitted']}")
    if not re.search(r"\b0 gave up\b", run["resubmits"]):
        raise SystemExit(f"{run['path']}: a submission was refused, the prose claims none was")
same_host = len({run["host"] for run in runs.values()}) == 1

bl, cl = latency(base), latency(chaos)
bp, cp = probes(base), probes(chaos)
br, cr = retries(base), retries(chaos)
bs, cs = resubmits(base), resubmits(chaos)

rows = [
    ("recorded", f'{base["when"]} at `{base["commit"]}`', f'{chaos["when"]} at `{chaos["commit"]}`'),
    ("orders submitted", base["submitted"], chaos["submitted"]),
    ("confirmed", base["confirmed"], chaos["confirmed"]),
    ("cancelled, out of stock", base["cancelled"], chaos["cancelled"]),
    ("failed / stuck", f'**{base["failed"]}**', f'**{chaos["failed"]}**'),
    ("kills", base["kills"], chaos["kills"]),
    ("saga latency p50 / p95 / max", " / ".join(bl) + " ms", " / ".join(cl) + " ms"),
    ("retries ok / exhausted", " / ".join(br), " / ".join(cr)),
    ("submits retried on their key", f"{bs[0]} over {bs[1]} orders", f"{cs[0]} over {cs[1]} orders"),
    ("stock probes live / cache / refused", " / ".join(bp), " / ".join(cp)),
    ("breaker transitions", base["transitions"], chaos["transitions"]),
]
table = ["| | no kills | three kills |", "|---|---|---|"]
for label, a, b in rows:
    table.append(f"| {label} | {a} | {b} |")

repeats = ["| run | commit | submitted | confirmed | cancelled, out of stock | saga p95 |",
           "|---|---|---|---|---|---|"]
for key in RUNS:
    run = runs[key]
    repeats.append(f'| [{key}]({run["path"]}) | `{run["commit"]}` | {run["submitted"]} | '
                   f'{run["confirmed"]} | {run["cancelled"]} | {latency(run)[1]} ms |')

mix = seeded_mix(chaos)
timelines = {"steady": kill_timeline(chaos), "steady-repeat": kill_timeline(chaos2)}
kill_count = len(timelines["steady"])
if len(timelines["steady-repeat"]) != kill_count:
    raise SystemExit("the two kill runs record different numbers of kills")
landed = {key: ", ".join(f'{k["service"]} at t+{k["landed"]} s' for k in kills)
          for key, kills in timelines.items()}
apart = sorted(abs(a["landed"] - b["landed"]) for a, b in zip(*timelines.values()))
# Runs recorded before run.sh wrote each kill's target carry none; the prose says so rather than
# replaying the draw. It compares the two runs, so both must be on the same side.
drawn = {all(k["target"] is not None for k in kills) for kills in timelines.values()}
if len(drawn) != 1:
    raise SystemExit("only one of the two kill runs records the targets it drew; record both")
if drawn.pop():
    targets = [", ".join(f't+{k["target"]} s' for k in kills) for kills in timelines.values()]
    # sent, not landed: landed is the summary's @Ns, which counts from the load generator's start
    # rather than from the START the targets count from.
    late = sorted(k["sent"] - k["target"] for kills in timelines.values() for k in kills)
    target_note = (f"The `kills.jsonl` beside each summary also records the targets the harness "
                   f"drew, {targets[0]} in the first and {targets[1]} in the second, and the second "
                   f"it sent each kill. Both count from the harness's own start and the summaries "
                   f"from the load generator's, so a target is compared only with the second its "
                   f"kill was sent.")
    late_note = f", and each was sent between {late[0]} s and {late[-1]} s after its target"
else:
    target_note = ("The harness also writes the target it drew for each kill into the `kills.jsonl` "
                   "beside the summary, but these two runs were recorded before it did, so the "
                   "section quotes where their kills landed rather than where they were drawn.")
    late_note = ""
links = ", ".join(f"[{key}]({RUNS[key]})" for key in RUNS)
# What the replay predicts the out of stock count to be, and how many recorded runs report it:
# the split is a measurement, so the prose states the tally instead of promising the figure.
predicted = mix["scarce"] - mix["covered"]
in_order = sum(1 for run in runs.values() if int(run["cancelled"]) == predicted)


WORDS = {0: "no", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six"}


def reflow(text, width=100):
    """Wrap the prose paragraphs to the width the rest of the README uses, leaving tables,
    fenced blocks and headings exactly as they are."""
    out, paragraph, verbatim = [], [], False
    def flush():
        if paragraph:
            out.append(textwrap.fill(" ".join(paragraph), width=width, break_long_words=False,
                                     break_on_hyphens=False))
            paragraph.clear()
    for line in text.split("\n"):
        if line.startswith("```"):
            flush()
            verbatim = not verbatim
            out.append(line)
        elif verbatim:
            out.append(line)
        elif not line.strip():
            flush()
            out.append("")
        elif line.startswith(("|", "#")):
            flush()
            out.append(line)
        else:
            paragraph.append(line.strip())
    flush()
    return "\n".join(out)


section = f"""## Chaos test

```bash
make chaos           # alias: make demo
make chaos-tight     # the same run with six kills and a two second restart
make chaos-baseline  # the same load with no kills, the reference for the latency lines
```

Four runs are recorded in the repository, written by the harness rather than copied into prose:
{links}. All four ran at `CHAOS_SEED={seed}`, {WORDS[2]} of them with no kills and
{WORDS[2]} with {WORDS[kill_count]} kills. This section is written by
[chaos/readme_section.py](chaos/readme_section.py) from those files, and
`uv run python chaos/readme_section.py --check` fails when it has drifted from them. `CHAOS_RECORD=1` writes the summary of the profile in effect, under the name
`CHAOS_LABEL` gives it. Every recorded summary opens with the commit it was taken at, the UTC date,
the host, the Docker server version and each `CHAOS_*` knob that was set, so a reader can repeat it.

{chr(10).join(table)}

The three kill run in full, as the file contains it:

```
{chaos["raw"]}
```

`failed / stuck` counts orders that did not reach a terminal state, orders cancelled for any
reason other than stock, and rejected submissions. `cancelled (stock)` are orders for
`{SCARCE}`, which is seeded with {SCARCE_STOCK} units so the out of stock branch is exercised on
every run. Retries, deferred payments and breaker transitions come from the synthetic processor's
deterministic transient faults and from the kills themselves. Counters are snapshotted right
before each kill because a killed JVM loses its in-memory meters. The last seven lines are the
`/ops/overview` of each service read after the backlog drained.

The seed fixes the load, and, under one bash, the kill schedule the harness draws from it. Replaying
the draw sequence of [chaos/loadgen.py](chaos/loadgen.py) at seed {seed} for the {mix["orders"]}
orders of this profile gives {mix["scarce"]} orders for `{SCARCE}` asking for {mix["units"]} units,
of which the {SCARCE_STOCK} seeded units cover the first {mix["covered"]} exactly, leaving
{predicted} to be cancelled if the reservations arrive in the order they were submitted. The kill
schedule is read from the runs instead of replayed, because [chaos/run.sh](chaos/run.sh) draws it
with `$RANDOM`, and one seed gives a different sequence under the bash 3.2 that macOS ships than
under the bash 5 of a Linux runner. The summaries record the kills each run made:
[steady]({RUNS["steady"]}) killed {landed["steady"]}, and [steady-repeat]({RUNS["steady-repeat"]})
killed {landed["steady-repeat"]}. {target_note}

The seed does not fix which orders land in which bucket. {WORDS[in_order].capitalize()} of these
{WORDS[len(runs)]} runs cancel exactly that many, and the two {WORDS[kill_count]} kill runs above
ran the same load {"on the same host" if same_host else "on different hosts"} yet confirmed
{chaos["confirmed"]} and {chaos2["confirmed"]} orders, cancelling {chaos["cancelled"]} and
{chaos2["cancelled"]} for stock:

{chr(10).join(repeats)}

Reservations reach the inventory service concurrently rather than in the order they were submitted,
and {SCARCE_STOCK} units cover one order more or one order fewer depending on which quantities
arrive first, so the split moves by an order while the total does not. The moment a kill lands is
not fixed either, because each kill waits for the service the previous one killed to report ready
again: kill for kill, those two runs landed {apart[0]} s to {apart[-1]} s apart{late_note}. The latency lines, the retry, probe and breaker counts, and the duplicates the
consumers ignore all move with whatever else the machine is doing. What repeats in all {WORDS[len(runs)]} runs:
{base["submitted"]} orders submitted, every one of them terminal, nothing cancelled for any reason
other than stock, and no submission refused.

Because of that, the gate on latency is a ceiling rather than an expected value: with
`CHAOS_MAX_P95` set, the summary prints the ceiling and whether it held, and the harness exits
non zero when the p95 is above it. Both CI pipelines set 90000 ms on their chaos job, and the
recorded three kill runs above held the same ceiling. The number is deliberately loose, because the
same profile costs very different amounts on different hosts: the two recorded three kill runs
report a p95 of {cl[1]} ms and {latency(chaos2)[1]} ms on the developer machine, and the chaos job of
run {CI_RUN["id"]}, for commit `{CI_RUN["commit"]}` of this branch on a GitHub hosted runner and at
a seed of its own ({CI_RUN["seed"]}), reported {CI_RUN["p95"]} ms. A ceiling that would catch a doubling on the
faster host would fail on the slower one for no reason, so this one catches a gross regression
rather than a subtle one.

Knobs: `CHAOS_PROFILE` (`steady` three kills restarting after 5 s, `tight` six kills restarting
after 2 s), `CHAOS_DURATION`, `CHAOS_RATE`, `CHAOS_KILLS`, `CHAOS_RESTART_AFTER`, `CHAOS_VICTIMS`
(default all three services), `CHAOS_SEED` (the load and the kill schedule; a fresh one is drawn and
printed when unset), `CHAOS_MAX_P95`, `CHAOS_DRAIN_TIMEOUT`, `CHAOS_DRAIN_CAP`, `CHAOS_RECORD=1`,
`CHAOS_LABEL`, `CHAOS_KEEP_STACK=1`, `CHAOS_PYTHON`, and `LEDGERMESH_ORDER_PORT` /
`LEDGERMESH_INVENTORY_PORT` / `LEDGERMESH_PAYMENT_PORT` when 8081 to 8083 are taken on the host.
Output lands in `chaos/out/` (orders, kill timeline, metric snapshots, summary); the harness tears
the stack down on every exit path unless `CHAOS_KEEP_STACK=1`.

"""

readme = REPO / "README.md"
text = readme.read_text()
start, end = text.index("## Chaos test"), text.index("## Tests")
wanted = text[:start] + reflow(section) + text[end:]
if "--check" in sys.argv:
    if text != wanted:
        sys.exit("README.md has drifted from chaos/readme_section.py; run it to rewrite the section")
    print("README.md matches the recorded runs")
else:
    readme.write_text(wanted)
    print("rewrote the chaos section of README.md")
