/* Console self-check for the simulation. Run: node src/sim/selfcheck.ts */
import { Cluster, formatSummary } from "./cluster.ts";
import { Topics } from "./events.ts";
import { LoadGenerator, planKills } from "./loadgen.ts";
import {
  BASELINE_RUN,
  CHAOS_RUN,
  allFigures,
  counterFigures,
  headlineFigures,
  tokenInSummary,
} from "./measured.ts";

let failures = 0;

function assert(cond: boolean, msg: string): void {
  if (!cond) {
    console.error("FAIL " + msg);
    failures++;
  } else {
    console.log("ok   " + msg);
  }
}

// 1. a 300 order run with a service killed mid way ends with zero failed or stuck orders
{
  const cluster = new Cluster({ seed: 11, stockSeed: { "SKU-ALPHA": 100000, "SKU-BRAVO": 100000, "SKU-CHARLIE": 100000, "SKU-SCARCE": 12 } });
  const load = new LoadGenerator({ rate: 20, durationMs: 15_000 });
  let killed = false;
  while (!load.finished(cluster.now)) {
    load.tick(cluster);
    if (!killed && cluster.now >= 7_000) {
      cluster.kill("inventory-service", 5000);
      killed = true;
    }
    cluster.step();
  }
  const left = cluster.drain(120_000);
  const s = cluster.stats();
  console.log(formatSummary(s, 15, 20));
  assert(s.submitted === 300, `submitted 300 orders (got ${s.submitted})`);
  assert(left === 0 && s.failed === 0, `0 failed / stuck after a kill (got ${s.failed}, open ${left})`);
  assert(s.confirmed + s.cancelledStock === 300, "every order confirmed or out of stock");
  assert(s.cancelledStock > 0, `out of stock branch exercised (${s.cancelledStock} cancelled)`);
  assert(s.stockProbes.cache > 0, `stock probes served from cache while inventory was down (${s.stockProbes.cache})`);
  assert(
    s.breakerTransitions.some((t) => t.startsWith("order-service/inventory CLOSED->OPEN")),
    "inventory breaker opened",
  );
}

// 2. full length chaos run with three kills, the shape of the README block
{
  const cluster = new Cluster({ seed: 7 });
  const load = new LoadGenerator({ rate: 20, durationMs: 60_000 });
  const plan = planKills(60_000, 3, 7);
  let next = 0;
  while (!load.finished(cluster.now)) {
    load.tick(cluster);
    if (next < plan.length && cluster.now >= plan[next].at) {
      const last = cluster.kills[cluster.kills.length - 1];
      if (!last || cluster.now >= last.readyAt) {
        cluster.kill(plan[next].service, 5000);
        next++;
      }
    }
    cluster.step();
  }
  const left = cluster.drain(180_000);
  const s = cluster.stats();
  console.log(formatSummary(s, 60, 20));
  assert(s.submitted === 1200, `submitted 1200 orders (got ${s.submitted})`);
  assert(s.kills.length === 3, "three kills");
  assert(left === 0 && s.failed === 0, `0 failed / stuck across three kills (got ${s.failed}, open ${left})`);
  assert(s.retries.successful_with_retry > 0, `transient faults retried (${s.retries.successful_with_retry})`);
  assert(
    s.breakerTransitions.some((t) => t.includes("HALF_OPEN->CLOSED")),
    "inventory breaker closed again after restart",
  );
  assert(s.duplicates > 0, `kills produced redeliveries that were ignored (${s.duplicates})`);
}

// 3. a duplicate delivery is ignored and reserves stock once
{
  const cluster = new Cluster({ seed: 3, trace: true, redeliverChance: 0 });
  cluster.submit("cust-1", [{ sku: "SKU-BRAVO", quantity: 2, unitPrice: 10 }]);
  cluster.runFor(400);
  const before = cluster.inventory.stock.get("SKU-BRAVO")!.available;
  assert(before === 99998, `first delivery reserved two units (${before})`);
  assert(cluster.injectDuplicate(Topics.ORDER_CREATED, "inventory-service"), "duplicate injected");
  cluster.runFor(400);
  const after = cluster.inventory.stock.get("SKU-BRAVO")!.available;
  assert(after === 99998, `duplicate ignored, stock unchanged (${after})`);
  assert(cluster.inventory.idempotent.duplicates === 1, "idempotent consumer counted one duplicate");
  cluster.drain(20_000);
  assert(cluster.stats().confirmed === 1, "order confirmed");
}

// 4. out of stock cancels; declined payment cancels with compensation that releases the units
{
  const cluster = new Cluster({ seed: 5, stockSeed: { "SKU-ALPHA": 10, "SKU-SCARCE": 1 }, redeliverChance: 0 });
  cluster.submit("cust-1", [{ sku: "SKU-SCARCE", quantity: 2, unitPrice: 10 }]);
  cluster.submit("cust-2-declined", [{ sku: "SKU-ALPHA", quantity: 3, unitPrice: 10 }]);
  cluster.drain(20_000);
  const [oos, declined] = cluster.submitted.map((id) => cluster.order.find(id)!);
  assert(oos.status === "CANCELLED" && oos.reason === "OUT_OF_STOCK", "out of stock order cancelled");
  assert(
    declined.status === "CANCELLED" && declined.reason === "PAYMENT_DECLINED",
    "declined card cancels the order",
  );
  assert(cluster.inventory.releases === 1, "compensation released the reservation once");
  assert(cluster.inventory.stock.get("SKU-ALPHA")!.available === 10, "released units are back in stock");
  assert(cluster.inventory.stock.get("SKU-SCARCE")!.available === 1, "rejected order never touched stock");
}

// 5. determinism: two clusters with the same seed produce the same summary
{
  const run = () => {
    const c = new Cluster({ seed: 42 });
    const l = new LoadGenerator({ rate: 20, durationMs: 5000 });
    while (!l.finished(c.now)) {
      l.tick(c);
      if (c.now === 2000) c.kill("payment-service", 3000);
      c.step();
    }
    c.drain(60_000);
    return formatSummary(c.stats(), 5, 20);
  };
  assert(run() === run(), "same seed, same summary");
}

// 6. every figure the page presents as measured is in the recorded summary it claims to come from
{
  const figures = allFigures();
  assert(figures.length > 0, "the recorded runs produced figures to check");
  for (const { run, figure } of figures) {
    const missing = figure.tokens.filter((token) => !tokenInSummary(run, token));
    assert(
      missing.length === 0,
      missing.length === 0
        ? `${run.source}: "${figure.label}" renders ${figure.text}, all of it recorded`
        : `${run.source}: "${figure.label}" renders ${figure.text}, not in the summary: ${missing.join(", ")}`,
    );
  }
  assert(
    headlineFigures(CHAOS_RUN).every((f) => f.tokens.length === 1),
    "each hero figure renders exactly one recorded number",
  );
  for (const run of [CHAOS_RUN, BASELINE_RUN]) {
    assert(
      run.failedStuck === 0 && run.stuckOrders === 0,
      `${run.source}: the recorded run ended with no failed or stuck order`,
    );
    assert(
      /^\d{4}-\d{2}-\d{2}T/.test(run.recordedAt) && /^[0-9a-f]{7,40}$/.test(run.commit),
      `${run.source}: carries a date and the commit it was recorded at (${run.recordedAt}, ${run.commit})`,
    );
  }
  assert(
    CHAOS_RUN.kills.length === 3 && CHAOS_RUN.kills.some((k) => k.service === "order-service"),
    `the recorded chaos run killed three services including the order service (${CHAOS_RUN.kills.map((k) => k.service).join(", ")})`,
  );
  assert(
    BASELINE_RUN.kills.length === 0,
    `the recorded baseline killed nothing (${BASELINE_RUN.kills.length})`,
  );
  assert(
    counterFigures(CHAOS_RUN).some((f) => f.label === "failed / stuck" && f.text === "0"),
    "the counter the page leans on is measured at zero",
  );
}

if (failures > 0) throw new Error(`${failures} self-check(s) failed`);
console.log("all self-checks passed");
