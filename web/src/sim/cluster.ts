import { Broker } from "./broker.ts";
import { StockCache } from "./cache.ts";
import type { ServiceName, Topic, Trace } from "./events.ts";
import { InventoryService } from "./inventoryService.ts";
import { OrderService, type Order, type OrderItem, type StockView } from "./orderService.ts";
import { PaymentService, type ProcessorConfig } from "./paymentService.ts";
import { Prng } from "./prng.ts";
import type { BreakerTransition } from "./resilience.ts";
import type { Service } from "./service.ts";
import { isTerminal } from "./stateMachine.ts";

export interface ClusterOptions {
  seed: number;
  stockSeed?: Record<string, number>;
  processor?: Partial<ProcessorConfig>;
  /** virtual ms per step */
  tickMs?: number;
  /** ms a restarted service needs before it is ready */
  bootMs?: number;
  deliveryMs?: number;
  redeliverChance?: number;
  trace?: boolean;
  maxTraces?: number;
}

export interface Kill {
  service: ServiceName;
  at: number;
  restartAt: number;
  readyAt: number;
}

export interface StockProbes {
  live: number;
  cache: number;
  unknown: number;
  error: number;
}

export const DEFAULT_STOCK: Record<string, number> = {
  "SKU-ALPHA": 100000,
  "SKU-BRAVO": 100000,
  "SKU-CHARLIE": 100000,
  "SKU-SCARCE": 40,
};

/**
 * The whole stack in one object: the broker, Redis, the three services and a virtual clock. Time
 * only advances through step(); nothing in here reads a wall clock.
 */
export class Cluster {
  now = 0;
  readonly tickMs: number;
  readonly rng: Prng;
  readonly broker: Broker;
  readonly redis: StockCache;
  readonly order: OrderService;
  readonly inventory: InventoryService;
  readonly payment: PaymentService;
  readonly traces: Trace[] = [];
  readonly kills: Kill[] = [];
  readonly stockProbes: StockProbes = { live: 0, cache: 0, unknown: 0, error: 0 };
  readonly breakerTransitions = new Map<string, number>();
  readonly breakerLog: { at: number; owner: string; from: string; to: string }[] = [];
  submitted: string[] = [];
  onTrace?: (t: Trace) => void;
  private readonly maxTraces: number;
  private pendingRestarts: { service: Service; at: number }[] = [];

  constructor(options: ClusterOptions) {
    this.tickMs = options.tickMs ?? 50;
    this.maxTraces = options.maxTraces ?? 400;
    this.rng = new Prng(options.seed);
    const trace = options.trace ? (t: Trace) => this.pushTrace(t) : undefined;
    this.broker = new Broker(
      new Prng(options.seed ^ 0x9e3779b9),
      { deliveryMs: options.deliveryMs ?? 60, redeliverChance: options.redeliverChance ?? 0.0008 },
      trace,
    );
    this.redis = new StockCache(60_000, trace);
    const env = {
      broker: this.broker,
      trace,
      outboxPollMs: 200,
      bootMs: options.bootMs ?? 2500,
      consumerConcurrency: 3,
    };
    this.inventory = new InventoryService(env, new Prng(options.seed + 1), this.redis);
    this.order = new OrderService(env, new Prng(options.seed + 2), () => this.inventory);
    this.payment = new PaymentService(env, new Prng(options.seed + 3), {
      limit: 10000,
      transientPercent: 5,
      slowPercent: 1,
      slowMillis: 3000,
      ...options.processor,
    });
    this.inventory.seed(options.stockSeed ?? DEFAULT_STOCK, 0);
    this.order.breaker.onTransition = (t) => this.recordTransition("order-service/inventory", t);
    this.payment.breaker.onTransition = (t) => this.recordTransition("payment-service/processor", t);
  }

  get services(): Service[] {
    return [this.order, this.inventory, this.payment];
  }

  service(name: ServiceName): Service {
    switch (name) {
      case "order-service":
        return this.order;
      case "inventory-service":
        return this.inventory;
      case "payment-service":
        return this.payment;
    }
  }

  /** POST /orders. Rejected while the order service is down (it never is in the chaos run). */
  submit(customerId: string, items: OrderItem[]): Order | null {
    if (!this.order.ready(this.now)) return null;
    const order = this.order.create(customerId, items, this.now);
    this.submitted.push(order.id);
    return order;
  }

  /** GET /stock/{sku} on the order service; the result lands asynchronously. */
  probeStock(sku: string, onDone?: (view: StockView) => void): void {
    if (!this.order.ready(this.now)) {
      this.stockProbes.error++;
      return;
    }
    this.order.stock(sku, this.now, (view) => {
      this.stockProbes[view.source]++;
      this.pushTrace({
        t: this.now,
        source: "client",
        kind: "probe",
        text: `GET /stock/${sku} -> ${view.available ?? "?"} (source: ${view.source})`,
      });
      onDone?.(view);
    });
  }

  kill(name: ServiceName, restartAfterMs = 5000): Kill | null {
    const service = this.service(name);
    if (!service.alive) return null;
    service.kill(this.now);
    const restartAt = this.now + restartAfterMs;
    this.pendingRestarts.push({ service, at: restartAt });
    const kill: Kill = { service: name, at: this.now, restartAt, readyAt: restartAt + 2500 };
    this.kills.push(kill);
    return kill;
  }

  /** Deliver the newest record on a topic to its consumer one more time. */
  injectDuplicate(topic: Topic, group: ServiceName): boolean {
    const record = this.broker.redeliverLast(group, topic);
    if (!record) return false;
    this.pushTrace({
      t: this.now,
      source: "broker",
      kind: "inject",
      text: `injected duplicate delivery of ${record.type} (${record.eventId}) to ${group}`,
      orderId: record.key,
    });
    return true;
  }

  step(): void {
    this.now += this.tickMs;
    if (this.pendingRestarts.length) {
      const due = this.pendingRestarts.filter((r) => r.at <= this.now);
      this.pendingRestarts = this.pendingRestarts.filter((r) => r.at > this.now);
      for (const r of due) {
        r.service.restart(this.now);
        const kill = this.kills.find((k) => k.service === r.service.name && k.restartAt === r.at);
        if (kill) kill.readyAt = r.service.readyAt;
      }
    }
    for (const s of this.services) s.tick(this.now);
  }

  runFor(ms: number): void {
    const until = this.now + ms;
    while (this.now < until) this.step();
  }

  /** Orders that have not reached a terminal state. */
  open(): number {
    let n = 0;
    for (const id of this.submitted) {
      const o = this.order.find(id);
      if (o && !isTerminal(o.status)) n++;
    }
    return n;
  }

  /** True when no order is open and nothing is left in any outbox, topic or in-flight call. */
  quiescent(): boolean {
    if (this.open() > 0) return false;
    for (const s of this.services) {
      if (s.outbox.backlog() > 0 || s.relay.inFlight() > 0 || s.lag() > 0) return false;
    }
    return this.payment.inFlight() === 0;
  }

  /** Advance until the stack is quiescent (compensations included) or the timeout passes. */
  drain(timeoutMs: number): number {
    const deadline = this.now + timeoutMs;
    while (this.now < deadline && !this.quiescent()) this.step();
    return this.open();
  }

  stats(): ClusterStats {
    let confirmed = 0;
    let cancelledStock = 0;
    let cancelledOther = 0;
    let stuck = 0;
    for (const id of this.submitted) {
      const o = this.order.find(id);
      if (!o) continue;
      if (o.status === "CONFIRMED") confirmed++;
      else if (o.status === "CANCELLED" && o.reason === "OUT_OF_STOCK") cancelledStock++;
      else if (o.status === "CANCELLED") cancelledOther++;
      else stuck++;
    }
    const latencies = this.order.latencies;
    return {
      now: this.now,
      submitted: this.submitted.length,
      confirmed,
      cancelledStock,
      cancelledOther,
      stuck,
      failed: stuck + cancelledOther,
      retries: { ...this.payment.retries },
      deferred: this.payment.deferred,
      duplicates:
        this.order.idempotent.duplicates +
        this.inventory.idempotent.duplicates +
        this.payment.idempotent.duplicates,
      releases: this.inventory.releases,
      stockProbes: { ...this.stockProbes },
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      max: latencies.length ? Math.max(...latencies) : 0,
      kills: this.kills.map((k) => ({ ...k })),
      breakerTransitions: [...this.breakerTransitions.entries()]
        .map(([k, v]) => `${k} x${v}`)
        .sort(),
      outboxBacklog:
        this.order.outbox.backlog() + this.inventory.outbox.backlog() + this.payment.outbox.backlog(),
      cache: { hits: this.redis.hits, misses: this.redis.misses },
    };
  }

  private recordTransition(owner: string, t: BreakerTransition): void {
    const key = `${owner} ${t.from}->${t.to}`;
    this.breakerTransitions.set(key, (this.breakerTransitions.get(key) ?? 0) + 1);
    this.breakerLog.push({ at: t.at, owner, from: t.from, to: t.to });
    this.pushTrace({
      t: t.at,
      source: owner.startsWith("order") ? "order-service" : "payment-service",
      kind: "breaker",
      text: `breaker ${owner.split("/")[1]} ${t.from} -> ${t.to}`,
    });
  }

  private pushTrace(t: Trace): void {
    this.traces.push(t);
    if (this.traces.length > this.maxTraces) this.traces.splice(0, this.traces.length - this.maxTraces);
    this.onTrace?.(t);
  }
}

export interface ClusterStats {
  now: number;
  submitted: number;
  confirmed: number;
  cancelledStock: number;
  cancelledOther: number;
  stuck: number;
  failed: number;
  retries: {
    successful_without_retry: number;
    successful_with_retry: number;
    failed_with_retry: number;
    failed_without_retry: number;
  };
  deferred: number;
  duplicates: number;
  releases: number;
  stockProbes: StockProbes;
  p50: number;
  p95: number;
  max: number;
  kills: Kill[];
  breakerTransitions: string[];
  outboxBacklog: number;
  cache: { hits: number; misses: number };
}

export function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const ordered = [...values].sort((a, b) => a - b);
  const k = ((ordered.length - 1) * p) / 100;
  const lo = Math.floor(k);
  const hi = Math.min(lo + 1, ordered.length - 1);
  return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo);
}

/** The chaos script's summary block, line for line. */
export function formatSummary(stats: ClusterStats, durationS: number, rate: number): string {
  const timeline = stats.kills.map((k) => `${k.service} @${Math.round(k.at / 1000)}s`).join(", ");
  const r = stats.retries;
  const probes = `{'live': ${stats.stockProbes.live}, 'cache': ${stats.stockProbes.cache}, 'unknown': ${stats.stockProbes.unknown}, 'error': ${stats.stockProbes.error}}`;
  return [
    "LedgerMesh chaos summary",
    `  load                 ${durationS}s at ${rate} orders/s`,
    `  orders submitted     ${stats.submitted}`,
    `  confirmed            ${stats.confirmed}`,
    `  cancelled (stock)    ${stats.cancelledStock}`,
    `  failed / stuck       ${stats.failed}`,
    `  kills                ${stats.kills.length}  (${timeline})`,
    `  saga latency         p50 ${Math.round(stats.p50)} ms   p95 ${Math.round(stats.p95)} ms   max ${Math.round(stats.max)} ms`,
    `  breaker transitions  ${stats.breakerTransitions.length ? stats.breakerTransitions.join("; ") : "none"}`,
    `  retries              with retry ${r.successful_with_retry} ok / ${r.failed_with_retry} exhausted, without retry ${r.successful_without_retry} ok / ${r.failed_without_retry} failed`,
    `  deferred payments    ${stats.deferred}`,
    `  duplicate events     ${stats.duplicates} ignored by idempotent consumers`,
    `  stock probes         ${probes}`,
  ].join("\n");
}
