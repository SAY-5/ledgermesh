import type { Cluster } from "./cluster.ts";
import type { ServiceName } from "./events.ts";
import { Prng } from "./prng.ts";

export const SKUS = ["SKU-ALPHA", "SKU-BRAVO", "SKU-CHARLIE", "SKU-SCARCE"] as const;
const WEIGHTS = [40, 30, 25, 5];

export interface LoadOptions {
  rate: number;
  durationMs: number;
  seed?: number;
  probeMs?: number;
}

/**
 * Mirrors chaos/loadgen.py: a fixed order rate with weighted skus, quantities 1..3, prices 5..80,
 * and a stock probe every 500 ms against the order service's breaker protected endpoint.
 */
export class LoadGenerator {
  private readonly rng: Prng;
  private startedAt = -1;
  private seq = 0;
  private nextProbeAt = 0;
  readonly probeSku = "SKU-ALPHA";

  constructor(private readonly options: LoadOptions) {
    this.rng = new Prng(options.seed ?? 7);
  }

  get elapsed(): number {
    return this.startedAt < 0 ? 0 : Math.max(0, this.elapsedFrom);
  }

  private elapsedFrom = 0;

  finished(now: number): boolean {
    return this.startedAt >= 0 && now - this.startedAt >= this.options.durationMs;
  }

  /** Submit whatever is due at `cluster.now`. Returns how many orders were submitted. */
  tick(cluster: Cluster): number {
    const now = cluster.now;
    if (this.startedAt < 0) this.startedAt = now;
    this.elapsedFrom = now - this.startedAt;
    let count = 0;
    const interval = 1000 / this.options.rate;
    while (
      now - this.startedAt < this.options.durationMs &&
      this.startedAt + this.seq * interval <= now
    ) {
      const sku = this.rng.choice(SKUS, WEIGHTS);
      const quantity = this.rng.int(1, 3);
      const unitPrice = this.rng.int(5, 80);
      cluster.submit(`cust-${this.seq % 250}`, [{ sku, quantity, unitPrice }]);
      this.seq++;
      count++;
    }
    if (now >= this.nextProbeAt && now - this.startedAt < this.options.durationMs) {
      this.nextProbeAt = now + (this.options.probeMs ?? 500);
      cluster.probeStock(this.probeSku);
    }
    return count;
  }
}

export interface KillPlan {
  at: number;
  service: ServiceName;
}

/** The kill schedule from chaos/run.sh: kills spread over the window with jitter. */
export function planKills(durationMs: number, kills: number, seed: number): KillPlan[] {
  const rng = new Prng(seed ^ 0x5eed);
  const victims: ServiceName[] = ["inventory-service", "payment-service"];
  const slot = Math.floor(durationMs / 1000 / (kills + 1));
  const plan: KillPlan[] = [];
  for (let i = 0; i < kills; i++) {
    let target = slot * (i + 1) + rng.int(0, Math.floor(slot / 2)) - Math.floor(slot / 4);
    if (target < 5) target = 5;
    plan.push({ at: target * 1000, service: victims[rng.int(0, victims.length - 1)] });
  }
  return plan;
}
