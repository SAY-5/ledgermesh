import type { Trace } from "./events.ts";

interface Entry {
  value: number;
  expiresAt: number;
}

/**
 * Redis stock:{sku} view. Shared infrastructure: it lives outside the inventory process and
 * survives a kill of the service. Every entry carries a TTL so a missed write heals itself.
 */
export class StockCache {
  private entries = new Map<string, Entry>();
  hits = 0;
  misses = 0;
  writes = 0;

  constructor(
    private readonly ttlMs: number,
    private readonly trace?: (t: Trace) => void,
  ) {}

  read(sku: string, now: number): number | undefined {
    const entry = this.entries.get("stock:" + sku);
    if (!entry || entry.expiresAt <= now) {
      if (entry) this.entries.delete("stock:" + sku);
      this.misses++;
      return undefined;
    }
    this.hits++;
    return entry.value;
  }

  /** Write-through, called after the owning transaction committed. */
  write(sku: string, available: number, now: number, orderId?: string): void {
    this.entries.set("stock:" + sku, { value: available, expiresAt: now + this.ttlMs });
    this.writes++;
    this.trace?.({
      t: now,
      source: "redis",
      kind: "write",
      text: `SET stock:${sku} ${available} EX ${Math.round(this.ttlMs / 1000)} (write-through after commit)`,
      orderId,
    });
  }

  ttlRemaining(sku: string, now: number): number {
    const entry = this.entries.get("stock:" + sku);
    return entry ? Math.max(0, entry.expiresAt - now) : 0;
  }

  snapshot(now: number): { sku: string; value: number; ttlMs: number }[] {
    const out: { sku: string; value: number; ttlMs: number }[] = [];
    for (const [key, entry] of this.entries) {
      if (entry.expiresAt > now) {
        out.push({ sku: key.slice(6), value: entry.value, ttlMs: entry.expiresAt - now });
      }
    }
    return out.sort((a, b) => a.sku.localeCompare(b.sku));
  }
}
