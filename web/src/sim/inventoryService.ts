import type { BusRecord } from "./broker.ts";
import type { StockCache } from "./cache.ts";
import {
  Topics,
  type InventoryRejected,
  type InventoryReserved,
  type OrderCancelled,
  type OrderCreated,
  type OrderLine,
} from "./events.ts";
import type { Prng } from "./prng.ts";
import { Service, type ServiceEnv } from "./service.ts";

export interface StockItem {
  sku: string;
  available: number;
}

export type Outcome = { kind: "reserved"; remaining: Map<string, number> } | { kind: "rejected"; reason: string };

/**
 * Reserves stock for a whole order atomically: rows locked in sku order, every line checked, then
 * every line decremented. The reservation, the outcome event and the processed marker share one
 * transaction; the cache write-through runs after that commit.
 */
export class InventoryService extends Service {
  readonly stock = new Map<string, StockItem>();
  releases = 0;

  constructor(
    env: ServiceEnv,
    private readonly rng: Prng,
    readonly cache: StockCache,
  ) {
    super("inventory-service", env, [Topics.ORDER_CREATED, Topics.ORDER_CANCELLED]);
  }

  seed(entries: Record<string, number>, now: number): void {
    for (const [sku, available] of Object.entries(entries)) {
      if (!this.stock.has(sku)) this.restock(sku, available, now, true);
    }
  }

  restock(sku: string, available: number, now: number, silent = false): void {
    const item = this.stock.get(sku) ?? { sku, available: 0 };
    item.available = available;
    this.stock.set(sku, item);
    this.cache.write(sku, available, now, undefined, silent);
  }

  reserve(lines: OrderLine[], now: number, orderId: string): Outcome {
    const wanted = merge(lines);
    const skus = [...wanted.keys()].sort();
    const items: StockItem[] = [];
    for (const sku of skus) {
      const item = this.stock.get(sku);
      if (!item) return { kind: "rejected", reason: "UNKNOWN_SKU" };
      items.push(item);
    }
    this.env.trace?.({
      t: now,
      source: "inventory-service",
      kind: "lock",
      text: `SELECT ... FOR UPDATE on ${skus.join(", ")} (sku order, no deadlocks)`,
      orderId,
    });
    for (const item of items) {
      if (item.available < (wanted.get(item.sku) ?? 0)) {
        return { kind: "rejected", reason: "OUT_OF_STOCK" };
      }
    }
    const remaining = new Map<string, number>();
    for (const item of items) {
      item.available -= wanted.get(item.sku) ?? 0;
      remaining.set(item.sku, item.available);
    }
    return { kind: "reserved", remaining };
  }

  release(lines: OrderLine[], now: number, orderId: string): void {
    const wanted = merge(lines);
    for (const sku of [...wanted.keys()].sort()) {
      const item = this.stock.get(sku);
      if (!item) continue;
      item.available += wanted.get(sku) ?? 0;
      this.cache.write(sku, item.available, now, orderId);
    }
  }

  /** GET /stock/{sku}: cache first, database on a miss, and the miss is written through. */
  available(sku: string, now: number): number | undefined {
    const cached = this.cache.read(sku, now);
    if (cached !== undefined) return cached;
    const item = this.stock.get(sku);
    if (item) this.cache.write(sku, item.available, now);
    return item?.available;
  }

  protected handle(record: BusRecord, now: number): void {
    if (record.topic === Topics.ORDER_CREATED) {
      const event = record.payload as OrderCreated;
      this.idempotent.once(record.eventId, record.key, now, () => this.onCreated(event, now));
    } else if (record.topic === Topics.ORDER_CANCELLED) {
      const event = record.payload as OrderCancelled;
      this.idempotent.once(record.eventId, record.key, now, () => {
        this.release(event.lines, now, event.orderId);
        this.releases++;
        this.env.trace?.({
          t: now,
          source: "inventory-service",
          kind: "release",
          text: `released reservation (${event.reason}), stock restored`,
          orderId: event.orderId,
        });
      });
    }
  }

  protected onKilled(): void {
    // nothing beyond the consumer positions and relay in the base class: stock rows are durable
  }

  protected onTick(): void {}

  private onCreated(event: OrderCreated, now: number): void {
    const outcome = this.reserve(event.lines, now, event.orderId);
    if (outcome.kind === "rejected") {
      const rejected: InventoryRejected = {
        type: "InventoryRejected",
        topic: Topics.INVENTORY_REJECTED,
        eventId: this.rng.id("evt"),
        orderId: event.orderId,
        correlationId: event.correlationId,
        occurredAt: now,
        reason: outcome.reason,
      };
      this.outbox.append(rejected, now);
      this.env.trace?.({
        t: now,
        source: "inventory-service",
        kind: "rejected",
        text: `rejected: ${outcome.reason}; INSERT outbox_event (InventoryRejected); INSERT processed_event; COMMIT`,
        orderId: event.orderId,
      });
      return;
    }
    const reserved: InventoryReserved = {
      type: "InventoryReserved",
      topic: Topics.INVENTORY_RESERVED,
      eventId: this.rng.id("evt"),
      orderId: event.orderId,
      correlationId: event.correlationId,
      occurredAt: now,
      customerId: event.customerId,
      lines: event.lines,
      amount: event.amount,
    };
    this.outbox.append(reserved, now);
    const detail = [...outcome.remaining].map(([sku, left]) => `${sku}=${left}`).join(", ");
    this.env.trace?.({
      t: now,
      source: "inventory-service",
      kind: "reserved",
      text: `reserved (${detail}); INSERT outbox_event (InventoryReserved); INSERT processed_event; COMMIT`,
      orderId: event.orderId,
    });
    for (const [sku, left] of outcome.remaining) this.cache.write(sku, left, now, event.orderId);
  }
}

function merge(lines: OrderLine[]): Map<string, number> {
  const wanted = new Map<string, number>();
  for (const line of lines) wanted.set(line.sku, (wanted.get(line.sku) ?? 0) + line.quantity);
  return wanted;
}
