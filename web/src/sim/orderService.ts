import type { BusRecord } from "./broker.ts";
import { CONFIG } from "./config.generated.ts";
import { Topics, type OrderLine, type OrderCancelled, type OrderCreated } from "./events.ts";
import type { Prng } from "./prng.ts";
import { CircuitBreaker, type BreakerConfig } from "./resilience.ts";
import { Service, type ServiceEnv } from "./service.ts";
import { applyTransition, isTerminal, type OrderStatus, type SagaEvent } from "./stateMachine.ts";
import type { InventoryService } from "./inventoryService.ts";

export interface OrderItem {
  sku: string;
  quantity: number;
  unitPrice: number;
}

export interface Order {
  id: string;
  customerId: string;
  items: OrderItem[];
  amount: number;
  status: OrderStatus;
  reason: string | null;
  correlationId: string;
  createdAt: number;
  updatedAt: number;
}

export type StockSource = "live" | "cache" | "unknown";

export interface StockView {
  sku: string;
  available: number | null;
  source: StockSource;
}

interface StockCall {
  sku: string;
  startedAt: number;
  resolveAt: number;
  ok: boolean;
  value: number | null;
  onDone: (view: StockView) => void;
}

export const INVENTORY_BREAKER: BreakerConfig = CONFIG.inventoryBreaker;

const TIME_LIMIT_MS = CONFIG.inventoryTimeLimitMs;

/**
 * Accepts orders, drives the saga and emits compensation. Also hosts the read-your-writes stock
 * check: a synchronous HTTP call to inventory behind a circuit breaker and a time limiter, with the
 * last value seen for the sku as the fallback.
 */
export class OrderService extends Service {
  readonly orders = new Map<string, Order>();
  readonly breaker = new CircuitBreaker("inventory", INVENTORY_BREAKER);
  /** Caffeine cache of the last live value per sku; in memory */
  private lastKnown = new Map<string, { view: StockView; at: number }>();
  private stockCalls: StockCall[] = [];
  latencies: number[] = [];
  /** terminal transitions with their virtual timestamp, for time based charts */
  completions: { at: number; ms: number }[] = [];
  transitions = { RESERVED: 0, CONFIRMED: 0, CANCELLED: 0 };
  onTerminal?: (order: Order, now: number) => void;

  constructor(
    env: ServiceEnv,
    private readonly rng: Prng,
    private readonly inventory: () => InventoryService,
  ) {
    super("order-service", env, [
      Topics.INVENTORY_RESERVED,
      Topics.INVENTORY_REJECTED,
      Topics.PAYMENT_COMPLETED,
      Topics.PAYMENT_FAILED,
    ]);
  }

  /** POST /orders: the order rows and the order.created outbox row commit together. */
  create(customerId: string, items: OrderItem[], now: number): Order {
    const id = this.rng.id("ord");
    const correlationId = this.rng.id("req");
    const amount = items.reduce((sum, i) => sum + i.quantity * i.unitPrice, 0);
    const order: Order = {
      id,
      customerId,
      items,
      amount,
      status: "PENDING",
      reason: null,
      correlationId,
      createdAt: now,
      updatedAt: now,
    };
    this.orders.set(id, order);
    const event: OrderCreated = {
      type: "OrderCreated",
      topic: Topics.ORDER_CREATED,
      eventId: this.rng.id("evt"),
      orderId: id,
      correlationId,
      occurredAt: now,
      customerId,
      lines: lines(order),
      amount,
    };
    this.outbox.append(event, now);
    this.env.trace?.({
      t: now,
      source: "order-service",
      kind: "tx",
      text: `BEGIN; INSERT orders (${id}, PENDING, ${amount.toFixed(2)}); INSERT outbox_event (OrderCreated); COMMIT`,
      orderId: id,
    });
    return order;
  }

  find(id: string): Order | undefined {
    return this.orders.get(id);
  }

  /** Applies a saga event inside the idempotent consumer's transaction. */
  apply(orderId: string, signal: SagaEvent, correlationId: string, now: number): void {
    const order = this.orders.get(orderId);
    if (!order) return;
    const transition = applyTransition(order.status, signal);
    if (!transition) {
      this.env.trace?.({
        t: now,
        source: "order-service",
        kind: "ignored",
        text: `event ${signal} ignored for order in state ${order.status}`,
        orderId,
      });
      return;
    }
    const from = order.status;
    order.status = transition.to;
    order.reason = transition.reason;
    order.updatedAt = now;
    this.transitions[transition.to as keyof typeof this.transitions]++;
    if (transition.releaseInventory) {
      const event: OrderCancelled = {
        type: "OrderCancelled",
        topic: Topics.ORDER_CANCELLED,
        eventId: this.rng.id("evt"),
        orderId,
        correlationId,
        occurredAt: now,
        reason: transition.reason ?? "",
        lines: lines(order),
      };
      this.outbox.append(event, now);
      this.env.trace?.({
        t: now,
        source: "order-service",
        kind: "compensate",
        text: `compensation: INSERT outbox_event (OrderCancelled ${transition.reason}) in the same transaction`,
        orderId,
      });
    }
    if (isTerminal(transition.to)) {
      this.latencies.push(now - order.createdAt);
      this.completions.push({ at: now, ms: now - order.createdAt });
      this.onTerminal?.(order, now);
    }
    this.env.trace?.({
      t: now,
      source: "order-service",
      kind: "transition",
      text: `order ${from} -> ${transition.to}${transition.reason ? " (" + transition.reason + ")" : ""} on ${signal}`,
      orderId,
    });
  }

  /** GET /stock/{sku} through the breaker and time limiter, cache fallback. */
  stock(sku: string, now: number, onDone: (view: StockView) => void): void {
    if (!this.breaker.tryAcquire(now)) {
      this.env.trace?.({
        t: now,
        source: "order-service",
        kind: "fallback",
        text: `breaker inventory is ${this.breaker.state}: CallNotPermitted, serving ${sku} from cache`,
      });
      onDone(this.fromCache(sku));
      return;
    }
    const inventory = this.inventory();
    const live = inventory.ready(now);
    const value = live ? inventory.available(sku, now) : null;
    const latency = live ? 12 + this.rng.int(0, 40) : TIME_LIMIT_MS;
    this.stockCalls.push({
      sku,
      startedAt: now,
      resolveAt: now + latency,
      ok: live,
      value: value ?? null,
      onDone,
    });
  }

  protected handle(record: BusRecord, now: number): void {
    const signal = signalFor(record.topic);
    if (!signal) return;
    this.idempotent.once(record.eventId, record.key, now, () =>
      this.apply(record.key, signal, record.correlationId, now),
    );
  }

  protected onKilled(): void {
    this.breaker.reset();
    this.stockCalls = [];
    this.lastKnown.clear();
  }

  protected onTick(now: number): void {
    if (this.stockCalls.length === 0) return;
    const inventory = this.inventory();
    const remaining: StockCall[] = [];
    for (const call of this.stockCalls) {
      // a call that was live when it started fails if inventory died before it answered
      if (call.ok && !inventory.alive) {
        call.ok = false;
        call.resolveAt = call.startedAt + TIME_LIMIT_MS;
      }
      if (call.resolveAt > now) {
        remaining.push(call);
        continue;
      }
      if (call.ok) {
        this.breaker.onSuccess(now);
        const view: StockView = { sku: call.sku, available: call.value, source: "live" };
        this.lastKnown.set(call.sku, { view, at: now });
        call.onDone(view);
      } else {
        this.breaker.onError(now);
        this.env.trace?.({
          t: now,
          source: "order-service",
          kind: "timeout",
          text: `GET inventory /stock/${call.sku} timed out after ${TIME_LIMIT_MS} ms (failure rate ${this.breaker.failureRate().toFixed(0)}%)`,
        });
        call.onDone(this.fromCache(call.sku));
      }
    }
    this.stockCalls = remaining;
  }

  private fromCache(sku: string): StockView {
    const cached = this.lastKnown.get(sku);
    return cached
      ? { ...cached.view, source: "cache" }
      : { sku, available: null, source: "unknown" };
  }
}

function lines(order: Order): OrderLine[] {
  return order.items.map((i) => ({ sku: i.sku, quantity: i.quantity }));
}

function signalFor(topic: string): SagaEvent | null {
  switch (topic) {
    case Topics.INVENTORY_RESERVED:
      return "INVENTORY_RESERVED";
    case Topics.INVENTORY_REJECTED:
      return "INVENTORY_REJECTED";
    case Topics.PAYMENT_COMPLETED:
      return "PAYMENT_COMPLETED";
    case Topics.PAYMENT_FAILED:
      return "PAYMENT_FAILED";
    default:
      return null;
  }
}
