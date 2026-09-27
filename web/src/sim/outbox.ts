import type { Broker } from "./broker.ts";
import type { DomainEvent, ServiceName, Trace } from "./events.ts";

export interface OutboxRow {
  id: number;
  eventId: string;
  topic: string;
  messageKey: string;
  eventType: string;
  payload: DomainEvent;
  createdAt: number;
  publishedAt?: number;
}

/** The outbox_event table: durable, survives a kill, only ever written inside a transaction. */
export class OutboxStore {
  rows: OutboxRow[] = [];
  private nextId = 1;

  append(event: DomainEvent, now: number): OutboxRow {
    const row: OutboxRow = {
      id: this.nextId++,
      eventId: event.eventId,
      topic: event.topic,
      messageKey: event.orderId,
      eventType: event.type,
      payload: event,
      createdAt: now,
    };
    this.rows.push(row);
    return row;
  }

  pending(limit: number): OutboxRow[] {
    const out: OutboxRow[] = [];
    for (const row of this.rows) {
      if (row.publishedAt === undefined) {
        out.push(row);
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  backlog(): number {
    let n = 0;
    for (const row of this.rows) if (row.publishedAt === undefined) n++;
    return n;
  }
}

/**
 * Polls unpublished rows in id order and sends each to the broker. A row is stamped published only
 * once the broker acknowledged it, which in this model lands on the following tick. Dying between
 * the send and the stamp leaves the row unpublished, so it is sent again after restart and the
 * downstream consumer sees a duplicate that it ignores by event id.
 */
export class OutboxRelay {
  /** rows sent whose acknowledgement has not been observed yet; in memory, lost on a kill */
  private awaitingAck: OutboxRow[] = [];
  private lastRun = -Infinity;

  constructor(
    private readonly service: ServiceName,
    private readonly store: OutboxStore,
    private readonly broker: Broker,
    private readonly pollMs: number,
    private readonly trace?: (t: Trace) => void,
  ) {}

  tick(now: number): number {
    for (const row of this.awaitingAck) {
      row.publishedAt = now;
    }
    this.awaitingAck = [];
    if (now - this.lastRun < this.pollMs) return 0;
    this.lastRun = now;
    const batch = this.store.pending(200);
    for (const row of batch) {
      this.broker.append(row.payload, now);
      this.awaitingAck.push(row);
      this.trace?.({
        t: now,
        source: this.service,
        kind: "relay",
        text: `relay sent ${row.eventType} to ${row.topic} (acks=all), awaiting broker ack`,
        orderId: row.messageKey,
      });
    }
    return batch.length;
  }

  /** Called on a kill: whatever was in flight is forgotten; the rows themselves survive. */
  reset(): void {
    this.awaitingAck = [];
    this.lastRun = -Infinity;
  }

  inFlight(): number {
    return this.awaitingAck.length;
  }
}
