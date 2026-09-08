import type { ServiceName, Trace } from "./events.ts";

/**
 * Runs a unit of work exactly once per event id. The processed marker is written in the same
 * transaction as the work: a crash before the commit leaves nothing behind, a crash after it makes
 * the redelivery a no-op.
 */
export class IdempotentConsumer {
  /** processed_event table, durable */
  private processed = new Set<string>();
  duplicates = 0;

  constructor(
    private readonly consumer: ServiceName,
    private readonly trace?: (t: Trace) => void,
  ) {}

  once(eventId: string, orderId: string, now: number, work: () => void): boolean {
    const key = this.consumer + ":" + eventId;
    if (this.processed.has(key)) {
      this.duplicates++;
      this.trace?.({
        t: now,
        source: this.consumer,
        kind: "duplicate",
        text: `duplicate event ${eventId} ignored by ${this.consumer}`,
        orderId,
      });
      return false;
    }
    work();
    this.processed.add(key);
    return true;
  }

  size(): number {
    return this.processed.size;
  }
}
