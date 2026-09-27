import type { DomainEvent, Topic, Trace } from "./events.ts";
import type { Prng } from "./prng.ts";

/** One record on a topic. Key is the order id so an order is always handled in sequence. */
export interface BusRecord {
  offset: number;
  topic: Topic;
  key: string;
  eventId: string;
  type: string;
  payload: DomainEvent;
  correlationId: string;
  appendedAt: number;
  /** set when the broker itself redelivered an already consumed record */
  redelivery?: boolean;
}

export interface BrokerOptions {
  /** end to end delivery latency in ms, applied before a consumer can poll a record */
  deliveryMs: number;
  /** probability per poll that the broker hands out the previous record again (at-least-once) */
  redeliverChance: number;
}

/**
 * A minimal log per topic with committed offsets per consumer group. Delivery is at-least-once:
 * offsets are only advanced by an explicit commit, and a tiny share of polls redeliver the record
 * before the committed one, the way a rebalance or a lost commit would.
 */
export class Broker {
  private logs = new Map<Topic, BusRecord[]>();
  private committed = new Map<string, number>();
  private nextOffsets = new Map<Topic, number>();
  private pendingRedelivery = new Map<string, BusRecord>();
  /** records handed out a second time, by chance or on request */
  redeliveries = 0;

  constructor(
    private readonly rng: Prng,
    private readonly options: BrokerOptions,
    private readonly trace?: (t: Trace) => void,
  ) {}

  append(event: DomainEvent, now: number): BusRecord {
    const topic = event.topic;
    const log = this.log(topic);
    const offset = this.nextOffsets.get(topic) ?? 0;
    this.nextOffsets.set(topic, offset + 1);
    const record: BusRecord = {
      offset,
      topic,
      key: event.orderId,
      eventId: event.eventId,
      type: event.type,
      payload: event,
      correlationId: event.correlationId,
      appendedAt: now,
    };
    log.push(record);
    return record;
  }

  committedOffset(group: string, topic: Topic): number {
    return this.committed.get(group + "|" + topic) ?? 0;
  }

  /** The record at `position` on `topic` if it has been delivered by `now`. */
  fetch(group: string, topic: Topic, position: number, now: number): BusRecord | undefined {
    const key = group + "|" + topic;
    const forced = this.pendingRedelivery.get(key);
    if (forced) {
      this.pendingRedelivery.delete(key);
      this.redeliveries++;
      this.trace?.({
        t: now,
        source: "broker",
        kind: "redelivery",
        text: `redelivered ${topic} offset ${forced.offset} (${forced.type}) to ${group}`,
        orderId: forced.key,
      });
      return { ...forced, redelivery: true };
    }
    const log = this.log(topic);
    const record = log[position];
    if (!record || record.appendedAt + this.options.deliveryMs > now) return undefined;
    if (position > 0 && this.rng.next() < this.options.redeliverChance) {
      const again = log[position - 1];
      this.redeliveries++;
      this.trace?.({
        t: now,
        source: "broker",
        kind: "redelivery",
        text: `at-least-once: ${topic} offset ${again.offset} (${again.type}) delivered again to ${group}`,
        orderId: again.key,
      });
      return { ...again, redelivery: true };
    }
    return record;
  }

  commit(group: string, topic: Topic, nextOffset: number): void {
    const key = group + "|" + topic;
    const current = this.committed.get(key) ?? 0;
    if (nextOffset > current) this.committed.set(key, nextOffset);
  }

  /** Force the most recent record on a topic to be delivered to `group` one more time. */
  redeliverLast(group: string, topic: Topic): BusRecord | undefined {
    const log = this.log(topic);
    const last = log[log.length - 1];
    if (!last) return undefined;
    this.pendingRedelivery.set(group + "|" + topic, last);
    return last;
  }

  size(topic: Topic): number {
    return this.log(topic).length;
  }

  /** Records appended but not yet committed by `group`. */
  lag(group: string, topic: Topic): number {
    return this.size(topic) - this.committedOffset(group, topic);
  }

  private log(topic: Topic): BusRecord[] {
    let log = this.logs.get(topic);
    if (!log) {
      log = [];
      this.logs.set(topic, log);
    }
    return log;
  }
}
