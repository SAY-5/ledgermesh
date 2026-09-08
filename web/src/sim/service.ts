import type { Broker, BusRecord } from "./broker.ts";
import type { ServiceName, Topic, Trace } from "./events.ts";
import { IdempotentConsumer } from "./idempotent.ts";
import { OutboxRelay, OutboxStore } from "./outbox.ts";

export type TraceFn = (t: Trace) => void;

export interface ServiceEnv {
  broker: Broker;
  trace?: TraceFn;
  outboxPollMs: number;
  bootMs: number;
  consumerConcurrency: number;
}

/**
 * What every service shares: a database (outbox + processed markers, durable), a relay and a set
 * of Kafka consumers (in memory, gone on a kill). Offsets are committed per record after the
 * listener returned, which in this model means on the next tick, exactly the window in which a
 * kill produces a redelivery.
 */
export abstract class Service {
  alive = true;
  readyAt = 0;
  readonly outbox = new OutboxStore();
  readonly relay: OutboxRelay;
  readonly idempotent: IdempotentConsumer;
  /** in-memory consumer positions, initialised from the committed offsets on (re)start */
  private positions = new Map<Topic, number>();
  private pendingCommits: { topic: Topic; next: number }[] = [];
  restarts = 0;

  protected constructor(
    public readonly name: ServiceName,
    protected readonly env: ServiceEnv,
    private readonly topics: Topic[],
  ) {
    this.relay = new OutboxRelay(name, this.outbox, env.broker, env.outboxPollMs, env.trace);
    this.idempotent = new IdempotentConsumer(name, env.trace);
  }

  protected abstract handle(record: BusRecord, now: number): void;

  /** Hook for in-memory state the subclass loses on a kill. */
  protected abstract onKilled(now: number): void;

  /** Periodic work (sweepers, in-flight calls). */
  protected abstract onTick(now: number): void;

  ready(now: number): boolean {
    return this.alive && now >= this.readyAt;
  }

  kill(now: number): void {
    if (!this.alive) return;
    this.alive = false;
    this.relay.reset();
    this.positions.clear();
    this.pendingCommits = [];
    this.onKilled(now);
    this.env.trace?.({ t: now, source: "chaos", kind: "kill", text: `SIGKILL ${this.name}` });
  }

  restart(now: number): void {
    if (this.alive) return;
    this.alive = true;
    this.readyAt = now + this.env.bootMs;
    this.restarts++;
    this.env.trace?.({
      t: now,
      source: "chaos",
      kind: "restart",
      text: `${this.name} starting, ready in ${this.env.bootMs} ms`,
    });
  }

  tick(now: number): void {
    if (!this.ready(now)) return;
    for (const c of this.pendingCommits) this.env.broker.commit(this.name, c.topic, c.next);
    this.pendingCommits = [];
    this.relay.tick(now);
    for (const topic of this.topics) {
      for (let i = 0; i < this.env.consumerConcurrency; i++) {
        const position =
          this.positions.get(topic) ?? this.env.broker.committedOffset(this.name, topic);
        const record = this.env.broker.fetch(this.name, topic, position, now);
        if (!record) break;
        this.handle(record, now);
        if (!record.redelivery) {
          this.positions.set(topic, position + 1);
          this.pendingCommits.push({ topic, next: position + 1 });
        }
      }
    }
    this.onTick(now);
  }

  /** Total records this service has not committed yet across its topics. */
  lag(): number {
    let n = 0;
    for (const t of this.topics) n += this.env.broker.lag(this.name, t);
    return n;
  }
}
