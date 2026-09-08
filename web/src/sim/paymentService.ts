import type { BusRecord } from "./broker.ts";
import {
  Topics,
  type InventoryReserved,
  type PaymentCompleted,
  type PaymentFailed,
} from "./events.ts";
import { bucket, hex, type Prng } from "./prng.ts";
import {
  CircuitBreaker,
  emptyRetryCounters,
  type BreakerConfig,
  type RetryCounters,
} from "./resilience.ts";
import { Service, type ServiceEnv } from "./service.ts";

export type PaymentStatus = "NEW" | "DEFERRED" | "AUTHORIZED" | "DECLINED";

export interface Payment {
  orderId: string;
  customerId: string;
  amount: number;
  correlationId: string;
  status: PaymentStatus;
  attempts: number;
  nextAttemptAt: number;
  lastError: string | null;
  createdAt: number;
}

export interface ProcessorConfig {
  limit: number;
  transientPercent: number;
  slowPercent: number;
  slowMillis: number;
}

export const PROCESSOR_BREAKER: BreakerConfig = {
  slidingWindowSize: 20,
  minimumNumberOfCalls: 10,
  failureRateThreshold: 60,
  waitDurationInOpenStateMs: 5000,
  permittedNumberOfCallsInHalfOpenState: 3,
};

const MAX_ATTEMPTS = 3;
const RETRY_WAIT_MS = 200;
const TIME_LIMIT_MS = 1000;
const SWEEP_MS = 1000;
const SWEEP_GRACE_MS = 3000;
const RETRY_DELAY_MS = 2000;

type Result =
  | { kind: "approved"; code: string; latency: number }
  | { kind: "declined"; reason: string; latency: number }
  | { kind: "transient"; latency: number }
  | { kind: "slow"; latency: number };

type Outcome =
  | { kind: "authorized"; code: string }
  | { kind: "declined"; reason: string }
  | { kind: "deferred"; reason: string };

interface Call {
  orderId: string;
  /** attempt counter of this authorizer invocation (fresh AtomicInteger per authorize call) */
  attempts: number;
  phase: "running" | "waiting";
  resolveAt: number;
  result?: Result;
}

/**
 * Two step flow: record the payment inside the consumer's idempotent transaction, then authorize
 * outside any transaction and commit the outcome plus its outbox event. A kill between the two
 * leaves an open payment that the deferred sweeper picks up.
 *
 * The processor call is Retry(CircuitBreaker(TimeLimiter(call))): a hung call is cut at 1 s and
 * counted by the breaker; transient faults and timeouts are retried up to three times; once the
 * breaker is open calls fail fast without retrying; exhaustion becomes Deferred, never a failure.
 */
export class PaymentService extends Service {
  readonly payments = new Map<string, Payment>();
  readonly breaker = new CircuitBreaker("processor", PROCESSOR_BREAKER);
  readonly retries: RetryCounters = emptyRetryCounters();
  deferred = 0;
  outcomes = { AUTHORIZED: 0, DECLINED: 0, DEFERRED: 0 };
  private calls: Call[] = [];
  private lastSweep = -Infinity;

  constructor(
    env: ServiceEnv,
    private readonly rng: Prng,
    private readonly processor: ProcessorConfig,
  ) {
    super("payment-service", env, [Topics.INVENTORY_RESERVED]);
  }

  record(event: InventoryReserved, now: number): Payment {
    let payment = this.payments.get(event.orderId);
    if (!payment) {
      payment = {
        orderId: event.orderId,
        customerId: event.customerId,
        amount: event.amount,
        correlationId: event.correlationId,
        status: "NEW",
        attempts: 0,
        nextAttemptAt: now + SWEEP_GRACE_MS,
        lastError: null,
        createdAt: now,
      };
      this.payments.set(event.orderId, payment);
      this.env.trace?.({
        t: now,
        source: "payment-service",
        kind: "record",
        text: `INSERT payment (NEW, ${event.amount.toFixed(2)}, next_attempt_at +3 s); INSERT processed_event; COMMIT`,
        orderId: event.orderId,
      });
    }
    return payment;
  }

  /** Starts an authorization for an open payment unless one is already running. */
  attempt(orderId: string, now: number): boolean {
    const payment = this.payments.get(orderId);
    if (!payment || !isOpen(payment.status)) return false;
    if (this.calls.some((c) => c.orderId === orderId)) return false;
    this.startAttempt({ orderId, attempts: 0, phase: "running", resolveAt: now }, now);
    return true;
  }

  inFlight(): number {
    return this.calls.length;
  }

  openPayments(): number {
    let n = 0;
    for (const p of this.payments.values()) if (isOpen(p.status)) n++;
    return n;
  }

  protected handle(record: BusRecord, now: number): void {
    if (record.topic !== Topics.INVENTORY_RESERVED) return;
    const event = record.payload as InventoryReserved;
    const recorded = this.idempotent.once(record.eventId, record.key, now, () =>
      this.record(event, now),
    );
    if (recorded) this.attempt(event.orderId, now);
  }

  protected onKilled(): void {
    // in-flight authorizations vanish with the JVM; the payment rows stay NEW and the sweeper
    // finds them once their grace period is over
    this.calls = [];
    this.breaker.reset();
    this.lastSweep = -Infinity;
  }

  protected onTick(now: number): void {
    const remaining: Call[] = [];
    for (const call of this.calls) {
      if (call.resolveAt > now) {
        remaining.push(call);
        continue;
      }
      if (call.phase === "waiting") {
        this.startAttempt(call, now);
        if (this.calls.includes(call) || call.phase === "running") remaining.push(call);
        continue;
      }
      const result = call.result;
      if (!result) continue;
      if (result.kind === "approved" || result.kind === "declined") {
        this.breaker.onSuccess(now);
        if (call.attempts > 1) this.retries.successful_with_retry++;
        else this.retries.successful_without_retry++;
        this.commit(
          call.orderId,
          result.kind === "approved"
            ? { kind: "authorized", code: result.code }
            : { kind: "declined", reason: result.reason },
          now,
        );
        continue;
      }
      // transient fault or time limiter timeout: counted by the breaker, retried with a pause
      this.breaker.onError(now);
      const error =
        result.kind === "slow" ? "TimeoutException (1 s time limit)" : "ProcessorUnavailableException";
      this.env.trace?.({
        t: now,
        source: "payment-service",
        kind: "retry",
        text: `attempt ${call.attempts} failed: ${error}${call.attempts < MAX_ATTEMPTS ? ", retrying in 200 ms" : ", retries exhausted"}`,
        orderId: call.orderId,
      });
      if (call.attempts < MAX_ATTEMPTS) {
        call.phase = "waiting";
        call.resolveAt = now + RETRY_WAIT_MS;
        call.result = undefined;
        remaining.push(call);
      } else {
        this.retries.failed_with_retry++;
        this.commit(call.orderId, { kind: "deferred", reason: error }, now);
      }
    }
    this.calls = remaining;
    if (now - this.lastSweep >= SWEEP_MS) {
      this.lastSweep = now;
      this.sweep(now);
    }
  }

  private startAttempt(call: Call, now: number): void {
    const payment = this.payments.get(call.orderId);
    if (!payment || !isOpen(payment.status)) return;
    if (!this.breaker.tryAcquire(now)) {
      // CallNotPermittedException is not in the retry list: fall back to Deferred immediately
      this.retries.failed_without_retry++;
      this.env.trace?.({
        t: now,
        source: "payment-service",
        kind: "breaker",
        text: `breaker processor is ${this.breaker.state}: CallNotPermitted, deferring instead of retrying`,
        orderId: call.orderId,
      });
      call.phase = "running";
      call.resolveAt = now;
      this.calls = this.calls.filter((c) => c !== call);
      this.commit(call.orderId, { kind: "deferred", reason: "CallNotPermittedException" }, now);
      return;
    }
    call.attempts++;
    // the processor is keyed on the order id and the attempt number; the row level attempt count
    // shifts the key so a payment swept after a deferral explores fresh outcomes
    const result = this.authorize(payment, payment.attempts * MAX_ATTEMPTS + call.attempts);
    call.result = result;
    call.phase = "running";
    call.resolveAt = now + Math.min(result.latency, TIME_LIMIT_MS);
    if (!this.calls.includes(call)) this.calls.push(call);
    this.env.trace?.({
      t: now,
      source: "payment-service",
      kind: "authorize",
      text: `attempt ${call.attempts}: Retry(CircuitBreaker(TimeLimiter(processor.authorize))) for ${payment.amount.toFixed(2)}`,
      orderId: call.orderId,
    });
  }

  /** Deterministic synthetic processor: outcome depends only on the order id and the attempt. */
  private authorize(payment: Payment, attempt: number): Result {
    const base = 40 + (bucket(payment.orderId + ":lat:" + attempt) % 80);
    if (payment.customerId.endsWith("-declined")) {
      return { kind: "declined", reason: "CARD_DECLINED", latency: base };
    }
    if (payment.amount > this.processor.limit) {
      return { kind: "declined", reason: "OVER_LIMIT", latency: base };
    }
    const b = bucket(payment.orderId + ":" + attempt);
    if (b < this.processor.transientPercent) return { kind: "transient", latency: 15 };
    if (b < this.processor.transientPercent + this.processor.slowPercent) {
      return { kind: "slow", latency: this.processor.slowMillis };
    }
    return { kind: "approved", code: "AUTH-" + hex(payment.orderId, 12).toUpperCase(), latency: base };
  }

  /** Persists the outcome and its event in one transaction. */
  private commit(orderId: string, outcome: Outcome, now: number): void {
    const payment = this.payments.get(orderId);
    if (!payment || !isOpen(payment.status)) return;
    payment.attempts++;
    switch (outcome.kind) {
      case "authorized": {
        payment.status = "AUTHORIZED";
        const event: PaymentCompleted = {
          type: "PaymentCompleted",
          topic: Topics.PAYMENT_COMPLETED,
          eventId: this.rng.id("evt"),
          orderId,
          correlationId: payment.correlationId,
          occurredAt: now,
          authorizationCode: outcome.code,
        };
        this.outbox.append(event, now);
        this.env.trace?.({
          t: now,
          source: "payment-service",
          kind: "authorized",
          text: `authorized ${outcome.code}; UPDATE payment AUTHORIZED; INSERT outbox_event (PaymentCompleted); COMMIT`,
          orderId,
        });
        break;
      }
      case "declined": {
        payment.status = "DECLINED";
        payment.lastError = outcome.reason;
        const event: PaymentFailed = {
          type: "PaymentFailed",
          topic: Topics.PAYMENT_FAILED,
          eventId: this.rng.id("evt"),
          orderId,
          correlationId: payment.correlationId,
          occurredAt: now,
          reason: outcome.reason,
        };
        this.outbox.append(event, now);
        this.env.trace?.({
          t: now,
          source: "payment-service",
          kind: "declined",
          text: `declined ${outcome.reason} (business result, never retried); INSERT outbox_event (PaymentFailed); COMMIT`,
          orderId,
        });
        break;
      }
      case "deferred": {
        const backoff = RETRY_DELAY_MS * Math.min(payment.attempts, 5);
        payment.status = "DEFERRED";
        payment.lastError = outcome.reason;
        payment.nextAttemptAt = now + backoff;
        this.deferred++;
        this.env.trace?.({
          t: now,
          source: "payment-service",
          kind: "deferred",
          text: `deferred (${outcome.reason}); UPDATE payment DEFERRED next_attempt_at +${backoff / 1000} s`,
          orderId,
        });
        break;
      }
    }
    this.outcomes[payment.status as keyof typeof this.outcomes]++;
  }

  /** Attempts every open payment whose retry time has come, oldest first, up to 100. */
  private sweep(now: number): number {
    const due: Payment[] = [];
    for (const p of this.payments.values()) {
      if (isOpen(p.status) && p.nextAttemptAt <= now) due.push(p);
    }
    due.sort((a, b) => a.nextAttemptAt - b.nextAttemptAt);
    let attempted = 0;
    for (const p of due.slice(0, 100)) {
      if (this.attempt(p.orderId, now)) {
        attempted++;
        this.env.trace?.({
          t: now,
          source: "payment-service",
          kind: "sweep",
          text: `sweeper picked up open payment (${p.status}, ${p.attempts} attempt(s) so far)`,
          orderId: p.orderId,
        });
      }
    }
    return attempted;
  }
}

function isOpen(status: PaymentStatus): boolean {
  return status === "NEW" || status === "DEFERRED";
}
