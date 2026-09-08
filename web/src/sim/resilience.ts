export type BreakerState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface BreakerConfig {
  slidingWindowSize: number;
  minimumNumberOfCalls: number;
  failureRateThreshold: number;
  waitDurationInOpenStateMs: number;
  permittedNumberOfCallsInHalfOpenState: number;
}

export interface BreakerTransition {
  at: number;
  from: BreakerState;
  to: BreakerState;
}

/**
 * Count based Resilience4j circuit breaker. Failures include time limiter timeouts. The breaker
 * moves to half open automatically after the open wait, permits a fixed number of trial calls,
 * and closes or reopens on their outcome.
 */
export class CircuitBreaker {
  state: BreakerState = "CLOSED";
  private window: boolean[] = [];
  private openedAt = 0;
  private halfOpenIssued = 0;
  private halfOpenResults: boolean[] = [];
  transitions: BreakerTransition[] = [];
  onTransition?: (t: BreakerTransition) => void;

  constructor(
    public readonly name: string,
    public readonly config: BreakerConfig,
  ) {}

  /** Whether a call may proceed right now. Also drives OPEN -> HALF_OPEN on the clock. */
  tryAcquire(now: number): boolean {
    if (this.state === "OPEN") {
      if (now - this.openedAt >= this.config.waitDurationInOpenStateMs) {
        this.transition("HALF_OPEN", now);
      } else {
        return false;
      }
    }
    if (this.state === "HALF_OPEN") {
      if (this.halfOpenIssued >= this.config.permittedNumberOfCallsInHalfOpenState) return false;
      this.halfOpenIssued++;
      return true;
    }
    return true;
  }

  onSuccess(now: number): void {
    this.record(true, now);
  }

  onError(now: number): void {
    this.record(false, now);
  }

  failureRate(): number {
    if (this.window.length === 0) return 0;
    const failures = this.window.filter((ok) => !ok).length;
    return (failures / this.window.length) * 100;
  }

  /** In-memory state is gone after a kill: the restarted JVM starts closed with an empty window. */
  reset(): void {
    this.state = "CLOSED";
    this.window = [];
    this.halfOpenIssued = 0;
    this.halfOpenResults = [];
  }

  /** Remaining ms before an open breaker tries again. */
  reopensIn(now: number): number {
    return this.state === "OPEN"
      ? Math.max(0, this.config.waitDurationInOpenStateMs - (now - this.openedAt))
      : 0;
  }

  private record(ok: boolean, now: number): void {
    if (this.state === "HALF_OPEN") {
      this.halfOpenResults.push(ok);
      if (this.halfOpenResults.length >= this.config.permittedNumberOfCallsInHalfOpenState) {
        const failures = this.halfOpenResults.filter((r) => !r).length;
        const rate = (failures / this.halfOpenResults.length) * 100;
        if (rate >= this.config.failureRateThreshold) {
          this.transition("OPEN", now);
        } else {
          this.transition("CLOSED", now);
        }
      }
      return;
    }
    if (this.state === "OPEN") return;
    this.window.push(ok);
    if (this.window.length > this.config.slidingWindowSize) this.window.shift();
    if (
      this.window.length >= this.config.minimumNumberOfCalls &&
      this.failureRate() >= this.config.failureRateThreshold
    ) {
      this.transition("OPEN", now);
    }
  }

  private transition(to: BreakerState, now: number): void {
    const from = this.state;
    if (from === to) return;
    this.state = to;
    if (to === "OPEN") {
      this.openedAt = now;
      this.window = [];
    }
    if (to === "HALF_OPEN") {
      this.halfOpenIssued = 0;
      this.halfOpenResults = [];
    }
    if (to === "CLOSED") {
      this.window = [];
    }
    const t = { at: now, from, to };
    this.transitions.push(t);
    this.onTransition?.(t);
  }
}

/** Retry bookkeeping in the shape of resilience4j.retry.calls{kind}. */
export interface RetryCounters {
  successful_without_retry: number;
  successful_with_retry: number;
  failed_with_retry: number;
  failed_without_retry: number;
}

export function emptyRetryCounters(): RetryCounters {
  return {
    successful_without_retry: 0,
    successful_with_retry: 0,
    failed_with_retry: 0,
    failed_without_retry: 0,
  };
}
