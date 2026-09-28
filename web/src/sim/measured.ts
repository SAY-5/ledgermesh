/**
 * The measured side of every figure the page shows. Values come from MEASURED, which is generated
 * from the chaos summaries committed under chaos/evidence/, so nothing here is typed by hand. Each
 * figure carries the numbers it renders, and the self-check asserts every one of them appears in
 * the summary text the generator read, which is what stops a figure from drifting.
 */
import { MEASURED } from "./measured.generated.ts";

export interface MeasuredKill {
  readonly service: string;
  readonly atSeconds: number;
}

/** The shape both recorded runs share; a baseline has no kills and no ceiling. */
export interface MeasuredRun {
  readonly source: string;
  /** the commit that introduced the bytes `source` holds, read from the history, for a permalink */
  readonly blobCommit: string;
  readonly recordedAt: string;
  readonly commit: string;
  readonly host: string;
  readonly knobs: string;
  readonly load: string;
  readonly submitted: number;
  readonly confirmed: number;
  readonly cancelledStock: number;
  readonly failedStuck: number;
  readonly kills: readonly MeasuredKill[];
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly maxMs: number;
  readonly p95CeilingMs: number | null;
  readonly p95CeilingHeld: boolean | null;
  readonly retriesWithRetry: number;
  readonly retriesExhausted: number;
  readonly deferred: number;
  readonly duplicates: number;
  readonly resubmits: number;
  readonly retriedOrders: number;
  readonly probes: {
    readonly live: number;
    readonly cache: number;
    readonly unknown: number;
    readonly error: number;
  };
  readonly breakerTransitions: string;
  readonly stuckOrders: number;
  readonly raw: string;
}

export const CHAOS_RUN: MeasuredRun = MEASURED.chaos;
export const BASELINE_RUN: MeasuredRun = MEASURED.baseline;

export interface MeasuredFigure {
  /** matches the label of the simulated counter it belongs beside */
  label: string;
  /** rendered exactly as the page shows it */
  text: string;
  /** the numbers inside `text` that must exist in the recorded summary */
  tokens: number[];
}

export function killTimeline(run: MeasuredRun): string {
  return run.kills.map((k) => `${k.service} @${k.atSeconds} s`).join(", ") || "no kills";
}

/** Where the run came from, in one line, for the note under a figure block. */
export function provenance(run: MeasuredRun): string {
  return `recorded ${run.recordedAt} at commit ${run.commit} on ${run.host}`;
}

/** The five figures the hero shows, in hero order. Exactly one number each. */
export function headlineFigures(run: MeasuredRun): MeasuredFigure[] {
  return [
    { label: "orders submitted", text: `${run.submitted}`, tokens: [run.submitted] },
    { label: "confirmed", text: `${run.confirmed}`, tokens: [run.confirmed] },
    {
      label: "cancelled, out of stock",
      text: `${run.cancelledStock}`,
      tokens: [run.cancelledStock],
    },
    { label: "failed / stuck", text: `${run.failedStuck}`, tokens: [run.failedStuck] },
    { label: "kills under load", text: `${run.kills.length}`, tokens: [run.kills.length] },
  ];
}

/**
 * The measured counterpart of each counter in the chaos section, keyed by the counter's own label.
 * Latencies stay in milliseconds because that is how the summary records them.
 */
export function counterFigures(run: MeasuredRun): MeasuredFigure[] {
  return [
    { label: "submitted", text: `${run.submitted}`, tokens: [run.submitted] },
    { label: "confirmed", text: `${run.confirmed}`, tokens: [run.confirmed] },
    { label: "cancelled (stock)", text: `${run.cancelledStock}`, tokens: [run.cancelledStock] },
    { label: "failed / stuck", text: `${run.failedStuck}`, tokens: [run.failedStuck] },
    {
      label: "retries (with retry ok)",
      text: `${run.retriesWithRetry}`,
      tokens: [run.retriesWithRetry],
    },
    { label: "deferred payments", text: `${run.deferred}`, tokens: [run.deferred] },
    { label: "duplicates ignored", text: `${run.duplicates}`, tokens: [run.duplicates] },
    {
      label: "probes live / cache",
      text: `${run.probes.live} / ${run.probes.cache}`,
      tokens: [run.probes.live, run.probes.cache],
    },
    {
      label: "saga p50 / p95",
      text: `p50 ${run.p50Ms} ms / p95 ${run.p95Ms} ms`,
      tokens: [run.p50Ms, run.p95Ms],
    },
    {
      label: "kill timeline",
      text: killTimeline(run),
      tokens: run.kills.map((k) => k.atSeconds),
    },
    {
      label: "retried submits",
      text: `${run.resubmits} over ${run.retriedOrders} orders`,
      tokens: [run.resubmits, run.retriedOrders],
    },
  ];
}

/** Every figure this module can render, for the self-check to walk. */
export function allFigures(): { run: MeasuredRun; figure: MeasuredFigure }[] {
  const out: { run: MeasuredRun; figure: MeasuredFigure }[] = [];
  for (const run of [CHAOS_RUN, BASELINE_RUN]) {
    for (const figure of [...headlineFigures(run), ...counterFigures(run)]) {
      out.push({ run, figure });
    }
  }
  return out;
}

/** A standalone number in the recorded summary, so 16 does not match inside 1600. */
export function tokenInSummary(run: MeasuredRun, token: number): boolean {
  return new RegExp(`(^|[^\\d.])${token}([^\\d.]|$)`).test(run.raw);
}
