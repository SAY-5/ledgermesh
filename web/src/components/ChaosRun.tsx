import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useMemo, useRef, useState } from "react";
import { useRunner } from "../hooks/useRunner.ts";
import { Cluster, formatSummary } from "../sim/cluster.ts";
import type { ServiceName, Trace } from "../sim/events.ts";
import { LoadGenerator, planKills, type KillPlan } from "../sim/loadgen.ts";
import { ServiceMap, type MapState } from "./ServiceMap.tsx";

const DURATION_MS = 60_000;
const RATE = 20;
const KILLS = 3;
const SEED = 7;
const NOTABLE = new Set(["kill", "restart", "breaker", "duplicate", "deferred", "sweep", "redelivery", "inject"]);

interface Session {
  cluster: Cluster;
  load: LoadGenerator;
  plan: KillPlan[];
  nextKill: number;
  log: Trace[];
}

function build(seed: number, onLog: (t: Trace) => void): Session {
  const cluster = new Cluster({ seed, trace: true, maxTraces: 1 });
  cluster.onTrace = (t) => {
    if (NOTABLE.has(t.kind)) onLog(t);
  };
  return {
    cluster,
    load: new LoadGenerator({ rate: RATE, durationMs: DURATION_MS, seed }),
    plan: planKills(DURATION_MS, KILLS, seed),
    nextKill: 0,
    log: [],
  };
}

export function ChaosRun() {
  const reduced = useReducedMotion();
  const [autoChaos, setAutoChaos] = useState(true);
  const [seed, setSeed] = useState(SEED);
  const autoRef = useRef(autoChaos);
  autoRef.current = autoChaos;
  const session = useRef<Session | null>(null);

  const factory = useCallback(() => {
    const s = build(seed, (t) => {
      s.log.push(t);
      if (s.log.length > 40) s.log.shift();
    });
    session.current = s;
    return s.cluster;
  }, [seed]);

  const runner = useRunner(factory, {
    speed: 1,
    autoStart: false,
    frameMs: 90,
    onStep: (cluster) => {
      const s = session.current;
      if (!s || s.cluster !== cluster) return;
      if (!s.load.finished(cluster.now)) s.load.tick(cluster);
      if (autoRef.current && s.nextKill < s.plan.length && cluster.now >= s.plan[s.nextKill].at) {
        const last = cluster.kills[cluster.kills.length - 1];
        if (!last || cluster.now >= last.readyAt) {
          cluster.kill(s.plan[s.nextKill].service, 5000);
          s.nextKill++;
        }
      }
    },
    stopWhen: (cluster) => {
      const s = session.current;
      return !!s && s.load.finished(cluster.now) && cluster.quiescent();
    },
  });
  const { cluster, running, speed, setSpeed, start, pause, reset } = runner;
  const s = session.current;
  const rawStats = cluster.stats();
  const loadDone = s ? s.load.finished(cluster.now) : false;
  const phase: "idle" | "running" | "draining" | "done" =
    cluster.now === 0 ? "idle" : !loadDone ? "running" : running ? "draining" : "done";
  // while orders are still moving, open orders are in flight, not stuck; the script only counts
  // stuck orders once the drain is over
  const stats =
    phase === "done" ? rawStats : { ...rawStats, failed: rawStats.cancelledOther, stuck: 0 };
  const inFlight = rawStats.stuck;
  const elapsed = Math.min(cluster.now, DURATION_MS) / 1000;

  const mapState: MapState = useMemo(() => {
    const view = (name: ServiceName) => {
      const svc = cluster.service(name);
      const kill = [...cluster.kills].reverse().find((k) => k.service === name);
      const note = !svc.alive
        ? `SIGKILL · restart in ${kill ? Math.max(0, (kill.restartAt - cluster.now) / 1000).toFixed(1) : "?"} s`
        : !svc.ready(cluster.now)
          ? `booting · ${((svc.readyAt - cluster.now) / 1000).toFixed(1)} s`
          : name === "order-service"
            ? "accepting orders"
            : "ready";
      return { alive: svc.alive, ready: svc.ready(cluster.now), note };
    };
    return {
      services: {
        "order-service": view("order-service"),
        "inventory-service": view("inventory-service"),
        "payment-service": view("payment-service"),
      },
      breakers: { inventory: cluster.order.breaker.state, processor: cluster.payment.breaker.state },
      lag: {
        created: cluster.broker.lag("inventory-service", "order.created"),
        reserved:
          cluster.broker.lag("order-service", "inventory.reserved") +
          cluster.broker.lag("payment-service", "inventory.reserved"),
        payment:
          cluster.broker.lag("order-service", "payment.completed") +
          cluster.broker.lag("order-service", "payment.failed"),
        cancelled: cluster.broker.lag("inventory-service", "order.cancelled"),
      },
      flow: phase === "running" ? 1 : phase === "draining" ? 0.6 : 0.34,
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cluster, runner.frame, phase]);

  const kill = (name: ServiceName) => {
    if (!running) start();
    cluster.kill(name, 5000);
  };

  const failedTone = stats.failed === 0 ? "is-ok" : "is-bad";
  const timelineEnd = Math.max(DURATION_MS * 1.3, cluster.now + 4000);

  return (
    <section className="section" id="chaos" aria-labelledby="chaos-title">
      <div className="wrap">
        <div className="section-head">
          <span className="section-index">03 · chaos run</span>
          <h2 className="section-title" id="chaos-title">
            Sixty seconds, twenty orders a second, three kills.
          </h2>
          <p className="section-lede">
            The same run as <code>make chaos</code>: a seeded load generator, kills spread across
            the window with jitter, a restart after 5 s, then a drain until every order is terminal.
            Kill whatever you like whenever you like. The <strong>failed / stuck</strong> counter is
            the one that matters.
          </p>
        </div>

        <div className="chaos-controls">
          <div className="chaos-buttons">
            {phase === "done" ? (
              <button type="button" className="btn btn-copper" onClick={() => reset()}>
                Run again
              </button>
            ) : running ? (
              <button type="button" className="btn" onClick={() => pause()}>
                Pause
              </button>
            ) : (
              <button type="button" className="btn btn-copper" onClick={() => start()}>
                {phase === "idle" ? "Start the run" : "Resume"}
              </button>
            )}
            <button type="button" className="btn" onClick={() => reset()} disabled={phase === "idle"}>
              Reset
            </button>
            <div className="speed" role="group" aria-label="Speed">
              {[1, 2, 4].map((x) => (
                <button
                  key={x}
                  type="button"
                  className={`btn btn-small ${speed === x ? "btn-active" : ""}`}
                  aria-pressed={speed === x}
                  onClick={() => setSpeed(x)}
                >
                  {x}x
                </button>
              ))}
            </div>
            <label className="toggle">
              <input type="checkbox" checked={autoChaos} onChange={(e) => setAutoChaos(e.target.checked)} />
              <span>auto chaos ({KILLS} scheduled kills)</span>
            </label>
            <label className="toggle seed-field">
              <span className="mono">seed</span>
              <input
                className="mono"
                type="number"
                min={1}
                max={9999}
                value={seed}
                disabled={phase !== "idle"}
                onChange={(e) => setSeed(Math.max(1, Number(e.target.value) || 1))}
                aria-label="Seed"
              />
            </label>
          </div>
          <div className="chaos-kill-buttons">
            <button
              type="button"
              className="btn btn-danger"
              disabled={!cluster.inventory.alive || phase === "done"}
              onClick={() => kill("inventory-service")}
            >
              Kill inventory-service
            </button>
            <button
              type="button"
              className="btn btn-danger"
              disabled={!cluster.payment.alive || phase === "done"}
              onClick={() => kill("payment-service")}
            >
              Kill payment-service
            </button>
            <span className="mono chaos-note">order-service is never killed: it only needs its own database to accept orders</span>
          </div>
        </div>

        <div className="chaos-grid">
          <div className="glass chaos-map">
            <div className="chaos-map-head">
              <span className={`pill ${phase === "running" ? "pill-copper" : phase === "done" ? "pill-ok" : ""}`}>
                {phase === "idle"
                  ? "ready"
                  : phase === "running"
                    ? `load · t+${elapsed.toFixed(1)} s`
                    : phase === "draining"
                      ? `draining · ${inFlight} open`
                      : "settled"}
              </span>
              <span className="mono muted">
                redis hits {stats.cache.hits} · misses {stats.cache.misses}
              </span>
            </div>
            <ServiceMap state={mapState} compact />
          </div>

          <dl className="chaos-counters">
            <Stat label="submitted" value={stats.submitted} />
            <Stat label="confirmed" value={stats.confirmed} tone="is-ok" />
            <Stat label="cancelled (stock)" value={stats.cancelledStock} />
            <Stat label="failed / stuck" value={stats.failed} tone={failedTone} big />
            <Stat label="in flight" value={inFlight} />
            <Stat label="retries (with retry ok)" value={stats.retries.successful_with_retry} />
            <Stat label="deferred payments" value={stats.deferred} tone="is-warn" />
            <Stat label="duplicates ignored" value={stats.duplicates} tone="is-warn" />
            <Stat label="probes live / cache" text={`${stats.stockProbes.live} / ${stats.stockProbes.cache}`} />
            <Stat label="saga p50 / p95" text={`${(stats.p50 / 1000).toFixed(1)} / ${(stats.p95 / 1000).toFixed(1)} s`} />
            <Stat label="open payments" value={cluster.payment.openPayments()} />
            <Stat label="outbox backlog" value={stats.outboxBacklog} />
          </dl>
        </div>

        <div className="chaos-charts">
          <div className="glass chart">
            <div className="chart-head">
              <span className="eyebrow">saga latency</span>
              <span className="mono muted">creation to terminal state, one dot per settled order</span>
            </div>
            <Sparkline cluster={cluster} end={timelineEnd} frame={runner.frame} />
          </div>
          <div className="glass chart">
            <div className="chart-head">
              <span className="eyebrow">kill timeline</span>
              <span className="mono muted">red kill · amber restart · green ready</span>
            </div>
            <KillTimeline cluster={cluster} end={timelineEnd} />
          </div>
        </div>

        <div className="chaos-bottom">
          <div className="glass chaos-log" aria-live="polite" aria-label="Notable events">
            <span className="eyebrow">notable events</span>
            <ol className="ledger">
              <AnimatePresence initial={false}>
                {[...(s?.log ?? [])].reverse().slice(0, 14).map((t, i) => (
                  <motion.li
                    key={`${t.t}-${t.kind}-${i}`}
                    className={`ledger-row src-${t.source} kind-${t.kind}`}
                    initial={reduced ? false : { opacity: 0, y: 4 }}
                    animate={{ opacity: 1, y: 0 }}
                  >
                    <span className="ledger-t mono">{(t.t / 1000).toFixed(1)}s</span>
                    <span className="ledger-src mono">{t.source.replace("-service", "")}</span>
                    <span className="ledger-text">{t.text}</span>
                  </motion.li>
                ))}
              </AnimatePresence>
              {!s || s.log.length === 0 ? <li className="ledger-row ledger-empty">kills, breaker transitions, redeliveries and deferrals show up here</li> : null}
            </ol>
          </div>

          <div className={`glass chaos-summary ${phase === "done" ? "is-done" : ""}`}>
            <div className="chart-head">
              <span className="eyebrow">chaos summary</span>
              {phase === "done" ? (
                <span className={`pill ${stats.failed === 0 ? "pill-ok" : "pill-bad"}`}>
                  {stats.failed === 0 ? "0 failed / stuck" : `${stats.failed} failed`}
                </span>
              ) : (
                <span className="mono muted">printed when the backlog drains</span>
              )}
            </div>
            <pre className="mono summary-pre">{formatSummary(stats, 60, RATE)}</pre>
          </div>
        </div>
      </div>
    </section>
  );
}

function Stat({
  label,
  value,
  text,
  tone = "",
  big = false,
}: {
  label: string;
  value?: number;
  text?: string;
  tone?: string;
  big?: boolean;
}) {
  return (
    <div className={`stat chaos-stat ${tone} ${big ? "is-big" : ""}`}>
      <dt>{label}</dt>
      <dd className="mono">{text ?? value?.toLocaleString("en-US")}</dd>
    </div>
  );
}

function Sparkline({ cluster, end, frame }: { cluster: Cluster; end: number; frame: number }) {
  const w = 600;
  const h = 110;
  const pad = 6;
  const points = cluster.order.completions;
  const maxMs = Math.max(2000, ...points.slice(-2000).map((p) => p.ms));
  const x = (t: number) => pad + (t / end) * (w - 2 * pad);
  const y = (ms: number) => h - pad - (ms / maxMs) * (h - 2 * pad);
  const dots = useMemo(
    () => points.slice(-1500).map((p) => `${x(p.at).toFixed(1)},${y(p.ms).toFixed(1)}`),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [points.length, end, frame],
  );
  return (
    <svg className="sparkline" viewBox={`0 0 ${w} ${h}`} role="img" aria-label={`Saga latency, maximum ${Math.round(maxMs)} ms`}>
      <line x1={pad} x2={w - pad} y1={h - pad} y2={h - pad} className="axis" />
      {cluster.kills.map((k) => (
        <line key={k.at} x1={x(k.at)} x2={x(k.at)} y1={pad} y2={h - pad} className="kill-line" />
      ))}
      <line x1={x(DURATION_MS)} x2={x(DURATION_MS)} y1={pad} y2={h - pad} className="load-end" />
      {dots.map((d, i) => {
        const [cx, cy] = d.split(",");
        return <circle key={i} cx={cx} cy={cy} r="1.6" className="dot" />;
      })}
      <text x={w - pad} y={pad + 10} textAnchor="end" className="axis-label">
        {(maxMs / 1000).toFixed(1)} s
      </text>
      <text x={pad} y={h - pad - 4} className="axis-label">
        0
      </text>
    </svg>
  );
}

function KillTimeline({ cluster, end }: { cluster: Cluster; end: number }) {
  const w = 600;
  const h = 64;
  const x = (t: number) => 8 + (t / end) * (w - 16);
  return (
    <svg className="timeline" viewBox={`0 0 ${w} ${h}`} role="img" aria-label="Kill timeline">
      <rect x="8" y="26" width={w - 16} height="10" rx="5" className="track" />
      <rect x="8" y="26" width={Math.max(0, x(Math.min(cluster.now, DURATION_MS)) - 8)} height="10" rx="5" className="load" />
      <rect x={x(DURATION_MS)} y="26" width={Math.max(0, x(cluster.now) - x(DURATION_MS))} height="10" rx="5" className="drain" />
      {cluster.kills.map((k) => (
        <g key={k.at}>
          <rect x={x(k.at)} y="22" width={Math.max(2, x(k.readyAt) - x(k.at))} height="18" rx="3" className="outage" />
          <circle cx={x(k.at)} cy="31" r="4" className="mark-kill" />
          <circle cx={x(k.restartAt)} cy="31" r="3" className="mark-restart" />
          <circle cx={x(k.readyAt)} cy="31" r="3" className="mark-ready" />
          <text x={x(k.at)} y="14" textAnchor="middle" className="mark-label">
            {k.service.replace("-service", "")} @{Math.round(k.at / 1000)}s
          </text>
        </g>
      ))}
      <line x1={x(cluster.now)} x2={x(cluster.now)} y1="18" y2="44" className="cursor" />
      <text x="8" y="58" className="axis-label">
        0 s
      </text>
      <text x={x(DURATION_MS)} y="58" textAnchor="middle" className="axis-label">
        60 s · load ends
      </text>
      <text x={w - 8} y="58" textAnchor="end" className="axis-label">
        drain
      </text>
    </svg>
  );
}
