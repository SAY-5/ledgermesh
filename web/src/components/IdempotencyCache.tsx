import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useRef } from "react";
import { useRunner } from "../hooks/useRunner.ts";
import { Cluster } from "../sim/cluster.ts";
import { Topics } from "../sim/events.ts";
import type { StockView } from "../sim/orderService.ts";
import type { BreakerState } from "../sim/resilience.ts";

const SKU = "SKU-ALPHA";

function buildDuplicates(): Cluster {
  return new Cluster({
    seed: 31,
    trace: true,
    redeliverChance: 0,
    stockSeed: { [SKU]: 100 },
    processor: { transientPercent: 0, slowPercent: 0 },
    maxTraces: 60,
  });
}

function buildBreaker(): Cluster {
  return new Cluster({
    seed: 32,
    trace: true,
    redeliverChance: 0,
    stockSeed: { [SKU]: 250 },
    processor: { transientPercent: 0, slowPercent: 0 },
    maxTraces: 60,
  });
}

interface ProbeEntry {
  at: number;
  view: StockView;
}

const BREAKER_STATES: { id: BreakerState; blurb: string }[] = [
  { id: "CLOSED", blurb: "calls flow, window of 10, opens at 50% failures after 4 calls" },
  { id: "OPEN", blurb: "fail fast for 5 s, every read served from the last known value" },
  { id: "HALF_OPEN", blurb: "2 trial calls decide: back to CLOSED, or OPEN again" },
];

export function IdempotencyCache() {
  const reduced = useReducedMotion();

  // panel A: duplicates
  const dupFactory = useCallback(buildDuplicates, []);
  const dup = useRunner(dupFactory, { speed: 1, autoStart: true, frameMs: 120 });
  const dupCluster = dup.cluster;
  const stock = dupCluster.inventory.stock.get(SKU)?.available ?? 0;
  const deliveries = dupCluster.traces.filter(
    (t) => t.kind === "reserved" || t.kind === "duplicate" || t.kind === "inject",
  );

  // panel B: breaker + cache
  const probes = useRef<ProbeEntry[]>([]);
  const nextProbe = useRef(0);
  const brkFactory = useCallback(() => {
    probes.current = [];
    nextProbe.current = 0;
    return buildBreaker();
  }, []);
  const brk = useRunner(brkFactory, {
    speed: 1,
    autoStart: true,
    frameMs: 100,
    onStep: (c) => {
      if (c.now >= nextProbe.current) {
        nextProbe.current = c.now + 500;
        c.probeStock(SKU, (view) => {
          probes.current.push({ at: c.now, view });
          if (probes.current.length > 18) probes.current.shift();
        });
      }
    },
  });
  const brkCluster = brk.cluster;
  const breaker = brkCluster.order.breaker;
  const inventory = brkCluster.inventory;
  const lastKill = brkCluster.kills[brkCluster.kills.length - 1];
  const inventoryStatus = !inventory.alive
    ? `killed, restart in ${lastKill ? Math.max(0, (lastKill.restartAt - brkCluster.now) / 1000).toFixed(1) : "?"} s`
    : !inventory.ready(brkCluster.now)
      ? `booting, ready in ${((inventory.readyAt - brkCluster.now) / 1000).toFixed(1)} s`
      : "ready";
  const redisKeys = brkCluster.redis.snapshot(brkCluster.now);

  return (
    <section className="section" id="idempotency" aria-labelledby="idem-title">
      <div className="wrap">
        <div className="section-head">
          <span className="section-index">02 · idempotency + cache</span>
          <h2 className="section-title" id="idem-title">
            Redeliveries are harmless. Outages are cached.
          </h2>
          <p className="section-lede">
            At-least-once delivery means every consumer will eventually see a record twice. The
            processed marker is written in the same transaction as the work, so the second delivery
            is a no-op. On the synchronous path, a breaker and an 800 ms time limiter keep the order
            service answering from its last known stock value while inventory is gone.
          </p>
        </div>

        <div className="resil-grid">
          <div className="glass panel">
            <div className="panel-head">
              <h3 className="panel-title">Inject a duplicate delivery</h3>
              <span className="pill">inventory-service · order.created</span>
            </div>
            <div className="panel-actions">
              <button
                type="button"
                className="btn btn-copper"
                onClick={() => {
                  dupCluster.submit("cust-8", [{ sku: SKU, quantity: 2, unitPrice: 12 }]);
                }}
              >
                Submit order (2 units)
              </button>
              <button
                type="button"
                className="btn"
                disabled={dupCluster.broker.size(Topics.ORDER_CREATED) === 0}
                onClick={() => dupCluster.injectDuplicate(Topics.ORDER_CREATED, "inventory-service")}
              >
                Redeliver last order.created
              </button>
              <button type="button" className="btn" onClick={() => dup.reset()}>
                Reset
              </button>
            </div>
            <dl className="stat-row">
              <div className="stat">
                <dt>stock {SKU}</dt>
                <dd className="mono">{stock}</dd>
              </div>
              <div className="stat">
                <dt>processed_event rows</dt>
                <dd className="mono">{dupCluster.inventory.idempotent.size()}</dd>
              </div>
              <div className="stat is-warn">
                <dt>duplicates ignored</dt>
                <dd className="mono">{dupCluster.inventory.idempotent.duplicates}</dd>
              </div>
              <div className="stat is-ok">
                <dt>confirmed</dt>
                <dd className="mono">{dupCluster.stats().confirmed}</dd>
              </div>
            </dl>
            <ol className="deliveries" aria-label="Deliveries of order.created">
              <AnimatePresence initial={false}>
                {deliveries.slice(-8).map((t, i) => (
                  <motion.li
                    key={`${t.t}-${t.kind}-${i}`}
                    className={`delivery kind-${t.kind}`}
                    initial={reduced ? false : { opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0 }}
                  >
                    <span className="mono delivery-t">{(t.t / 1000).toFixed(2)}s</span>
                    <span className="delivery-kind mono">
                      {t.kind === "reserved" ? "reserved once" : t.kind === "duplicate" ? "ignored" : "redelivered"}
                    </span>
                    <span className="delivery-text">{t.text}</span>
                  </motion.li>
                ))}
              </AnimatePresence>
              {deliveries.length === 0 ? (
                <li className="delivery delivery-empty">submit an order, then redeliver its event</li>
              ) : null}
            </ol>
          </div>

          <div className="glass panel">
            <div className="panel-head">
              <h3 className="panel-title">Take inventory down, keep answering</h3>
              <span className={`pill ${inventory.ready(brkCluster.now) ? "pill-ok" : "pill-bad"}`}>
                inventory {inventoryStatus}
              </span>
            </div>
            <div className="panel-actions">
              <button
                type="button"
                className="btn btn-danger"
                disabled={!inventory.alive}
                onClick={() => brkCluster.kill("inventory-service", 5000)}
              >
                Kill inventory-service (restart in 5 s)
              </button>
              <button type="button" className="btn" onClick={() => brk.reset()}>
                Reset
              </button>
              <span className="mono panel-clock">probe GET /stock/{SKU} every 500 ms</span>
            </div>

            <ol className="breaker-diagram" aria-label="Circuit breaker state">
              {BREAKER_STATES.map((s) => (
                <li key={s.id} className={`breaker-state state-${s.id} ${breaker.state === s.id ? "is-on" : ""}`}>
                  <span className="breaker-name mono">{s.id.replace("_", " ")}</span>
                  <span className="breaker-blurb">{s.blurb}</span>
                  {breaker.state === s.id && s.id === "OPEN" ? (
                    <span className="breaker-timer mono">half open in {(breaker.reopensIn(brkCluster.now) / 1000).toFixed(1)} s</span>
                  ) : null}
                  {breaker.state === s.id && s.id === "CLOSED" ? (
                    <span className="breaker-timer mono">failure rate {breaker.failureRate().toFixed(0)}%</span>
                  ) : null}
                </li>
              ))}
            </ol>

            <div className="probe-strip" aria-label="Recent stock probes">
              {probes.current.map((p) => (
                <span
                  key={p.at}
                  className={`probe src-${p.view.source}`}
                  title={`${(p.at / 1000).toFixed(1)}s ${p.view.source} ${p.view.available ?? "?"}`}
                >
                  <span className="probe-val mono">{p.view.available ?? "?"}</span>
                  <span className="probe-src mono">{p.view.source}</span>
                </span>
              ))}
            </div>

            <div className="redis-and-log">
              <div className="redis-keys">
                <span className="eyebrow">redis</span>
                {redisKeys.length === 0 ? <span className="mono muted">no keys</span> : null}
                {redisKeys.map((k) => (
                  <span key={k.sku} className="mono redis-key">
                    stock:{k.sku} = {k.value} <span className="muted">ttl {(k.ttlMs / 1000).toFixed(0)} s</span>
                  </span>
                ))}
                <span className="mono muted">
                  hits {brkCluster.redis.hits} · misses {brkCluster.redis.misses}
                </span>
              </div>
              <ol className="transition-log" aria-label="Breaker transitions">
                {brkCluster.breakerLog.slice(-5).map((t, i) => (
                  <li key={`${t.at}-${i}`} className="mono">
                    <span className="muted">{(t.at / 1000).toFixed(1)}s</span> {t.from} {"->"} {t.to}
                  </li>
                ))}
                {brkCluster.breakerLog.length === 0 ? <li className="mono muted">no transitions yet</li> : null}
              </ol>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
