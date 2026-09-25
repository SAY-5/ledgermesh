import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRunner } from "../hooks/useRunner.ts";
import { Cluster } from "../sim/cluster.ts";
import type { Trace, TraceSource } from "../sim/events.ts";
import type { OutboxRow } from "../sim/outbox.ts";
import type { OrderStatus } from "../sim/stateMachine.ts";

type Mode = "happy" | "declined" | "oos";

const MODES: { id: Mode; label: string; hint: string }[] = [
  { id: "happy", label: "Happy path", hint: "PENDING > RESERVED > CONFIRMED" },
  { id: "declined", label: "Card declined", hint: "compensation releases the units" },
  { id: "oos", label: "Out of stock", hint: "rejected before any reservation" },
];

const STATES: OrderStatus[] = ["PENDING", "RESERVED", "CONFIRMED"];

function build(mode: Mode): Cluster {
  const cluster = new Cluster({
    seed: mode === "happy" ? 21 : mode === "declined" ? 22 : 23,
    trace: true,
    redeliverChance: 0,
    stockSeed: { "SKU-ALPHA": 120, "SKU-SCARCE": 1 },
    processor: { transientPercent: 0, slowPercent: 0 },
  });
  if (mode === "oos") {
    cluster.submit("cust-17", [{ sku: "SKU-SCARCE", quantity: 2, unitPrice: 42 }]);
  } else {
    cluster.submit(mode === "declined" ? "cust-17-declined" : "cust-17", [
      { sku: "SKU-ALPHA", quantity: 2, unitPrice: 42 },
    ]);
  }
  return cluster;
}

const SOURCE_LABEL: Record<TraceSource, string> = {
  "order-service": "order",
  "inventory-service": "inventory",
  "payment-service": "payment",
  broker: "kafka",
  redis: "redis",
  client: "client",
  chaos: "chaos",
};

export function SagaTrace() {
  const [mode, setMode] = useState<Mode>("happy");
  const factory = useCallback(() => build(mode), [mode]);
  const runner = useRunner(factory, {
    speed: 0.16,
    autoStart: false,
    frameMs: 60,
    stopWhen: (c) => c.now > 400 && c.quiescent(),
  });
  const reduced = useReducedMotion();
  const { cluster, reset, start, running } = runner;

  useEffect(() => {
    reset();
  }, [mode, reset]);

  useEffect(() => {
    if (!running && cluster.now === 0) start();
  }, [cluster, running, start]);

  const order = cluster.order.find(cluster.submitted[0]);
  const traces = cluster.traces;
  const ledgerRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = ledgerRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [traces.length]);
  const done = !running && cluster.now > 0;
  const outboxes = useMemo(
    () => [
      { name: "order-service", rows: cluster.order.outbox.rows },
      { name: "inventory-service", rows: cluster.inventory.outbox.rows },
      { name: "payment-service", rows: cluster.payment.outbox.rows },
    ],
    [cluster, runner.frame],
  );

  const steps = STATES.map((s) => {
    if (!order) return { state: s, on: false };
    if (order.status === "CANCELLED") {
      const reached = s === "PENDING" || (s === "RESERVED" && order.reason === "PAYMENT_DECLINED");
      return { state: s, on: reached };
    }
    return { state: s, on: STATES.indexOf(s) <= STATES.indexOf(order.status) };
  });

  return (
    <section className="section" id="saga" aria-labelledby="saga-title">
      <div className="wrap">
        <div className="section-head">
          <span className="section-index">01 · outbox + saga</span>
          <h2 className="section-title" id="saga-title">
            One order, every hop, one transaction at a time.
          </h2>
          <p className="section-lede">
            The order row and its <code>order.created</code> outbox row commit together. A relay
            publishes the row and only then stamps <code>published_at</code>. Each consumer runs its
            work and its <code>processed_event</code> marker in one transaction, then writes its own
            outbox row. No service ever calls Kafka from business code.
          </p>
        </div>

        <div className="saga-controls" role="group" aria-label="Scenario">
          {MODES.map((m) => (
            <button
              key={m.id}
              type="button"
              className={`btn ${mode === m.id ? "btn-active" : ""}`}
              aria-pressed={mode === m.id}
              onClick={() => setMode(m.id)}
            >
              {m.label}
              <span className="saga-hint mono">{m.hint}</span>
            </button>
          ))}
          <button type="button" className="btn" onClick={() => reset()}>
            Replay
          </button>
          <span className="saga-clock mono" aria-live="off">
            t = {(cluster.now / 1000).toFixed(2)} s · slow motion
          </span>
        </div>

        <div className="saga-grid">
          <div className="glass saga-ribbon" aria-label="Order state">
            <ol className="saga-states">
              {steps.map((s, i) => (
                <li key={s.state} className={`saga-state ${s.on ? "is-on" : ""}`}>
                  <span className="saga-state-index mono">{i + 1}</span>
                  <span className="saga-state-name">{s.state}</span>
                </li>
              ))}
              <li
                className={`saga-state saga-state-cancel ${order?.status === "CANCELLED" ? "is-on is-bad" : ""}`}
              >
                <span className="saga-state-index mono">x</span>
                <span className="saga-state-name">
                  CANCELLED{order?.reason ? <small className="mono"> {order.reason}</small> : null}
                </span>
              </li>
            </ol>
            <div className="saga-order mono">
              <span>order {order?.id ?? "..."}</span>
              <span>customer {order?.customerId}</span>
              <span>amount {order?.amount.toFixed(2)}</span>
              <AnimatePresence>
                {done ? (
                  <motion.span
                    key="done"
                    className={`pill ${order?.status === "CONFIRMED" ? "pill-ok" : "pill-copper"}`}
                    initial={reduced ? false : { opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                  >
                    settled in {order ? ((order.updatedAt - order.createdAt) / 1000).toFixed(2) : "?"} s
                  </motion.span>
                ) : null}
              </AnimatePresence>
            </div>
          </div>

          <div className="saga-outboxes">
            {outboxes.map((o) => (
              <div key={o.name} className="glass outbox-table">
                <div className="outbox-head">
                  <span className="outbox-title">{o.name}</span>
                  <span className="mono outbox-tname">outbox_event</span>
                </div>
                <table>
                  <thead>
                    <tr>
                      <th>id</th>
                      <th>type</th>
                      <th>published_at</th>
                    </tr>
                  </thead>
                  <tbody>
                    <AnimatePresence initial={false}>
                      {o.rows.length === 0 ? (
                        <tr className="outbox-empty">
                          <td colSpan={3}>no rows</td>
                        </tr>
                      ) : (
                        o.rows.map((row: OutboxRow) => (
                          <motion.tr
                            key={row.id}
                            initial={reduced ? false : { opacity: 0, x: -8 }}
                            animate={{ opacity: 1, x: 0 }}
                            className={row.publishedAt === undefined ? "is-pending" : "is-published"}
                          >
                            <td className="mono">{row.id}</td>
                            <td>{row.eventType}</td>
                            <td className="mono">
                              {row.publishedAt === undefined ? "NULL" : `${(row.publishedAt / 1000).toFixed(2)} s`}
                            </td>
                          </motion.tr>
                        ))
                      )}
                    </AnimatePresence>
                  </tbody>
                </table>
              </div>
            ))}
          </div>

          <div className="glass saga-ledger" aria-live="polite" aria-label="Trace" ref={ledgerRef}>
            <ol className="ledger">
              <AnimatePresence initial={false}>
                {traces.map((t: Trace, i: number) => (
                  <motion.li
                    key={`${t.t}-${i}`}
                    className={`ledger-row src-${t.source} kind-${t.kind}`}
                    initial={reduced ? false : { opacity: 0, y: 6 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.35 }}
                  >
                    <span className="ledger-t mono">{(t.t / 1000).toFixed(2)}s</span>
                    <span className="ledger-src mono">{SOURCE_LABEL[t.source]}</span>
                    <span className="ledger-text">{t.text}</span>
                  </motion.li>
                ))}
              </AnimatePresence>
              {traces.length === 0 ? <li className="ledger-row ledger-empty">waiting for the first tick</li> : null}
            </ol>
          </div>
        </div>
      </div>
    </section>
  );
}
