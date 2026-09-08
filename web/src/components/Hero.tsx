import { motion, useReducedMotion } from "framer-motion";
import { Counter } from "./Counter.tsx";
import { ServiceMap } from "./ServiceMap.tsx";

const REPO = "https://github.com/SAY-5/ledgermesh";

const STATS = [
  { label: "orders submitted", value: 1200, tone: "" },
  { label: "confirmed", value: 1162, tone: "" },
  { label: "cancelled, out of stock", value: 38, tone: "" },
  { label: "failed / stuck", value: 0, tone: "is-zero" },
  { label: "kills under load", value: 3, tone: "is-copper" },
];

export function Hero() {
  const reduced = useReducedMotion();
  const rise = (delay: number) =>
    reduced
      ? {}
      : {
          initial: { opacity: 0, y: 22 },
          animate: { opacity: 1, y: 0 },
          transition: { duration: 0.9, delay, ease: [0.16, 1, 0.3, 1] as const },
        };
  return (
    <header className="hero">
      <div className="wrap hero-grid">
        <div className="hero-copy">
          <motion.p className="eyebrow hero-eyebrow" {...rise(0)}>
            LedgerMesh · three Spring Boot services · Kafka · Redis · Docker
          </motion.p>
          <motion.h1 className="hero-title" {...rise(0.08)}>
            Kill a service.
            <br />
            <span className="hero-title-accent">Every order still settles.</span>
          </motion.h1>
          <motion.p className="hero-lede" {...rise(0.18)}>
            A transactional outbox makes every event durable before it is sent, idempotent consumers
            make every redelivery harmless, and a deferred queue makes a lost payment attempt
            resumable. This page runs a faithful TypeScript port of the services in your browser so
            you can pull the plug yourself.
          </motion.p>
          <motion.div className="hero-actions" {...rise(0.26)}>
            <a className="btn btn-copper" href="#chaos">
              Run the chaos test
            </a>
            <a className="btn" href="#saga">
              Trace one order
            </a>
            <a className="btn" href={REPO} target="_blank" rel="noreferrer">
              Source
            </a>
          </motion.div>
        </div>

        <motion.div className="hero-map glass" {...rise(0.32)}>
          <ServiceMap />
        </motion.div>

        <motion.dl className="hero-stats" {...rise(0.4)}>
          {STATS.map((s) => (
            <div key={s.label} className={`hero-stat ${s.tone}`}>
              <dt>{s.label}</dt>
              <dd className="mono">
                <Counter value={s.value} whenVisible={false} duration={1.8} />
              </dd>
            </div>
          ))}
          <p className="hero-stats-note">
            Measured run recorded in the repository: 60 s at 20 orders/s, inventory killed at 22 s
            and 56 s, payment at 40 s, each restarted after 5 s. 40 stock probes were served from
            cache while inventory was down; 1 duplicate delivery was ignored.
          </p>
        </motion.dl>
      </div>
    </header>
  );
}
