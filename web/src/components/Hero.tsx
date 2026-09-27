import { motion, useReducedMotion } from "framer-motion";
import { CHAOS_RUN, headlineFigures, killTimeline, provenance } from "../sim/measured.ts";
import { Counter } from "./Counter.tsx";
import { ServiceMap } from "./ServiceMap.tsx";

const REPO = "https://github.com/SAY-5/ledgermesh";

const TONES: Record<string, string> = {
  "failed / stuck": "is-zero",
  "kills under load": "is-copper",
};

// Read from chaos/evidence/steady/summary.txt through src/sim/measured.generated.ts, so the hero
// cannot state a figure the recorded run does not contain.
const STATS = headlineFigures(CHAOS_RUN).map((figure) => ({
  label: figure.label,
  value: figure.tokens[0],
  tone: TONES[figure.label] ?? "",
}));

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
            resumable. This page runs a TypeScript simulation of those v1 mechanisms in the browser,
            with the constants generated from the services' configuration, so a service can be
            killed from here. Saga deadlines, dead letters, idempotency keys and the ops overview
            exist only in the services.
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
            <strong className="hero-stats-tag">measured</strong> {CHAOS_RUN.load}, kills{" "}
            {killTimeline(CHAOS_RUN)}. Stock probes {CHAOS_RUN.probes.live} live,{" "}
            {CHAOS_RUN.probes.cache} from cache, {CHAOS_RUN.probes.error} refused while a service was
            down; {CHAOS_RUN.resubmits} submits retried on their idempotency key over{" "}
            {CHAOS_RUN.retriedOrders} orders. {provenance(CHAOS_RUN)}. Read from{" "}
            <a
              href={`${REPO}/blob/${CHAOS_RUN.blobCommit}/${CHAOS_RUN.source}`}
              target="_blank"
              rel="noreferrer"
            >
              {CHAOS_RUN.source}
            </a>
            ; every figure below this line is simulated in the browser on a virtual clock.
          </p>
        </motion.dl>
      </div>
    </header>
  );
}
