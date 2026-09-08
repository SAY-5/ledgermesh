import { useReducedMotion } from "framer-motion";
import type { ServiceName } from "../sim/events.ts";
import type { BreakerState } from "../sim/resilience.ts";

export interface ServiceView {
  alive: boolean;
  ready: boolean;
  note?: string;
}

export interface MapState {
  services: Record<ServiceName, ServiceView>;
  breakers?: { inventory: BreakerState; processor: BreakerState };
  lag?: { created: number; reserved: number; payment: number; cancelled: number };
  /** particle density 0..1 */
  flow?: number;
}

const IDLE: MapState = {
  services: {
    "order-service": { alive: true, ready: true },
    "inventory-service": { alive: true, ready: true },
    "payment-service": { alive: true, ready: true },
  },
  flow: 1,
};

interface Edge {
  id: string;
  d: string;
  label: string;
  labelAt: [number, number];
  from: ServiceName;
  to: ServiceName;
  dashed?: boolean;
  particles: number;
  dur: number;
  lagKey?: keyof NonNullable<MapState["lag"]>;
}

const EDGES: Edge[] = [
  {
    id: "created",
    d: "M 250 200 C 400 120, 520 96, 660 96",
    label: "order.created",
    labelAt: [455, 108],
    from: "order-service",
    to: "inventory-service",
    particles: 3,
    dur: 2.6,
    lagKey: "created",
  },
  {
    id: "reserved",
    d: "M 660 138 C 520 150, 420 182, 250 224",
    label: "inventory.reserved / rejected",
    labelAt: [455, 190],
    from: "inventory-service",
    to: "order-service",
    particles: 3,
    dur: 2.6,
    lagKey: "reserved",
  },
  {
    id: "topay",
    d: "M 760 152 C 760 220, 760 260, 760 320",
    label: "inventory.reserved",
    labelAt: [812, 240],
    from: "inventory-service",
    to: "payment-service",
    particles: 2,
    dur: 1.8,
  },
  {
    id: "payment",
    d: "M 660 372 C 520 372, 420 300, 250 256",
    label: "payment.completed / failed",
    labelAt: [455, 340],
    from: "payment-service",
    to: "order-service",
    particles: 3,
    dur: 2.6,
    lagKey: "payment",
  },
  {
    id: "cancelled",
    d: "M 200 292 C 240 400, 560 430, 700 160",
    label: "order.cancelled (compensation)",
    labelAt: [470, 414],
    from: "order-service",
    to: "inventory-service",
    dashed: true,
    particles: 1,
    dur: 4.2,
    lagKey: "cancelled",
  },
];

const NODES: { name: ServiceName; x: number; y: number; port: number; store: string }[] = [
  { name: "order-service", x: 50, y: 178, port: 8081, store: "orders + outbox_event" },
  { name: "inventory-service", x: 660, y: 56, port: 8082, store: "stock_item + outbox_event" },
  { name: "payment-service", x: 660, y: 320, port: 8083, store: "payment + outbox_event" },
];

interface Props {
  state?: MapState;
  className?: string;
  /** used by the chaos section to attach kill buttons visually */
  compact?: boolean;
}

/**
 * The three services, their topics and the compensation loop as one SVG. Particles ride the edges
 * with SMIL animateMotion; under reduced motion they are drawn as static markers.
 */
export function ServiceMap({ state = IDLE, className, compact }: Props) {
  const reduced = useReducedMotion();
  const flow = state.flow ?? 1;
  return (
    <svg
      className={["service-map", compact ? "service-map-compact" : "", className ?? ""].join(" ")}
      viewBox="0 0 960 460"
      role="img"
      aria-labelledby="service-map-title service-map-desc"
    >
      <title id="service-map-title">LedgerMesh service map</title>
      <desc id="service-map-desc">
        order-service publishes order.created to inventory-service, which answers with
        inventory.reserved or inventory.rejected and forwards reservations to payment-service;
        payment-service answers with payment.completed or payment.failed; a declined payment emits
        order.cancelled back to inventory as compensation.
      </desc>
      <defs>
        <linearGradient id="edge-copper" x1="0" x2="1">
          <stop offset="0" stopColor="#d99a4e" stopOpacity="0.15" />
          <stop offset="0.5" stopColor="#d99a4e" stopOpacity="0.7" />
          <stop offset="1" stopColor="#d99a4e" stopOpacity="0.15" />
        </linearGradient>
        <radialGradient id="dot-glow">
          <stop offset="0" stopColor="#f6c47c" />
          <stop offset="0.5" stopColor="#d99a4e" />
          <stop offset="1" stopColor="#d99a4e" stopOpacity="0" />
        </radialGradient>
        <filter id="soft" x="-30%" y="-30%" width="160%" height="160%">
          <feGaussianBlur stdDeviation="3" />
        </filter>
      </defs>

      {/* broker band */}
      <g className="map-broker">
        <rect x="300" y="60" width="330" height="360" rx="26" />
        <text x="465" y="46" textAnchor="middle" className="map-broker-label">
          Redpanda (Kafka) · 6 topics · 3 partitions · keyed by order id
        </text>
      </g>

      {EDGES.map((edge) => {
        const src = state.services[edge.from];
        const dst = state.services[edge.to];
        const stalled = !dst.ready;
        const quiet = !src.ready;
        const lag = edge.lagKey && state.lag ? state.lag[edge.lagKey] : 0;
        return (
          <g key={edge.id} className={`map-edge ${stalled ? "is-stalled" : ""} ${quiet ? "is-quiet" : ""}`}>
            <path id={`edge-${edge.id}`} d={edge.d} className={edge.dashed ? "edge-line dashed" : "edge-line"} />
            <text x={edge.labelAt[0]} y={edge.labelAt[1]} textAnchor="middle" className="edge-label">
              {edge.label}
            </text>
            {lag ? (
              <text x={edge.labelAt[0]} y={edge.labelAt[1] + 16} textAnchor="middle" className="edge-lag">
                {lag} waiting on the topic
              </text>
            ) : null}
            {!quiet &&
              Array.from({ length: Math.max(1, Math.round(edge.particles * flow)) }).map((_, i) =>
                reduced ? (
                  <circle key={i} r="4" className="edge-dot">
                    <animateMotion dur="0.001s" fill="freeze" begin={`${(i / edge.particles) * 0.001}s`}>
                      <mpath href={`#edge-${edge.id}`} />
                    </animateMotion>
                  </circle>
                ) : (
                  <g key={i}>
                    <circle r="9" fill="url(#dot-glow)" opacity={stalled ? 0.35 : 0.9} filter="url(#soft)">
                      <animateMotion
                        dur={`${edge.dur}s`}
                        repeatCount="indefinite"
                        begin={`${-(i / edge.particles) * edge.dur}s`}
                        keyPoints={stalled ? "0;0.45;0.45" : undefined}
                        keyTimes={stalled ? "0;0.6;1" : undefined}
                        calcMode={stalled ? "linear" : undefined}
                      >
                        <mpath href={`#edge-${edge.id}`} />
                      </animateMotion>
                    </circle>
                    <circle r="3.2" className="edge-dot" opacity={stalled ? 0.5 : 1}>
                      <animateMotion
                        dur={`${edge.dur}s`}
                        repeatCount="indefinite"
                        begin={`${-(i / edge.particles) * edge.dur}s`}
                        keyPoints={stalled ? "0;0.45;0.45" : undefined}
                        keyTimes={stalled ? "0;0.6;1" : undefined}
                        calcMode={stalled ? "linear" : undefined}
                      >
                        <mpath href={`#edge-${edge.id}`} />
                      </animateMotion>
                    </circle>
                  </g>
                ),
              )}
          </g>
        );
      })}

      {/* redis attached to inventory */}
      <g className={`map-redis ${state.services["inventory-service"].ready ? "" : "is-detached"}`}>
        <path d="M 860 100 C 900 100, 900 100, 900 100" className="edge-line" />
        <rect x="878" y="72" width="70" height="56" rx="12" />
        <text x="913" y="96" textAnchor="middle" className="node-title">
          Redis
        </text>
        <text x="913" y="114" textAnchor="middle" className="node-sub">
          stock:{"{sku}"} TTL 60s
        </text>
      </g>

      {/* stock check HTTP call */}
      <g className="map-http">
        <path d="M 250 190 C 380 60, 520 40, 660 76" className="edge-line http" />
        <text x="420" y="54" textAnchor="middle" className="edge-label http">
          GET /stock/{"{sku}"} · breaker + 800 ms time limit · cache fallback
        </text>
        {state.breakers ? (
          <text x="420" y="70" textAnchor="middle" className={`edge-breaker state-${state.breakers.inventory}`}>
            breaker {state.breakers.inventory}
          </text>
        ) : null}
      </g>

      {NODES.map((node) => {
        const view = state.services[node.name];
        const status = !view.alive ? "killed" : !view.ready ? "booting" : "ready";
        return (
          <g key={node.name} className={`map-node is-${status}`} transform={`translate(${node.x} ${node.y})`}>
            <rect width="200" height="96" rx="18" className="node-body" />
            <rect x="0" y="0" width="200" height="96" rx="18" className="node-halo" />
            <circle cx="22" cy="24" r="5" className="node-led" />
            <text x="36" y="29" className="node-title">
              {node.name}
            </text>
            <text x="36" y="48" className="node-sub">
              :{node.port} · Postgres
            </text>
            <text x="36" y="64" className="node-sub">
              {node.store}
            </text>
            <text x="36" y="84" className="node-status">
              {view.note ?? (status === "killed" ? "SIGKILL" : status === "booting" ? "starting JVM" : "ready")}
            </text>
            {node.name === "payment-service" && state.breakers ? (
              <text x="188" y="84" textAnchor="end" className={`node-breaker state-${state.breakers.processor}`}>
                processor {state.breakers.processor}
              </text>
            ) : null}
          </g>
        );
      })}
    </svg>
  );
}
