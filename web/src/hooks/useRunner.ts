import { useCallback, useEffect, useRef, useState } from "react";
import type { Cluster } from "../sim/cluster.ts";

export interface RunnerOptions {
  /** virtual ms per real ms */
  speed?: number;
  autoStart?: boolean;
  /** re-render cadence in real ms */
  frameMs?: number;
  /** called before every step while running */
  onStep?: (cluster: Cluster) => void;
  /** return true to stop the runner */
  stopWhen?: (cluster: Cluster) => boolean;
}

export interface Runner {
  cluster: Cluster;
  running: boolean;
  speed: number;
  /** bumps on every rendered frame so consumers re-read the cluster */
  frame: number;
  start(): void;
  pause(): void;
  setSpeed(speed: number): void;
  reset(): void;
  /** run a bounded amount of virtual time immediately */
  advance(ms: number): void;
}

/**
 * Drives a Cluster in real time from requestAnimationFrame. Virtual time only advances here, never
 * during render, and the wall clock is read only inside the frame callback.
 */
export function useRunner(factory: () => Cluster, options: RunnerOptions = {}): Runner {
  const [cluster, setCluster] = useState<Cluster>(factory);
  const [running, setRunning] = useState(options.autoStart ?? false);
  const [speed, setSpeedState] = useState(options.speed ?? 1);
  const [frame, setFrame] = useState(0);
  const speedRef = useRef(speed);
  const optionsRef = useRef(options);
  optionsRef.current = options;
  speedRef.current = speed;

  useEffect(() => {
    if (!running) return;
    let raf = 0;
    let last = -1;
    let acc = 0;
    let lastFrame = 0;
    const frameMs = optionsRef.current.frameMs ?? 80;
    const loop = (t: number) => {
      if (last < 0) last = t;
      const dt = Math.min(t - last, 250);
      last = t;
      acc += dt * speedRef.current;
      let stepped = false;
      while (acc >= cluster.tickMs) {
        optionsRef.current.onStep?.(cluster);
        cluster.step();
        acc -= cluster.tickMs;
        stepped = true;
        if (optionsRef.current.stopWhen?.(cluster)) {
          setRunning(false);
          setFrame((f) => f + 1);
          return;
        }
      }
      if (stepped && t - lastFrame >= frameMs) {
        lastFrame = t;
        setFrame((f) => f + 1);
      }
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [running, cluster]);

  const start = useCallback(() => setRunning(true), []);
  const pause = useCallback(() => setRunning(false), []);
  const setSpeed = useCallback((s: number) => setSpeedState(s), []);
  const reset = useCallback(() => {
    setRunning(false);
    setCluster(factory());
    setFrame((f) => f + 1);
  }, [factory]);
  const advance = useCallback(
    (ms: number) => {
      const until = cluster.now + ms;
      while (cluster.now < until) {
        optionsRef.current.onStep?.(cluster);
        cluster.step();
      }
      setFrame((f) => f + 1);
    },
    [cluster],
  );

  return { cluster, running, speed, frame, start, pause, setSpeed, reset, advance };
}
