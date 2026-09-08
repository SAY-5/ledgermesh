import { animate, useInView, useMotionValue, useReducedMotion, useTransform, motion } from "framer-motion";
import { useEffect, useRef } from "react";

interface Props {
  value: number;
  duration?: number;
  className?: string;
  /** animate only once the element scrolls into view */
  whenVisible?: boolean;
  format?: (n: number) => string;
}

/** Tabular numeral that counts up. Respects reduced motion by jumping straight to the value. */
export function Counter({ value, duration = 1.4, className, whenVisible = true, format }: Props) {
  const ref = useRef<HTMLSpanElement>(null);
  const inView = useInView(ref, { once: true, margin: "-10% 0px" });
  const reduced = useReducedMotion();
  const mv = useMotionValue(0);
  const text = useTransform(mv, (v) => (format ? format(Math.round(v)) : Math.round(v).toLocaleString("en-US")));

  useEffect(() => {
    if (whenVisible && !inView) return;
    if (reduced) {
      mv.set(value);
      return;
    }
    const controls = animate(mv, value, { duration, ease: [0.16, 1, 0.3, 1] });
    return () => controls.stop();
  }, [value, inView, whenVisible, reduced, mv, duration]);

  return (
    <motion.span ref={ref} className={className}>
      {text}
    </motion.span>
  );
}
