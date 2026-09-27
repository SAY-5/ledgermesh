import { useEffect, useState } from "react";

/**
 * True while the element is at least partly on screen. Panels use it so a simulation steps only
 * while somebody can see it; without an IntersectionObserver the element counts as visible.
 */
export function useOnScreen(ref: { current: Element | null }, margin = "120px"): boolean {
  const [onScreen, setOnScreen] = useState(true);
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof IntersectionObserver === "undefined") return;
    setOnScreen(false);
    const observer = new IntersectionObserver(
      (entries) => setOnScreen(entries.some((e) => e.isIntersecting)),
      { rootMargin: margin },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [ref, margin]);
  return onScreen;
}
