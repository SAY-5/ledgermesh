/** Seeded PRNG (mulberry32) and a deterministic string hash. The sim never uses unseeded randomness. */
export class Prng {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0;
  }

  /** Uniform float in [0, 1). */
  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0;
    let t = this.state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** Integer in [lo, hi] inclusive. */
  int(lo: number, hi: number): number {
    return lo + Math.floor(this.next() * (hi - lo + 1));
  }

  /** Weighted choice, mirrors random.choices(population, weights). */
  choice<T>(items: readonly T[], weights: readonly number[]): T {
    const total = weights.reduce((a, b) => a + b, 0);
    let r = this.next() * total;
    for (let i = 0; i < items.length; i++) {
      r -= weights[i];
      if (r < 0) return items[i];
    }
    return items[items.length - 1];
  }

  /** Compact unique id in the shape of a uuid fragment. */
  id(prefix: string): string {
    const a = Math.floor(this.next() * 0xffffffff)
      .toString(16)
      .padStart(8, "0");
    const b = Math.floor(this.next() * 0xffff)
      .toString(16)
      .padStart(4, "0");
    return `${prefix}-${a}-${b}`;
  }
}

/** FNV-1a 32 bit; stands in for the SHA-256 digest the synthetic processor uses. */
export function hash32(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  // one extra avalanche so short suffixes such as ":1" spread out
  h ^= h >>> 13;
  h = Math.imul(h, 0x5bd1e995);
  h ^= h >>> 15;
  return h >>> 0;
}

/** Percent bucket in [0, 100), the same role as SyntheticProcessor.bucket. */
export function bucket(input: string): number {
  return hash32(input) % 100;
}

export function hex(input: string, length: number): string {
  let out = "";
  let i = 0;
  while (out.length < length) {
    out += hash32(input + ":" + i).toString(16).padStart(8, "0");
    i++;
  }
  return out.slice(0, length);
}
