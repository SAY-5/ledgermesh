#!/usr/bin/env node
// Reads the three services' application.yml files and chaos/loadgen.py and writes
// src/sim/config.generated.ts, so the simulation's constants and the figures the page quotes come
// from the configuration the services actually run with. Standard library only.
// Usage: node scripts/extract-config.mjs [--check]
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "..", "..");
const target = resolve(here, "..", "src", "sim", "config.generated.ts");

function read(path) {
  return readFileSync(resolve(repo, path), "utf8");
}

/** The subset of YAML these files use: nested maps with scalar leaves; list items are skipped. */
function parseYaml(text) {
  const root = {};
  const stack = [{ indent: -1, obj: root }];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#") || line.startsWith("- ")) continue;
    const indent = raw.length - raw.trimStart().length;
    const at = line.indexOf(":");
    if (at < 0) continue;
    const key = line.slice(0, at).trim();
    const value = line.slice(at + 1).trim();
    while (stack[stack.length - 1].indent >= indent) stack.pop();
    const parent = stack[stack.length - 1].obj;
    if (value === "") {
      const child = {};
      parent[key] = child;
      stack.push({ indent, obj: child });
    } else {
      parent[key] = value.replace(/^\$\{[^:}]+:([^}]*)\}$/, "$1").replace(/^"(.*)"$/, "$1");
    }
  }
  return root;
}

function at(obj, path) {
  const value = path.split(".").reduce((o, k) => (o == null ? undefined : o[k]), obj);
  if (value === undefined) throw new Error(`missing ${path}`);
  return value;
}

const num = (v) => {
  const n = Number(v);
  if (Number.isNaN(n)) throw new Error(`not a number: ${v}`);
  return n;
};

/** Spring durations: "800ms", "5s", or a bare number of milliseconds. */
function ms(v) {
  if (/^\d+ms$/.test(v)) return Number(v.slice(0, -2));
  if (/^\d+s$/.test(v)) return Number(v.slice(0, -1)) * 1000;
  return num(v);
}

function breaker(instance) {
  return {
    slidingWindowSize: num(instance["sliding-window-size"]),
    minimumNumberOfCalls: num(instance["minimum-number-of-calls"]),
    failureRateThreshold: num(instance["failure-rate-threshold"]),
    waitDurationInOpenStateMs: ms(instance["wait-duration-in-open-state"]),
    permittedNumberOfCallsInHalfOpenState: num(instance["permitted-number-of-calls-in-half-open-state"]),
  };
}

const order = parseYaml(read("order-service/src/main/resources/application.yml"));
const inventory = parseYaml(read("inventory-service/src/main/resources/application.yml"));
const payment = parseYaml(read("payment-service/src/main/resources/application.yml"));
const loadgen = read("chaos/loadgen.py");

const list = (name) => {
  const m = loadgen.match(new RegExp(`^${name} = \\[([^\\]]*)\\]`, "m"));
  if (!m) throw new Error(`missing ${name} in chaos/loadgen.py`);
  return m[1].split(",").map((s) => s.trim().replace(/^"(.*)"$/, "$1"));
};
const range = (variable) => {
  const m = loadgen.match(new RegExp(`${variable} = rng\\.randint\\((\\d+), (\\d+)\\)`));
  if (!m) throw new Error(`missing randint for ${variable} in chaos/loadgen.py`);
  return [Number(m[1]), Number(m[2])];
};
const stockSeed = Object.fromEntries(
  at(inventory, "ledgermesh.inventory.seed")
    .split(",")
    .map((entry) => entry.split("="))
    .map(([sku, units]) => [sku.trim(), num(units.trim())]),
);

const config = {
  inventoryBreaker: breaker(at(order, "resilience4j.circuitbreaker.instances.inventory")),
  inventoryTimeLimitMs: ms(at(order, "resilience4j.timelimiter.instances.inventory.timeout-duration")),
  processorBreaker: breaker(at(payment, "resilience4j.circuitbreaker.instances.processor")),
  processorTimeLimitMs: ms(at(payment, "resilience4j.timelimiter.instances.processor.timeout-duration")),
  processorRetry: {
    maxAttempts: num(at(payment, "resilience4j.retry.instances.processor.max-attempts")),
    waitDurationMs: ms(at(payment, "resilience4j.retry.instances.processor.wait-duration")),
  },
  payment: {
    sweepMs: ms(at(payment, "ledgermesh.payment.sweep-ms")),
    sweepGraceMs: ms(at(payment, "ledgermesh.payment.sweep-grace")),
    retryDelayMs: ms(at(payment, "ledgermesh.payment.retry-delay")),
  },
  processor: {
    limit: num(at(payment, "ledgermesh.processor.limit")),
    transientPercent: num(at(payment, "ledgermesh.processor.transient-percent")),
    slowPercent: num(at(payment, "ledgermesh.processor.slow-percent")),
    slowMillis: num(at(payment, "ledgermesh.processor.slow-millis")),
  },
  cacheTtlMs: ms(at(inventory, "ledgermesh.cache.ttl")),
  outboxPollMs: ms(at(order, "ledgermesh.outbox.poll-ms")),
  saga: {
    reservationMs: ms(at(order, "ledgermesh.saga.reservation")),
    paymentMs: ms(at(order, "ledgermesh.saga.payment")),
    maxRedrives: num(at(order, "ledgermesh.saga.max-redrives")),
    reaperMs: ms(at(order, "ledgermesh.saga.reaper-ms")),
  },
  dlq: {
    maxAttempts: num(at(order, "ledgermesh.dlq.max-attempts")),
    maxReplays: num(at(order, "ledgermesh.dlq.max-replays")),
  },
  stockSeed,
  load: {
    skus: list("SKUS"),
    weights: list("WEIGHTS").map(num),
    quantity: range("qty"),
    unitPrice: range("price"),
  },
};

const text =
  "// Generated by web/scripts/extract-config.mjs from the services' application.yml files and\n" +
  "// chaos/loadgen.py. Do not edit; run `npm run config` after changing the configuration.\n" +
  "export const CONFIG = " +
  JSON.stringify(config, null, 2) +
  " as const;\n";

if (process.argv.includes("--check")) {
  let current = "";
  try {
    current = readFileSync(target, "utf8");
  } catch {
    current = "";
  }
  if (current !== text) {
    console.error("src/sim/config.generated.ts is out of date; run `npm run config`");
    process.exit(1);
  }
  console.log("config.generated.ts matches the configuration");
} else {
  writeFileSync(target, text);
  console.log(`wrote ${target}`);
}
