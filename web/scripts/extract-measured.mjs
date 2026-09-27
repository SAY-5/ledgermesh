#!/usr/bin/env node
// Reads the recorded chaos summaries under chaos/evidence/ and writes
// src/sim/measured.generated.ts, so every figure the page presents as measured comes from a run
// that is committed to this repository, together with the raw text it was read from. Standard
// library only. Usage: node scripts/extract-measured.mjs [--check]
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "..", "..");
const target = resolve(here, "..", "src", "sim", "measured.generated.ts");

const RUNS = [
  { key: "baseline", path: "chaos/evidence/baseline/summary.txt" },
  { key: "chaos", path: "chaos/evidence/steady/summary.txt" },
];

function git(args) {
  try {
    return execFileSync("git", args, { cwd: repo, encoding: "utf8", maxBuffer: 1 << 24 });
  } catch (err) {
    const detail = String(err.stderr || err.message).trim();
    throw new Error(`git ${args.join(" ")} failed: ${detail}`);
  }
}

/**
 * The commit that introduced the bytes `path` holds, read from this repository's own history, so
 * the page links to a permalink that shows the text the figures were read from.
 *
 * This used to be a `blobCommit` constant beside each run, re-derived by hand. `--check` did fail on
 * a re-recorded summary, because the figures and raw text it compares moved with the file. What it
 * missed was the link: once `npm run measured` had refreshed them, it regenerated from the same
 * constant, so a permalink still naming the earlier recording passed, and a reader cannot tell that
 * version apart from the current one.
 *
 * The commit is keyed on the content, not on the last commit to touch the path: it is the oldest
 * commit git log reports as adding the blob the working copy holds at that path, and each one it
 * reports is checked to hold that blob, so whichever is found, the link shows those bytes. A merge
 * counts, compared with its first parent, since git log would otherwise not diff merges and a
 * conflict resolved to new text would have no commit at all. A later commit that touches the file
 * without changing its bytes, or restores bytes it held before, leaves the link where it was; a
 * re-recorded summary moves it, and `--check` fails until the generated file is refreshed. A working
 * copy that differs from the version committed at HEAD is refused, and so is a shallow clone, whose
 * truncated history cannot show where the blob came from.
 *
 * `--check` proves local consistency: the generated file matches what this clone's history derives.
 * It cannot prove the commit exists anywhere else. CI does, because it checks out from the remote
 * with the full history and runs the same derivation, so the commit it finds is one the remote has.
 */
function blobCommit(path, raw) {
  if (git(["rev-parse", "--is-shallow-repository"]).trim() === "true") {
    throw new Error(
      "the history is truncated by a shallow clone, so the commit that introduced the summary " +
        "cannot be identified. Run `git fetch --unshallow`, or check out with fetch-depth: 0",
    );
  }
  const blob = blobAt("HEAD", path);
  if (!blob) {
    throw new Error("HEAD does not contain it; commit the recorded run first");
  }
  if (git(["cat-file", "blob", blob]) !== raw) {
    throw new Error(
      "differs from the version committed at HEAD: commit the recorded run, then run " +
        "`npm run measured`, so the link resolves to the text these figures were read from",
    );
  }
  // Every commit that adds or removes the blob at this path, newest first. The ones whose tree
  // holds it introduced it; the last of those introduced it first.
  const introduced = git([
    "log", "--format=%H", "--diff-merges=first-parent", `--find-object=${blob}`, "--", path,
  ])
    .split("\n")
    .filter((commit) => commit && blobAt(commit, path) === blob);
  const commit = introduced.at(-1);
  if (!commit) {
    throw new Error(`no commit in this history introduces blob ${blob}`);
  }
  return commit;
}

/** The blob `rev` holds at `path`, or "" when it holds none. */
function blobAt(rev, path) {
  try {
    return git(["rev-parse", "--verify", "--quiet", `${rev}:${path}`]).trim();
  } catch {
    return "";
  }
}

function field(raw, label) {
  const m = raw.match(new RegExp(`^  ${label}\\s+(.*)$`, "m"));
  if (!m) throw new Error(`missing "${label}" line`);
  return m[1].trim();
}

function int(raw, label, pattern) {
  const value = field(raw, label);
  const m = value.match(pattern);
  if (!m) throw new Error(`cannot read ${label} from "${value}"`);
  return Number(m[1]);
}

function probes(raw) {
  const value = field(raw, "stock probes");
  const out = {};
  for (const key of ["live", "cache", "unknown", "error"]) {
    const m = value.match(new RegExp(`'${key}': (\\d+)`));
    if (!m) throw new Error(`cannot read probe ${key} from "${value}"`);
    out[key] = Number(m[1]);
  }
  return out;
}

function kills(raw) {
  const value = field(raw, "kills");
  const count = Number(value.match(/^(\d+)/)[1]);
  const inside = value.match(/\(([^)]*)\)/);
  const list = [];
  if (count > 0 && inside) {
    for (const part of inside[1].split(",")) {
      const m = part.trim().match(/^(\S+) @(\d+)s$/);
      if (!m) throw new Error(`cannot read a kill from "${part}"`);
      list.push({ service: m[1], atSeconds: Number(m[2]) });
    }
  }
  if (list.length !== count) throw new Error(`kill count ${count} does not match "${value}"`);
  return list;
}

function parse(raw) {
  const recorded = field(raw, "recorded");
  const at = recorded.match(/^(\S+) at commit (\S+)/);
  if (!at) throw new Error(`cannot read the provenance line "${recorded}"`);
  if (/with uncommitted changes/.test(recorded)) {
    throw new Error("recorded from a dirty working tree; re-run the harness on a committed tree");
  }
  const latency = field(raw, "saga latency").match(
    /p50 (\d+) ms\s+p95 (\d+) ms\s+max (\d+) ms/,
  );
  if (!latency) throw new Error("cannot read the saga latency line");
  const retries = field(raw, "retries").match(
    /with retry (\d+) ok \/ (\d+) exhausted, without retry (\d+) ok \/ (\d+) failed/,
  );
  if (!retries) throw new Error("cannot read the retries line");
  const resubmits = field(raw, "resubmits").match(/^(\d+) retried submits over (\d+) orders/);
  if (!resubmits) throw new Error("cannot read the resubmits line");
  const ceiling = raw.match(/^  p95 ceiling\s+(\d+) ms \(CHAOS_MAX_P95\) (\w+)$/m);
  return {
    recordedAt: at[1],
    commit: at[2],
    host: field(raw, "host"),
    knobs: field(raw, "knobs"),
    load: field(raw, "load"),
    submitted: int(raw, "orders submitted", /^(\d+)$/),
    confirmed: int(raw, "confirmed", /^(\d+)$/),
    cancelledStock: int(raw, "cancelled \\(stock\\)", /^(\d+)$/),
    failedStuck: int(raw, "failed / stuck", /^(\d+)$/),
    kills: kills(raw),
    p50Ms: Number(latency[1]),
    p95Ms: Number(latency[2]),
    maxMs: Number(latency[3]),
    p95CeilingMs: ceiling ? Number(ceiling[1]) : null,
    p95CeilingHeld: ceiling ? ceiling[2] === "held" : null,
    retriesWithRetry: Number(retries[1]),
    retriesExhausted: Number(retries[2]),
    deferred: int(raw, "deferred payments", /^(\d+)$/),
    duplicates: int(raw, "duplicate events", /^(\d+)/),
    resubmits: Number(resubmits[1]),
    retriedOrders: Number(resubmits[2]),
    probes: probes(raw),
    breakerTransitions: field(raw, "breaker transitions"),
    stuckOrders: int(raw, "stuck orders", /^(\d+)$/),
  };
}

const runs = {};
for (const run of RUNS) {
  const raw = readFileSync(resolve(repo, run.path), "utf8");
  try {
    runs[run.key] = {
      source: run.path,
      blobCommit: blobCommit(run.path, raw),
      ...parse(raw),
      raw,
    };
  } catch (err) {
    throw new Error(`${run.path}: ${err.message}`);
  }
}

const header = [
  "// Written by web/scripts/extract-measured.mjs from the chaos summaries recorded under",
  "// chaos/evidence/. Do not edit; run `npm run measured` after recording a run. `raw` is the",
  "// summary text the figures were read from, so a hand written figure can be caught.",
].join("\n");
const body = `${header}\nexport const MEASURED = ${JSON.stringify(runs, null, 2)} as const;\n`;

if (process.argv.includes("--check")) {
  const current = readFileSync(target, "utf8");
  if (current !== body) {
    console.error(
      "src/sim/measured.generated.ts is stale: run `npm run measured` and commit the result",
    );
    process.exit(1);
  }
  console.log("measured.generated.ts matches the recorded summaries");
} else {
  writeFileSync(target, body);
  console.log(`wrote ${target}`);
}
