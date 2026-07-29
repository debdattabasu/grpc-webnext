//! The conformance matrix's **second client driver**.
//!
//! `conformance.test.ts` runs the cases through the TypeScript client. This file runs
//! the same cases through the **Rust** client, by spawning the `conformance-driver`
//! binary against the same server implementations. The matrix is
//! `{client drivers} × {server impls}`, and until now the first axis had one entry —
//! which meant a client-side reading of the protocol could be wrong without anything
//! disagreeing, because there was nothing to disagree with.
//!
//! The Rust client speaks only the binary h2ts profile (no JSON codec, no custom
//! `Frame` path), so it covers a subset and reports the rest as SKIP. This harness
//! asserts the *size* of that subset as well as its result — a driver that quietly
//! skipped everything would exit 0 and prove nothing, which is the exact failure mode
//! `conformance/README.md` warns about under "A passing case can still test nothing".

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { load } from "js-yaml";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../../..");
const rustRoot = path.join(repoRoot, "rust");
const goRoot = path.join(repoRoot, "go");
const casesDir = path.join(repoRoot, "conformance", "cases");
const driverBin = path.join(rustRoot, "target", "debug", "conformance-driver");

type Case = {
  name: string;
  rest?: unknown;
  codecs?: string[];
  requires?: { max_message_bytes?: number; transcoder?: boolean };
};
type Suite = { suite: string; cases: Case[] };

const caseFiles = fs
  .readdirSync(casesDir)
  .filter((f) => f.endsWith(".yaml"))
  .sort()
  .map((f) => path.join(casesDir, f));
const suites: Suite[] = caseFiles.map((f) => load(fs.readFileSync(f, "utf8")) as Suite);

/** Must match `Case::requires_key` in the Rust driver and `requiresKey` in the TS one. */
function requiresKey(r?: Case["requires"]): string {
  const p: string[] = [];
  if (r?.max_message_bytes) p.push(`max:${r.max_message_bytes}`);
  if (r?.transcoder === false) p.push("notranscoder");
  return p.length ? p.join(",") : "default";
}
function requiresEnv(r?: Case["requires"]): Record<string, string> {
  const env: Record<string, string> = {};
  if (r?.max_message_bytes) env.CONFORMANCE_MAX_MESSAGE_BYTES = String(r.max_message_bytes);
  if (r?.transcoder === false) env.CONFORMANCE_TRANSCODER = "0";
  return env;
}

/** The cases the Rust client can actually run: not REST, and covering the proto codec. */
const runnable = suites.flatMap((s) =>
  s.cases.filter((c) => !c.rest && (c.codecs ?? ["proto", "json"]).includes("proto")),
);
/** How many runnable cases each server profile owns. */
const expectedPerProfile = new Map<string, number>();
for (const c of runnable) {
  const k = requiresKey(c.requires);
  expectedPerProfile.set(k, (expectedPerProfile.get(k) ?? 0) + 1);
}

interface ServerImpl {
  name: string;
  prepare(): void;
  command: () => { file: string; args: string[]; cwd: string };
}
const goBinary = path.join(goRoot, "target", "conformance-server");
const SERVERS: ServerImpl[] = [
  {
    name: "rust",
    prepare: () => execFileSync("cargo", ["build", "--quiet", "-p", "conformance-server"], { cwd: rustRoot }),
    command: () => ({ file: "cargo", args: ["run", "--quiet", "-p", "conformance-server"], cwd: rustRoot }),
  },
  {
    name: "go",
    prepare: () => execFileSync("go", ["build", "-o", goBinary, "./examples/conformance-server"], { cwd: goRoot }),
    command: () => ({ file: goBinary, args: [], cwd: goRoot }),
  },
];

const servers = new Map<string, { proc: ChildProcess; baseUrl: string }>();

function spawnServer(impl: ServerImpl, env: Record<string, string>) {
  const { file, args, cwd } = impl.command();
  const proc = spawn(file, args, { cwd, stdio: ["ignore", "pipe", "inherit"], env: { ...process.env, ...env } });
  return new Promise<{ proc: ChildProcess; baseUrl: string }>((resolve, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const m = line.match(/^LISTENING (\S+)$/);
      if (m) resolve({ proc, baseUrl: m[1] });
    });
    proc.on("error", (e) => reject(new Error(`${impl.name} conformance-server failed to start: ${e.message}`)));
    proc.on("exit", (code) => reject(new Error(`${impl.name} conformance-server exited early: ${code}`)));
  });
}

/** Run the Rust driver once, returning its report. Never throws on a non-zero exit —
 *  the report is the diagnostic, so the assertion belongs in the test, not here. */
function runDriver(baseUrl: string, profile: string): { code: number; out: string } {
  const args = ["--base-url", baseUrl, "--profile", profile, ...caseFiles];
  try {
    return { code: 0, out: execFileSync(driverBin, args, { encoding: "utf8", maxBuffer: 32 * 1024 * 1024 }) };
  } catch (e) {
    const err = e as { status?: number; stdout?: string; stderr?: string };
    return { code: err.status ?? 1, out: `${err.stdout ?? ""}${err.stderr ?? ""}` };
  }
}

const countLines = (out: string, prefix: string) =>
  out.split("\n").filter((l) => l.startsWith(prefix)).length;

beforeAll(async () => {
  execFileSync("cargo", ["build", "--quiet", "-p", "conformance-driver"], { cwd: rustRoot });
  for (const impl of SERVERS) impl.prepare();
  await Promise.all(
    SERVERS.flatMap((impl) =>
      [...expectedPerProfile.keys()].map(async (key) => {
        const requires = runnable.find((c) => requiresKey(c.requires) === key)?.requires;
        servers.set(`${impl.name}|${key}`, await spawnServer(impl, requiresEnv(requires)));
      }),
    ),
  );
}, 300_000);

afterAll(() => {
  for (const { proc } of servers.values()) proc.kill("SIGKILL");
});

for (const impl of SERVERS) {
  describe(`conformance: rust client driver [${impl.name} server]`, () => {
    for (const [profile, expected] of expectedPerProfile) {
      it(`runs every proto/h2ts case [profile ${profile}]`, () => {
        const server = servers.get(`${impl.name}|${profile}`)!;
        const { code, out } = runDriver(server.baseUrl, profile);
        expect(code, `driver reported failures:\n${out}`).toBe(0);
        // The load-bearing half: a driver that skipped everything also exits 0.
        // Pin the number of cases it actually ran, so a case silently dropping out of
        // the Rust driver's reach is a failure rather than a quieter report.
        expect(countLines(out, "PASS"), `expected ${expected} PASS lines:\n${out}`).toBe(expected);
      }, 120_000);
    }

    it("skips the cases it cannot run, visibly and with a reason", () => {
      // The contract requires an unsupported case to be *reported* as skipped rather
      // than silently passed. REST and json-only cases are outside the Rust client by
      // design, and that has to be legible in the report.
      const server = servers.get(`${impl.name}|default`)!;
      const { out } = runDriver(server.baseUrl, "default");
      const skips = out.split("\n").filter((l) => l.startsWith("SKIP"));
      expect(skips.length).toBeGreaterThan(0);
      for (const line of skips) expect(line, "every SKIP must carry a reason").toContain("—");
      expect(out).toContain("REST case");
      expect(out).toContain("json-only case");
    }, 120_000);
  });
}
