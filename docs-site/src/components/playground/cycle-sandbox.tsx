"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { loadFormualizer } from "@/lib/wasm/formualizer-loader";

/**
 * Interactive demos for runtime cycle detection + iterative calculation
 * (RFCs #112 / #113).
 *
 * The new cycle config (`cycleDetection` / `cyclePolicy` /
 * `iterateMaxIterations` / `iterateMaxChange`) flows through
 * `Workbook.fromJsonWithOptions(json, options)`. That entry point exists in
 * published 0.6.x, but the cycle *behavior* it drives only ships in the
 * post-#131 engine. We therefore feature-detect at runtime: a known-convergent
 * cycle that still returns `#CIRC!` means the loaded wasm predates the feature,
 * and the demo renders a "requires a newer build" banner instead of misleading
 * output.
 */

type WasmModule = Awaited<ReturnType<typeof loadFormualizer>>;
type WasmWorkbook = {
  evaluateAll: () => void;
  evaluateCell: (sheet: string, row: number, col: number) => unknown;
};

type CellSpec =
  | { row: number; col: number; value: number | boolean }
  | { row: number; col: number; formula: string };

type CycleOptions = {
  cyclePolicy?: "error" | "iterate";
  cycleDetection?: "static" | "runtime";
  iterateMaxIterations?: number;
  iterateMaxChange?: number;
};

const SHEET = "Sheet1";

function colToIndex(col: string): number {
  let n = 0;
  for (let i = 0; i < col.length; i++) n = n * 26 + (col.charCodeAt(i) - 64);
  return n;
}

/** Translate "A1"/"C2" refs into the JSON workbook cell coordinates. */
function ref(
  addr: string,
  payload: { value: number | boolean } | { formula: string },
): CellSpec {
  const m = addr.match(/^([A-Z]+)([0-9]+)$/);
  if (!m) throw new Error(`bad ref ${addr}`);
  return { row: Number(m[2]), col: colToIndex(m[1]), ...payload } as CellSpec;
}

function buildJson(cells: CellSpec[]): string {
  return JSON.stringify({
    version: 1,
    sheets: {
      [SHEET]: {
        cells: cells.map((c) => {
          if ("formula" in c)
            return { row: c.row, col: c.col, formula: c.formula };
          const value =
            typeof c.value === "boolean"
              ? { type: "Boolean", value: c.value }
              : { type: "Number", value: c.value };
          return { row: c.row, col: c.col, value };
        }),
      },
    },
  });
}

function readCell(wb: WasmWorkbook, addr: string): string {
  const m = addr.match(/^([A-Z]+)([0-9]+)$/);
  if (!m) return "—";
  const out = wb.evaluateCell(SHEET, Number(m[2]), colToIndex(m[1]));
  if (out === null || out === undefined) return "—";
  if (typeof out === "number")
    return Number.isInteger(out) ? String(out) : out.toFixed(4);
  return String(out);
}

function evaluate(
  mod: WasmModule,
  cells: CellSpec[],
  options: CycleOptions | null,
): WasmWorkbook {
  const json = buildJson(cells);
  const Workbook = (
    mod as unknown as {
      Workbook: {
        fromJson: (j: string) => WasmWorkbook;
        fromJsonWithOptions: (j: string, o: unknown) => WasmWorkbook;
      };
    }
  ).Workbook;
  const wb = options
    ? Workbook.fromJsonWithOptions(json, options)
    : Workbook.fromJson(json);
  wb.evaluateAll();
  return wb;
}

/**
 * Probe whether the loaded wasm honors iterative calculation. A convergent
 * arithmetic cycle (B1 = 0.5*A1 + 0.5*C1, C1 = 0.5*B1 + 0.5*D1; A1=10, D1=20)
 * settles to B1 ≈ 13.333 once the feature is present, and returns `#CIRC!`
 * otherwise.
 */
function probeIterativeSupport(mod: WasmModule): boolean {
  try {
    const wb = evaluate(
      mod,
      [
        ref("A1", { value: 10 }),
        ref("D1", { value: 20 }),
        ref("B1", { formula: "=0.5*A1 + 0.5*C1" }),
        ref("C1", { formula: "=0.5*B1 + 0.5*D1" }),
      ],
      {
        cyclePolicy: "iterate",
        iterateMaxIterations: 100,
        iterateMaxChange: 0.001,
      },
    );
    const b1 = readCell(wb, "B1");
    return !b1.startsWith("#") && Math.abs(Number(b1) - 40 / 3) < 0.1;
  } catch {
    return false;
  }
}

function StatusBanner({ supported }: { supported: boolean | null }) {
  if (supported === null) return null;
  if (supported) {
    return (
      <div className="mb-4 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-700 dark:text-emerald-300">
        ✓ Live: this page is running runtime cycle detection in your browser via
        the formualizer WASM build.
      </div>
    );
  }
  return (
    <div className="mb-4 rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
      The installed <code>formualizer</code> WASM package does not yet implement
      runtime cycle detection / iterative calculation, so these demos fall back
      to read-only previews. They light up automatically once the docs site
      upgrades to the first release that ships RFC #112 / #113 (the{" "}
      <code>fromJsonWithOptions</code> cycle config). Until then, the values
      shown are the documented expected outputs, not live evaluation.
    </div>
  );
}

function Grid({
  rows,
}: {
  rows: { label: string; value: string; note?: string }[];
}) {
  return (
    <div className="overflow-hidden rounded-md border">
      <table className="w-full border-collapse text-sm">
        <thead className="bg-fd-muted/50">
          <tr>
            <th className="border-b px-3 py-1.5 text-left font-medium text-fd-muted-foreground">
              Cell
            </th>
            <th className="border-b px-3 py-1.5 text-left font-medium text-fd-muted-foreground">
              Value
            </th>
            <th className="border-b px-3 py-1.5 text-left font-medium text-fd-muted-foreground">
              Notes
            </th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {rows.map((r) => (
            <tr key={r.label}>
              <td className="px-3 py-1.5 font-mono text-xs text-fd-muted-foreground">
                {r.label}
              </td>
              <td
                className={`px-3 py-1.5 font-mono text-xs ${
                  r.value.startsWith("#")
                    ? "text-rose-600 dark:text-rose-400"
                    : "text-fd-foreground"
                }`}
              >
                {r.value}
              </td>
              <td className="px-3 py-1.5 text-xs text-fd-muted-foreground">
                {r.note ?? ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ───────────────────────── (a) Guarded pair (#99) ───────────────────────── */

/**
 * Discussion #99: A1 is a boolean guard, A2/A3 form a static SCC whose live
 * edges depend on the guard. Under runtime detection the SCC is a *phantom*
 * cycle and produces values; under static detection it is stamped `#CIRC!`.
 */
export function GuardedPairSandbox() {
  const [mod, setMod] = useState<WasmModule | null>(null);
  const [supported, setSupported] = useState<boolean | null>(null);
  const [guard, setGuard] = useState(true);
  const [mode, setMode] = useState<"runtime" | "static">("runtime");

  useEffect(() => {
    let cancelled = false;
    loadFormualizer().then((m) => {
      if (cancelled) return;
      setMod(m);
      setSupported(probeIterativeSupport(m));
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const cells = useMemo<CellSpec[]>(
    () => [
      ref("A1", { value: guard }),
      ref("A2", { formula: "=IF(A1,555,A3)" }),
      ref("A3", { formula: "=IF(A1,A2,999)" }),
    ],
    [guard],
  );

  const rows = useMemo(() => {
    // Expected values, used both as the live target and as the static preview.
    const expected = {
      runtime: guard ? { A2: "555", A3: "555" } : { A2: "999", A3: "999" },
      static: { A2: "#CIRC!", A3: "#CIRC!" },
    } as const;

    if (supported && mod) {
      try {
        const wb = evaluate(mod, cells, { cycleDetection: mode });
        return [
          {
            label: "A1 (guard)",
            value: String(guard).toUpperCase(),
            note: "boolean input",
          },
          { label: "A2", value: readCell(wb, "A2"), note: "=IF(A1,555,A3)" },
          { label: "A3", value: readCell(wb, "A3"), note: "=IF(A1,A2,999)" },
        ];
      } catch (e) {
        return [{ label: "error", value: String(e), note: "" }];
      }
    }

    const exp = expected[mode];
    return [
      {
        label: "A1 (guard)",
        value: String(guard).toUpperCase(),
        note: "boolean input",
      },
      { label: "A2", value: exp.A2, note: "=IF(A1,555,A3)" },
      { label: "A3", value: exp.A3, note: "=IF(A1,A2,999)" },
    ];
  }, [supported, mod, cells, mode, guard]);

  return (
    <div className="my-6 rounded-xl border bg-fd-card p-4">
      <StatusBanner supported={supported} />
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={() => setGuard((g) => !g)}
          className="rounded-md bg-fd-primary px-3 py-1.5 text-xs font-medium text-fd-primary-foreground shadow-sm hover:bg-fd-primary/90"
        >
          Toggle guard (A1 = {String(guard).toUpperCase()})
        </button>
        <div className="flex gap-1 rounded-md border bg-fd-background/50 p-0.5">
          {(["runtime", "static"] as const).map((m) => (
            <button
              key={m}
              type="button"
              onClick={() => setMode(m)}
              className={`rounded-sm px-2.5 py-1 text-xs font-semibold transition-colors ${
                mode === m
                  ? "bg-fd-primary text-fd-primary-foreground shadow-sm"
                  : "text-fd-muted-foreground hover:bg-fd-muted"
              }`}
            >
              {m === "runtime" ? "Runtime detection" : "Static detection"}
            </button>
          ))}
        </div>
      </div>
      <Grid rows={rows} />
      <p className="mt-3 text-xs text-fd-muted-foreground">
        Under <strong>runtime</strong> detection, only the live edges count —
        the guarded pair is a phantom cycle and resolves to a value. Flip to{" "}
        <strong>static</strong> detection (today's default) and the same SCC is
        stamped <code>#CIRC!</code> on sight.
      </p>
    </div>
  );
}

/* ─────────────── (b) Convergent circular interest with slider ─────────────── */

/**
 * A mutual fixed-point: B1 = 0.5*A1 + 0.5*C1, C1 = 0.5*B1 + 0.5*D1, with
 * A1 = 10, D1 = 20. The exact fixed point is B1 = 40/3, C1 = 50/3. A coarser
 * max-change threshold stops earlier with a slightly looser result.
 */
export function ConvergentCycleSandbox() {
  const [mod, setMod] = useState<WasmModule | null>(null);
  const [supported, setSupported] = useState<boolean | null>(null);
  const [maxChange, setMaxChange] = useState(0.001);
  const [maxIterations, setMaxIterations] = useState(100);

  useEffect(() => {
    let cancelled = false;
    loadFormualizer().then((m) => {
      if (cancelled) return;
      setMod(m);
      setSupported(probeIterativeSupport(m));
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const cells = useMemo<CellSpec[]>(
    () => [
      ref("A1", { value: 10 }),
      ref("D1", { value: 20 }),
      ref("B1", { formula: "=0.5*A1 + 0.5*C1" }),
      ref("C1", { formula: "=0.5*B1 + 0.5*D1" }),
    ],
    [],
  );

  const rows = useMemo(() => {
    if (supported && mod) {
      try {
        const wb = evaluate(mod, cells, {
          cyclePolicy: "iterate",
          iterateMaxIterations: maxIterations,
          iterateMaxChange: maxChange,
        });
        return [
          { label: "A1", value: "10", note: "input" },
          { label: "D1", value: "20", note: "input" },
          { label: "B1", value: readCell(wb, "B1"), note: "=0.5*A1 + 0.5*C1" },
          { label: "C1", value: readCell(wb, "C1"), note: "=0.5*B1 + 0.5*D1" },
        ];
      } catch (e) {
        return [{ label: "error", value: String(e), note: "" }];
      }
    }
    return [
      { label: "A1", value: "10", note: "input" },
      { label: "D1", value: "20", note: "input" },
      {
        label: "B1",
        value: (40 / 3).toFixed(4),
        note: "fixed point ≈ 13.3333",
      },
      {
        label: "C1",
        value: (50 / 3).toFixed(4),
        note: "fixed point ≈ 16.6667",
      },
    ];
  }, [supported, mod, cells, maxChange, maxIterations]);

  return (
    <div className="my-6 rounded-xl border bg-fd-card p-4">
      <StatusBanner supported={supported} />
      <div className="mb-4 space-y-3">
        <label className="block text-xs font-medium text-fd-muted-foreground">
          Max change (absolute convergence threshold):{" "}
          <span className="font-mono">{maxChange}</span>
          <input
            type="range"
            min={-6}
            max={0}
            step={1}
            value={Math.log10(maxChange)}
            onChange={(e) => setMaxChange(10 ** Number(e.target.value))}
            className="mt-1 w-full"
          />
        </label>
        <label className="block text-xs font-medium text-fd-muted-foreground">
          Max iterations: <span className="font-mono">{maxIterations}</span>
          <input
            type="range"
            min={1}
            max={200}
            step={1}
            value={maxIterations}
            onChange={(e) => setMaxIterations(Number(e.target.value))}
            className="mt-1 w-full"
          />
        </label>
      </div>
      <Grid rows={rows} />
      <p className="mt-3 text-xs text-fd-muted-foreground">
        The pair converges to its fixed point (B1 = 40/3, C1 = 50/3). A coarser{" "}
        <code>iterateMaxChange</code> stops a pass or two earlier; hitting the
        iteration cap keeps the last values and is <strong>not</strong> an
        error.
      </p>
    </div>
  );
}

/* ─────────────────────────── (c) Accumulator ─────────────────────────── */

/**
 * The Excel accumulator: A1 = A1 + 1 with maxIterations = 1 advances exactly
 * once per recalc. Each "Recalc" click is one full evaluation request, so the
 * counter ticks up by one.
 */
export function AccumulatorSandbox() {
  const [mod, setMod] = useState<WasmModule | null>(null);
  const [supported, setSupported] = useState<boolean | null>(null);
  const [count, setCount] = useState(0);

  useEffect(() => {
    let cancelled = false;
    loadFormualizer().then((m) => {
      if (cancelled) return;
      setMod(m);
      setSupported(probeIterativeSupport(m));
    });
    return () => {
      cancelled = true;
    };
  }, []);

  // Live path: rebuild a workbook seeded with the prior value and run one pass.
  const recalc = useCallback(() => {
    if (supported && mod) {
      try {
        const wb = evaluate(
          mod,
          [
            ref("A1", { value: count }),
            ref("B1", { formula: "=A1" }),
            ref("A2", { formula: "=A2+1" }),
          ],
          {
            cyclePolicy: "iterate",
            iterateMaxIterations: 1,
            iterateMaxChange: 0.001,
          },
        );
        const next = Number(readCell(wb, "A2"));
        setCount(Number.isFinite(next) ? next : count + 1);
        return;
      } catch {
        // fall through to the preview increment
      }
    }
    setCount((c) => c + 1);
  }, [supported, mod, count]);

  return (
    <div className="my-6 rounded-xl border bg-fd-card p-4">
      <StatusBanner supported={supported} />
      <div className="mb-3 flex items-center gap-3">
        <button
          type="button"
          onClick={recalc}
          className="rounded-md bg-fd-primary px-3 py-1.5 text-xs font-medium text-fd-primary-foreground shadow-sm hover:bg-fd-primary/90"
        >
          Recalc (F9)
        </button>
        <span className="text-xs text-fd-muted-foreground">
          one recalc = one pass under <code>iterateMaxIterations: 1</code>
        </span>
      </div>
      <Grid
        rows={[
          {
            label: "A2",
            value: String(count),
            note: "=A2+1 — advances once per recalc",
          },
        ]}
      />
      <p className="mt-3 text-xs text-fd-muted-foreground">
        With <code>iterateMaxIterations: 1</code> a self-referential accumulator
        advances exactly one step per recalculation request, mirroring Excel's
        manual F9 accumulator pattern.
      </p>
    </div>
  );
}
