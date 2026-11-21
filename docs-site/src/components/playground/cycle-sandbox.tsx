"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { loadFormualizer } from "@/lib/wasm/formualizer-loader";

type WasmModule = Awaited<ReturnType<typeof loadFormualizer>>;
type WasmWorkbook = InstanceType<WasmModule["Workbook"]>;

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
  const { value: out } = wb.inspectCell({
    sheet: SHEET,
    row: Number(m[2]),
    column: colToIndex(m[1]),
  }).cell;
  if (out && typeof out === "object" && "kind" in out && out.kind === "error") {
    return out.code;
  }
  if (out === null || out === undefined) return "—";
  if (typeof out === "number")
    return Number.isInteger(out) ? String(out) : out.toFixed(4);
  return String(out);
}

function cycleWorkbook(
  mod: WasmModule,
  cells: CellSpec[],
  options: CycleOptions,
) {
  // The package types omit this static method on the wrapped constructor.
  const Workbook = mod.Workbook as WasmModule["Workbook"] & {
    fromJsonWithOptions(json: string, options: CycleOptions): WasmWorkbook;
  };
  return Workbook.fromJsonWithOptions(buildJson(cells), options);
}

function evaluate<T>(
  mod: WasmModule,
  cells: CellSpec[],
  options: CycleOptions,
  read: (workbook: WasmWorkbook) => T,
): T {
  const workbook = cycleWorkbook(mod, cells, options);
  try {
    workbook.evaluateAll();
    return read(workbook);
  } finally {
    workbook.free();
  }
}

function useCycleEngine() {
  const [mod, setMod] = useState<WasmModule | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    loadFormualizer().then(
      (module) => {
        if (!cancelled) setMod(module);
      },
      (cause: unknown) => {
        if (!cancelled) setError(String(cause));
      },
    );
    return () => {
      cancelled = true;
    };
  }, []);
  return { mod, error };
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
  const { mod, error } = useCycleEngine();
  const [guard, setGuard] = useState(true);
  const [mode, setMode] = useState<"runtime" | "static">("runtime");

  const cells = useMemo<CellSpec[]>(
    () => [
      ref("A1", { value: guard }),
      ref("A2", { formula: "=IF(A1,555,A3)" }),
      ref("A3", { formula: "=IF(A1,A2,999)" }),
    ],
    [guard],
  );

  const rows = useMemo(() => {
    if (!mod) return [{ label: "Engine", value: error ?? "Loading…" }];
    try {
      return evaluate(mod, cells, { cycleDetection: mode }, (wb) => [
        {
          label: "A1 (guard)",
          value: String(guard).toUpperCase(),
          note: "boolean input",
        },
        { label: "A2", value: readCell(wb, "A2"), note: "=IF(A1,555,A3)" },
        { label: "A3", value: readCell(wb, "A3"), note: "=IF(A1,A2,999)" },
      ]);
    } catch (cause) {
      return [{ label: "Engine", value: String(cause) }];
    }
  }, [mod, cells, mode, guard, error]);

  return (
    <div className="my-6 rounded-xl border bg-fd-card p-4">
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
        <strong>static</strong> detection (the default before #570) and the same
        SCC is stamped <code>#CIRC!</code> on sight.
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
  const { mod, error } = useCycleEngine();
  const [maxChange, setMaxChange] = useState(0.001);
  const [maxIterations, setMaxIterations] = useState(100);

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
    if (!mod) return [{ label: "Engine", value: error ?? "Loading…" }];
    try {
      return evaluate(
        mod,
        cells,
        {
          cyclePolicy: "iterate",
          iterateMaxIterations: maxIterations,
          iterateMaxChange: maxChange,
        },
        (wb) => [
          { label: "A1", value: "10", note: "input" },
          { label: "D1", value: "20", note: "input" },
          { label: "B1", value: readCell(wb, "B1"), note: "=0.5*A1 + 0.5*C1" },
          { label: "C1", value: readCell(wb, "C1"), note: "=0.5*B1 + 0.5*D1" },
        ],
      );
    } catch (cause) {
      return [{ label: "Engine", value: String(cause) }];
    }
  }, [mod, cells, maxChange, maxIterations, error]);

  return (
    <div className="my-6 rounded-xl border bg-fd-card p-4">
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
  const { mod, error } = useCycleEngine();
  const workbook = useRef<WasmWorkbook | null>(null);
  const [value, setValue] = useState("0");

  useEffect(() => {
    if (!mod) return;
    const wb = cycleWorkbook(mod, [ref("A2", { formula: "=A2+1" })], {
      cyclePolicy: "iterate",
      iterateMaxIterations: 1,
      iterateMaxChange: 0.001,
    });
    workbook.current = wb;
    return () => {
      workbook.current = null;
      wb.free();
    };
  }, [mod]);

  const recalc = useCallback(() => {
    const wb = workbook.current;
    if (!wb) return;
    try {
      wb.evaluateAll();
      setValue(readCell(wb, "A2"));
    } catch (cause) {
      setValue(String(cause));
    }
  }, []);

  return (
    <div className="my-6 rounded-xl border bg-fd-card p-4">
      <div className="mb-3 flex items-center gap-3">
        <button
          type="button"
          onClick={recalc}
          disabled={!mod}
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
            value: error ?? value,
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
