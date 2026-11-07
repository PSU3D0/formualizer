import * as wasm from '../pkg/formualizer_wasm.js';
import type { CellAddress, FiniteRangeAddress } from './index.js';

export interface RecalcJobOptions {
  collectDeltas?: boolean;
  maxDeltaRecords?: number;
  /** Bounded, inclusive, 1-based viewport rectangles. */
  priority?: FiniteRangeAddress[];
}
export interface RecalcResult {
  computedVertices: number;
  cycleErrors: number;
  elapsedMs: number;
}
export interface RecalcProgress {
  pass: number;
  completedInPass: number;
  scheduledInPass: number;
  cumulative: number;
  totalExact: boolean;
  stagedRemaining: number;
  priorityPending: boolean;
}
export interface RecalcLongestUnit {
  kind: 'GraphBuild' | 'ScheduleBuild' | 'SingleCell' | 'FamilyRun' | 'Cycle';
  representativeCell: CellAddress | null;
  vertexCount: number;
  elapsedMs: number;
}
interface StepReportBase {
  progress: RecalcProgress;
  stepComputed: number;
  stepElapsedMs: number;
  changed: FiniteRangeAddress[];
  overflowSheets: string[];
  longestUnit: RecalcLongestUnit | null;
}
/** Detached per-step data. Only Finished carries a result. */
export type StepReport = StepReportBase & (
  | { phase: 'Finished'; result: RecalcResult }
  | { phase: 'BuildingGraph' | 'Scheduling' | 'Evaluating' | 'Cancelled'; result: null }
);
export interface RecalcJob {
  readonly active: boolean;
  /** null/undefined means no soft deadline; zero still progresses. */
  step(budgetMs?: number | null): StepReport;
  prioritize(ranges: FiniteRangeAddress[]): void;
  cancel(): StepReport | null;
  free(): void;
  [Symbol.dispose](): void;
}
/** The actual generated WASM class; jobs are created by Workbook.beginRecalc. */
export const RecalcJob = wasm.RecalcJob as unknown as { readonly prototype: RecalcJob };
export type RecalcOutcome =
  | { status: 'Finished'; result: RecalcResult }
  | { status: 'Cancelled' }
  | { status: 'Paused'; job: RecalcJob };

export interface RecalcEvaluationCause extends Error {
  kind: string;
  excel_kind?: string;
  excel_message?: string;
  resource_reason?: string;
  limit?: string;
  observed?: string;
  request_id?: string | null;
  function_name?: string;
  function_source?: string;
}
export type RecalcJobError = Error & (
  | { code: 'ALREADY_PENDING'; explicit: boolean; kind: 'RecalcJobError' }
  | { code: 'EVALUATION'; cause: RecalcEvaluationCause }
  | { code: 'NO_ACTIVE_JOB' | 'ENGINE_POISONED' | 'REENTRANT_CALL' | 'INVALID_OPTIONS' | 'INVALID_ADDRESS'; kind: 'RecalcJobError' }
);
/** Existing evaluation errors retain their kind/Excel fields. */
export interface RecalcJobActiveError extends Error {
  code: 'RECALC_JOB_ACTIVE';
  kind: string;
  workbook_kind?: string;
  excel_kind?: string;
}
export interface RecalcOptions extends RecalcJobOptions {
  budgetMs?: number | null;
  onStep?: (report: StepReport, job: RecalcJob) => void | boolean;
}
export interface EvaluateWithYieldOptions extends RecalcJobOptions {
  budgetMs?: number | null;
  onProgress?: (report: StepReport, job: RecalcJob) => unknown | PromiseLike<unknown>;
  signal?: AbortSignal;
  /** Defaults to a real task turn via setTimeout(0). */
  yieldControl?: () => unknown | PromiseLike<unknown>;
}

type DriverWorkbook = wasm.Workbook;
// Compilable behavioral-red skeleton; replaced after package-level reds.
export function recalc(this: DriverWorkbook, _options?: RecalcOptions): RecalcOutcome {
  return { status: 'Cancelled' };
}
export async function evaluateWithYield(this: DriverWorkbook, _options?: EvaluateWithYieldOptions): Promise<RecalcOutcome> {
  return { status: 'Cancelled' };
}
