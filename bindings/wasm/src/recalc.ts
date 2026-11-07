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
export const RecalcJob = wasm.RecalcJob as unknown as {
  readonly prototype: RecalcJob;
  [Symbol.hasInstance](value: unknown): value is RecalcJob;
};
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
  function?: string;
  panic_message?: string | null;
  extra?: { kind: string; function?: string; message?: string | null };
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
type OwnedJob = RecalcJob & Pick<wasm.RecalcJob, '_releaseDriver' | '_cancelRequested' | '_requestCancel'>;

function invalidOptions(message: string): never {
  throw Object.assign(new Error(message), {
    name: 'RecalcJobError', kind: 'RecalcJobError', code: 'INVALID_OPTIONS',
  });
}

function optionsForDriver(options: RecalcOptions | EvaluateWithYieldOptions | undefined, async: boolean) {
  const value = options === undefined ? {} : options;
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalidOptions('expected options object');
  const allowed = ['collectDeltas', 'maxDeltaRecords', 'priority', 'budgetMs',
    ...(async ? ['onProgress', 'signal', 'yieldControl'] : ['onStep'])];
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== 'string' || !allowed.includes(key)) invalidOptions('unknown option');
  }
  const budgetMs = value.budgetMs === undefined ? 5 : value.budgetMs;
  // Match Duration's representable range before admitting a job.
  if (budgetMs !== null && (typeof budgetMs !== 'number' || !Number.isFinite(budgetMs) ||
      budgetMs < 0 || budgetMs / 1000 >= 2 ** 64)) invalidOptions('invalid budgetMs');
  for (const name of async ? ['onProgress', 'yieldControl'] as const : ['onStep'] as const) {
    if (name in value && (value as Record<string, unknown>)[name] !== undefined &&
        typeof (value as Record<string, unknown>)[name] !== 'function') invalidOptions('invalid ' + name);
  }
  const signal = (value as EvaluateWithYieldOptions).signal;
  if (signal !== undefined && (!signal || typeof signal.aborted !== 'boolean' ||
      typeof signal.addEventListener !== 'function' || typeof signal.removeEventListener !== 'function')) {
    invalidOptions('invalid signal');
  }
  const jobOptions: RecalcJobOptions = {};
  for (const name of ['collectDeltas', 'maxDeltaRecords', 'priority'] as const) {
    if (Object.hasOwn(value, name)) Object.assign(jobOptions, { [name]: value[name] });
  }
  return { budgetMs, jobOptions };
}

function terminal(report: StepReport, job: OwnedJob): RecalcOutcome | undefined {
  // A callback may have cancelled this generation; never step a stale handle.
  if (report.phase === 'Cancelled' || job._cancelRequested) {
    if (job.active) job.cancel();
    return { status: 'Cancelled' };
  }
  if (report.phase === 'Finished') return { status: 'Finished', result: report.result };
  if (!job.active) return { status: 'Cancelled' };
}

function cleanup(job: OwnedJob, paused: boolean) {
  try {
    if (!paused && job.active) job.cancel();
  } finally {
    job._releaseDriver();
  }
}

/** Drive synchronously. Returning exactly false from onStep pauses an active job. */
export function recalc(this: DriverWorkbook, options?: RecalcOptions): RecalcOutcome {
  const { budgetMs, jobOptions } = optionsForDriver(options, false);
  const job = this._beginRecalcDriver(jobOptions) as OwnedJob;
  let paused = false, failed = false;
  try {
    for (;;) {
      const report = job.step(budgetMs);
      const proceed = options?.onStep?.(report, job);
      const outcome = terminal(report, job);
      if (outcome) return outcome;
      if (proceed === false) {
        paused = true;
        return { status: 'Paused', job };
      }
    }
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    try { cleanup(job, paused); } catch (error) { if (!failed) throw error; }
  }
}

function abortError(): Error {
  return new DOMException('Recalculation aborted', 'AbortError');
}
const taskYield = () => new Promise<void>(resolve => setTimeout(resolve, 0));

/** Drive between real task turns, with cooperative AbortSignal cancellation. */
export async function evaluateWithYield(
  this: DriverWorkbook, options?: EvaluateWithYieldOptions,
): Promise<RecalcOutcome> {
  const { budgetMs, jobOptions } = optionsForDriver(options, true);
  const signal = options?.signal;
  const checkAbort = () => { if (signal?.aborted) throw abortError(); };
  checkAbort();
  const job = this._beginRecalcDriver(jobOptions) as OwnedJob;
  let failed = false;
  let notifyAbort: (() => void) | undefined;
  // This listener must remain lock-free: abort may fire inside a synchronous UDF.
  const onAbort = () => { try { job._requestCancel(); } finally { notifyAbort?.(); } };
  const boundary = async (hook: () => unknown): Promise<void> => {
    checkAbort();
    let rejectAbort: (error: Error) => void = () => {};
    const aborted = new Promise<never>((_, reject) => { rejectAbort = reject; });
    notifyAbort = () => rejectAbort(abortError());
    try {
      // Attach handlers before invoking the hook, including for synchronous abort.
      const completion = Promise.resolve().then(() => { checkAbort(); return hook(); });
      await Promise.race([completion, aborted]);
      checkAbort();
    } finally {
      // Promise.race observes the losing promise too, including late rejection.
      notifyAbort = undefined;
    }
  };
  try {
    signal?.addEventListener('abort', onAbort, { once: true });
    checkAbort();
    for (;;) {
      checkAbort();
      const report = job.step(budgetMs);
      checkAbort();
      if (options?.onProgress) await boundary(() => options.onProgress!(report, job));
      checkAbort();
      const outcome = terminal(report, job);
      if (outcome) return outcome;
      await boundary(options?.yieldControl ?? taskYield);
      checkAbort();
    }
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    try {
      signal?.removeEventListener('abort', onAbort);
      cleanup(job, false);
    } catch (error) { if (!failed) throw error; }
  }
}
