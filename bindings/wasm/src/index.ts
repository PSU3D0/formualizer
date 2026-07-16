import * as wasm from '../pkg/formualizer_wasm.js';

let wasmInitialized = false;
let wasmInitPromise: Promise<void> | null = null;

/**
 * Initialize the WASM module. This must be called before using any other functions.
 * Safe to call multiple times - subsequent calls will return the same promise.
 */
export async function initializeWasm(): Promise<void> {
  if (!wasmInitPromise) {
    // wasm-pack `--target bundler` initializes at module import time.
    wasmInitPromise = Promise.resolve().then(() => {
      wasmInitialized = true;
    });
  }
  return wasmInitPromise;
}

/**
 * Ensure WASM is initialized before calling a function
 */
async function ensureInitialized<T>(fn: () => T): Promise<T> {
  if (!wasmInitialized) {
    await initializeWasm();
  }
  return fn();
}

export interface Token {
  tokenType: string;
  subtype: string;
  value: string;
  pos: number;
  end: number;
}

export interface ReferenceData {
  sheet?: string;
  rowStart: number;
  colStart: number;
  rowEnd: number;
  colEnd: number;
  rowAbsStart: boolean;
  colAbsStart: boolean;
  rowAbsEnd: boolean;
  colAbsEnd: boolean;
}

export interface ASTNodeData {
  type: 'number' | 'text' | 'boolean' | 'reference' | 'function' | 'binaryOp' | 'unaryOp' | 'array' | 'error';
  value?: number | string | boolean;
  reference?: ReferenceData;
  name?: string;
  args?: ASTNodeData[];
  op?: string;
  left?: ASTNodeData;
  right?: ASTNodeData;
  operand?: ASTNodeData;
  elements?: ASTNodeData[][];
  message?: string;
  sourceStart?: number;
  sourceEnd?: number;
  sourceTokenType?: string;
  sourceTokenSubtype?: string;
}

export enum FormulaDialect {
  Excel = 'excel',
  OpenFormula = 'openFormula',
}

function resolveDialect(dialect?: FormulaDialect): wasm.FormulaDialect | undefined {
  if (dialect === undefined) {
    return undefined;
  }

  return dialect === FormulaDialect.OpenFormula
    ? wasm.FormulaDialect.OpenFormula
    : wasm.FormulaDialect.Excel;
}

function normalizeWasmValue<T>(value: unknown): T {
  if (value instanceof Map) {
    const obj: Record<string, unknown> = {};
    for (const [k, v] of value.entries()) {
      obj[String(k)] = normalizeWasmValue(v);
    }
    return obj as T;
  }

  if (Array.isArray(value)) {
    return value.map((item) => normalizeWasmValue(item)) as T;
  }

  if (value && typeof value === 'object') {
    const obj: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      obj[k] = normalizeWasmValue(v);
    }
    return obj as T;
  }

  return value as T;
}

export class Tokenizer {
  private inner: wasm.Tokenizer;

  constructor(formula: string, dialect?: FormulaDialect) {
    this.inner = new wasm.Tokenizer(formula, resolveDialect(dialect));
  }

  get tokens(): Token[] {
    return normalizeWasmValue<Token[]>(this.inner.tokens() as unknown);
  }

  render(): string {
    return this.inner.render();
  }

  get length(): number {
    return this.inner.length();
  }

  getToken(index: number): Token {
    return normalizeWasmValue<Token>(this.inner.getToken(index) as unknown);
  }

  toString(): string {
    return this.inner.toString();
  }
}

export class Parser {
  private inner: wasm.Parser;

  constructor(formula: string, dialect?: FormulaDialect) {
    this.inner = new wasm.Parser(formula, resolveDialect(dialect));
  }

  parse(): ASTNodeData {
    const ast = this.inner.parse();
    return normalizeWasmValue<ASTNodeData>(ast.toJSON() as unknown);
  }
}

export class ASTNode {
  private inner: wasm.ASTNode;

  constructor(inner: wasm.ASTNode) {
    this.inner = inner;
  }

  toJSON(): ASTNodeData {
    return normalizeWasmValue<ASTNodeData>(this.inner.toJSON() as unknown);
  }

  toString(): string {
    return this.inner.toString();
  }

  getType(): string {
    return this.inner.getType();
  }
}

export class Reference {
  private inner: wasm.Reference;

  constructor(
    sheet: string | undefined,
    rowStart: number,
    colStart: number,
    rowEnd: number,
    colEnd: number,
    rowAbsStart: boolean,
    colAbsStart: boolean,
    rowAbsEnd: boolean,
    colAbsEnd: boolean,
  ) {
    this.inner = new wasm.Reference(
      sheet,
      rowStart,
      colStart,
      rowEnd,
      colEnd,
      rowAbsStart,
      colAbsStart,
      rowAbsEnd,
      colAbsEnd,
    );
  }

  get sheet(): string | undefined {
    return this.inner.sheet;
  }

  get rowStart(): number {
    return this.inner.rowStart;
  }

  get colStart(): number {
    return this.inner.colStart;
  }

  get rowEnd(): number {
    return this.inner.rowEnd;
  }

  get colEnd(): number {
    return this.inner.colEnd;
  }

  get rowAbsStart(): boolean {
    return this.inner.rowAbsStart;
  }

  get colAbsStart(): boolean {
    return this.inner.colAbsStart;
  }

  get rowAbsEnd(): boolean {
    return this.inner.rowAbsEnd;
  }

  get colAbsEnd(): boolean {
    return this.inner.colAbsEnd;
  }

  isSingleCell(): boolean {
    return this.inner.isSingleCell();
  }

  isRange(): boolean {
    return this.inner.isRange();
  }

  toString(): string {
    return this.inner.toString();
  }

  toJSON(): ReferenceData {
    return normalizeWasmValue<ReferenceData>(this.inner.toJSON() as unknown);
  }
}

/**
 * Tokenize a formula string
 */
export async function tokenize(
  formula: string,
  dialect?: FormulaDialect,
): Promise<Tokenizer> {
  return ensureInitialized(() => new Tokenizer(formula, dialect));
}

/**
 * Parse a formula string into an AST
 */
export async function parse(
  formula: string,
  dialect?: FormulaDialect,
): Promise<ASTNodeData> {
  return ensureInitialized(() => {
    const ast = wasm.parse(formula, resolveDialect(dialect));
    return normalizeWasmValue<ASTNodeData>(ast.toJSON() as unknown);
  });
}

export type CellScalar = null | undefined | boolean | number | string;
export type CellArray = CellScalar[] | CellScalar[][];
export type CellValue = CellScalar | CellArray;

export interface CustomFunctionOptions {
  minArgs?: number;
  maxArgs?: number | null;
  volatile?: boolean;
  threadSafe?: boolean;
  deterministic?: boolean;
  allowOverrideBuiltin?: boolean;
}

export interface RegisteredFunctionInfo {
  name: string;
  minArgs: number;
  maxArgs: number | null;
  volatile: boolean;
  threadSafe: boolean;
  deterministic: boolean;
  allowOverrideBuiltin: boolean;
}

export type DeterministicTimezone = 'utc' | 'local' | number;

export interface SheetPortEvaluateOptions {
  freezeVolatile?: boolean;
  rngSeed?: number;
  deterministicTimestampUtc?: Date | string;
  deterministicTimezone?: DeterministicTimezone;
}

/**
 * Options accepted by the `Workbook` constructor and the
 * `fromJsonWithOptions` / `fromXlsxBytesWithOptions` loaders.
 */
export interface WorkbookLoadOptions {
  /** Opt into experimental FormulaPlane span evaluation. */
  spanEvaluation?: boolean;
  /** Cycle detection mode (spec §2). */
  cycleDetection?: 'static' | 'runtime';
  /** Cycle policy for live cycles (spec §2). `'iterate'` implies runtime detection. */
  cyclePolicy?: 'error' | 'iterate';
  /** Iterative-calculation max passes per SCC per recalc (Excel default 100). */
  iterateMaxIterations?: number;
  /** Iterative-calculation absolute convergence threshold (Excel default 0.001). */
  iterateMaxChange?: number;
  /** Maximum evaluation work units for one outer request. */
  maxWorkUnits?: number;
  /** Maximum elapsed evaluation time in milliseconds for one outer request. */
  maxEvalTimeMs?: number;
}

/**
 * Per-recalc telemetry from runtime SCC / iterative-calculation evaluation
 * (RFC #113, spec §10). Counters reset at the start of every evaluation
 * request; all-zero when cycle detection is `'static'` or nothing cyclic
 * was evaluated.
 */
export interface CycleTelemetry {
  /** SCC tasks executed (static SCCs that reached Runtime evaluation). */
  staticSccs: number;
  /** SCC tasks whose live subgraph was acyclic - values produced. */
  phantomSccs: number;
  /** Distinct live cycles witnessed across all SCC tasks. */
  liveCyclesWitnessed: number;
  /** Cells stamped `#CIRC!` by Runtime SCC tasks. */
  circCellsStamped: number;
  /** Evaluation sweeps over (subsets of) SCC members, totalled across tasks. */
  settlePassesTotal: number;
  /** Largest pass count any single SCC task needed. */
  maxPassesSingleScc: number;
  /** SCC tasks that entered iterative calculation. */
  iteratedSccs: number;
  /** Iterating SCC tasks that stopped because every member converged. */
  convergedSccs: number;
  /** SCC tasks that stopped at a pass cap (NOT an error under iterate). */
  cappedSccs: number;
  /** Largest |delta| observed in any member's final-pass convergence check. */
  maxAbsDeltaAtStop: number;
  /** Identical-bit NaN comparisons treated as converged (spec §6 NaN rule). */
  nanConverged: number;
  /** Wall-clock milliseconds spent inside Runtime SCC tasks. */
  elapsedMs: number;
}

export interface WorkbookApi extends wasm.Workbook {
  registerFunction(
    name: string,
    callback: (...args: CellValue[]) => CellValue,
    options?: CustomFunctionOptions,
  ): void;
  unregisterFunction(name: string): void;
  listFunctions(): RegisteredFunctionInfo[];
  lastCycleTelemetry(): CycleTelemetry;
}

export type XlsxBytesSource = Uint8Array | ArrayBufferLike;

export type WorkbookConstructor = {
  new (options?: WorkbookLoadOptions): WorkbookApi;
  fromJson(json: string): WorkbookApi;
  fromXlsxBytes(bytes: XlsxBytesSource): WorkbookApi;
  prototype: WorkbookApi;
};

export interface SheetPortSessionApi extends wasm.SheetPortSession {
  evaluateOnce(options?: SheetPortEvaluateOptions): Record<string, unknown>;
}

export type SheetPortSessionConstructor = {
  fromManifestYaml(yaml: string, workbook: WorkbookApi): SheetPortSessionApi;
  prototype: SheetPortSessionApi;
};

const rawWorkbookCtor = wasm.Workbook as unknown as {
  new (options?: WorkbookLoadOptions): WorkbookApi;
  fromJson(json: string): WorkbookApi;
  fromXlsxBytes(bytes: Uint8Array): WorkbookApi;
  prototype: WorkbookApi;
};
const rawWorkbookFromXlsxBytes = rawWorkbookCtor.fromXlsxBytes.bind(rawWorkbookCtor);

export const Workbook = Object.assign(rawWorkbookCtor, {
  fromXlsxBytes(bytes: XlsxBytesSource): WorkbookApi {
    return rawWorkbookFromXlsxBytes(
      bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes),
    );
  },
}) as WorkbookConstructor;
export const SheetPortSession = wasm.SheetPortSession as unknown as SheetPortSessionConstructor;

// Re-export the initialization function as default
export default initializeWasm;
