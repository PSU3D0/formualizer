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

/** An owned, absolute, 1-based workbook address. */
export interface CellAddress {
  sheet: string;
  row: number;
  column: number;
}

/**
 * An owned, absolute range whose null bounds denote open sides. The runtime
 * input also accepts omitted bounds as open; this shared input/output shape
 * keeps them required because reports always emit all four keys.
 */
export interface RangeArea {
  sheet: string;
  startRow: number | null;
  startColumn: number | null;
  endRow: number | null;
  endColumn: number | null;
}

/** A finite, inclusive, 1-based range. */
export interface FiniteRangeAddress {
  sheet: string;
  startRow: number;
  startColumn: number;
  endRow: number;
  endColumn: number;
}

/**
 * Correlates an owned report with engine state. Both u64 counters are decimal
 * strings so identity remains exact beyond JavaScript's safe-integer range.
 */
export interface StateStamp {
  mutationRevision: string;
  recalcEpoch: string;
}

export interface TraceErrorValue {
  kind: 'error';
  code: string;
  message?: string;
}

export interface TraceTemporalValue {
  /** `time` is engine-reachable but current WASM ingestion promotes it to `datetime`. */
  kind: 'date' | 'datetime' | 'time' | 'duration';
  /** Duration currently uses `TimeDelta { secs: N, nanos: N }`, inherited from workbook values. */
  value: string;
}

export interface TracePendingValue {
  kind: 'pending';
}

/**
 * Inspection value shape. Ordinary primitives and two-dimensional arrays use
 * the existing workbook value convention; errors and ambiguous non-JSON
 * values use stable tagged objects.
 */
export type TraceValue =
  | null
  | boolean
  | number
  | string
  | TraceValue[][]
  | TraceErrorValue
  | TraceTemporalValue
  | TracePendingValue;

export type Staleness = 'current' | 'dirty' | 'neverEvaluated';

export type SpillRole =
  | { role: 'anchor'; extent: FiniteRangeAddress }
  | { role: 'member'; anchor: CellAddress };

export interface CellSnapshot {
  address: CellAddress;
  /** Canonical formula text, not the original whitespace. */
  formula: string | null;
  /**
   * Null for an empty value, when excluded, and for `neverEvaluated` cells;
   * inspect `valueIncluded` and `staleness` to distinguish those cases.
   */
  value: TraceValue | null;
  valueIncluded: boolean;
  staleness: Staleness;
  volatile: boolean;
  spill: SpillRole | null;
}

export interface CellSnapshotReport {
  stamp: StateStamp;
  cell: CellSnapshot;
}

export type NameResolution =
  | { kind: 'cell'; address: CellAddress }
  | { kind: 'range'; declared: RangeArea; resolved: FiniteRangeAddress | null }
  | { kind: 'literal'; value: TraceValue }
  /** Engine-reachable, but unavailable through current WASM ingestion paths. */
  | { kind: 'formula'; formula: string; value: TraceValue | null }
  | { kind: 'unresolved' };

export type SemanticReference =
  | { kind: 'cell'; address: CellAddress }
  | {
      kind: 'range';
      declared: RangeArea;
      resolved: FiniteRangeAddress | null;
      /** Grid-bounded and therefore represented exactly as a JS number. */
      cellCount: number;
    }
  | { kind: 'name'; name: string; resolution: NameResolution }
  | {
      kind: 'table';
      name: string;
      specifier: string;
      resolved: FiniteRangeAddress;
    }
  | { kind: 'external'; raw: string }
  | { kind: 'unsupported'; text: string; reason: string };

/** `observed` is reserved for forward compatibility and is not emitted by phase-1 core. */
export type Provenance = 'declared' | 'observed';
export type TraceLinkKind = 'formula' | 'spillAnchor' | 'spillReader';
export type LinkDisposition = 'expanded' | 'convergent' | 'cycle' | 'elided';

/** Counts are decimal u64 strings, including lower bounds. */
export type OmittedCount =
  | { kind: 'exact'; count: string }
  | { kind: 'atLeast'; count: string };

/**
 * Budget exhaustion degrades in-band. `incomplete: true` with `omitted: null`
 * means the omitted count is unknown rather than zero.
 */
export interface TruncationReport {
  incomplete: boolean;
  omitted: OmittedCount | null;
}

export interface Precedent {
  reference: SemanticReference;
  provenance: Provenance;
}

export interface PrecedentReport {
  stamp: StateStamp;
  cell: CellAddress;
  precedents: Precedent[];
  truncation: TruncationReport;
}

export interface Dependent {
  cell: CellAddress;
  /** Spill member addresses used to discover a spill-anchor reader. */
  via: CellAddress[];
}

export interface DependentsReport {
  stamp: StateStamp;
  cell: CellAddress;
  dependents: Dependent[];
  truncation: TruncationReport;
}

export type TraceDirection = 'precedents' | 'dependents';

export interface TraceLinkTarget {
  /** Response-local node id; never reuse it as an engine input. */
  node: number;
  disposition: LinkDisposition;
}

export interface TraceLink {
  reference: SemanticReference;
  kind: TraceLinkKind;
  /** Present only when `kind` is `formula`. */
  provenance?: Provenance;
  targets: TraceLinkTarget[];
  omitted: OmittedCount | null;
}

export interface TraceNode {
  /** Response-local node id. */
  id: number;
  cell: CellSnapshot;
  links: TraceLink[];
}

export interface TraceGraph {
  stamp: StateStamp;
  direction: TraceDirection;
  /** One response-local id per requested root, preserving request order. */
  roots: number[];
  nodes: TraceNode[];
  truncation: TruncationReport;
}

export interface RangePage {
  stamp: StateStamp;
  declared: RangeArea;
  resolved: FiniteRangeAddress | null;
  total: number;
  /** Echoes the requested row-major offset, even when beyond `total`. */
  offset: number;
  items: CellSnapshot[];
  /** Null means this page completed the resolved area. */
  nextOffset: number | null;
}

export interface SnapshotOptions {
  /** Defaults to true. */
  includeValues?: boolean;
}

export interface PrecedentOptions {
  /** Defaults to 256; zero degrades to in-band truncation. */
  maxLinks?: number;
  /** Defaults to 100,000; non-negative safe integer, with exhaustion in-band. */
  maxWork?: number;
}

export interface DependentsOptions {
  /**
   * Maximum output count. When bounded, the result retains the address-least N
   * candidates discovered within `maxWork`, in canonical sheet/row/column order.
   * Defaults to 256; zero is allowed.
   */
  maxResults?: number;
  /** Defaults to 100,000; non-negative safe integer, with exhaustion in-band. */
  maxWork?: number;
}

/** Trace requires a non-empty roots array. */
export interface TraceOptions {
  /** Defaults to `precedents`. */
  direction?: TraceDirection;
  /** Defaults to 6; zero degrades to elided links. */
  maxDepth?: number;
  /** Defaults to 512 and must be at least the number of unique roots. */
  maxNodes?: number;
  /** Defaults to 1,024; zero degrades to in-band truncation. */
  maxLinks?: number;
  /** Defaults to 100,000; non-negative safe integer, with exhaustion in-band. */
  maxWork?: number;
  /** Defaults to 256; global range-member expansion budget for the whole call. */
  rangeMemberBudget?: number;
  /** Defaults to true. */
  includeValues?: boolean;
}

export interface RangePageOptions {
  /** Defaults to 0; zero-based row-major non-negative safe integer. */
  offset?: number;
  /** Defaults to 100 and must be at least 1. */
  limit?: number;
  /** Defaults to true. */
  includeValues?: boolean;
  /** Defaults to unchecked; a mismatch throws `REVISION_MISMATCH`. */
  expectedStamp?: StateStamp;
}

/** Stable machine-readable properties carried by thrown inspection errors. */
export interface InspectJsError extends Error {
  kind: 'InspectError';
  code:
    | 'SHEET_NOT_FOUND'
    | 'INVALID_ADDRESS'
    | 'INVALID_OPTIONS'
    | 'DEPENDENCY_STATE_UNAVAILABLE'
    | 'REVISION_MISMATCH'
    | 'RESOURCE_EXHAUSTED'
    | 'INSPECT_ERROR';
  inspect_code: InspectJsError['code'];
}

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
  inspectCell(cell: CellAddress, options?: SnapshotOptions): CellSnapshotReport;
  precedents(cell: CellAddress, options?: PrecedentOptions): PrecedentReport;
  dependents(cell: CellAddress, options?: DependentsOptions): DependentsReport;
  trace(roots: CellAddress[], options?: TraceOptions): TraceGraph;
  rangePage(area: RangeArea, options?: RangePageOptions): RangePage;
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
