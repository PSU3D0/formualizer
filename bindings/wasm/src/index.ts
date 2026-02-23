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

export interface WorkbookApi extends wasm.Workbook {
  registerFunction(
    name: string,
    callback: (...args: CellValue[]) => CellValue,
    options?: CustomFunctionOptions,
  ): void;
  unregisterFunction(name: string): void;
  listFunctions(): RegisteredFunctionInfo[];
}

export type WorkbookConstructor = {
  new (): WorkbookApi;
  fromJson(json: string): WorkbookApi;
  prototype: WorkbookApi;
};

export const Workbook = wasm.Workbook as unknown as WorkbookConstructor;
export const SheetPortSession = wasm.SheetPortSession;

// Re-export the initialization function as default
export default initializeWasm;
