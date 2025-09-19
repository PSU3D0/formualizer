import init, {
  Tokenizer as WasmTokenizer,
  Parser as WasmParser,
  ASTNode as WasmASTNode,
  Reference as WasmReference,
  parse as wasmParse,
  FormulaDialect as WasmFormulaDialect,
} from '../pkg/formualizer_wasm.js';

let wasmInitialized = false;
let wasmInitPromise: Promise<void> | null = null;

/**
 * Initialize the WASM module. This must be called before using any other functions.
 * Safe to call multiple times - subsequent calls will return the same promise.
 */
export async function initializeWasm(): Promise<void> {
  if (!wasmInitPromise) {
    wasmInitPromise = init().then(() => {
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
  value: string;
  pos: number;
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
}

export enum FormulaDialect {
  Excel = 'excel',
  OpenFormula = 'openFormula',
}

function resolveDialect(dialect?: FormulaDialect): WasmFormulaDialect | undefined {
  if (dialect === undefined) {
    return undefined;
  }

  return dialect === FormulaDialect.OpenFormula
    ? WasmFormulaDialect.OpenFormula
    : WasmFormulaDialect.Excel;
}

export class Tokenizer {
  private inner: WasmTokenizer;

  constructor(formula: string, dialect?: FormulaDialect) {
    this.inner = new WasmTokenizer(formula, resolveDialect(dialect));
  }

  get tokens(): Token[] {
    const tokensJson = this.inner.tokens();
    return JSON.parse(tokensJson);
  }

  render(): string {
    return this.inner.render();
  }

  get length(): number {
    return this.inner.length();
  }

  getToken(index: number): Token {
    const tokenJson = this.inner.getToken(index);
    return JSON.parse(tokenJson);
  }

  toString(): string {
    return this.inner.toString();
  }
}

export class Parser {
  private inner: WasmParser;

  constructor(formula: string, dialect?: FormulaDialect) {
    this.inner = new WasmParser(formula, resolveDialect(dialect));
  }

  parse(): ASTNodeData {
    const ast = this.inner.parse();
    const json = ast.toJSON();
    return JSON.parse(json);
  }
}

export class ASTNode {
  private inner: WasmASTNode;

  constructor(inner: WasmASTNode) {
    this.inner = inner;
  }

  toJSON(): ASTNodeData {
    const json = this.inner.toJSON();
    return JSON.parse(json);
  }

  toString(): string {
    return this.inner.toString();
  }

  getType(): string {
    return this.inner.getType();
  }
}

export class Reference {
  private inner: WasmReference;

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
    this.inner = new WasmReference(
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
    const json = this.inner.toJSON();
    return JSON.parse(json);
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
    const ast = wasmParse(formula, resolveDialect(dialect));
    const json = ast.toJSON();
    return JSON.parse(json);
  });
}

// Re-export the initialization function as default
export default initializeWasm;
