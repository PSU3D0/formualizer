/**
 * Pure TypeScript bulk-write packers and types for Formualizer WASM & Web Worker.
 *
 * Intentionally has ZERO runtime imports from WASM (`../pkg/*`) or `./index.js` so
 * `formualizer/worker` and `formualizer/arrow` can import from this module without
 * eagerly loading the WebAssembly binary.
 */

export interface WriteRangeReport {
  startRow: number;
  startCol: number;
  endRow: number;
  endCol: number;
  historyCleared: boolean;
}

export interface CellErrorInput {
  type: 'Error';
  value: string;
  message?: string;
}

export interface TraceErrorInput {
  kind: 'error';
  code: string;
  message?: string;
}

export type CellScalarInput =
  | null
  | undefined
  | boolean
  | number
  | bigint
  | string
  | Date
  | CellErrorInput
  | TraceErrorInput;

/**
 * Declared public wire tag values (decoupled from internal `arrow_store::TypeTag`).
 * Private `Skip = 255` is reserved for internal ragged-row packing and never exposed here.
 */
export const CellTag = {
  Empty: 0,
  Number: 1,
  Boolean: 2,
  Text: 3,
  Error: 4,
  DateTime: 5,
  Duration: 6,
  Date: 7,
  Time: 8,
} as const;
export type CellTag = (typeof CellTag)[keyof typeof CellTag];

/** Canonical Excel error wire codes (`1..=13`), matching `arrow_store::map_error_code`. */
export const ExcelErrorCode = {
  Null: 1,
  Ref: 2,
  Name: 3,
  Value: 4,
  Div: 5,
  Na: 6,
  Num: 7,
  Error: 8,
  NImpl: 9,
  Spill: 10,
  Calc: 11,
  Circ: 12,
  Cancelled: 13,
} as const;
export type ExcelErrorCode = (typeof ExcelErrorCode)[keyof typeof ExcelErrorCode];

export type DenseTextValues =
  | readonly (string | null | undefined)[]
  | { bytes: Uint8Array; offsets: Uint32Array };

export type DenseColumnInput =
  | Float64Array
  | Int32Array
  | Uint32Array
  | {
      kind: 'number';
      values: Float64Array | Int32Array | Uint32Array;
      validity?: Uint8Array;
      name?: string;
    }
  | {
      kind: 'boolean';
      values: Uint8Array | readonly (boolean | null | undefined)[];
      validity?: Uint8Array;
      name?: string;
    }
  | {
      kind: 'text';
      values: DenseTextValues;
      validity?: Uint8Array;
      name?: string;
    }
  | {
      kind: 'date';
      values: Float64Array;
      validity?: Uint8Array;
      name?: string;
    }
  | {
      kind: 'datetime';
      values: Float64Array;
      validity?: Uint8Array;
      name?: string;
    }
  | {
      kind: 'duration';
      values: Float64Array;
      validity?: Uint8Array;
      name?: string;
    }
  | {
      kind: 'null';
      length: number;
      name?: string;
    }
  | {
      kind: 'mixed';
      values: readonly CellScalarInput[];
      name?: string;
    };

export interface SetColumnsOptions {
  /** When true, write each column's `name` into `startRow` and start data at `startRow + 1`. */
  headers?: boolean;
  /** Worker transport option; defaults to `false`. Ignored on direct `Sheet` / `Workbook` calls. */
  transfer?: boolean;
}

export interface SetArrowIpcOptions {
  /** When true, write Arrow schema field names into `startRow` and start data at `startRow + 1`. */
  headers?: boolean;
  /** Worker transport option; defaults to `false`. Ignored on direct `Sheet` / `Workbook` calls. */
  transfer?: boolean;
}

export interface SparseCellBlockInput {
  rows: Uint32Array;
  cols: Uint32Array;
  /** Wire tags (`CellTag`: `0..=8`). Alias: `kinds`. */
  tags?: Uint8Array;
  /** Alias for `tags` (`#627` typed columnar ergonomics). */
  kinds?: Uint8Array;
  numbers: Float64Array;
  textBytes?: Uint8Array;
  textOffsets?: Uint32Array;
  strings?: readonly string[];
  /** Alias for `strings` or `{ bytes, offsets }` (`#627` typed columnar ergonomics). */
  texts?: readonly string[] | { bytes: Uint8Array; offsets: Uint32Array };
}

export type SparseCellTuple = readonly [row: number, col: number, value: CellScalarInput];

export interface SparseCellObject {
  row: number;
  col: number;
  value: CellScalarInput;
}

export type SetCellsInput =
  | readonly SparseCellTuple[]
  | readonly SparseCellObject[]
  | SparseCellBlockInput;

export interface PackedBlockTarget {
  _setPackedBlock(
    mode: number,
    startRow: number,
    startCol: number,
    nrows: number,
    ncols: number,
    tags: Uint8Array,
    numbers: Float64Array,
    stringBytes: Uint8Array,
    stringOffsets: Uint32Array,
    rows: Uint32Array,
    cols: Uint32Array,
  ): WriteRangeReport;
  _dateSystem?(): '1900' | '1904';
}

export const PACKED_MODE_DENSE_ROW_MAJOR = 0;
export const PACKED_MODE_DENSE_COL_MAJOR = 1;
export const PACKED_MODE_SPARSE = 2;
const WIRE_TAG_SKIP = 255;

const MAX_ROW = 1_048_576;
const MAX_COL = 16_384;
/** Maximum exact IEEE-754 f64 integer magnitude (`2^53`). */
export const MAX_SAFE_F64_BIGINT = 9_007_199_254_740_992n;
/**
 * Bounded per-WASM-call cell budget to cap linear-memory high-water mark
 * (~18 MB transient buffer per call for 2M cells; 100k & 1M calls run in one call).
 */
export const MAX_CELLS_PER_WASM_CALL = 2_000_000;

const EMPTY_U8 = new Uint8Array(0);
const EMPTY_U32 = new Uint32Array(0);
const sharedTextEncoder = new TextEncoder();

const ERROR_CODE_BY_TOKEN: Readonly<Record<string, ExcelErrorCode>> = {
  '#null!': ExcelErrorCode.Null,
  '#ref!': ExcelErrorCode.Ref,
  '#name?': ExcelErrorCode.Name,
  '#value!': ExcelErrorCode.Value,
  '#div/0!': ExcelErrorCode.Div,
  '#n/a': ExcelErrorCode.Na,
  '#num!': ExcelErrorCode.Num,
  '#error!': ExcelErrorCode.Error,
  '#n/impl!': ExcelErrorCode.NImpl,
  '#spill!': ExcelErrorCode.Spill,
  '#calc!': ExcelErrorCode.Calc,
  '#circ!': ExcelErrorCode.Circ,
  '#cancelled!': ExcelErrorCode.Cancelled,
};

export function emptyWriteRangeReport(): WriteRangeReport {
  return { startRow: 0, startCol: 0, endRow: 0, endCol: 0, historyCleared: false };
}

export function mergeWriteRangeReports(
  a: WriteRangeReport,
  b: WriteRangeReport,
): WriteRangeReport {
  const aEmpty = a.endRow === 0 || a.endCol === 0;
  const bEmpty = b.endRow === 0 || b.endCol === 0;
  if (aEmpty && bEmpty) {
    return {
      startRow: 0,
      startCol: 0,
      endRow: 0,
      endCol: 0,
      historyCleared: a.historyCleared || b.historyCleared,
    };
  }
  if (aEmpty) {
    return { ...b, historyCleared: a.historyCleared || b.historyCleared };
  }
  if (bEmpty) {
    return { ...a, historyCleared: a.historyCleared || b.historyCleared };
  }
  return {
    startRow: Math.min(a.startRow, b.startRow),
    startCol: Math.min(a.startCol, b.startCol),
    endRow: Math.max(a.endRow, b.endRow),
    endCol: Math.max(a.endCol, b.endCol),
    historyCleared: a.historyCleared || b.historyCleared,
  };
}

export function assertNotDetached(view: ArrayBufferView, label: string): void {
  const buf = view.buffer as ArrayBuffer & { detached?: boolean };
  if (buf.detached === true) {
    throw new TypeError(
      `${label}: underlying ArrayBuffer is detached (was it already transferred with transfer: true?)`,
    );
  }
  if (typeof buf.detached !== 'boolean' && typeof buf.slice === 'function') {
    try {
      buf.slice(0, 0);
    } catch {
      throw new TypeError(
        `${label}: underlying ArrayBuffer is detached (was it already transferred with transfer: true?)`,
      );
    }
  }
}

export function validateCoord(row: number, col: number, label = 'coordinate'): void {
  if (!Number.isInteger(row) || !Number.isInteger(col) || row < 1 || col < 1) {
    throw new RangeError(`${label}: row/col must be 1-based integers (row=${row}, col=${col})`);
  }
  if (row > MAX_ROW || col > MAX_COL) {
    throw new RangeError(
      `${label}: row/col exceed Excel grid bounds ${MAX_ROW}x${MAX_COL} (row=${row}, col=${col})`,
    );
  }
}

export function validateBlockBounds(
  startRow: number,
  startCol: number,
  nrows: number,
  ncols: number,
): void {
  validateCoord(startRow, startCol, 'start');
  if (nrows === 0 || ncols === 0) {
    return;
  }
  const endRow = startRow + nrows - 1;
  const endCol = startCol + ncols - 1;
  if (endRow > MAX_ROW) {
    throw new RangeError(`block ending at row ${endRow} exceeds maximum ${MAX_ROW}`);
  }
  if (endCol > MAX_COL) {
    throw new RangeError(`block ending at col ${endCol} exceeds maximum ${MAX_COL}`);
  }
}

export function parseExcelErrorCode(token: string): ExcelErrorCode {
  const code = ERROR_CODE_BY_TOKEN[token.trim().toLowerCase()];
  if (code === undefined) {
    throw new TypeError(`unknown Excel error code: ${JSON.stringify(token)}`);
  }
  return code;
}

const MS_PER_DAY = 86_400_000;
const DAYS_1899_12_31_TO_1970_01_01 = 25_568;
const EXCEL_1900_PHANTOM_CUTOFF_UTC_DAYS = -25_508; // 1900-03-01
const DAYS_1904_01_01_TO_1970_01_01 = 24_107;

/** Convert UTC days since 1970-01-01 to an Excel date serial. */
export function utcDaysToExcelSerial(
  utcDays: number,
  dateSystem: '1900' | '1904' = '1900',
): number {
  if (dateSystem === '1904') {
    return utcDays + DAYS_1904_01_01_TO_1970_01_01;
  }
  const daysSinceEpoch = utcDays + DAYS_1899_12_31_TO_1970_01_01;
  return utcDays >= EXCEL_1900_PHANTOM_CUTOFF_UTC_DAYS ? daysSinceEpoch + 1 : daysSinceEpoch;
}

/**
 * Convert UTC milliseconds since 1970-01-01T00:00:00Z to an Excel serial,
 * matching `formualizer_common::datetime_to_serial_for` (exact UTC midnight yields an
 * integer serial -> `FormatId::DATE`; sub-second precision preserved for `FormatId::DATETIME`).
 */
export function utcMillisToExcelSerial(
  ms: number,
  dateSystem: '1900' | '1904' = '1900',
): number {
  if (!Number.isFinite(ms)) {
    return Number.NaN;
  }
  const utcDays = Math.floor(ms / MS_PER_DAY);
  const msOfDay = ms - utcDays * MS_PER_DAY;
  const dateSerial = utcDaysToExcelSerial(utcDays, dateSystem);
  if (msOfDay === 0) {
    return dateSerial;
  }
  const frac =
    msOfDay % 1000 === 0
      ? msOfDay / 1000 / 86_400
      : (msOfDay * 1_000_000) / 86_400_000_000_000;
  return dateSerial + frac;
}

export class StringTableBuilder {
  private map = new Map<string, number>();
  private list: string[] = [];
  private totalUtf16 = 0;

  intern(value: string): number {
    let idx = this.map.get(value);
    if (idx === undefined) {
      idx = this.list.length;
      this.map.set(value, idx);
      this.list.push(value);
      this.totalUtf16 += value.length;
    }
    return idx;
  }

  get size(): number {
    return this.list.length;
  }

  finish(): { bytes: Uint8Array; offsets: Uint32Array } {
    return packOrderedStrings(this.list, this.totalUtf16);
  }
}

export function packOrderedStrings(
  strings: readonly string[],
  hintUtf16Len?: number,
): { bytes: Uint8Array; offsets: Uint32Array } {
  const count = strings.length;
  if (count === 0) {
    return { bytes: new Uint8Array(0), offsets: new Uint32Array(0) };
  }
  let totalUtf16 = hintUtf16Len ?? 0;
  if (hintUtf16Len === undefined) {
    for (let i = 0; i < count; i++) {
      const s = strings[i];
      if (typeof s !== 'string') {
        throw new TypeError(`string table entry ${i} must be a string`);
      }
      totalUtf16 += s.length;
    }
  }
  const offsets = new Uint32Array(count + 1);
  let cap = Math.max(64, totalUtf16 * 2);
  let buf = new Uint8Array(cap);
  let pos = 0;
  for (let i = 0; i < count; i++) {
    const str = strings[i];
    const maxNeeded = str.length * 3;
    if (pos + maxNeeded > buf.length) {
      while (pos + maxNeeded > cap) {
        cap *= 2;
      }
      const next = new Uint8Array(cap);
      next.set(buf.subarray(0, pos));
      buf = next;
    }
    const { written } = sharedTextEncoder.encodeInto(str, buf.subarray(pos));
    pos += written;
    offsets[i + 1] = pos;
  }
  return { bytes: buf.subarray(0, pos), offsets };
}

function parseErrorObject(value: Record<string, unknown>): ExcelErrorCode | null {
  let codeProp: string | null = null;
  let allowedKeys: readonly string[] | null = null;
  if (value.type === 'Error') {
    codeProp = 'value';
    allowedKeys = ['type', 'value', 'message'];
  } else if (value.kind === 'error') {
    codeProp = 'code';
    allowedKeys = ['kind', 'code', 'message'];
  } else {
    return null;
  }

  for (const key of Object.keys(value)) {
    if (!allowedKeys.includes(key)) {
      throw new TypeError(
        `unexpected error object key \`${key}\`; expected one of ${allowedKeys.join(', ')}`,
      );
    }
  }

  const rawCode = value[codeProp];
  if (typeof rawCode !== 'string') {
    throw new TypeError(`error object requires a string \`${codeProp}\` (e.g. "#CIRC!")`);
  }
  const msg = value.message;
  if (msg !== undefined && msg !== null && typeof msg !== 'string') {
    throw new TypeError('error object `message` must be a string');
  }
  return parseExcelErrorCode(rawCode);
}

function resolveTargetDateSystem(
  target: PackedBlockTarget,
  dateSystem?: '1900' | '1904',
): '1900' | '1904' {
  if (dateSystem !== undefined) return dateSystem;
  return typeof target._dateSystem === 'function' ? target._dateSystem() : '1900';
}

function validateScalarValue(value: unknown, columnLabel?: string): void {
  if (
    typeof value === 'number' ||
    value === null ||
    value === undefined ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    value instanceof Date
  ) {
    return;
  }
  if (typeof value === 'bigint') {
    if (value < -MAX_SAFE_F64_BIGINT || value > MAX_SAFE_F64_BIGINT) {
      const prefix = columnLabel ? `Column '${columnLabel}': ` : '';
      throw new TypeError(
        `${prefix}BigInt value ${value} exceeds exact IEEE-754 f64 range (±2^53); cast explicitly to string or number`,
      );
    }
    return;
  }
  if (Array.isArray(value)) {
    throw new TypeError('nested arrays are not valid scalar cell values');
  }
  if (typeof value === 'object') {
    const code = parseErrorObject(value as Record<string, unknown>);
    if (code !== null) {
      return;
    }
  }
  throw new TypeError(
    "unsupported cell value: expected null, undefined, boolean, number, safe bigint, string, Date, or error object ({ type: 'Error', value } or { kind: 'error', code })",
  );
}

function encodeScalarInto(
  value: unknown,
  idx: number,
  tags: Uint8Array,
  numbers: Float64Array,
  strings: StringTableBuilder,
  dateSystem: '1900' | '1904' = '1900',
  columnLabel?: string,
): void {
  if (typeof value === 'number') {
    if (Number.isFinite(value)) {
      tags[idx] = CellTag.Number;
      numbers[idx] = value;
    } else {
      tags[idx] = CellTag.Error;
      numbers[idx] = ExcelErrorCode.Num;
    }
    return;
  }
  if (value === null || value === undefined) {
    tags[idx] = CellTag.Empty;
    numbers[idx] = 0;
    return;
  }
  if (typeof value === 'string') {
    tags[idx] = CellTag.Text;
    numbers[idx] = strings.intern(value);
    return;
  }
  if (typeof value === 'boolean') {
    tags[idx] = CellTag.Boolean;
    numbers[idx] = value ? 1 : 0;
    return;
  }
  if (typeof value === 'bigint') {
    if (value < -MAX_SAFE_F64_BIGINT || value > MAX_SAFE_F64_BIGINT) {
      const prefix = columnLabel ? `Column '${columnLabel}': ` : '';
      throw new TypeError(
        `${prefix}BigInt value ${value} exceeds exact IEEE-754 f64 range (±2^53); cast explicitly to string or number`,
      );
    }
    tags[idx] = CellTag.Number;
    numbers[idx] = Number(value);
    return;
  }
  if (Array.isArray(value)) {
    throw new TypeError('nested arrays are not valid scalar cell values');
  }
  if (value instanceof Date) {
    const ms = value.getTime();
    const serial = utcMillisToExcelSerial(ms, dateSystem);
    if (Number.isFinite(serial)) {
      const utcDays = Math.floor(ms / MS_PER_DAY);
      const msOfDay = ms - utcDays * MS_PER_DAY;
      tags[idx] = msOfDay === 0 ? CellTag.Date : CellTag.DateTime;
      numbers[idx] = serial;
    } else {
      tags[idx] = CellTag.Error;
      numbers[idx] = ExcelErrorCode.Num;
    }
    return;
  }
  if (typeof value === 'object') {
    const code = parseErrorObject(value as Record<string, unknown>);
    if (code !== null) {
      tags[idx] = CellTag.Error;
      numbers[idx] = code;
      return;
    }
  }
  throw new TypeError(
    "unsupported cell value: expected null, undefined, boolean, number, safe bigint, string, Date, or error object ({ type: 'Error', value } or { kind: 'error', code })",
  );
}

/**
 * Execute `sheet.setValues(startRow, startCol, rows)` by packing in TypeScript into the
 * single packed wire format and calling `_setPackedBlock`, chunking oversized inputs to
 * bound WASM linear memory.
 */
export function executeSetValues(
  target: PackedBlockTarget,
  startRow: number,
  startCol: number,
  rows: readonly (readonly CellScalarInput[])[],
  dateSystem?: '1900' | '1904',
): WriteRangeReport {
  if (!Array.isArray(rows)) {
    throw new TypeError('setValues: rows must be a 2D array');
  }
  const nrows = rows.length;
  let ncols = 0;
  for (let r = 0; r < nrows; r++) {
    const row = rows[r];
    if (!Array.isArray(row)) {
      throw new TypeError(`setValues row ${r + 1} must be an array`);
    }
    if (row.length > ncols) {
      ncols = row.length;
    }
  }
  validateBlockBounds(startRow, startCol, nrows, ncols);
  if (nrows === 0 || ncols === 0) {
    return emptyWriteRangeReport();
  }

  const resolvedDateSystem = resolveTargetDateSystem(target, dateSystem);
  const rowsPerChunk = Math.max(1, Math.floor(MAX_CELLS_PER_WASM_CALL / ncols));
  let combined = emptyWriteRangeReport();

  for (let r0 = 0; r0 < nrows; r0 += rowsPerChunk) {
    const chunkRows = Math.min(rowsPerChunk, nrows - r0);
    const total = chunkRows * ncols;
    const tags = new Uint8Array(total);
    const numbers = new Float64Array(total);
    const strings = new StringTableBuilder();

    for (let dr = 0; dr < chunkRows; dr++) {
      const row = rows[r0 + dr];
      const rowLen = row.length;
      const base = dr * ncols;
      for (let c = 0; c < rowLen; c++) {
        encodeScalarInto(row[c], base + c, tags, numbers, strings, resolvedDateSystem);
      }
      for (let c = rowLen; c < ncols; c++) {
        tags[base + c] = WIRE_TAG_SKIP;
      }
    }

    const { bytes, offsets } = strings.finish();
    const report = target._setPackedBlock(
      PACKED_MODE_DENSE_ROW_MAJOR,
      startRow + r0,
      startCol,
      chunkRows,
      ncols,
      tags,
      numbers,
      bytes,
      offsets,
      EMPTY_U32,
      EMPTY_U32,
    );
    combined = mergeWriteRangeReports(combined, report);
  }

  return combined;
}

interface NormalizedColumn {
  name?: string;
  label: string;
  length: number;
  kind: 'number' | 'boolean' | 'text' | 'date' | 'datetime' | 'duration' | 'null' | 'mixed';
  numValues?: Float64Array | Int32Array | Uint32Array;
  boolValues?: Uint8Array | readonly (boolean | null | undefined)[];
  textArray?: readonly (string | null | undefined)[];
  textPacked?: { bytes: Uint8Array; offsets: Uint32Array };
  mixedValues?: readonly CellScalarInput[];
  validity?: Uint8Array;
}

function isValidityBitSet(validity: Uint8Array | undefined, idx: number): boolean {
  if (validity === undefined) return true;
  return ((validity[idx >> 3] >> (idx & 7)) & 1) !== 0;
}

function validateValidityBitmap(
  validity: Uint8Array | undefined,
  len: number,
  label: string,
): void {
  if (validity === undefined) return;
  if (!(validity instanceof Uint8Array)) {
    throw new TypeError(`${label}: validity must be a Uint8Array (Arrow LSB bitmap)`);
  }
  assertNotDetached(validity, `${label}.validity`);
  const needed = Math.ceil(len / 8);
  if (validity.byteLength < needed) {
    throw new RangeError(
      `${label}: validity bitmap has ${validity.byteLength} bytes, expected at least ${needed} for ${len} rows`,
    );
  }
}

function validatePackedText(
  packed: { bytes: Uint8Array; offsets: Uint32Array },
  label: string,
): number {
  if (!(packed.bytes instanceof Uint8Array) || !(packed.offsets instanceof Uint32Array)) {
    throw new TypeError(`${label}: packed text must provide { bytes: Uint8Array, offsets: Uint32Array }`);
  }
  assertNotDetached(packed.bytes, `${label}.bytes`);
  assertNotDetached(packed.offsets, `${label}.offsets`);
  if (packed.offsets.length === 0) {
    return 0;
  }
  let prev = packed.offsets[0];
  if (prev > packed.bytes.byteLength) {
    throw new RangeError(`${label}: initial UTF-8 offset ${prev} exceeds bytes length ${packed.bytes.byteLength}`);
  }
  for (let i = 1; i < packed.offsets.length; i++) {
    const cur = packed.offsets[i];
    if (cur < prev) {
      throw new RangeError(`${label}: non-monotonic UTF-8 offsets at index ${i - 1} (${prev} -> ${cur})`);
    }
    if (cur > packed.bytes.byteLength) {
      throw new RangeError(`${label}: UTF-8 offset ${cur} exceeds bytes length ${packed.bytes.byteLength}`);
    }
    prev = cur;
  }
  return packed.offsets.length - 1;
}

function normalizeColumn(col: DenseColumnInput, index: number): NormalizedColumn {
  if (col instanceof Float64Array || col instanceof Int32Array || col instanceof Uint32Array) {
    const label = `column ${index}`;
    assertNotDetached(col, label);
    return {
      label,
      length: col.length,
      kind: 'number',
      numValues: col,
    };
  }

  if (!col || typeof col !== 'object') {
    throw new TypeError(`setColumns: column ${index} must be a TypedArray or column descriptor object`);
  }

  const name = typeof col.name === 'string' ? col.name : undefined;
  const label = name ? `column '${name}' (#${index})` : `column ${index}`;

  switch (col.kind) {
    case 'number': {
      const vals = col.values;
      if (
        !(vals instanceof Float64Array) &&
        !(vals instanceof Int32Array) &&
        !(vals instanceof Uint32Array)
      ) {
        throw new TypeError(
          `${label}: 'number' values must be Float64Array, Int32Array, or Uint32Array`,
        );
      }
      assertNotDetached(vals, label);
      validateValidityBitmap(col.validity, vals.length, label);
      return { name, label, length: vals.length, kind: 'number', numValues: vals, validity: col.validity };
    }
    case 'boolean': {
      const vals = col.values;
      if (vals instanceof Uint8Array) {
        assertNotDetached(vals, label);
      } else if (!Array.isArray(vals)) {
        throw new TypeError(`${label}: 'boolean' values must be Uint8Array or boolean[]`);
      }
      validateValidityBitmap(col.validity, vals.length, label);
      return { name, label, length: vals.length, kind: 'boolean', boolValues: vals, validity: col.validity };
    }
    case 'text': {
      const vals = col.values;
      if (Array.isArray(vals)) {
        validateValidityBitmap(col.validity, vals.length, label);
        return { name, label, length: vals.length, kind: 'text', textArray: vals, validity: col.validity };
      }
      if (vals && typeof vals === 'object' && 'bytes' in vals && 'offsets' in vals) {
        const len = validatePackedText(vals, label);
        validateValidityBitmap(col.validity, len, label);
        return { name, label, length: len, kind: 'text', textPacked: vals, validity: col.validity };
      }
      throw new TypeError(
        `${label}: 'text' values must be string[] or { bytes: Uint8Array, offsets: Uint32Array }`,
      );
    }
    case 'date':
    case 'datetime':
    case 'duration': {
      const vals = col.values;
      if (!(vals instanceof Float64Array)) {
        throw new TypeError(`${label}: '${col.kind}' values must be Float64Array`);
      }
      assertNotDetached(vals, label);
      validateValidityBitmap(col.validity, vals.length, label);
      return { name, label, length: vals.length, kind: col.kind, numValues: vals, validity: col.validity };
    }
    case 'null': {
      if (!Number.isInteger(col.length) || col.length < 0) {
        throw new RangeError(`${label}: 'null' length must be a non-negative integer`);
      }
      return { name, label, length: col.length, kind: 'null' };
    }
    case 'mixed': {
      if (!Array.isArray(col.values)) {
        throw new TypeError(`${label}: 'mixed' values must be an array`);
      }
      return { name, label, length: col.values.length, kind: 'mixed', mixedValues: col.values };
    }
    default:
      throw new TypeError(`${label}: unsupported column kind ${JSON.stringify((col as { kind?: unknown }).kind)}`);
  }
}

class CombinedStringArena {
  private builder = new StringTableBuilder();
  private rawSegments: { bytes: Uint8Array; offsets: Uint32Array }[] = [];
  private rawStringCount = 0;
  private rawByteCount = 0;

  /** Append a pre-packed UTF-8 segment and return its base string-table index. */
  appendRawSlice(
    bytes: Uint8Array,
    offsets: Uint32Array,
    rowStart: number,
    rowCount: number,
  ): number {
    if (rowCount === 0) return this.rawStringCount;
    const baseIdx = this.rawStringCount;
    const byteStart = offsets[rowStart];
    const byteEnd = offsets[rowStart + rowCount];
    const subBytes = bytes.subarray(byteStart, byteEnd);
    let subOffsets: Uint32Array;
    if (rowStart === 0 && byteStart === 0 && rowCount + 1 === offsets.length) {
      subOffsets = offsets;
    } else {
      subOffsets = new Uint32Array(rowCount + 1);
      for (let i = 0; i <= rowCount; i++) {
        subOffsets[i] = offsets[rowStart + i] - byteStart;
      }
    }
    this.rawSegments.push({ bytes: subBytes, offsets: subOffsets });
    this.rawStringCount += rowCount;
    this.rawByteCount += subBytes.byteLength;
    return baseIdx;
  }

  /** Intern a JS string (always placed AFTER any raw segments; call finishRawFirst if mixing). */
  intern(s: string): number {
    return this.builder.intern(s);
  }

  get rawCount(): number {
    return this.rawStringCount;
  }

  finish(): { bytes: Uint8Array; offsets: Uint32Array; builderBaseIndex: number } {
    const hasBuilder = this.builder.size > 0;
    if (this.rawSegments.length === 0) {
      const built = this.builder.finish();
      return { ...built, builderBaseIndex: 0 };
    }
    if (!hasBuilder && this.rawSegments.length === 1) {
      return {
        bytes: this.rawSegments[0].bytes,
        offsets: this.rawSegments[0].offsets,
        builderBaseIndex: this.rawStringCount,
      };
    }

    const built = hasBuilder ? this.builder.finish() : { bytes: EMPTY_U8, offsets: EMPTY_U32 };
    const totalStrings = this.rawStringCount + (hasBuilder ? this.builder.size : 0);
    const totalBytes = this.rawByteCount + built.bytes.byteLength;
    const outBytes = new Uint8Array(totalBytes);
    const outOffsets = new Uint32Array(totalStrings + 1);

    let bytePos = 0;
    let strPos = 0;
    for (const seg of this.rawSegments) {
      outBytes.set(seg.bytes, bytePos);
      const count = seg.offsets.length - 1;
      for (let i = 0; i < count; i++) {
        outOffsets[strPos + i + 1] = bytePos + seg.offsets[i + 1];
      }
      bytePos += seg.bytes.byteLength;
      strPos += count;
    }
    if (hasBuilder) {
      outBytes.set(built.bytes, bytePos);
      const count = built.offsets.length - 1;
      for (let i = 0; i < count; i++) {
        outOffsets[strPos + i + 1] = bytePos + built.offsets[i + 1];
      }
    }
    return { bytes: outBytes, offsets: outOffsets, builderBaseIndex: this.rawStringCount };
  }
}

/**
 * Execute `sheet.setColumns(startRow, startCol, columns, options?)` by packing in TypeScript
 * into the single column-major wire format and calling `_setPackedBlock`.
 */
export function executeSetColumns(
  target: PackedBlockTarget,
  startRow: number,
  startCol: number,
  columns: readonly DenseColumnInput[],
  options?: SetColumnsOptions,
  dateSystem?: '1900' | '1904',
): WriteRangeReport {
  if (!Array.isArray(columns)) {
    throw new TypeError('setColumns: columns must be an array');
  }
  validateCoord(startRow, startCol, 'start');
  const ncols = columns.length;
  if (ncols === 0) {
    return emptyWriteRangeReport();
  }

  const headers = options?.headers === true;
  const normalized: NormalizedColumn[] = new Array(ncols);
  for (let c = 0; c < ncols; c++) {
    normalized[c] = normalizeColumn(columns[c], c);
  }

  const nrows = normalized[0].length;
  for (let c = 1; c < ncols; c++) {
    if (normalized[c].length !== nrows) {
      throw new RangeError(
        `setColumns: ${normalized[c].label} length (${normalized[c].length}) does not match column 0 length (${nrows})`,
      );
    }
  }

  let headerNames: string[] | null = null;
  if (headers) {
    headerNames = new Array(ncols);
    for (let c = 0; c < ncols; c++) {
      const name = normalized[c].name;
      if (name === undefined) {
        throw new TypeError(
          `setColumns: headers: true requires every column to provide a string 'name' (missing on ${normalized[c].label})`,
        );
      }
      headerNames[c] = name;
    }
    validateBlockBounds(startRow, startCol, nrows + 1, ncols);
  } else {
    validateBlockBounds(startRow, startCol, nrows, ncols);
  }

  const resolvedDateSystem = resolveTargetDateSystem(target, dateSystem);

  if (nrows === 0) {
    if (headerNames !== null) {
      return executeSetValues(target, startRow, startCol, [headerNames], resolvedDateSystem);
    }
    return emptyWriteRangeReport();
  }

  const rowsPerChunk = Math.max(1, Math.floor(MAX_CELLS_PER_WASM_CALL / ncols));
  const firstChunkMaxDataRows = headerNames !== null ? Math.max(1, rowsPerChunk - 1) : rowsPerChunk;

  // If the write spans multiple chunks, pre-validate JS-typed columns across all rows before
  // committing chunk 0 so invalid values in later chunks never leave partial writes or headers.
  if (nrows > firstChunkMaxDataRows) {
    for (let c = 0; c < ncols; c++) {
      const col = normalized[c];
      if (col.kind === 'text' && col.textArray) {
        const arr = col.textArray;
        const validity = col.validity;
        for (let r = 0; r < nrows; r++) {
          if (!isValidityBitSet(validity, r)) continue;
          const s = arr[r];
          if (s !== null && s !== undefined && typeof s !== 'string') {
            throw new TypeError(`${col.label} row ${r}: expected string or null`);
          }
        }
      } else if (col.kind === 'mixed' && col.mixedValues) {
        const vals = col.mixedValues;
        for (let r = 0; r < nrows; r++) {
          validateScalarValue(vals[r], col.label);
        }
      }
    }
  }

  let combined = emptyWriteRangeReport();
  const dataStartRow = headerNames !== null ? startRow + 1 : startRow;

  let r0 = 0;
  while (r0 < nrows) {
    const includeHeader = headerNames !== null && r0 === 0;
    const headerRows = includeHeader ? 1 : 0;
    const maxDataRows = includeHeader ? firstChunkMaxDataRows : rowsPerChunk;
    const chunkRows = Math.min(maxDataRows, nrows - r0);
    const blockRows = chunkRows + headerRows;
    const total = blockRows * ncols;
    const tags = new Uint8Array(total);
    const numbers = new Float64Array(total);

    // First pass: register all pre-packed UTF-8 columns so their base indices are known
    // before any JS strings are interned (builder strings get offset by `rawTotal` afterward).
    const arena = new CombinedStringArena();
    const rawColBases = new Int32Array(ncols).fill(-1);
    for (let c = 0; c < ncols; c++) {
      const col = normalized[c];
      if (col.kind === 'text' && col.textPacked) {
        rawColBases[c] = arena.appendRawSlice(
          col.textPacked.bytes,
          col.textPacked.offsets,
          r0,
          chunkRows,
        );
      }
    }
    const builderBase = arena.rawCount;
    const builderColumns: number[] = [];

    for (let c = 0; c < ncols; c++) {
      const col = normalized[c];
      const colBase = c * blockRows;
      if (includeHeader) {
        tags[colBase] = CellTag.Text;
        numbers[colBase] = builderBase + arena.intern(headerNames![c]);
      }
      const dataBase = colBase + headerRows;
      const validity = col.validity;

      switch (col.kind) {
        case 'number': {
          const vals = col.numValues!;
          if (validity === undefined && vals instanceof Float64Array) {
            // Fast path: bulk copy Float64Array slice, check non-finite only if needed.
            const sub = r0 === 0 && chunkRows === vals.length ? vals : vals.subarray(r0, r0 + chunkRows);
            tags.fill(CellTag.Number, dataBase, dataBase + chunkRows);
            numbers.set(sub, dataBase);
            for (let i = 0; i < chunkRows; i++) {
              if (!Number.isFinite(sub[i])) {
                tags[dataBase + i] = CellTag.Error;
                numbers[dataBase + i] = ExcelErrorCode.Num;
              }
            }
          } else {
            for (let i = 0; i < chunkRows; i++) {
              const srcIdx = r0 + i;
              if (!isValidityBitSet(validity, srcIdx)) {
                tags[dataBase + i] = CellTag.Empty;
                numbers[dataBase + i] = 0;
              } else {
                const v = vals[srcIdx];
                if (Number.isFinite(v)) {
                  tags[dataBase + i] = CellTag.Number;
                  numbers[dataBase + i] = v;
                } else {
                  tags[dataBase + i] = CellTag.Error;
                  numbers[dataBase + i] = ExcelErrorCode.Num;
                }
              }
            }
          }
          break;
        }
        case 'boolean': {
          const vals = col.boolValues!;
          for (let i = 0; i < chunkRows; i++) {
            const srcIdx = r0 + i;
            if (!isValidityBitSet(validity, srcIdx)) {
              tags[dataBase + i] = CellTag.Empty;
              numbers[dataBase + i] = 0;
              continue;
            }
            const v = vals[srcIdx];
            if (v === null || v === undefined) {
              tags[dataBase + i] = CellTag.Empty;
              numbers[dataBase + i] = 0;
            } else {
              tags[dataBase + i] = CellTag.Boolean;
              numbers[dataBase + i] = v ? 1 : 0;
            }
          }
          break;
        }
        case 'text': {
          if (col.textPacked) {
            const baseStr = rawColBases[c];
            for (let i = 0; i < chunkRows; i++) {
              const srcIdx = r0 + i;
              if (!isValidityBitSet(validity, srcIdx)) {
                tags[dataBase + i] = CellTag.Empty;
                numbers[dataBase + i] = 0;
              } else {
                tags[dataBase + i] = CellTag.Text;
                numbers[dataBase + i] = baseStr + i;
              }
            }
          } else {
            const arr = col.textArray!;
            builderColumns.push(c);
            for (let i = 0; i < chunkRows; i++) {
              const srcIdx = r0 + i;
              if (!isValidityBitSet(validity, srcIdx)) {
                tags[dataBase + i] = CellTag.Empty;
                numbers[dataBase + i] = 0;
                continue;
              }
              const s = arr[srcIdx];
              if (s === null || s === undefined) {
                tags[dataBase + i] = CellTag.Empty;
                numbers[dataBase + i] = 0;
              } else if (typeof s === 'string') {
                tags[dataBase + i] = CellTag.Text;
                numbers[dataBase + i] = builderBase + arena.intern(s);
              } else {
                throw new TypeError(`${col.label} row ${srcIdx}: expected string or null`);
              }
            }
          }
          break;
        }
        case 'date':
        case 'datetime': {
          const vals = col.numValues!;
          const wireTag = col.kind === 'date' ? CellTag.Date : CellTag.DateTime;
          for (let i = 0; i < chunkRows; i++) {
            const srcIdx = r0 + i;
            if (!isValidityBitSet(validity, srcIdx)) {
              tags[dataBase + i] = CellTag.Empty;
              numbers[dataBase + i] = 0;
            } else {
              const v = vals[srcIdx];
              if (Number.isFinite(v)) {
                tags[dataBase + i] = wireTag;
                numbers[dataBase + i] = v;
              } else {
                tags[dataBase + i] = CellTag.Error;
                numbers[dataBase + i] = ExcelErrorCode.Num;
              }
            }
          }
          break;
        }
        case 'duration': {
          const vals = col.numValues!;
          for (let i = 0; i < chunkRows; i++) {
            const srcIdx = r0 + i;
            if (!isValidityBitSet(validity, srcIdx)) {
              tags[dataBase + i] = CellTag.Empty;
              numbers[dataBase + i] = 0;
            } else {
              const v = vals[srcIdx];
              if (Number.isFinite(v)) {
                tags[dataBase + i] = CellTag.Duration;
                numbers[dataBase + i] = v;
              } else {
                tags[dataBase + i] = CellTag.Error;
                numbers[dataBase + i] = ExcelErrorCode.Num;
              }
            }
          }
          break;
        }
        case 'null': {
          // tags/numbers are already zero-initialized (CellTag.Empty = 0).
          break;
        }
        case 'mixed': {
          const vals = col.mixedValues!;
          builderColumns.push(c);
          // Encode using a temporary StringTableBuilder offset via arena.intern:
          const proxyBuilder = {
            intern: (s: string) => builderBase + arena.intern(s),
          } as unknown as StringTableBuilder;
          for (let i = 0; i < chunkRows; i++) {
            encodeScalarInto(
              vals[r0 + i],
              dataBase + i,
              tags,
              numbers,
              proxyBuilder,
              resolvedDateSystem,
              col.label,
            );
          }
          break;
        }
      }
    }

    void builderColumns;
    const { bytes, offsets } = arena.finish();
    const blockStartRow = includeHeader ? startRow : dataStartRow + r0;
    const report = target._setPackedBlock(
      PACKED_MODE_DENSE_COL_MAJOR,
      blockStartRow,
      startCol,
      blockRows,
      ncols,
      tags,
      numbers,
      bytes,
      offsets,
      EMPTY_U32,
      EMPTY_U32,
    );
    combined = mergeWriteRangeReports(combined, report);
    r0 += chunkRows;
  }

  return combined;
}

export function isSparseCellBlockInput(cells: SetCellsInput): cells is SparseCellBlockInput {
  return (
    !Array.isArray(cells) &&
    cells !== null &&
    typeof cells === 'object' &&
    'rows' in cells &&
    'cols' in cells &&
    'numbers' in cells
  );
}

export function resolveSparseBlockBuffers(block: SparseCellBlockInput): {
  rows: Uint32Array;
  cols: Uint32Array;
  tags: Uint8Array;
  numbers: Float64Array;
  textBytes: Uint8Array;
  textOffsets: Uint32Array;
} {
  if (!(block.rows instanceof Uint32Array)) {
    throw new TypeError('setCells: block.rows must be a Uint32Array');
  }
  if (!(block.cols instanceof Uint32Array)) {
    throw new TypeError('setCells: block.cols must be a Uint32Array');
  }
  if (!(block.numbers instanceof Float64Array)) {
    throw new TypeError('setCells: block.numbers must be a Float64Array');
  }
  const tags = block.tags ?? block.kinds;
  if (!(tags instanceof Uint8Array)) {
    throw new TypeError('setCells: block.tags (or block.kinds) must be a Uint8Array');
  }
  if (block.tags !== undefined && block.kinds !== undefined && block.tags !== block.kinds) {
    throw new TypeError('setCells: provide either block.tags or block.kinds, not two different arrays');
  }

  assertNotDetached(block.rows, 'setCells.rows');
  assertNotDetached(block.cols, 'setCells.cols');
  assertNotDetached(tags, 'setCells.tags');
  assertNotDetached(block.numbers, 'setCells.numbers');

  const n = block.rows.length;
  if (block.cols.length !== n || tags.length !== n || block.numbers.length !== n) {
    throw new RangeError(
      `setCells: mismatched typed array lengths (rows=${n}, cols=${block.cols.length}, tags=${tags.length}, numbers=${block.numbers.length})`,
    );
  }

  let textBytes: Uint8Array = new Uint8Array(0);
  let textOffsets: Uint32Array = new Uint32Array(0);

  if (block.texts !== undefined) {
    if (Array.isArray(block.texts)) {
      const packed = packOrderedStrings(block.texts);
      textBytes = packed.bytes;
      textOffsets = packed.offsets;
    } else if (block.texts && typeof block.texts === 'object' && 'bytes' in block.texts && 'offsets' in block.texts) {
      validatePackedText(block.texts, 'setCells.texts');
      textBytes = block.texts.bytes;
      textOffsets = block.texts.offsets;
    } else {
      throw new TypeError('setCells: block.texts must be string[] or { bytes: Uint8Array, offsets: Uint32Array }');
    }
  } else if (block.strings !== undefined) {
    if (!Array.isArray(block.strings)) {
      throw new TypeError('setCells: block.strings must be string[]');
    }
    const packed = packOrderedStrings(block.strings);
    textBytes = packed.bytes;
    textOffsets = packed.offsets;
  } else if (block.textBytes !== undefined || block.textOffsets !== undefined) {
    if (!(block.textBytes instanceof Uint8Array) || !(block.textOffsets instanceof Uint32Array)) {
      throw new TypeError('setCells: block.textBytes and block.textOffsets must both be provided as Uint8Array and Uint32Array');
    }
    validatePackedText({ bytes: block.textBytes, offsets: block.textOffsets }, 'setCells');
    textBytes = block.textBytes;
    textOffsets = block.textOffsets;
  }

  return {
    rows: block.rows,
    cols: block.cols,
    tags,
    numbers: block.numbers,
    textBytes,
    textOffsets,
  };
}

/**
 * Execute `sheet.setCells(cells)` supporting `[row, col, value][]`, `{ row, col, value }[]`,
 * and typed columnar `SparseCellBlockInput` (`rows`, `cols`, `tags`/`kinds`, `numbers`, `texts`/`strings`/`textBytes`+`textOffsets`).
 */
export function executeSetCells(
  target: PackedBlockTarget,
  cells: SetCellsInput,
  dateSystem?: '1900' | '1904',
): WriteRangeReport {
  if (isSparseCellBlockInput(cells)) {
    const { rows, cols, tags, numbers, textBytes, textOffsets } = resolveSparseBlockBuffers(cells);
    const n = rows.length;
    if (n === 0) {
      return emptyWriteRangeReport();
    }
    if (n <= MAX_CELLS_PER_WASM_CALL) {
      return target._setPackedBlock(
        PACKED_MODE_SPARSE,
        0,
        0,
        0,
        0,
        tags,
        numbers,
        textBytes,
        textOffsets,
        rows,
        cols,
      );
    }
    let combined = emptyWriteRangeReport();
    for (let i0 = 0; i0 < n; i0 += MAX_CELLS_PER_WASM_CALL) {
      const i1 = Math.min(i0 + MAX_CELLS_PER_WASM_CALL, n);
      const report = target._setPackedBlock(
        PACKED_MODE_SPARSE,
        0,
        0,
        0,
        0,
        tags.subarray(i0, i1),
        numbers.subarray(i0, i1),
        textBytes,
        textOffsets,
        rows.subarray(i0, i1),
        cols.subarray(i0, i1),
      );
      combined = mergeWriteRangeReports(combined, report);
    }
    return combined;
  }

  if (!Array.isArray(cells)) {
    throw new TypeError(
      'setCells: expected [row, col, value][], { row, col, value }[], or typed columnar block',
    );
  }
  const n = cells.length;
  if (n === 0) {
    return emptyWriteRangeReport();
  }

  const resolvedDateSystem = resolveTargetDateSystem(target, dateSystem);
  let combined = emptyWriteRangeReport();
  for (let i0 = 0; i0 < n; i0 += MAX_CELLS_PER_WASM_CALL) {
    const chunkLen = Math.min(MAX_CELLS_PER_WASM_CALL, n - i0);
    const rows = new Uint32Array(chunkLen);
    const cols = new Uint32Array(chunkLen);
    const tags = new Uint8Array(chunkLen);
    const numbers = new Float64Array(chunkLen);
    const strings = new StringTableBuilder();

    for (let j = 0; j < chunkLen; j++) {
      const entry = cells[i0 + j];
      let row: number;
      let col: number;
      let value: unknown;
      if (Array.isArray(entry)) {
        if (entry.length < 3) {
          throw new TypeError(`setCells entry ${i0 + j}: tuple must have [row, col, value]`);
        }
        row = entry[0] as number;
        col = entry[1] as number;
        value = entry[2];
      } else if (entry && typeof entry === 'object' && 'row' in entry && 'col' in entry) {
        row = (entry as SparseCellObject).row;
        col = (entry as SparseCellObject).col;
        value = (entry as SparseCellObject).value;
      } else {
        throw new TypeError(
          `setCells entry ${i0 + j}: expected [row, col, value] tuple or { row, col, value } object`,
        );
      }
      validateCoord(row, col, `setCells[${i0 + j}]`);
      rows[j] = row;
      cols[j] = col;
      encodeScalarInto(value, j, tags, numbers, strings, resolvedDateSystem);
    }

    const { bytes, offsets } = strings.finish();
    const report = target._setPackedBlock(
      PACKED_MODE_SPARSE,
      0,
      0,
      0,
      0,
      tags,
      numbers,
      bytes,
      offsets,
      rows,
      cols,
    );
    combined = mergeWriteRangeReports(combined, report);
  }

  return combined;
}
