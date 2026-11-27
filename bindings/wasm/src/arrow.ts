/**
 * Zero-WASM, zero-dependency Arrow JS / Flechette / DuckDB-WASM adapter for `sheet.setColumns(...)`.
 *
 * Structurally reads Arrow Table column internals (`data.values`, `data.valueOffsets`,
 * `data.nullBitmap`) and converts them directly into `DenseColumnInput[]` without
 * importing `apache-arrow` or loading any WebAssembly binary.
 */
import {
  MAX_SAFE_F64_BIGINT,
  utcDaysToExcelSerial,
  utcMillisToExcelSerial,
  type DenseColumnInput,
} from './bulk.js';

export interface ArrowTypeLike {
  typeId?: number;
  bitWidth?: number;
  isSigned?: boolean;
  precision?: number;
  unit?: number;
  scale?: number;
  timezone?: string | null;
  valueType?: ArrowTypeLike;
  indices?: ArrowTypeLike;
  toString?: () => string;
}

export interface ArrowDataLike {
  type?: ArrowTypeLike | string;
  length: number;
  offset?: number;
  nullCount?: number;
  values?: ArrayBufferView | readonly unknown[] | null;
  valueOffsets?: Int32Array | Uint32Array | BigInt64Array | null;
  nullBitmap?: Uint8Array | null;
  dictionary?: ArrowDataLike | ArrowColumnLike | null;
}

export interface ArrowColumnLike {
  type?: ArrowTypeLike | string;
  length?: number;
  data?: readonly ArrowDataLike[];
  // Direct single-chunk / flechette-style fields
  offset?: number;
  nullCount?: number;
  values?: ArrayBufferView | readonly unknown[] | null;
  valueOffsets?: Int32Array | Uint32Array | BigInt64Array | null;
  nullBitmap?: Uint8Array | null;
  dictionary?: ArrowDataLike | ArrowColumnLike | null;
}

export interface ArrowFieldLike {
  name: string;
  type?: ArrowTypeLike | string;
}

export interface ArrowSchemaLike {
  fields: readonly ArrowFieldLike[];
}

export interface ArrowTableLike {
  schema: ArrowSchemaLike;
  numRows?: number;
  numCols?: number;
  getChildAt?: (index: number) => ArrowColumnLike | null | undefined;
  getChild?: (name: string) => ArrowColumnLike | null | undefined;
  batches?: readonly {
    numRows: number;
    getChildAt?: (index: number) => ArrowColumnLike | null | undefined;
    data?: { children?: readonly ArrowDataLike[] };
  }[];
}

export interface ArrowColumnsOptions {
  /** Target Excel date system for Date32 / Date64 / Timestamp columns. Defaults to `'1900'`. */
  dateSystem?: '1900' | '1904';
}

// Standard Apache Arrow JS `Type` enum IDs (consistent across apache-arrow, flechette, DuckDB-WASM).
const ARROW_TYPE_DICTIONARY = -1;
const ARROW_TYPE_NULL = 1;
const ARROW_TYPE_INT = 2;
const ARROW_TYPE_FLOAT = 3;
const ARROW_TYPE_BINARY = 4;
const ARROW_TYPE_UTF8 = 5;
const ARROW_TYPE_BOOL = 6;
const ARROW_TYPE_DECIMAL = 7;
const ARROW_TYPE_DATE = 8;
const ARROW_TYPE_TIME = 9;
const ARROW_TYPE_TIMESTAMP = 10;
const ARROW_TYPE_INTERVAL = 11;
const ARROW_TYPE_LIST = 12;
const ARROW_TYPE_STRUCT = 13;
const ARROW_TYPE_UNION = 14;
const ARROW_TYPE_FIXED_SIZE_BINARY = 15;
const ARROW_TYPE_FIXED_SIZE_LIST = 16;
const ARROW_TYPE_MAP = 17;
const ARROW_TYPE_DURATION = 18;
const ARROW_TYPE_LARGE_BINARY = 19;
const ARROW_TYPE_LARGE_UTF8 = 20;

const MS_PER_DAY = 86_400_000;
const US_PER_DAY = 86_400_000_000;
const NS_PER_DAY = 86_400_000_000_000;
const SECONDS_PER_DAY = 86_400;

function arrowColError(colName: string, message: string): TypeError {
  return new TypeError(`Column '${colName}': ${message}`);
}

function getChunks(
  table: ArrowTableLike,
  colIndex: number,
  field: ArrowFieldLike,
): ArrowDataLike[] {
  const child = table.getChildAt?.(colIndex) ?? table.getChild?.(field.name);
  if (child) {
    if (Array.isArray(child.data)) {
      return [...child.data];
    }
    if (typeof child.length === 'number') {
      return [
        {
          type: child.type ?? field.type,
          length: child.length,
          offset: child.offset ?? 0,
          nullCount: child.nullCount,
          values: child.values,
          valueOffsets: child.valueOffsets,
          nullBitmap: child.nullBitmap,
          dictionary: child.dictionary,
        },
      ];
    }
  }
  if (Array.isArray(table.batches)) {
    const out: ArrowDataLike[] = [];
    for (const batch of table.batches) {
      const bChild = batch.getChildAt?.(colIndex);
      if (bChild) {
        if (Array.isArray(bChild.data)) {
          out.push(...bChild.data);
        } else {
          out.push({
            type: bChild.type ?? field.type,
            length: bChild.length ?? batch.numRows,
            offset: bChild.offset ?? 0,
            nullCount: bChild.nullCount,
            values: bChild.values,
            valueOffsets: bChild.valueOffsets,
            nullBitmap: bChild.nullBitmap,
            dictionary: bChild.dictionary,
          });
        }
      } else if (batch.data?.children?.[colIndex]) {
        out.push(batch.data.children[colIndex]);
      }
    }
    return out;
  }
  throw arrowColError(field.name, 'table does not expose column data via getChildAt, getChild, or batches');
}

function isChunkValid(chunk: ArrowDataLike, rowInChunk: number): boolean {
  const bm = chunk.nullBitmap;
  if (!bm || chunk.nullCount === 0) return true;
  const bit = (chunk.offset ?? 0) + rowInChunk;
  return ((bm[bit >> 3] >> (bit & 7)) & 1) !== 0;
}

function extractCombinedValidity(chunks: readonly ArrowDataLike[], totalRows: number): Uint8Array | undefined {
  let hasNulls = false;
  for (const ch of chunks) {
    if (ch.nullBitmap && ch.nullCount !== 0) {
      hasNulls = true;
      break;
    }
  }
  if (!hasNulls) return undefined;

  if (chunks.length === 1 && (chunks[0].offset ?? 0) === 0 && chunks[0].nullBitmap) {
    const needed = Math.ceil(totalRows / 8);
    return chunks[0].nullBitmap.subarray(0, needed);
  }

  const out = new Uint8Array(Math.ceil(totalRows / 8));
  let anyNull = false;
  let dst = 0;
  for (const ch of chunks) {
    for (let i = 0; i < ch.length; i++, dst++) {
      if (isChunkValid(ch, i)) {
        out[dst >> 3] |= 1 << (dst & 7);
      } else {
        anyNull = true;
      }
    }
  }
  return anyNull ? out : undefined;
}

/**
 * Parse Arrow Timestamp timezone per ADR Q4:
 * - null / undefined / "" / "UTC" / "Z" / "Etc/UTC" -> 0 offset seconds
 * - fixed offset ±HH:MM, ±HHMM, ±HH -> offset seconds
 * - IANA zone names -> throws TypeError naming the column
 */
function parseArrowTimezoneOffsetSeconds(
  tz: string | null | undefined,
  colName: string,
): number {
  if (tz === null || tz === undefined || tz === '') return 0;
  const upper = tz.toUpperCase();
  if (upper === 'UTC' || upper === 'Z' || upper === 'ETC/UTC') return 0;
  const m = /^([+-])(\d{2})(?::?(\d{2}))?$/.exec(tz);
  if (m) {
    const sign = m[1] === '-' ? -1 : 1;
    const hours = Number(m[2]);
    const mins = m[3] !== undefined ? Number(m[3]) : 0;
    if (hours <= 23 && mins <= 59) {
      return sign * (hours * 3600 + mins * 60);
    }
  }
  throw arrowColError(
    colName,
    `IANA timezone '${tz}' is not supported directly; localize or strip the timezone to naive/UTC/fixed-offset before calling Formualizer`,
  );
}

type ResolvedKind =
  | { kind: 'null' }
  | { kind: 'float'; precision: number }
  | { kind: 'int'; bitWidth: number; isSigned: boolean }
  | { kind: 'bool' }
  | { kind: 'utf8'; large: boolean }
  | { kind: 'decimal'; scale: number; bitWidth: number }
  | { kind: 'date'; unit: number }
  | { kind: 'time'; unit: number }
  | { kind: 'timestamp'; unit: number; timezone?: string | null }
  | { kind: 'duration'; unit: number }
  | { kind: 'dictionary'; valueType?: ArrowTypeLike | string }
  | { kind: 'unsupported'; label: string };

function resolveArrowType(
  rawType: ArrowTypeLike | string | undefined,
  sampleChunk: ArrowDataLike | undefined,
): ResolvedKind {
  if (typeof rawType === 'string') {
    const norm = rawType.trim().toLowerCase();
    if (norm === 'null') return { kind: 'null' };
    if (norm === 'float64' || norm === 'double') return { kind: 'float', precision: 2 };
    if (norm === 'float32' || norm === 'float') return { kind: 'float', precision: 1 };
    if (norm === 'float16') return { kind: 'float', precision: 0 };
    if (norm === 'int8') return { kind: 'int', bitWidth: 8, isSigned: true };
    if (norm === 'int16') return { kind: 'int', bitWidth: 16, isSigned: true };
    if (norm === 'int32') return { kind: 'int', bitWidth: 32, isSigned: true };
    if (norm === 'int64') return { kind: 'int', bitWidth: 64, isSigned: true };
    if (norm === 'uint8') return { kind: 'int', bitWidth: 8, isSigned: false };
    if (norm === 'uint16') return { kind: 'int', bitWidth: 16, isSigned: false };
    if (norm === 'uint32') return { kind: 'int', bitWidth: 32, isSigned: false };
    if (norm === 'uint64') return { kind: 'int', bitWidth: 64, isSigned: false };
    if (norm === 'bool' || norm === 'boolean') return { kind: 'bool' };
    if (norm === 'utf8' || norm === 'string') return { kind: 'utf8', large: false };
    if (norm === 'largeutf8' || norm === 'largestring') return { kind: 'utf8', large: true };
    if (norm === 'date32' || norm === 'date32<day>') return { kind: 'date', unit: 0 };
    if (norm === 'date64' || norm === 'date64<millisecond>') return { kind: 'date', unit: 1 };
    if (norm.startsWith('timestamp')) {
      const unit = norm.includes('nano') ? 3 : norm.includes('micro') ? 2 : norm.includes('milli') ? 1 : 0;
      return { kind: 'timestamp', unit };
    }
    if (norm.startsWith('duration')) {
      const unit = norm.includes('nano') ? 3 : norm.includes('micro') ? 2 : norm.includes('milli') ? 1 : 0;
      return { kind: 'duration', unit };
    }
    if (norm.startsWith('time')) {
      const unit = norm.includes('nano') ? 3 : norm.includes('micro') ? 2 : norm.includes('milli') ? 1 : 0;
      return { kind: 'time', unit };
    }
    return { kind: 'unsupported', label: rawType };
  }

  if (rawType && typeof rawType === 'object') {
    const tid = rawType.typeId;
    if (tid === ARROW_TYPE_DICTIONARY || sampleChunk?.dictionary != null) {
      return { kind: 'dictionary', valueType: rawType.valueType };
    }
    switch (tid) {
      case ARROW_TYPE_NULL:
        return { kind: 'null' };
      case ARROW_TYPE_INT:
        return {
          kind: 'int',
          bitWidth: rawType.bitWidth ?? 32,
          isSigned: rawType.isSigned ?? true,
        };
      case ARROW_TYPE_FLOAT:
        return { kind: 'float', precision: rawType.precision ?? 2 };
      case ARROW_TYPE_BOOL:
        return { kind: 'bool' };
      case ARROW_TYPE_UTF8:
        return { kind: 'utf8', large: false };
      case ARROW_TYPE_LARGE_UTF8:
        return { kind: 'utf8', large: true };
      case ARROW_TYPE_DECIMAL:
        return { kind: 'decimal', scale: rawType.scale ?? 0, bitWidth: rawType.bitWidth ?? 128 };
      case ARROW_TYPE_DATE:
        return { kind: 'date', unit: rawType.unit ?? 0 };
      case ARROW_TYPE_TIME:
        return { kind: 'time', unit: rawType.unit ?? 1 };
      case ARROW_TYPE_TIMESTAMP:
        return { kind: 'timestamp', unit: rawType.unit ?? 1, timezone: rawType.timezone };
      case ARROW_TYPE_DURATION:
        return { kind: 'duration', unit: rawType.unit ?? 1 };
      case ARROW_TYPE_BINARY:
      case ARROW_TYPE_LARGE_BINARY:
      case ARROW_TYPE_FIXED_SIZE_BINARY:
      case ARROW_TYPE_LIST:
      case ARROW_TYPE_FIXED_SIZE_LIST:
      case ARROW_TYPE_STRUCT:
      case ARROW_TYPE_MAP:
      case ARROW_TYPE_UNION:
      case ARROW_TYPE_INTERVAL:
        return { kind: 'unsupported', label: rawType.toString?.() ?? `typeId(${tid})` };
      default:
        break;
    }

    if (typeof rawType.toString === 'function') {
      const s = rawType.toString();
      if (s && s !== '[object Object]') {
        return resolveArrowType(s, sampleChunk);
      }
    }
  }

  // Fallback inference from typed array values when type metadata is omitted in simple mocks.
  const vals = sampleChunk?.values;
  if (vals instanceof Float64Array) return { kind: 'float', precision: 2 };
  if (vals instanceof Float32Array) return { kind: 'float', precision: 1 };
  if (vals instanceof Int32Array) return { kind: 'int', bitWidth: 32, isSigned: true };
  if (vals instanceof Uint32Array) return { kind: 'int', bitWidth: 32, isSigned: false };
  if (vals instanceof BigInt64Array) return { kind: 'int', bitWidth: 64, isSigned: true };
  if (vals instanceof BigUint64Array) return { kind: 'int', bitWidth: 64, isSigned: false };
  if (vals instanceof Uint8Array && sampleChunk?.valueOffsets) return { kind: 'utf8', large: false };

  return { kind: 'unsupported', label: 'unknown' };
}

function readInt64AsBigInt(
  values: ArrayBufferView | readonly unknown[],
  index: number,
  isSigned: boolean,
  colName: string,
): bigint {
  if (values instanceof BigInt64Array || values instanceof BigUint64Array) {
    return values[index];
  }
  // Older Arrow JS versions represent 64-bit integers as Int32Array / Uint32Array lo-hi pairs.
  if (values instanceof Int32Array || values instanceof Uint32Array) {
    const lo = BigInt(values[index * 2] >>> 0);
    const hiRaw = values[index * 2 + 1];
    const hi = isSigned ? BigInt(hiRaw | 0) : BigInt(hiRaw >>> 0);
    return (hi << 32n) | lo;
  }
  if (Array.isArray(values)) {
    const v = values[index];
    if (typeof v === 'bigint') return v;
    if (typeof v === 'number' && Number.isInteger(v)) return BigInt(v);
  }
  throw arrowColError(colName, 'unsupported 64-bit integer buffer layout');
}

function readFloat16ToNumber(bits: number): number {
  const sign = (bits & 0x8000) !== 0 ? -1 : 1;
  const exp = (bits >> 10) & 0x1f;
  const frac = bits & 0x03ff;
  if (exp === 0) {
    return sign * 2 ** -14 * (frac / 1024);
  }
  if (exp === 31) {
    return frac === 0 ? sign * Infinity : Number.NaN;
  }
  return sign * 2 ** (exp - 15) * (1 + frac / 1024);
}

function convertBigIntTimestampToSerial(
  raw: bigint,
  unit: number,
  tzOffsetSecs: number,
  dateSystem: '1900' | '1904',
): number {
  let secsBig: bigint;
  let remNanos: bigint;
  switch (unit) {
    case 0: // Second
      secsBig = raw;
      remNanos = 0n;
      break;
    case 1: { // Millisecond
      let q = raw / 1_000n;
      let r = raw % 1_000n;
      if (r < 0n) {
        q -= 1n;
        r += 1_000n;
      }
      secsBig = q;
      remNanos = r * 1_000_000n;
      break;
    }
    case 2: { // Microsecond
      let q = raw / 1_000_000n;
      let r = raw % 1_000_000n;
      if (r < 0n) {
        q -= 1n;
        r += 1_000_000n;
      }
      secsBig = q;
      remNanos = r * 1_000n;
      break;
    }
    case 3: { // Nanosecond
      let q = raw / 1_000_000_000n;
      let r = raw % 1_000_000_000n;
      if (r < 0n) {
        q -= 1n;
        r += 1_000_000_000n;
      }
      secsBig = q;
      remNanos = r;
      break;
    }
    default:
      return Number.NaN;
  }

  const wallSecs = secsBig + BigInt(tzOffsetSecs);
  let daysBig = wallSecs / 86_400n;
  let secOfDayBig = wallSecs % 86_400n;
  if (secOfDayBig < 0n) {
    daysBig -= 1n;
    secOfDayBig += 86_400n;
  }
  const dateSerial = utcDaysToExcelSerial(Number(daysBig), dateSystem);
  const secOfDay = Number(secOfDayBig);
  const nanos = Number(remNanos);
  if (secOfDay === 0 && nanos === 0) {
    return dateSerial;
  }
  const timeFrac =
    nanos === 0
      ? secOfDay / SECONDS_PER_DAY
      : (secOfDay * 1_000_000_000 + nanos) / NS_PER_DAY;
  return dateSerial + timeFrac;
}

function convertChunks(
  chunks: readonly ArrowDataLike[],
  resolved: ResolvedKind,
  colName: string,
  dateSystem: '1900' | '1904',
): DenseColumnInput {
  let totalRows = 0;
  for (const ch of chunks) totalRows += ch.length;

  if (resolved.kind === 'null') {
    return { kind: 'null', length: totalRows, name: colName };
  }

  if (resolved.kind === 'unsupported') {
    throw arrowColError(colName, `unsupported Arrow data type: ${resolved.label}`);
  }

  const validity = extractCombinedValidity(chunks, totalRows);

  switch (resolved.kind) {
    case 'float': {
      // Single-chunk zero-offset Float64Array fast path (zero copy!).
      if (
        resolved.precision === 2 &&
        chunks.length === 1 &&
        (chunks[0].offset ?? 0) === 0 &&
        chunks[0].values instanceof Float64Array
      ) {
        const vals = chunks[0].values.subarray(0, totalRows);
        return { kind: 'number', values: vals, validity, name: colName };
      }
      const out = new Float64Array(totalRows);
      let dst = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values;
        if (resolved.precision === 0 && vals instanceof Uint16Array) {
          for (let i = 0; i < ch.length; i++, dst++) {
            out[dst] = isChunkValid(ch, i) ? readFloat16ToNumber(vals[off + i]) : 0;
          }
        } else if (vals instanceof Float64Array || vals instanceof Float32Array) {
          for (let i = 0; i < ch.length; i++, dst++) {
            out[dst] = isChunkValid(ch, i) ? vals[off + i] : 0;
          }
        } else {
          throw arrowColError(colName, 'expected Float64Array/Float32Array/Uint16Array values');
        }
      }
      return { kind: 'number', values: out, validity, name: colName };
    }

    case 'int': {
      if (resolved.bitWidth === 64) {
        const out = new Float64Array(totalRows);
        let dst = 0;
        for (const ch of chunks) {
          const off = ch.offset ?? 0;
          const vals = ch.values;
          if (!vals) throw arrowColError(colName, 'missing 64-bit integer values buffer');
          for (let i = 0; i < ch.length; i++, dst++) {
            if (!isChunkValid(ch, i)) {
              out[dst] = 0;
              continue;
            }
            const v = readInt64AsBigInt(vals, off + i, resolved.isSigned, colName);
            if (v < -MAX_SAFE_F64_BIGINT || v > MAX_SAFE_F64_BIGINT) {
              throw arrowColError(
                colName,
                `row ${dst}: ${resolved.isSigned ? 'Int64' : 'UInt64'} value ${v} exceeds exact f64 range (±2^53); cast explicitly to text or float`,
              );
            }
            out[dst] = Number(v);
          }
        }
        return { kind: 'number', values: out, validity, name: colName };
      }

      // Single-chunk zero-offset Int32Array / Uint32Array fast path.
      if (
        resolved.bitWidth === 32 &&
        chunks.length === 1 &&
        (chunks[0].offset ?? 0) === 0 &&
        ((resolved.isSigned && chunks[0].values instanceof Int32Array) ||
          (!resolved.isSigned && chunks[0].values instanceof Uint32Array))
      ) {
        const vals = chunks[0].values.subarray(0, totalRows);
        return { kind: 'number', values: vals, validity, name: colName };
      }

      const out = new Float64Array(totalRows);
      let dst = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values as ArrayLike<number> | null | undefined;
        if (!vals) throw arrowColError(colName, 'missing integer values buffer');
        for (let i = 0; i < ch.length; i++, dst++) {
          out[dst] = isChunkValid(ch, i) ? Number(vals[off + i]) : 0;
        }
      }
      return { kind: 'number', values: out, validity, name: colName };
    }

    case 'bool': {
      // Arrow Bool vectors store 1 bit per boolean (LSB first) in `data.values: Uint8Array`.
      const out = new Uint8Array(totalRows);
      let dst = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values;
        if (vals instanceof Uint8Array) {
          for (let i = 0; i < ch.length; i++, dst++) {
            if (!isChunkValid(ch, i)) {
              out[dst] = 0;
              continue;
            }
            const bit = off + i;
            out[dst] = (vals[bit >> 3] >> (bit & 7)) & 1;
          }
        } else if (Array.isArray(vals)) {
          for (let i = 0; i < ch.length; i++, dst++) {
            out[dst] = isChunkValid(ch, i) && vals[off + i] ? 1 : 0;
          }
        } else {
          throw arrowColError(colName, 'expected bit-packed Uint8Array values for Bool column');
        }
      }
      return { kind: 'boolean', values: out, validity, name: colName };
    }

    case 'utf8': {
      // Fast path: single zero-offset chunk with Uint8Array + Uint32Array offsets.
      if (
        !resolved.large &&
        chunks.length === 1 &&
        (chunks[0].offset ?? 0) === 0 &&
        chunks[0].values instanceof Uint8Array &&
        chunks[0].valueOffsets instanceof Uint32Array &&
        chunks[0].valueOffsets.length >= totalRows + 1
      ) {
        return {
          kind: 'text',
          values: {
            bytes: chunks[0].values,
            offsets: chunks[0].valueOffsets.subarray(0, totalRows + 1),
          },
          validity,
          name: colName,
        };
      }

      // General path: handle Int32Array offsets (standard apache-arrow JS), BigInt64Array (LargeUtf8),
      // non-zero chunk offsets, and multi-chunk tables.
      let totalBytes = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const offs = ch.valueOffsets;
        if (!(ch.values instanceof Uint8Array) || !offs) {
          throw arrowColError(colName, 'Utf8 column must provide Uint8Array values and valueOffsets');
        }
        const startByte = Number(offs[off]);
        const endByte = Number(offs[off + ch.length]);
        if (startByte < 0 || endByte < startByte || endByte > ch.values.byteLength) {
          throw arrowColError(colName, 'invalid Utf8 valueOffsets bounds');
        }
        totalBytes += endByte - startByte;
      }

      // Single-chunk zero-offset Int32Array offsets can reuse `ch.values` directly while converting offsets to Uint32Array.
      if (
        chunks.length === 1 &&
        (chunks[0].offset ?? 0) === 0 &&
        chunks[0].values instanceof Uint8Array &&
        chunks[0].valueOffsets &&
        Number(chunks[0].valueOffsets[0]) === 0
      ) {
        const offs = chunks[0].valueOffsets;
        const u32 = new Uint32Array(totalRows + 1);
        for (let i = 0; i <= totalRows; i++) {
          const v = Number(offs[i]);
          if (v < 0 || v > 0xffffffff) {
            throw arrowColError(colName, `UTF-8 offset ${v} out of u32 range`);
          }
          u32[i] = v;
        }
        return {
          kind: 'text',
          values: { bytes: chunks[0].values, offsets: u32 },
          validity,
          name: colName,
        };
      }

      const outBytes = new Uint8Array(totalBytes);
      const outOffsets = new Uint32Array(totalRows + 1);
      let bytePos = 0;
      let rowPos = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const bytes = ch.values as Uint8Array;
        const offs = ch.valueOffsets!;
        const baseByte = Number(offs[off]);
        const endByte = Number(offs[off + ch.length]);
        outBytes.set(bytes.subarray(baseByte, endByte), bytePos);
        for (let i = 0; i < ch.length; i++) {
          const next = Number(offs[off + i + 1]) - baseByte;
          outOffsets[rowPos + i + 1] = bytePos + next;
        }
        bytePos += endByte - baseByte;
        rowPos += ch.length;
      }
      return {
        kind: 'text',
        values: { bytes: outBytes, offsets: outOffsets },
        validity,
        name: colName,
      };
    }

    case 'date': {
      const out = new Float64Array(totalRows);
      let dst = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values;
        if (!vals) throw arrowColError(colName, 'missing Date values buffer');
        for (let i = 0; i < ch.length; i++, dst++) {
          if (!isChunkValid(ch, i)) {
            out[dst] = 0;
            continue;
          }
          if (resolved.unit === 0) {
            // Date32: Int32Array days since 1970-01-01
            const days = (vals as Int32Array)[off + i];
            out[dst] = utcDaysToExcelSerial(days, dateSystem);
          } else {
            // Date64: ms since 1970-01-01 (Float64Array or 64-bit int)
            const ms =
              vals instanceof Float64Array
                ? vals[off + i]
                : Number(readInt64AsBigInt(vals, off + i, true, colName));
            out[dst] = utcMillisToExcelSerial(ms, dateSystem);
          }
        }
      }
      return { kind: 'date', values: out, validity, name: colName };
    }

    case 'timestamp': {
      const tzOffsetSecs = parseArrowTimezoneOffsetSeconds(resolved.timezone, colName);
      const out = new Float64Array(totalRows);
      let dst = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values;
        if (!vals) throw arrowColError(colName, 'missing Timestamp values buffer');
        for (let i = 0; i < ch.length; i++, dst++) {
          if (!isChunkValid(ch, i)) {
            out[dst] = 0;
            continue;
          }
          const raw =
            vals instanceof Float64Array
              ? BigInt(Math.trunc(vals[off + i]))
              : readInt64AsBigInt(vals, off + i, true, colName);
          out[dst] = convertBigIntTimestampToSerial(raw, resolved.unit, tzOffsetSecs, dateSystem);
        }
      }
      return { kind: 'date', values: out, validity, name: colName };
    }

    case 'time': {
      const out = new Float64Array(totalRows);
      let dst = 0;
      const denom =
        resolved.unit === 0
          ? SECONDS_PER_DAY
          : resolved.unit === 1
            ? MS_PER_DAY
            : resolved.unit === 2
              ? US_PER_DAY
              : NS_PER_DAY;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values;
        if (!vals) throw arrowColError(colName, 'missing Time values buffer');
        for (let i = 0; i < ch.length; i++, dst++) {
          if (!isChunkValid(ch, i)) {
            out[dst] = 0;
            continue;
          }
          const v =
            vals instanceof Int32Array || vals instanceof Float64Array
              ? vals[off + i]
              : Number(readInt64AsBigInt(vals, off + i, true, colName));
          out[dst] = v / denom;
        }
      }
      return { kind: 'date', values: out, validity, name: colName };
    }

    case 'duration': {
      const out = new Float64Array(totalRows);
      let dst = 0;
      const denom =
        resolved.unit === 0
          ? SECONDS_PER_DAY
          : resolved.unit === 1
            ? MS_PER_DAY
            : resolved.unit === 2
              ? US_PER_DAY
              : NS_PER_DAY;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values;
        if (!vals) throw arrowColError(colName, 'missing Duration values buffer');
        for (let i = 0; i < ch.length; i++, dst++) {
          if (!isChunkValid(ch, i)) {
            out[dst] = 0;
            continue;
          }
          const v =
            vals instanceof Float64Array
              ? vals[off + i]
              : Number(readInt64AsBigInt(vals, off + i, true, colName));
          out[dst] = v / denom;
        }
      }
      return { kind: 'duration', values: out, validity, name: colName };
    }

    case 'decimal': {
      if (resolved.bitWidth !== 128) {
        throw arrowColError(colName, `unsupported Decimal bitWidth ${resolved.bitWidth} (expected 128)`);
      }
      const scaleDiv = 10 ** resolved.scale;
      const out = new Float64Array(totalRows);
      let dst = 0;
      for (const ch of chunks) {
        const off = ch.offset ?? 0;
        const vals = ch.values;
        if (!(vals instanceof Uint32Array) && !(vals instanceof Int32Array)) {
          throw arrowColError(colName, 'Decimal128 column must provide 32-bit word buffer');
        }
        for (let i = 0; i < ch.length; i++, dst++) {
          if (!isChunkValid(ch, i)) {
            out[dst] = 0;
            continue;
          }
          const base = (off + i) * 4;
          const w0 = BigInt(vals[base] >>> 0);
          const w1 = BigInt(vals[base + 1] >>> 0);
          const w2 = BigInt(vals[base + 2] >>> 0);
          const w3 = BigInt(vals[base + 3] | 0);
          const int128 = (w3 << 96n) | (w2 << 64n) | (w1 << 32n) | w0;
          out[dst] = Number(int128) / scaleDiv;
        }
      }
      return { kind: 'number', values: out, validity, name: colName };
    }

    case 'dictionary': {
      // Decode dictionary chunks by materializing keys against each chunk's dictionary values.
      if (chunks.length === 0) {
        return { kind: 'null', length: 0, name: colName };
      }
      const firstDict = chunks[0].dictionary;
      if (!firstDict) {
        throw arrowColError(colName, 'Dictionary column is missing dictionary values');
      }
      const firstDictChunk: ArrowDataLike =
        'data' in firstDict && Array.isArray(firstDict.data)
          ? firstDict.data[0]
          : (firstDict as ArrowDataLike);
      const valKind = resolveArrowType(resolved.valueType ?? firstDictChunk.type, firstDictChunk);
      if (valKind.kind === 'utf8') {
        const decoder = new TextDecoder();
        const outStrings: (string | null)[] = new Array(totalRows);
        let dst = 0;
        for (const ch of chunks) {
          const dictRef = ch.dictionary;
          if (!dictRef) throw arrowColError(colName, 'Dictionary chunk is missing dictionary values');
          const dictChunk: ArrowDataLike =
            'data' in dictRef && Array.isArray(dictRef.data)
              ? dictRef.data[0]
              : (dictRef as ArrowDataLike);
          const dictBytes = dictChunk.values as Uint8Array;
          const dictOffs = dictChunk.valueOffsets!;
          const dictBaseOff = dictChunk.offset ?? 0;
          const dictCache = new Array<string | null>(dictChunk.length);
          for (let d = 0; d < dictChunk.length; d++) {
            if (!isChunkValid(dictChunk, d)) {
              dictCache[d] = null;
            } else {
              const s0 = Number(dictOffs[dictBaseOff + d]);
              const s1 = Number(dictOffs[dictBaseOff + d + 1]);
              dictCache[d] = decoder.decode(dictBytes.subarray(s0, s1));
            }
          }
          const keys = ch.values as ArrayLike<number>;
          const keyOff = ch.offset ?? 0;
          for (let i = 0; i < ch.length; i++, dst++) {
            if (!isChunkValid(ch, i)) {
              outStrings[dst] = null;
              continue;
            }
            const k = Number(keys[keyOff + i]);
            if (k < 0 || k >= dictCache.length) {
              throw arrowColError(colName, `dictionary key ${k} out of bounds (len ${dictCache.length})`);
            }
            outStrings[dst] = dictCache[k];
          }
        }
        return { kind: 'text', values: outStrings, name: colName };
      }

      // Non-string dictionary: decode dictionary values and index into them.
      const dictDecoded = convertChunks([firstDictChunk], valKind, colName, dateSystem);
      if (typeof dictDecoded === 'object' && 'kind' in dictDecoded && dictDecoded.kind === 'number') {
        const out = new Float64Array(totalRows);
        const outValidity = new Uint8Array(Math.ceil(totalRows / 8));
        let anyNull = false;
        let dst = 0;
        for (const ch of chunks) {
          const keys = ch.values as ArrayLike<number>;
          const keyOff = ch.offset ?? 0;
          for (let i = 0; i < ch.length; i++, dst++) {
            if (!isChunkValid(ch, i)) {
              anyNull = true;
              continue;
            }
            const k = Number(keys[keyOff + i]);
            if (k < 0 || k >= dictDecoded.values.length) {
              throw arrowColError(colName, `dictionary key ${k} out of bounds`);
            }
            const dictValid =
              dictDecoded.validity === undefined ||
              ((dictDecoded.validity[k >> 3] >> (k & 7)) & 1) !== 0;
            if (!dictValid) {
              anyNull = true;
              continue;
            }
            out[dst] = dictDecoded.values[k];
            outValidity[dst >> 3] |= 1 << (dst & 7);
          }
        }
        return {
          kind: 'number',
          values: out,
          validity: anyNull ? outValidity : undefined,
          name: colName,
        };
      }
      throw arrowColError(colName, `unsupported dictionary value kind: ${valKind.kind}`);
    }
  }
}

/**
 * Convert an Apache Arrow JS / Flechette / DuckDB-WASM `Table` into `DenseColumnInput[]`
 * ready for `sheet.setColumns(startRow, startCol, arrowColumns(table), { headers? })`.
 *
 * Costs zero WASM bytes and requires no runtime dependency on `apache-arrow`.
 */
export function arrowColumns(
  table: ArrowTableLike,
  options?: ArrowColumnsOptions,
): DenseColumnInput[] {
  if (!table || typeof table !== 'object' || !table.schema || !Array.isArray(table.schema.fields)) {
    throw new TypeError('arrowColumns: expected an Arrow Table with `schema.fields`');
  }
  const dateSystem = options?.dateSystem ?? '1900';
  const fields = table.schema.fields;
  const out: DenseColumnInput[] = new Array(fields.length);

  for (let c = 0; c < fields.length; c++) {
    const field = fields[c];
    const colName = typeof field?.name === 'string' ? field.name : `col_${c}`;
    const chunks = getChunks(table, c, field);
    const sampleChunk = chunks[0];
    const resolved = resolveArrowType(field.type ?? sampleChunk?.type, sampleChunk);
    out[c] = convertChunks(chunks, resolved, colName, dateSystem);
  }

  return out;
}
