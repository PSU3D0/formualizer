import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import { Worker } from 'node:worker_threads';
import {
  Workbook,
  CellTag,
  ExcelErrorCode,
} from 'formualizer';
import { arrowColumns } from 'formualizer/arrow';
import {
  createInProcessWorkbookWorker,
  createWorkbookWorker,
  prepareTransferable,
  prepareTransferableViews,
} from 'formualizer/worker';
import * as runtime from 'formualizer';
import { Sheet } from '../pkg/formualizer_wasm.js';

let passed = 0;
async function test(name, fn) {
  await fn();
  passed++;
  console.log(`ok ${passed} - ${name}`);
}

const enc = new TextEncoder();

function makePackedUtf8(strings) {
  const parts = strings.map((s) => enc.encode(s ?? ''));
  const total = parts.reduce((acc, p) => acc + p.byteLength, 0);
  const bytes = new Uint8Array(total);
  const offsets = new Uint32Array(strings.length + 1);
  let pos = 0;
  for (let i = 0; i < parts.length; i++) {
    bytes.set(parts[i], pos);
    pos += parts[i].byteLength;
    offsets[i + 1] = pos;
  }
  return { bytes, offsets };
}

function makeInt32OffsetsUtf8(strings) {
  const { bytes, offsets } = makePackedUtf8(strings);
  return { bytes, valueOffsets: new Int32Array(offsets) };
}

function makeBitmap(bools) {
  const out = new Uint8Array(Math.ceil(bools.length / 8));
  for (let i = 0; i < bools.length; i++) {
    if (bools[i]) out[i >> 3] |= 1 << (i & 7);
  }
  return out;
}

// 1. Verify default build artifacts do NOT leak _setPackedBlock/_dateSystem or arrow-ipc, and dist/arrow.js is zero-WASM.
await test('default build hides _setPackedBlock/_dateSystem, omits setArrowIpc, and arrow adapter has zero WASM imports', () => {
  const dts = fs.readFileSync(new URL('../pkg/formualizer_wasm.d.ts', import.meta.url), 'utf8');
  assert.ok(!dts.includes('_setPackedBlock'), '_setPackedBlock must be excluded from .d.ts');
  assert.ok(!dts.includes('_dateSystem'), '_dateSystem must be excluded from .d.ts');
  assert.ok(!dts.includes('setArrowIpc'), 'setArrowIpc must NOT appear in default build .d.ts');

  const arrowJs = fs.readFileSync(new URL('../dist/arrow.js', import.meta.url), 'utf8');
  assert.doesNotMatch(arrowJs, /^import\s+.*(?:formualizer_wasm|pkg\/)/m, 'dist/arrow.js must not import WASM');
  assert.doesNotMatch(arrowJs, /^import\s+.*['"]apache-arrow/m, 'dist/arrow.js must not depend on apache-arrow');

  const workerJs = fs.readFileSync(new URL('../dist/worker.js', import.meta.url), 'utf8');
  assert.doesNotMatch(
    workerJs,
    /^import\s+(?!type).*['"](?:\.\/index\.js|\.\.\/pkg\/formualizer_wasm\.js)['"]/m,
    'dist/worker.js must not runtime-import index.js or wasm',
  );
});

// 2. setValues: ragged rows, -0.0, NaN/Inf, Date UTC, Error shapes, BigInt, TypeErrors, WriteRangeReport
await test('setValues packs values, preserves ragged rows, -0.0, Dates, Errors, and enforces TypeErrors', () => {
  const wb = new Workbook();
  wb.addSheet('S');
  const sheet = wb.sheet('S');

  // Pre-populate cell (2, 2) so we can verify ragged row 2 leaves it untouched.
  sheet.setValue(2, 2, 999);

  const utcMidnight = new Date('2024-01-15T00:00:00.000Z');
  const utcDateTime = new Date('2024-01-15T12:30:45.250Z');
  const invalidDate = new Date(Number.NaN);

  const report = sheet.setValues(1, 1, [
    [-0.0, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY],
    [42n], // ragged: only 1 cell, must NOT overwrite (2, 2) = 999
    [utcMidnight, utcDateTime, invalidDate, true],
    [
      { type: 'Error', value: '#CIRC!' },
      { kind: 'error', code: '#N/A' },
      'hello',
      'hello', // duplicate string exercises string table deduplication
    ],
    [null, undefined, false, 9_007_199_254_740_992n],
  ]);

  assert.deepEqual(report, {
    startRow: 1,
    startCol: 1,
    endRow: 5,
    endCol: 4,
    historyCleared: false,
  });

  // -0.0 preserved bit-for-bit
  assert.ok(Object.is(sheet.getValue(1, 1), -0), 'expected -0.0 sign bit preserved');
  assert.equal(sheet.getValue(1, 2), '#NUM!');
  assert.equal(sheet.getValue(1, 3), '#NUM!');
  assert.equal(sheet.getValue(1, 4), '#NUM!');

  // Ragged row preserved untouched trailing cell
  assert.equal(sheet.getValue(2, 1), 42);
  assert.equal(sheet.getValue(2, 2), 999);

  // Dates
  const d1 = sheet.getValue(3, 1);
  assert.ok(d1 instanceof Date);
  assert.equal(d1.toISOString(), '2024-01-15T00:00:00.000Z');
  const d2 = sheet.getValue(3, 2);
  assert.ok(d2 instanceof Date);
  assert.equal(d2.toISOString(), '2024-01-15T12:30:45.250Z');
  assert.equal(sheet.getValue(3, 3), '#NUM!');
  assert.equal(sheet.getValue(3, 4), true);

  // Errors & deduplicated strings
  sheet.setFormula(6, 1, '=ERROR.TYPE(A4)');
  sheet.setFormula(6, 2, '=ERROR.TYPE(B4)');
  wb.evaluateAll();
  assert.equal(sheet.getValue(4, 1), '#CIRC!');
  assert.equal(sheet.getValue(6, 1), 12);
  assert.equal(sheet.getValue(4, 2), '#N/A');
  assert.equal(sheet.getValue(6, 2), 7);
  assert.equal(sheet.getValue(4, 3), 'hello');
  assert.equal(sheet.getValue(4, 4), 'hello');

  assert.equal(sheet.getValue(5, 1), null);
  assert.equal(sheet.getValue(5, 2), null);
  assert.equal(sheet.getValue(5, 3), false);
  assert.equal(sheet.getValue(5, 4), 9_007_199_254_740_992);

  // Rejections: BigInt > 2^53, nested array, plain object, unknown error code
  assert.throws(() => sheet.setValues(1, 1, [[9_007_199_254_740_993n]]), TypeError);
  assert.throws(() => sheet.setValues(1, 1, [[-9_007_199_254_740_993n]]), TypeError);
  assert.throws(() => sheet.setValues(1, 1, [[[1, 2]]]), TypeError);
  assert.throws(() => sheet.setValues(1, 1, [[{ foo: 'bar' }]]), TypeError);
  assert.throws(
    () => sheet.setValues(1, 1, [[{ type: 'Error', value: '#NOPE!' }]]),
    TypeError,
  );

  // Workbook-level setValues forwarding
  const wbReport = wb.setValues('S', 10, 1, [[1, 2], [3, 4]]);
  assert.deepEqual(wbReport, {
    startRow: 10,
    startCol: 1,
    endRow: 11,
    endCol: 2,
    historyCleared: false,
  });
  assert.equal(sheet.getValue(11, 2), 4);

  sheet.free();
  wb.free();
});

// 3. setColumns: all column kinds, validity bitmaps, headers, length mismatch, detached buffers
await test('setColumns handles typed arrays, validity bitmaps, headers, and detached buffers', () => {
  const wb = new Workbook();
  wb.addSheet('Cols');
  const sheet = wb.sheet('Cols');

  const packedText = makePackedUtf8(['alpha', 'beta', 'gamma']);
  const report = sheet.setColumns(
    1,
    1,
    [
      { kind: 'number', name: 'f64', values: new Float64Array([1.5, -0.0, Number.NaN]), validity: makeBitmap([true, true, true]) },
      { kind: 'number', name: 'i32', values: new Int32Array([10, 20, 30]), validity: makeBitmap([true, false, true]) },
      { kind: 'boolean', name: 'flag', values: new Uint8Array([1, 0, 1]), validity: makeBitmap([true, true, false]) },
      { kind: 'text', name: 'strArr', values: ['x', null, 'x'] },
      { kind: 'text', name: 'strPacked', values: packedText, validity: makeBitmap([true, false, true]) },
      { kind: 'date', name: 'dt', values: new Float64Array([45306, 45306.5, 45307]) },
      { kind: 'null', name: 'empty', length: 3 },
      { kind: 'mixed', name: 'mix', values: [1, 'two', { type: 'Error', value: '#DIV/0!' }] },
    ],
    { headers: true },
  );

  assert.deepEqual(report, {
    startRow: 1,
    startCol: 1,
    endRow: 4,
    endCol: 8,
    historyCleared: false,
  });

  // Headers in row 1
  assert.equal(sheet.getValue(1, 1), 'f64');
  assert.equal(sheet.getValue(1, 8), 'mix');

  // Data starts in row 2
  assert.equal(sheet.getValue(2, 1), 1.5);
  assert.ok(Object.is(sheet.getValue(3, 1), -0));
  assert.equal(sheet.getValue(4, 1), '#NUM!');

  assert.equal(sheet.getValue(2, 2), 10);
  assert.equal(sheet.getValue(3, 2), null);
  assert.equal(sheet.getValue(4, 2), 30);

  assert.equal(sheet.getValue(2, 3), true);
  assert.equal(sheet.getValue(3, 3), false);
  assert.equal(sheet.getValue(4, 3), null);

  assert.equal(sheet.getValue(2, 4), 'x');
  assert.equal(sheet.getValue(3, 4), null);
  assert.equal(sheet.getValue(4, 4), 'x');

  assert.equal(sheet.getValue(2, 5), 'alpha');
  assert.equal(sheet.getValue(3, 5), null);
  assert.equal(sheet.getValue(4, 5), 'gamma');

  assert.equal(sheet.getValue(2, 6).toISOString(), '2024-01-15T00:00:00.000Z');
  assert.equal(sheet.getValue(3, 6).toISOString(), '2024-01-15T12:00:00.000Z');
  assert.equal(sheet.getValue(2, 7), null);
  assert.equal(sheet.getValue(4, 8), '#DIV/0!');

  // Bare TypedArray shorthand
  const bareReport = wb.setColumns('Cols', 10, 1, [
    new Float64Array([100, 200]),
    new Int32Array([-5, 5]),
    new Uint32Array([7, 8]),
  ]);
  assert.deepEqual(bareReport, {
    startRow: 10,
    startCol: 1,
    endRow: 11,
    endCol: 3,
    historyCleared: false,
  });
  assert.equal(sheet.getValue(11, 3), 8);

  // Column length mismatch rejected
  assert.throws(
    () => sheet.setColumns(1, 1, [new Float64Array([1, 2]), new Float64Array([1])]),
    RangeError,
  );

  // headers: true without name rejected
  assert.throws(
    () => sheet.setColumns(1, 1, [new Float64Array([1, 2])], { headers: true }),
    TypeError,
  );

  // Detached ArrayBuffer rejected loudly
  const detached = new Float64Array([1, 2, 3]);
  detached.buffer.transfer();
  assert.throws(() => sheet.setColumns(1, 1, [detached]), TypeError);

  sheet.free();
  wb.free();
});

// 4. setCells: tuples, objects, typed block (tags+strings AND #627 kinds+texts), duplicates, historyCleared
await test('setCells handles tuples, objects, #627 typed blocks, duplicates, and historyCleared', () => {
  const wb = new Workbook({ enableChangelog: true });
  wb.addSheet('Sp');
  const sheet = wb.sheet('Sp');

  // Tuples with duplicate coordinate (last-write-wins)
  const r1 = sheet.setCells([
    [1, 1, 10],
    [1, 1, 25],
    [2, 3, 'ok'],
  ]);
  assert.deepEqual(r1, { startRow: 1, startCol: 1, endRow: 2, endCol: 3, historyCleared: false });
  assert.equal(sheet.getValue(1, 1), 25);
  assert.equal(sheet.getValue(2, 3), 'ok');

  // Undoable because small batch fits history_limit
  assert.equal(wb.canUndo(), true);
  wb.undo();
  assert.equal(sheet.getValue(1, 1), null);
  wb.redo();
  assert.equal(sheet.getValue(1, 1), 25);

  // Object array
  sheet.setCells([
    { row: 3, col: 1, value: true },
    { row: 3, col: 2, value: { type: 'Error', value: '#REF!' } },
  ]);
  assert.equal(sheet.getValue(3, 1), true);
  assert.equal(sheet.getValue(3, 2), '#REF!');

  // #627 ergonomics: { rows, cols, kinds, numbers, texts: string[] }
  const r2 = sheet.setCells({
    rows: new Uint32Array([5, 5, 6, 6]),
    cols: new Uint32Array([1, 2, 1, 2]),
    kinds: new Uint8Array([CellTag.Number, CellTag.Text, CellTag.Boolean, CellTag.Error]),
    numbers: new Float64Array([3.14, 0, 1, ExcelErrorCode.Div]),
    texts: ['checker'],
  });
  assert.deepEqual(r2, { startRow: 5, startCol: 1, endRow: 6, endCol: 2, historyCleared: false });
  assert.equal(sheet.getValue(5, 1), 3.14);
  assert.equal(sheet.getValue(5, 2), 'checker');
  assert.equal(sheet.getValue(6, 1), true);
  assert.equal(sheet.getValue(6, 2), '#DIV/0!');

  // Packed texts { bytes, offsets }
  const packed = makePackedUtf8(['foo', 'bar']);
  sheet.setCells({
    rows: new Uint32Array([7, 7]),
    cols: new Uint32Array([1, 2]),
    kinds: new Uint8Array([CellTag.Text, CellTag.Text]),
    numbers: new Float64Array([1, 0]),
    texts: packed,
  });
  assert.equal(sheet.getValue(7, 1), 'bar');
  assert.equal(sheet.getValue(7, 2), 'foo');

  // Date (7) and Time (8) tags succeed; unknown tag (9) and Skip (255) rejected atomically
  sheet.setCells({
    rows: new Uint32Array([8, 8]),
    cols: new Uint32Array([3, 4]),
    tags: new Uint8Array([CellTag.Date, CellTag.Time]),
    numbers: new Float64Array([45306, 0.5]),
  });
  assert.equal(sheet.getValue(8, 3).toISOString(), '2024-01-15T00:00:00.000Z');
  assert.equal(sheet.getValue(8, 4), '12:00:00');

  assert.throws(
    () =>
      sheet.setCells({
        rows: new Uint32Array([8, 8]),
        cols: new Uint32Array([1, 2]),
        tags: new Uint8Array([CellTag.Number, 9]),
        numbers: new Float64Array([99, 0]),
      }),
  );
  assert.equal(sheet.getValue(8, 1), null, 'atomic preflight must not write cell before error');

  assert.throws(
    () =>
      sheet.setCells({
        rows: new Uint32Array([8]),
        cols: new Uint32Array([1]),
        tags: new Uint8Array([255]),
        numbers: new Float64Array([0]),
      }),
  );

  // Detached buffer rejected loudly
  const rows = new Uint32Array([1]);
  rows.buffer.transfer();
  assert.throws(
    () =>
      sheet.setCells({
        rows,
        cols: new Uint32Array([1]),
        kinds: new Uint8Array([CellTag.Number]),
        numbers: new Float64Array([1]),
      }),
    TypeError,
  );

  // Oversized batch while changelog is enabled clears history and reports historyCleared: true
  const bigN = 12_000; // default max_changelog_events is 10_000
  const bigRows = new Uint32Array(bigN);
  const bigCols = new Uint32Array(bigN);
  const bigTags = new Uint8Array(bigN).fill(CellTag.Number);
  const bigNums = new Float64Array(bigN);
  for (let i = 0; i < bigN; i++) {
    bigRows[i] = i + 1;
    bigCols[i] = 10;
    bigNums[i] = i;
  }
  const bigReport = sheet.setCells({
    rows: bigRows,
    cols: bigCols,
    tags: bigTags,
    numbers: bigNums,
  });
  assert.equal(bigReport.historyCleared, true);
  assert.equal(wb.canUndo(), false);
  assert.equal(sheet.getValue(bigN, 10), bigN - 1);

  sheet.free();
  wb.free();
});

// 5. formualizer/arrow: structurally-typed mock Arrow Tables covering every type & edge case
await test('formualizer/arrow arrowColumns converts Float64/Int32/Int64/Bool/Utf8/Date/Timestamp/Null/Validity/MultiChunk', () => {
  const utf8Chunk1 = makeInt32OffsetsUtf8(['alpha', 'nullme']);
  const utf8Chunk2 = makeInt32OffsetsUtf8(['gamma', 'delta']);

  // Date32: days since 1970-01-01 -> 2024-01-15 is 19737
  const days20240115 = Math.floor(Date.UTC(2024, 0, 15) / 86_400_000);
  // Timestamp(ms, +09:00): 2024-01-15T00:00:00Z + 09:00 -> 2024-01-15T09:00:00 wall clock
  const tsMs = BigInt(Date.UTC(2024, 0, 15, 0, 0, 0));

  const columnsData = [
    // Float64 with null at row 2
    {
      data: [
        {
          type: { typeId: 3, precision: 2 },
          length: 4,
          offset: 0,
          nullCount: 1,
          values: new Float64Array([1.25, -0.0, 999.0, 4.75]),
          nullBitmap: makeBitmap([true, true, false, true]),
        },
      ],
    },
    // Int32
    {
      data: [
        {
          type: { typeId: 2, bitWidth: 32, isSigned: true },
          length: 4,
          offset: 0,
          nullCount: 0,
          values: new Int32Array([-10, 0, 20, 30]),
          nullBitmap: null,
        },
      ],
    },
    // Int64 (safe values + null at row 1 whose raw payload is > 2^53 and MUST be ignored!)
    {
      data: [
        {
          type: { typeId: 2, bitWidth: 64, isSigned: true },
          length: 4,
          offset: 0,
          nullCount: 1,
          values: new BigInt64Array([100n, 9_007_199_254_740_999n, 9_007_199_254_740_992n, -9_007_199_254_740_992n]),
          nullBitmap: makeBitmap([true, false, true, true]),
        },
      ],
    },
    // Bool (Arrow bit-packed LSB: [true, false, true, null])
    {
      data: [
        {
          type: { typeId: 6 },
          length: 4,
          offset: 0,
          nullCount: 1,
          values: makeBitmap([true, false, true, true]),
          nullBitmap: makeBitmap([true, true, true, false]),
        },
      ],
    },
    // Multi-chunk Utf8 with Int32Array offsets + validity
    {
      data: [
        {
          type: { typeId: 5 },
          length: 2,
          offset: 0,
          nullCount: 1,
          values: utf8Chunk1.bytes,
          valueOffsets: utf8Chunk1.valueOffsets,
          nullBitmap: makeBitmap([true, false]),
        },
        {
          type: { typeId: 5 },
          length: 2,
          offset: 0,
          nullCount: 0,
          values: utf8Chunk2.bytes,
          valueOffsets: utf8Chunk2.valueOffsets,
          nullBitmap: null,
        },
      ],
    },
    // Date32
    {
      data: [
        {
          type: { typeId: 8, unit: 0 },
          length: 4,
          offset: 0,
          nullCount: 1,
          values: new Int32Array([days20240115, days20240115 + 1, 0, days20240115 + 2]),
          nullBitmap: makeBitmap([true, true, false, true]),
        },
      ],
    },
    // Timestamp (ms, fixed offset +09:00)
    {
      data: [
        {
          type: { typeId: 10, unit: 1, timezone: '+09:00' },
          length: 4,
          offset: 0,
          nullCount: 0,
          values: new BigInt64Array([tsMs, tsMs + 3_600_000n, tsMs + 500n, tsMs]),
          nullBitmap: null,
        },
      ],
    },
    // Null column
    {
      data: [{ type: { typeId: 1 }, length: 4, offset: 0, nullCount: 4 }],
    },
  ];

  const table = {
    numRows: 4,
    schema: {
      fields: [
        { name: 'price', type: { typeId: 3, precision: 2 } },
        { name: 'qty', type: { typeId: 2, bitWidth: 32, isSigned: true } },
        { name: 'big_id', type: { typeId: 2, bitWidth: 64, isSigned: true } },
        { name: 'active', type: { typeId: 6 } },
        { name: 'label', type: { typeId: 5 } },
        { name: 'day', type: { typeId: 8, unit: 0 } },
        { name: 'ts', type: { typeId: 10, unit: 1, timezone: '+09:00' } },
        { name: 'empty_col', type: { typeId: 1 } },
      ],
    },
    getChildAt: (i) => columnsData[i],
  };

  const cols = arrowColumns(table);
  assert.equal(cols.length, 8);
  assert.equal(cols[7].kind, 'null');

  const wb = new Workbook();
  wb.addSheet('Arrow');
  const sheet = wb.sheet('Arrow');
  const report = sheet.setColumns(1, 1, cols, { headers: true });
  assert.deepEqual(report, {
    startRow: 1,
    startCol: 1,
    endRow: 5,
    endCol: 8,
    historyCleared: false,
  });

  // Headers
  assert.equal(sheet.getValue(1, 1), 'price');
  assert.equal(sheet.getValue(1, 5), 'label');

  // Float64 + -0.0 + null
  assert.equal(sheet.getValue(2, 1), 1.25);
  assert.ok(Object.is(sheet.getValue(3, 1), -0));
  assert.equal(sheet.getValue(4, 1), null);
  assert.equal(sheet.getValue(5, 1), 4.75);

  // Int32 & Int64
  assert.equal(sheet.getValue(2, 2), -10);
  assert.equal(sheet.getValue(2, 3), 100);
  assert.equal(sheet.getValue(3, 3), null);
  assert.equal(sheet.getValue(4, 3), 9_007_199_254_740_992);
  assert.equal(sheet.getValue(5, 3), -9_007_199_254_740_992);

  // Bool
  assert.equal(sheet.getValue(2, 4), true);
  assert.equal(sheet.getValue(3, 4), false);
  assert.equal(sheet.getValue(4, 4), true);
  assert.equal(sheet.getValue(5, 4), null);

  // Multi-chunk Utf8
  assert.equal(sheet.getValue(2, 5), 'alpha');
  assert.equal(sheet.getValue(3, 5), null);
  assert.equal(sheet.getValue(4, 5), 'gamma');
  assert.equal(sheet.getValue(5, 5), 'delta');

  // Date32 & Timestamp (+09:00 wall clock with sub-second precision)
  assert.equal(sheet.getValue(2, 6).toISOString(), '2024-01-15T00:00:00.000Z');
  assert.equal(sheet.getValue(3, 6).toISOString(), '2024-01-16T00:00:00.000Z');
  assert.equal(sheet.getValue(4, 6), null);
  assert.equal(sheet.getValue(2, 7).toISOString(), '2024-01-15T09:00:00.000Z');
  assert.equal(sheet.getValue(3, 7).toISOString(), '2024-01-15T10:00:00.000Z');
  assert.equal(sheet.getValue(4, 7).toISOString(), '2024-01-15T09:00:00.500Z');

  // Null column
  assert.equal(sheet.getValue(2, 8), null);

  // Formula evaluation over Arrow-ingested columns
  sheet.setFormula(6, 1, '=SUM(A2:A5)');
  sheet.setFormula(6, 2, '=SUM(B2:B5)');
  wb.evaluateAll();
  assert.equal(sheet.getValue(6, 1), 6);
  assert.equal(sheet.getValue(6, 2), 40);

  // Int64 > 2^53 rejection MUST name the column
  const badInt64Table = {
    schema: { fields: [{ name: 'snowflake_id', type: { typeId: 2, bitWidth: 64, isSigned: true } }] },
    getChildAt: () => ({
      data: [{ type: { typeId: 2, bitWidth: 64, isSigned: true }, length: 1, values: new BigInt64Array([9_007_199_254_740_993n]) }],
    }),
  };
  assert.throws(() => arrowColumns(badInt64Table), /snowflake_id/);

  // IANA timezone rejection MUST name the column
  const badTzTable = {
    schema: { fields: [{ name: 'created_at', type: { typeId: 10, unit: 1, timezone: 'America/New_York' } }] },
    getChildAt: () => ({
      data: [{ type: { typeId: 10, unit: 1, timezone: 'America/New_York' }, length: 1, values: new BigInt64Array([0n]) }],
    }),
  };
  assert.throws(() => arrowColumns(badTzTable), /created_at/);

  // Unsupported nested List/Struct/Binary rejection MUST name the column
  const badNestedTable = {
    schema: { fields: [{ name: 'nested_items', type: { typeId: 12 } }] },
    getChildAt: () => ({ data: [{ type: { typeId: 12 }, length: 1 }] }),
  };
  assert.throws(() => arrowColumns(badNestedTable), /nested_items/);

  sheet.free();
  wb.free();
});

// 6. Generalized prepareTransferable & prepareTransferableViews safety rules
await test('prepareTransferable safely copies subviews, SharedArrayBuffer, WebAssembly.Memory, deduplicates shared buffers, and rejects detached', () => {
  // Subview -> copied, original untouched
  const backing = new Float64Array([10, 20, 30, 40]);
  const sub = backing.subarray(1, 3);
  const subRes = prepareTransferable(sub, true);
  assert.equal(backing.buffer.detached, false);
  assert.deepEqual([...subRes.view], [20, 30]);
  assert.equal(subRes.transfer.length, 1);
  assert.notEqual(subRes.transfer[0], backing.buffer);

  // SharedArrayBuffer -> copied
  const sab = new SharedArrayBuffer(16);
  const sabView = new Float64Array(sab);
  sabView.set([1.5, 2.5]);
  const sabRes = prepareTransferable(sabView, true);
  assert.ok(sabRes.view.buffer instanceof ArrayBuffer);
  assert.deepEqual([...sabRes.view], [1.5, 2.5]);
  assert.equal(sabRes.transfer.length, 1);

  // Foreign WebAssembly.Memory (BOTH full-buffer view AND subview!) -> copied, WASM memory untouched!
  const wasmMem = new WebAssembly.Memory({ initial: 1 });
  const fullWasmView = new Uint8Array(wasmMem.buffer);
  fullWasmView[0] = 77;
  fullWasmView[1] = 88;
  const wasmFullRes = prepareTransferable(fullWasmView, true);
  assert.equal(wasmMem.buffer.detached, false);
  assert.equal(fullWasmView[0], 77);
  assert.equal(wasmFullRes.view[0], 77);
  assert.equal(wasmFullRes.view[1], 88);
  assert.equal(wasmFullRes.transfer.length, 1);
  assert.notEqual(wasmFullRes.transfer[0], wasmMem.buffer);

  const wasmSubView = new Float64Array(wasmMem.buffer, 16, 2);
  wasmSubView.set([3.25, 6.5]);
  const wasmSubRes = prepareTransferable(wasmSubView, true);
  assert.equal(wasmMem.buffer.detached, false);
  assert.deepEqual([...wasmSubRes.view], [3.25, 6.5]);

  // Deduplication of shared ArrayBuffer across two full-buffer views
  const sharedAb = new ArrayBuffer(16);
  const v1 = new Uint32Array(sharedAb);
  const v2 = new Float64Array(sharedAb);
  v1[0] = 1234;
  const dedupRes = prepareTransferableViews([v1, v2], true);
  assert.equal(dedupRes.transfer.length, 1, 'shared ArrayBuffer must be deduplicated');
  assert.equal(dedupRes.views[0].buffer, dedupRes.views[1].buffer);
  assert.equal(dedupRes.views[0][0], 1234);

  // Already-detached buffer throws TypeError loudly (both transfer: false and transfer: true)
  const detached = new Float64Array([1, 2]);
  detached.buffer.transfer();
  assert.throws(() => prepareTransferable(detached, false), TypeError);
  assert.throws(() => prepareTransferable(detached, true), TypeError);

  // Zero-length view on transfer: true is never placed in transfer list and never detached (B4)
  const emptyView = new Uint8Array(0);
  const emptyRes = prepareTransferable(emptyView, true);
  assert.equal(emptyRes.transfer.length, 0);
  assert.equal(emptyView.buffer.detached, false);
  assert.equal(emptyRes.view.byteLength, 0);

  // Subview on transfer: false compact-copies only the slice instead of cloning the full parent buffer
  const bigBuf = new ArrayBuffer(1024 * 1024);
  const bigView = new Float64Array(bigBuf, 64, 2);
  bigView.set([11.5, 22.5]);
  const subNoTransfer = prepareTransferable(bigView, false);
  assert.equal(subNoTransfer.transfer.length, 0);
  assert.equal(subNoTransfer.view.buffer.byteLength, 16);
  assert.equal(subNoTransfer.view.byteOffset, 0);
  assert.deepEqual([...subNoTransfer.view], [11.5, 22.5]);
  assert.equal(bigBuf.detached, false);
});

// 6b. B2 regression: sliced apache-arrow tables (single & multi-chunk) and Flechette tables
await test('B2: formualizer/arrow handles sliced apache-arrow tables (single & multi-chunk) and Flechette tables', () => {
  // Exact apache-arrow `table.slice(2, 8)` layout:
  // `values` & `valueOffsets` are ALREADY sliced; `offset = 2` applies ONLY to bitmaps (nullBitmap & Bool values).
  const fullUtf8 = makeInt32OffsetsUtf8(['s0', 's1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9']);
  const slicedUtf8Offsets = fullUtf8.valueOffsets.subarray(2, 9); // length 7 = 6 + 1, starts at non-zero byte offset 4!
  const dictUtf8 = makeInt32OffsetsUtf8(['a', 'b', 'c']);
  const slicedTable = {
    numRows: 6,
    schema: {
      fields: [
        { name: 'f64', type: { typeId: 3, precision: 2 } },
        { name: 'i32', type: { typeId: 2, bitWidth: 32, isSigned: true } },
        { name: 'i64', type: { typeId: 2, bitWidth: 64, isSigned: true } },
        { name: 'dict', type: { typeId: -1, valueType: { typeId: 5 } } },
        { name: 'utf8', type: { typeId: 5 } },
        { name: 'bool', type: { typeId: 6 } },
      ],
    },
    getChildAt: (i) =>
      [
        // Float64 sliced(2, 8): [2.5, null, 4.5, 5.5, 6.5, 7.5] (bit 3 in full nullBitmap is 0 -> row 1 of slice!)
        {
          data: [
            {
              type: { typeId: 3, precision: 2 },
              length: 6,
              offset: 2,
              nullCount: 1,
              values: new Float64Array([2.5, 0, 4.5, 5.5, 6.5, 7.5]),
              nullBitmap: makeBitmap([true, true, true, false, true, true, true, true, true, true]),
            },
          ],
        },
        // Int32 sliced(2, 8): [20, 30, 40, 50, 60, 70]
        {
          data: [
            {
              type: { typeId: 2, bitWidth: 32, isSigned: true },
              length: 6,
              offset: 2,
              nullCount: 0,
              values: new Int32Array([20, 30, 40, 50, 60, 70]),
              nullBitmap: new Uint8Array(0),
            },
          ],
        },
        // Int64 sliced(2, 8): [2000n..7000n]
        {
          data: [
            {
              type: { typeId: 2, bitWidth: 64, isSigned: true },
              length: 6,
              offset: 2,
              nullCount: 0,
              values: new BigInt64Array([2000n, 3000n, 4000n, 5000n, 6000n, 7000n]),
              nullBitmap: new Uint8Array(0),
            },
          ],
        },
        // Dictionary<Utf8> sliced(2, 8): keys [2, 0, 1, 2, 0, 1] -> ['c', 'a', 'b', 'c', 'a', 'b']
        {
          data: [
            {
              type: { typeId: -1, valueType: { typeId: 5 } },
              length: 6,
              offset: 2,
              nullCount: 0,
              values: new Int32Array([2, 0, 1, 2, 0, 1]),
              nullBitmap: new Uint8Array(0),
              dictionary: {
                data: [
                  {
                    type: { typeId: 5 },
                    length: 3,
                    offset: 0,
                    nullCount: 0,
                    values: dictUtf8.bytes,
                    valueOffsets: dictUtf8.valueOffsets,
                  },
                ],
              },
            },
          ],
        },
        // Utf8 sliced(2, 8): ['s2', 's3', 's4', 's5', 's6', 's7']
        {
          data: [
            {
              type: { typeId: 5 },
              length: 6,
              offset: 2,
              nullCount: 0,
              values: fullUtf8.bytes,
              valueOffsets: slicedUtf8Offsets,
              nullBitmap: new Uint8Array(0),
            },
          ],
        },
        // Bool sliced(2, 8): bit-packed full buffer [F, F, T, F, null(T), T, F, T, F, F] -> [T, F, null, T, F, T]
        {
          data: [
            {
              type: { typeId: 6 },
              length: 6,
              offset: 2,
              nullCount: 1,
              values: makeBitmap([false, false, true, false, true, true, false, true, false, false]),
              nullBitmap: makeBitmap([true, true, true, true, false, true, true, true, true, true]),
            },
          ],
        },
      ][i],
  };

  const wb = new Workbook();
  wb.addSheet('Sliced');
  const s = wb.sheet('Sliced');
  s.setColumns(1, 1, arrowColumns(slicedTable));
  assert.deepEqual(
    [1, 2, 3, 4, 5, 6].map((r) => s.getValue(r, 1)),
    [2.5, null, 4.5, 5.5, 6.5, 7.5],
  );
  assert.deepEqual(
    [1, 2, 3, 4, 5, 6].map((r) => s.getValue(r, 2)),
    [20, 30, 40, 50, 60, 70],
  );
  assert.deepEqual(
    [1, 2, 3, 4, 5, 6].map((r) => s.getValue(r, 3)),
    [2000, 3000, 4000, 5000, 6000, 7000],
  );
  assert.deepEqual(
    [1, 2, 3, 4, 5, 6].map((r) => s.getValue(r, 4)),
    ['c', 'a', 'b', 'c', 'a', 'b'],
  );
  assert.deepEqual(
    [1, 2, 3, 4, 5, 6].map((r) => s.getValue(r, 5)),
    ['s2', 's3', 's4', 's5', 's6', 's7'],
  );
  assert.deepEqual(
    [1, 2, 3, 4, 5, 6].map((r) => s.getValue(r, 6)),
    [true, false, null, true, false, true],
  );

  // Multi-chunk sliced table across chunk boundary
  const c1Utf8 = makeInt32OffsetsUtf8(['a0', 'a1', 'a2', 'a3']);
  const c2Utf8 = makeInt32OffsetsUtf8(['b0', 'b1', 'b2', 'b3']);
  const multiSliced = {
    numRows: 4,
    schema: {
      fields: [
        { name: 'f', type: { typeId: 3, precision: 2 } },
        { name: 's', type: { typeId: 5 } },
      ],
    },
    getChildAt: (i) =>
      [
        {
          data: [
            { type: { typeId: 3, precision: 2 }, length: 2, offset: 2, nullCount: 0, values: new Float64Array([2.5, 3.5]) },
            { type: { typeId: 3, precision: 2 }, length: 2, offset: 0, nullCount: 0, values: new Float64Array([4.5, 5.5]) },
          ],
        },
        {
          data: [
            { type: { typeId: 5 }, length: 2, offset: 2, nullCount: 0, values: c1Utf8.bytes, valueOffsets: c1Utf8.valueOffsets.subarray(2, 5) },
            { type: { typeId: 5 }, length: 2, offset: 0, nullCount: 0, values: c2Utf8.bytes, valueOffsets: c2Utf8.valueOffsets.subarray(0, 3) },
          ],
        },
      ][i],
  };
  s.setColumns(10, 1, arrowColumns(multiSliced));
  assert.deepEqual([10, 11, 12, 13].map((r) => s.getValue(r, 1)), [2.5, 3.5, 4.5, 5.5]);
  assert.deepEqual([10, 11, 12, 13].map((r) => s.getValue(r, 2)), ['a2', 'a3', 'b0', 'b1']);

  // Exact Flechette layout (`validity`, `offsets`, `signed`, `dictionary` type field)
  const flechStr = makeInt32OffsetsUtf8(['alpha', '', 'gamma', 'delta']);
  // Flechette can over-allocate offsets with trailing zeros:
  const flechOffsets = new Int32Array([...flechStr.valueOffsets, 0]);
  const flechDict = makeInt32OffsetsUtf8(['x', 'y']);
  const flechetteTable = {
    numRows: 4,
    schema: {
      fields: [
        { name: 'fn', type: { typeId: 3, precision: 2 } },
        { name: 'str', type: { typeId: 5 } },
        { name: 'cat', type: { typeId: -1, dictionary: { typeId: 5 }, indices: { typeId: 2, bitWidth: 32, signed: true } } },
        { name: 'u32', type: { typeId: 2, bitWidth: 32, signed: false } },
      ],
    },
    getChildAt: (i) =>
      [
        {
          data: [
            {
              type: { typeId: 3, precision: 2 },
              length: 4,
              nullCount: 1,
              values: new Float64Array([1.5, 0, 3.5, 4.5]),
              validity: makeBitmap([true, false, true, true]),
            },
          ],
        },
        {
          data: [
            {
              type: { typeId: 5 },
              length: 4,
              nullCount: 1,
              values: flechStr.bytes,
              offsets: flechOffsets,
              validity: makeBitmap([true, false, true, true]),
            },
          ],
        },
        {
          data: [
            {
              type: { typeId: -1, dictionary: { typeId: 5 } },
              length: 4,
              nullCount: 1,
              values: new Int32Array([0, 1, 0, 0]),
              validity: makeBitmap([true, true, false, true]),
              dictionary: {
                type: { typeId: 5 },
                length: 2,
                data: [
                  {
                    type: { typeId: 5 },
                    length: 2,
                    nullCount: 0,
                    values: flechDict.bytes,
                    offsets: flechDict.valueOffsets,
                    validity: new Uint8Array(0),
                  },
                ],
              },
            },
          ],
        },
        {
          data: [
            {
              type: { typeId: 2, bitWidth: 32, signed: false },
              length: 4,
              nullCount: 0,
              values: new Uint32Array([10, 20, 30, 40]),
              validity: new Uint8Array(0),
            },
          ],
        },
      ][i],
  };

  wb.addSheet('Flechette');
  const fsSheet = wb.sheet('Flechette');
  fsSheet.setColumns(1, 1, arrowColumns(flechetteTable), { headers: true });
  assert.deepEqual([2, 3, 4, 5].map((r) => fsSheet.getValue(r, 1)), [1.5, null, 3.5, 4.5]);
  assert.deepEqual([2, 3, 4, 5].map((r) => fsSheet.getValue(r, 2)), ['alpha', null, 'gamma', 'delta']);
  assert.deepEqual([2, 3, 4, 5].map((r) => fsSheet.getValue(r, 3)), ['x', 'y', null, 'x']);
  assert.deepEqual([2, 3, 4, 5].map((r) => fsSheet.getValue(r, 4)), [10, 20, 30, 40]);

  // Missing both valueOffsets and offsets on Utf8 throws a clear TypeError naming the column
  const badUtf8 = {
    schema: { fields: [{ name: 'broken_str', type: { typeId: 5 } }] },
    getChildAt: () => ({ data: [{ type: { typeId: 5 }, length: 1, values: new Uint8Array(4) }] }),
  };
  assert.throws(() => arrowColumns(badUtf8), /broken_str.*valueOffsets|offsets/);

  fsSheet.free();
  s.free();
  wb.free();
});

// 6c. B3 regression: 1904-date-system JS Date conversion matches setValue across all bulk methods
await test('B3: 1904-date-system workbook preserves JS Date across setValue, setValues, setCells, and setColumns (direct & worker)', async () => {
  const json1904 = JSON.stringify({ sheets: { S: { cells: [], date_system_1904: true } } });
  const wb = Workbook.fromJson(json1904);
  const s = wb.sheet('S');
  const dt = new Date(Date.UTC(2024, 0, 15, 12, 0, 0));
  const midnight = new Date(Date.UTC(2024, 0, 15, 0, 0, 0));

  s.setValue(1, 1, dt);
  s.setValue(1, 2, midnight);
  s.setValues(2, 1, [[dt, midnight]]);
  s.setCells([[3, 1, dt], [3, 2, midnight]]);
  s.setColumns(4, 1, [
    { kind: 'mixed', values: [dt] },
    { kind: 'mixed', values: [midnight] },
  ]);

  for (const row of [1, 2, 3, 4]) {
    const vDt = s.getValue(row, 1);
    const vMid = s.getValue(row, 2);
    assert.ok(vDt instanceof Date, `row ${row} col 1 must be Date`);
    assert.equal(vDt.toISOString(), '2024-01-15T12:00:00.000Z', `row ${row} col 1 must not shift by 1462 days`);
    assert.ok(vMid instanceof Date, `row ${row} col 2 must be Date`);
    assert.equal(vMid.toISOString(), '2024-01-15T00:00:00.000Z', `row ${row} col 2 must not shift by 1462 days`);
  }

  // Underlying stored 1904 serials match setValue exactly (43844.5 and 43844, not 1900's 45306.5 / 45306)
  assert.equal(wb.inspectCell({ sheet: 'S', row: 2, column: 1 }).cell.value, wb.inspectCell({ sheet: 'S', row: 1, column: 1 }).cell.value);
  assert.equal(wb.inspectCell({ sheet: 'S', row: 2, column: 2 }).cell.value, 43844);
  assert.equal(wb.inspectCell({ sheet: 'S', row: 4, column: 2 }).cell.value, 43844);

  s.free();
  wb.free();

  // Worker path in a 1904 workbook
  const client = await createInProcessWorkbookWorker({ runtime, source: { kind: 'json', json: json1904 } });
  try {
    await client.setValue('S', 1, 1, dt);
    await client.setValues('S', 2, 1, [[dt, midnight]]);
    await client.setCells('S', [[3, 1, dt], [3, 2, midnight]]);
    await client.setColumns('S', 4, 1, [
      { kind: 'mixed', values: [dt] },
      { kind: 'mixed', values: [midnight] },
    ]);
    for (const row of [1, 2, 3, 4]) {
      assert.equal((await client.getValue('S', row, 1)).toISOString(), '2024-01-15T12:00:00.000Z');
    }
    for (const row of [2, 3, 4]) {
      assert.equal((await client.getValue('S', row, 2)).toISOString(), '2024-01-15T00:00:00.000Z');
    }
  } finally {
    await client.dispose();
  }
});

// 6d. B5 regression: setColumns(..., { headers: true }) is strictly atomic (direct & worker) and undoes as one action
await test('B5: setColumns with headers: true is strictly atomic on bad data (direct & worker) and records one undo step', async () => {
  const wb = new Workbook({ enableChangelog: true });
  wb.addSheet('Atomic');
  const s = wb.sheet('Atomic');

  // Fable's exact failing repro: bad nested array in mixed column must NOT commit headers!
  assert.throws(
    () =>
      s.setColumns(
        1,
        1,
        [
          { kind: 'number', values: new Float64Array([1, 2]), name: 'a' },
          { kind: 'mixed', values: [1, [2]], name: 'b' },
        ],
        { headers: true },
      ),
    TypeError,
  );
  assert.equal(s.getValue(1, 1), null, 'header cell A1 must remain empty on failed setColumns');
  assert.equal(s.getValue(1, 2), null, 'header cell B1 must remain empty on failed setColumns');
  assert.equal(s.getValue(2, 1), null, 'data cell A2 must remain empty on failed setColumns');

  // Valid call with headers: true records a single undo action (one undo removes both headers and data)
  s.setColumns(
    1,
    1,
    [
      { kind: 'number', values: new Float64Array([10, 20]), name: 'a' },
      { kind: 'mixed', values: [1, 'ok'], name: 'b' },
    ],
    { headers: true },
  );
  assert.equal(s.getValue(1, 1), 'a');
  assert.equal(s.getValue(2, 1), 10);
  assert.equal(s.getValue(3, 2), 'ok');
  assert.equal(wb.canUndo(), true);
  wb.undo();
  assert.equal(s.getValue(1, 1), null, 'single undo must clear header row too');
  assert.equal(s.getValue(2, 1), null, 'single undo must clear data rows too');

  s.free();
  wb.free();

  // Worker repro: rejection must leave header empty, zero revision drift, and zero mutation frames
  const client = await createInProcessWorkbookWorker({ runtime });
  try {
    await client.addSheet('AtomicW');
    const frames = [];
    const sub = await client.subscribeViewport({
      rect: { sheet: 'AtomicW', startRow: 1, startColumn: 1, endRow: 5, endColumn: 5 },
      mode: 'deltas',
      throttleMs: 0,
      onFrame: (f) => frames.push(f),
    });
    await client.sheetNames(); // flush initial frame
    const revBefore = client.state.revision;
    const mutFramesBefore = frames.filter((f) => f.reason === 'mutation').length;

    await assert.rejects(
      client.setColumns(
        'AtomicW',
        1,
        1,
        [
          { kind: 'number', values: new Float64Array([1, 2]), name: 'a' },
          { kind: 'mixed', values: [1, [2]], name: 'b' },
        ],
        { headers: true },
      ),
      (err) => err?.name === 'TypeError' || /nested arrays/.test(String(err?.message)),
    );
    await client.sheetNames();
    assert.equal(await client.getValue('AtomicW', 1, 1), null, 'worker header cell must remain empty');
    assert.equal(await client.getValue('AtomicW', 1, 2), null);
    assert.equal(client.state.revision, revBefore, 'worker revision must not drift on rejected setColumns');
    assert.equal(
      frames.filter((f) => f.reason === 'mutation').length,
      mutFramesBefore,
      'rejected setColumns must not emit mutation frames',
    );
    await sub.unsubscribe();
  } finally {
    await client.dispose();
  }
});

await test('detached typed columns reject without the detached getter, including header-only writes', () => {
  const wb = new Workbook();
  wb.addSheet('S');
  const sheet = wb.sheet('S');
  const values = new Float64Array([1, 2]);
  structuredClone(values, { transfer: [values.buffer] });
  const descriptor = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'detached');
  try {
    delete ArrayBuffer.prototype.detached;
    assert.throws(() => sheet.setColumns(1, 1, [{ kind: 'number', values, name: 'lost' }], { headers: true }), /detached/i);
    assert.equal(sheet.getValue(1, 1), null);
  } finally {
    if (descriptor) Object.defineProperty(ArrayBuffer.prototype, 'detached', descriptor);
    sheet.free();
    wb.free();
  }
});

await test('out-of-range Dates match setValue in all bulk representations', () => {
  const wb = new Workbook();
  wb.addSheet('S');
  const s = wb.sheet('S');
  for (const ms of [-8.64e15, 8.64e15]) {
    const d = new Date(ms);
    s.setValue(1, 1, d);
    s.setValues(1, 2, [[d]]);
    s.setColumns(1, 3, [{ kind: 'mixed', values: [d] }]);
    s.setCells([[1, 4, d]]);
    for (let col = 1; col <= 4; col++) assert.equal(s.getValue(1, col), '#NUM!');
  }
  s.free();
  wb.free();
});

await test('invalid input beyond the first packed chunk leaves the sheet untouched', () => {
  const wb = new Workbook();
  wb.addSheet('S');
  const s = wb.sheet('S');
  const wide = Array(16384).fill(1);
  const rows = [[42], ...Array(121).fill([]), wide];
  wide[wide.length - 1] = { kind: 'error', code: 'constructor' };
  assert.throws(() => s.setValues(1, 1, rows), TypeError);
  assert.equal(s.getValue(1, 1), null);

  assert.throws(() => s.setColumns(1, 1, [{ kind: 'boolean', name: 'flag', values: [true, 'false'] }], { headers: true }), /expected boolean/);
  assert.equal(s.getValue(1, 1), null);

  const data = Array(1001).fill('ok');
  const columns = Array(2000).fill({ kind: 'text', values: data, name: 'label' });
  const offsets = Uint32Array.from({ length: 1002 }, (_, i) => i);
  const bytes = new Uint8Array(1001).fill(65);
  bytes[1000] = 0xff;
  columns[0] = { kind: 'text', values: { bytes, offsets }, name: 'packed' };
  assert.throws(() => s.setColumns(1, 1, columns, { headers: true }), TypeError);
  assert.equal(s.getValue(1, 1), null);

  const n = 2_000_001;
  const entries = Array(n).fill([1, 1, 42]);
  entries[n - 1] = { row: 0, col: 1, value: 50 };
  assert.throws(() => s.setCells(entries), RangeError);
  assert.equal(s.getValue(1, 1), null);
  const block = { rows: new Uint32Array(n).fill(1), cols: new Uint32Array(n).fill(1),
    tags: new Uint8Array(n).fill(CellTag.Number), numbers: new Float64Array(n).fill(42) };
  block.tags[n - 1] = CellTag.Text;
  assert.throws(() => s.setCells(block), /payload/);
  assert.equal(s.getValue(1, 1), null);
  s.free();
  wb.free();
});

await test('worker publishes a committed chunk when a later engine call fails, including in editBatch', async () => {
  const client = await createInProcessWorkbookWorker({ runtime });
  const proto = Sheet.prototype;
  const write = proto._setPackedBlock;
  try {
    await client.addSheet('S');
    const frames = [];
    const sub = await client.subscribeViewport({ rect: { sheet: 'S', startRow: 1, startColumn: 1, endRow: 1, endColumn: 1 },
      mode: 'values', throttleMs: 0, onFrame: frame => frames.push(frame) });
    // Inject a refusal at the WASM boundary after the real first chunk commits.
    proto._setPackedBlock = function(mode, row, ...rest) {
      if (row > 1) throw new Error('engine refused second chunk');
      return write.call(this, mode, row, ...rest);
    };
    for (const value of [42, 43]) {
      const values = [[value], ...Array(121).fill([]), Array(16384).fill(1)];
      const revision = client.state.revision;
      const op = { op: 'setValues', sheet: 'S', startRow: 1, startCol: 1, values };
      await assert.rejects(value === 42 ? client.setValues('S', 1, 1, values) : client.editBatch([op]), /second chunk/);
      assert.equal(await client.getValue('S', 1, 1), value);
      assert.equal(await client.getValue('S', 123, 1), null);
      assert.equal(client.state.revision, revision + 1);
      assert.ok(frames.some(f => f.reason === 'mutation' && f.revision === revision + 1));
    }
    await sub.unsubscribe();
  } finally {
    proto._setPackedBlock = write;
    await client.dispose();
  }
});

// 7. Worker client end-to-end: setValues, setColumns, setCells, transfer: false/true, viewport changedRects
await test('worker client exposes setValues, setColumns, setCells with safe transferables and targeted viewport rects', async () => {
  for (const mode of ['channel', 'thread']) {
    const client =
      mode === 'channel'
        ? await createInProcessWorkbookWorker({ runtime })
        : await createWorkbookWorker(
            new Worker(new URL('./worker-test-host.mjs', import.meta.url)),
            { terminateOnDispose: true },
          );
    try {
      await client.addSheet('W');

      const frames = [];
      const sub = await client.subscribeViewport({
        rect: { sheet: 'W', startRow: 1, startColumn: 1, endRow: 20, endColumn: 5 },
        mode: 'deltas',
        throttleMs: 0,
        onFrame: (f) => frames.push(f),
      });

      // setValues returns WriteRangeReport + WorkerStateHeader
      const rVals = await client.setValues('W', 1, 1, [
        [1, 2],
        [3],
      ]);
      assert.equal(rVals.startRow, 1);
      assert.equal(rVals.endRow, 2);
      assert.equal(rVals.endCol, 2);
      assert.equal(rVals.historyCleared, false);
      assert.equal(await client.getValue('W', 2, 1), 3);

      // setColumns default transfer: false keeps caller's TypedArray intact
      const reusable = new Float64Array([10, 20, 30]);
      const rColsDefault = await client.setColumns('W', 3, 1, [
        reusable,
        { kind: 'text', values: ['a', 'b', 'c'] },
      ]);
      assert.equal(reusable.buffer.detached, false, 'default transfer: false must NOT detach caller buffer');
      assert.equal(rColsDefault.startRow, 3);
      assert.equal(rColsDefault.endRow, 5);
      assert.equal(rColsDefault.endCol, 2);
      assert.equal(await client.getValue('W', 5, 2), 'c');

      // setColumns with transfer: true detaches owned buffer; reusing it throws TypeError loudly!
      const owned = new Float64Array([100, 200]);
      const wasmMem = new WebAssembly.Memory({ initial: 1 });
      const wasmNums = new Float64Array(wasmMem.buffer, 0, 2);
      wasmNums.set([7, 8]);
      await client.setColumns('W', 6, 1, [owned, wasmNums, { kind: 'time', values: new Float64Array([0.5, 0.25]) }], { transfer: true });
      assert.equal(await client.getValue('W', 6, 3), '12:00:00');
      assert.equal(owned.buffer.detached, true, 'transfer: true detaches owned full-buffer view');
      assert.equal(wasmMem.buffer.detached, false, 'transfer: true must copy foreign WebAssembly.Memory view');
      assert.equal(await client.getValue('W', 7, 2), 8);
      await assert.rejects(client.setColumns('W', 6, 1, [owned]), TypeError);

      // setCells with typed columnar block (#627 kinds + texts) and transfer: true
      const spRows = new Uint32Array([10, 11]);
      const spCols = new Uint32Array([1, 2]);
      const spKinds = new Uint8Array([CellTag.Number, CellTag.Text]);
      const spNums = new Float64Array([42.5, 0]);
      const rCells = await client.setCells(
        'W',
        { rows: spRows, cols: spCols, kinds: spKinds, numbers: spNums, texts: ['worker-ok'] },
        { transfer: true },
      );
      assert.equal(rCells.startRow, 10);
      assert.equal(rCells.endRow, 11);
      assert.equal(rCells.endCol, 2);
      assert.equal(await client.getValue('W', 10, 1), 42.5);
      assert.equal(await client.getValue('W', 11, 2), 'worker-ok');

      // B4 regression: repeated transfer: true setCells without text + setColumns with empty text must never detach shared empty buffers
      const mkNoTextBlk = (r) => ({
        rows: new Uint32Array([r]),
        cols: new Uint32Array([4]),
        tags: new Uint8Array([CellTag.Number]),
        numbers: new Float64Array([r * 10]),
      });
      await client.setCells('W', mkNoTextBlk(1), { transfer: true });
      await client.setCells('W', mkNoTextBlk(2), { transfer: true });
      await client.setCells('W', mkNoTextBlk(3), { transfer: true });
      await client.setCells('W', mkNoTextBlk(4)); // transfer: false still works
      await client.setColumns('W', 20, 1, [{ kind: 'text', values: [] }], { transfer: true });
      assert.equal(await client.getValue('W', 1, 4), 10);
      assert.equal(await client.getValue('W', 3, 4), 30);
      assert.equal(await client.getValue('W', 4, 4), 40);

      // editBatch supports setColumns and setCells
      await client.editBatch([
        { op: 'setColumns', sheet: 'W', startRow: 12, startCol: 1, columns: [new Float64Array([9])] },
        { op: 'setCells', sheet: 'W', cells: [[12, 2, 'batched']] },
      ]);
      assert.equal(await client.getValue('W', 12, 1), 9);
      assert.equal(await client.getValue('W', 12, 2), 'batched');

      await client.sheetNames(); // flush viewport frames
      assert.ok(
        frames.some((f) => f.changedRects.some((r) => r.startRow === 10 && r.endColumn === 2)),
        'viewport must receive targeted changedRects with startColumn/endColumn',
      );
      await sub.unsubscribe();
    } finally {
      await client.dispose();
    }
  }
});

// 8. TypeScript declaration conformance for index, arrow, and worker
await test('TypeScript declaration conformance passes for formualizer, formualizer/arrow, and formualizer/worker', () => {
  const dir = new URL('../pkg-node/bulk-types/', import.meta.url);
  fs.mkdirSync(dir, { recursive: true });
  const file = new URL('consumer.ts', dir);
  fs.writeFileSync(
    file,
    `
import {
  Workbook,
  CellTag,
  ExcelErrorCode,
  type WorkbookApi,
  type WriteRangeReport,
  type DenseColumnInput,
  type SparseCellBlockInput,
} from '../../dist/index.js';
import { arrowColumns, type ArrowTableLike } from '../../dist/arrow.js';
import {
  prepareTransferable,
  prepareTransferableViews,
  type WorkbookWorkerClient,
} from '../../dist/worker.js';

const wb: WorkbookApi = new Workbook();
const sheet = wb.sheet('S');
const r1: WriteRangeReport = sheet.setValues(1, 1, [[1, 'a', true, new Date(), { type: 'Error', value: '#N/A' }]]);
const cols: DenseColumnInput[] = [new Float64Array([1, 2]), { kind: 'text', values: ['a', 'b'], name: 't' }];
const r2: WriteRangeReport = sheet.setColumns(1, 1, cols, { headers: false });
const block: SparseCellBlockInput = {
  rows: new Uint32Array([1]),
  cols: new Uint32Array([1]),
  kinds: new Uint8Array([CellTag.Error]),
  numbers: new Float64Array([ExcelErrorCode.Num]),
  texts: ['x'],
};
const r3: WriteRangeReport = sheet.setCells(block);
const r4: WriteRangeReport = wb.setCells('S', [[1, 1, 42]]);

declare const table: ArrowTableLike;
const ac: DenseColumnInput[] = arrowColumns(table, { dateSystem: '1900' });
sheet.setColumns(1, 1, ac, { headers: true });

declare const client: WorkbookWorkerClient;
const p1: Promise<WriteRangeReport> = client.setColumns('S', 1, 1, cols, { headers: false, transfer: false });
const p2: Promise<WriteRangeReport> = client.setCells('S', block, { transfer: true });
const t1 = prepareTransferable(new Float64Array([1]), true);
const t2 = prepareTransferableViews([new Uint32Array([1]), new Float64Array([2])] as const, true);
void [r1, r2, r3, r4, p1, p2, t1, t2];
`,
  );
  execFileSync(
    new URL('../node_modules/.bin/tsc', import.meta.url).pathname,
    [
      '--strict',
      '--noEmit',
      '--skipLibCheck',
      '--target',
      'ES2022',
      '--module',
      'ES2022',
      '--moduleResolution',
      'bundler',
      file.pathname,
    ],
    { stdio: 'inherit' },
  );
});

console.log(`All ${passed} bulk ingest conformance checks passed.`);
