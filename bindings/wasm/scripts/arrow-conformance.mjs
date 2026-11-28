import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import * as arrow from 'apache-arrow';
import * as f from '@uwdata/flechette';
import { Workbook } from 'formualizer';
import { arrowColumns } from 'formualizer/arrow';

const read = (file) => readFileSync(new URL(file, import.meta.url), 'utf8');
const pkg = JSON.parse(read('../package.json'));
assert.deepEqual(Object.keys(pkg.dependencies ?? {}), []);
assert.deepEqual(pkg.exports['./arrow'], { types: './dist/arrow.d.ts', import: './dist/arrow.js' });
assert.deepEqual(pkg.files, ['dist', 'pkg', 'README.md', 'LICENSE-MIT', 'LICENSE-APACHE']);
const imports = (file) => Array.from(read(file).matchAll(/(?:from\s+|import\s*\(?\s*)['"]([^'"]+)['"]/g), m => m[1]);
assert.deepEqual(imports('../dist/arrow.js'), ['./bulk.js']);
assert.deepEqual(imports('../dist/bulk.js'), []);

// Each column has non-repeating neighbors and nulls on both sides of the batch boundary.
const columns = [];
function add(name, apacheType, flechetteType, expected, apacheInput = expected, flechetteInput = apacheInput) {
  columns.push({ name, apacheType, flechetteType, expected, apacheInput, flechetteInput });
}
add('empty', new arrow.Null(), f.nullType(), () => null);
for (const width of [8, 16, 32, 64]) {
  for (const unsigned of [false, true]) {
    const name = (unsigned ? 'Uint' : 'Int') + width;
    const value = i => unsigned ? i + 10 : -i - 10;
    add(name, new arrow[name](), f[name.toLowerCase()](), value, width === 64 ? i => BigInt(value(i)) : value);
  }
}
for (const width of [16, 32, 64]) {
  add('float' + width, new arrow['Float' + width](), f['float' + width](), i => i + 0.25);
}
add('flag', new arrow.Bool(), f.bool(), i => i % 2 === 0);
add('text', new arrow.Utf8(), f.utf8(), i => '猫' + i);
add('largeText', new arrow.LargeUtf8(), f.largeUtf8(), i => 'λ' + i);
const midnight = i => Date.UTC(2024, 1, i + 1);
for (const [name, type, ft] of [['day', new arrow.DateDay(), f.dateDay()], ['dateMs', new arrow.DateMillisecond(), f.dateMillisecond()]]) {
  add(name, type, ft, i => new Date(midnight(i)), midnight);
}
for (const [unit, unitName] of ['Second', 'Millisecond', 'Microsecond', 'Nanosecond'].entries()) {
  const unitsPerSecond = 1000 ** unit;
  add('clock' + unitName, new arrow['Time' + unitName](), f.time(unit),
    i => '01:00:' + String(i).padStart(2, '0'),
    i => unit < 2 ? (3600 + i) * unitsPerSecond : BigInt((3600 + i) * unitsPerSecond));
  add('elapsed' + unitName, new arrow['Duration' + unitName](), f.duration(unit),
    i => `TimeDelta { secs: ${86400 + i}, nanos: 0 }`, i => BigInt((86400 + i) * unitsPerSecond));
  add('timestamp' + unitName, new arrow['Timestamp' + unitName]('+02:00'), f.timestamp(unit, '+02:00'),
    i => new Date(midnight(i) + 7200000), midnight);
}
function decimalWords(scaled) {
  const bits = BigInt.asUintN(128, scaled);
  return Uint32Array.from([0n, 32n, 64n, 96n], shift => Number((bits >> shift) & 0xffffffffn));
}
const scaled = i => -900719925474099300n + BigInt(i * 100);
add('decimal', new arrow.Decimal(2, 24), f.decimal128(24, 2),
  i => Number(`${scaled(i)}e-2`), i => decimalWords(scaled(i)), scaled);
add('labels', new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32(), 1), f.dictionary(f.utf8()), i => 'é' + (i % 3));
add('codes', new arrow.Dictionary(new arrow.Int32(), new arrow.Int32(), 2), f.dictionary(f.int32()), i => 40 + i % 4);

const rows = (getValue) => Array.from({ length: 10 }, (_, i) => i === 2 || i === 6 ? null : getValue(i));
function check(table, start, end, label) {
  const wb = new Workbook();
  wb.addSheet('S');
  const sheet = wb.sheet('S');
  try {
    sheet.setColumns(1, 1, arrowColumns(table), { headers: true });
    for (const [c, col] of columns.entries()) {
      assert.equal(sheet.getValue(1, c + 1), col.name);
      const expected = rows(col.expected).slice(start, end);
      for (const [r, value] of expected.entries()) {
        assert.deepEqual(sheet.getValue(r + 2, c + 1), value, `${label} ${col.name} row ${start + r}`);
      }
    }
    console.log('ok - ' + label);
  } finally {
    sheet.free();
    wb.free();
  }
}

const apache = new arrow.Table(Object.fromEntries(columns.map(c => [
  c.name, arrow.vectorFromArray(rows(c.apacheInput), c.apacheType),
])));
const multi = apache.slice(0, 5).concat(apache.slice(5));
for (const [label, table, start, end] of [
  ['single', apache, 0, 10], ['sliced', apache.slice(1, 9), 1, 9],
  ['multi', multi, 0, 10], ['sliced across batches', multi.slice(3, 8), 3, 8],
]) {
  check(table, start, end, 'Apache Arrow ' + label);
}

// Flechette has no row slice method. Decode an actual sliced IPC stream, in
// addition to testing Flechette's native single- and multi-batch builders.
for (const maxBatchRows of [10, 5]) {
  const table = f.tableFromArrays(Object.fromEntries(columns.map(c => [c.name, rows(c.flechetteInput)])), {
    types: Object.fromEntries(columns.map(c => [c.name, c.flechetteType])), maxBatchRows,
  });
  check(table, 0, 10, 'Flechette batch size ' + maxBatchRows);
}
check(f.tableFromIPC(arrow.tableToIPC(multi.slice(3, 8))), 3, 8, 'Flechette sliced IPC batches');

// Some structural producers retain only the Arrow type's string representation.
const stamp = apache.getChild('timestampMillisecond');
function timestampColumn(timezone) {
  return arrowColumns({ schema: { fields: [{ name: 'stamp', type: new arrow.TimestampMillisecond(timezone).toString() }] },
    getChildAt: () => stamp });
}
const wb = new Workbook();
wb.addSheet('T');
const sheet = wb.sheet('T');
sheet.setColumns(1, 1, timestampColumn('+05:30'));
assert.deepEqual(sheet.getValue(1, 1), new Date(midnight(0) + 5.5 * 3600000));
assert.throws(() => timestampColumn('America/New_York'), /stamp.*America\/New_York/);
sheet.free();
wb.free();

// Keys in each record batch can refer to a different dictionary, and each
// dictionary itself may comprise multiple value chunks with nulls.
for (const [type, first, second] of [
  [new arrow.Int32(), [1, null, 3], [5, 6, 7]],
  [new arrow.Utf8(), ['a', null, 'c'], ['猫', 'λ', 'g']],
]) {
  const dictType = new arrow.Dictionary(type, new arrow.Int32());
  const dictBatch = values => arrow.makeData({
    type: dictType, length: 3, data: new Int32Array([2, 1, 0]),
    dictionary: arrow.vectorFromArray(values.slice(0, 1), type).concat(arrow.vectorFromArray(values.slice(1), type)),
  });
  const table = new arrow.Table({ dict: new arrow.Vector([dictBatch(first), dictBatch(second)]) });
  const wb = new Workbook();
  wb.addSheet('D');
  const s = wb.sheet('D');
  s.setColumns(1, 1, arrowColumns(table));
  assert.deepEqual(Array.from({ length: 6 }, (_, i) => s.getValue(i + 1, 1)), [...first.reverse(), ...second.reverse()]);
  s.free();
  wb.free();
}
