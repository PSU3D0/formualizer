import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { deflateRawSync } from 'node:zlib';
import ts from 'typescript';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const packageDir = process.env.FORMUALIZER_WASM_NODE_PACKAGE
  ? path.resolve(process.env.FORMUALIZER_WASM_NODE_PACKAGE) : path.join(root, 'pkg-node');
if (!process.env.FORMUALIZER_WASM_NODE_PACKAGE) {
  execFileSync('wasm-pack', ['build', '--target', 'nodejs', '--out-dir', packageDir, '--release'],
    { cwd: root, stdio: 'inherit' });
}
const entry = pathToFileURL(path.join(packageDir, 'formualizer_wasm.js')).href;
const raw = (await import(entry)).Workbook;
let passed = 0;
function test(name, run) {
  run();
  passed++;
  console.log(`ok ${passed} - ${name}`);
}

// Small deterministic ZIPs exercise actual core admission, not mocked forwarding.
function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let i = 0; i < 8; i++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
const required = [
  ['_rels/.rels', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="r" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>'],
  ['xl/workbook.xml', '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="s"/></sheets></workbook>'],
  ['xl/_rels/workbook.xml.rels', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="s" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>'],
  ['xl/worksheets/sheet1.xml', '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1"><v>42</v></c></row></sheetData></worksheet>'],
];
function zip(parts, corruptName) {
  const locals = [], directory = [];
  let offset = 0;
  for (const [name, text] of parts) {
    const bytes = Buffer.from(text), filename = Buffer.from(name), compressed = deflateRawSync(bytes);
    const crc = (crc32(bytes) ^ (name === corruptName ? 1 : 0)) >>> 0;
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50); local.writeUInt16LE(20, 4); local.writeUInt16LE(8, 8);
    local.writeUInt32LE(crc, 14); local.writeUInt32LE(compressed.length, 18);
    local.writeUInt32LE(bytes.length, 22); local.writeUInt16LE(filename.length, 26);
    locals.push(local, filename, compressed);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50); central.writeUInt16LE(20, 4); central.writeUInt16LE(20, 6);
    central.writeUInt16LE(8, 10); central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(compressed.length, 20); central.writeUInt32LE(bytes.length, 24);
    central.writeUInt16LE(filename.length, 28); central.writeUInt32LE(offset, 42);
    directory.push(central, filename);
    offset += local.length + filename.length + compressed.length;
  }
  const central = Buffer.concat(directory), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50); end.writeUInt16LE(parts.length, 8); end.writeUInt16LE(parts.length, 10);
  end.writeUInt32LE(central.length, 12); end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, central, end]);
}
const bytes = zip(required);
const load = (options, data = bytes) => raw.fromXlsxBytesWithOptions(data, options);
const success = (options, data) => {
  const wb = load(options, data);
  assert.equal(wb.sheet('Sheet1').getValue(1, 1), 42);
  wb.free();
};
const fields = ['maxInputBytes', 'maxEntries', 'maxExpandedBytes', 'maxPartBytes', 'maxXmlDepth'];
test('default and partial limits; unrelated top-level compatibility', () => {
  success(undefined); success({ loadLimits: {} });
  success({ loadLimits: { maxEntries: 4 }, futureOption: true });
});
test('all aliases share enforcement and success', () => {
  for (const backend of [undefined, 'native', 'calamine']) {
    success({ backend, loadLimits: { maxInputBytes: bytes.length } });
    assert.throws(() => load({ backend, loadLimits: { maxInputBytes: bytes.length - 1 } }), /input|limit/i);
  }
});
test('zero means zero for every field', () => {
  for (const field of fields) assert.throws(() => load({ loadLimits: { [field]: 0 } }));
});
test('reject invalid numbers and unsafe/host-width integers for every field before admission', () => {
  for (const field of fields) {
    for (const value of [true, false, -1, 1.5, NaN, Infinity, -Infinity, '1', null, undefined,
      1n, Number.MAX_SAFE_INTEGER + 1, Number.MAX_SAFE_INTEGER, 2 ** 31, 2 ** 32]) {
      assert.throws(() => load({ loadLimits: { [field]: value } }, new Uint8Array()), /loadLimits/);
    }
    // Highest wasm32 isize value is representable, not rounded or saturated.
    success({ loadLimits: { [field]: 2 ** 31 - 1 } });
  }
});
test('reject malformed and unknown nested keys, including symbols', () => {
  for (const loadLimits of [null, true, [], 'limits', { maxEntrys: 1 }, { [Symbol('x')]: 1 }]) {
    assert.throws(() => load({ loadLimits }), /loadLimits/);
  }
});
test('XLSX options refused for empty and JSON constructors', () => {
  for (const loadLimits of [{}, { maxEntries: 4 }]) {
    assert.throws(() => new raw({ loadLimits }), /XLSX/);
    assert.throws(() => raw.fromJsonWithOptions('{}', { loadLimits }), /XLSX/);
  }
});
test('lowered and raised actual input, part and aggregate boundaries', () => {
  success({ loadLimits: { maxInputBytes: bytes.length } });
  assert.throws(() => load({ loadLimits: { maxInputBytes: bytes.length - 1 } }));
  const part = Math.max(...required.map(([, text]) => Buffer.byteLength(text)));
  success({ loadLimits: { maxPartBytes: part } });
  assert.throws(() => load({ loadLimits: { maxPartBytes: part - 1 } }));
  // Locate actual aggregate boundary, allowing existing lazy metadata rereads.
  let low = 0, high = 16384;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    try { success({ loadLimits: { maxExpandedBytes: mid } }); high = mid; }
    catch { low = mid + 1; }
  }
  assert.ok(low > 0 && low < 16384);
  assert.throws(() => load({ loadLimits: { maxExpandedBytes: low - 1 } }));
  success({ loadLimits: { maxExpandedBytes: low } });
  console.log(`actual consumed aggregate boundary: ${low} bytes`);
});
test('unread corrupt and oversized payload accepted; required corruption refused', () => {
  const extra = [...required, ['xl/media/unused.bin', 'x'.repeat(64 * 1024)]];
  success({ loadLimits: { maxPartBytes: 1024, maxExpandedBytes: 16384 } }, zip(extra, 'xl/media/unused.bin'));
  assert.throws(() => load(undefined, zip(required, 'xl/worksheets/sheet1.xml')), /CRC|checksum|corrupt/i);
});

// Run the actual public wrapper against the same built WASM, with only its
// bundler-specific import rewritten for Node. No loader behavior is mocked.
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'formualizer-load-wrapper-'));
try {
  const source = fs.readFileSync(path.join(root, 'src/index.ts'), 'utf8')
    .replace("'../pkg/formualizer_wasm.js'", JSON.stringify(entry));
  fs.writeFileSync(path.join(temp, 'package.json'), '{"type":"module"}');
  fs.writeFileSync(path.join(temp, 'manifest.js'), ts.transpileModule(
    fs.readFileSync(path.join(root, 'src/manifest.ts'), 'utf8'),
    { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText);
  fs.writeFileSync(path.join(temp, 'index.mjs'), ts.transpileModule(source,
    { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText);
  const wrapper = await import(pathToFileURL(path.join(temp, 'index.mjs')).href);
  test('generated/public declarations accept exactly five optional numeric fields', () => {
    const importPath = path.join(root, 'src/index.js').replaceAll('\\', '/');
    fs.writeFileSync(path.join(temp, 'types.ts'), `
import { Workbook, type WorkbookLoadOptions, type XlsxLoadLimits } from ${JSON.stringify(importPath)};
import { Workbook as RawWorkbook } from ${JSON.stringify(path.join(root, 'pkg/formualizer_wasm.js').replaceAll('\\', '/'))};
RawWorkbook.fromXlsxBytesWithOptions(new Uint8Array());
RawWorkbook.fromXlsxBytesWithOptions(new Uint8Array(), null);
RawWorkbook.fromXlsxBytesWithOptions(new Uint8Array(), { loadLimits: { maxEntries: 4 }, futureOption: true });
const limits: XlsxLoadLimits = { maxInputBytes: 1, maxEntries: 2, maxExpandedBytes: 3, maxPartBytes: 4, maxXmlDepth: 5 };
const options: WorkbookLoadOptions = { loadLimits: limits, backend: 'calamine', spanEvaluation: true };
Workbook.fromXlsxBytes(new Uint8Array(), options);
Workbook.fromXlsxBytes(new ArrayBuffer(0), { loadLimits: {} });
// @ts-expect-error Unknown nested keys must be rejected by declarations too.
const typo: XlsxLoadLimits = { maxEntrys: 1 };
// @ts-expect-error Only numeric values are accepted.
const bool: XlsxLoadLimits = { maxEntries: true };
`);
    execFileSync(path.join(root, 'node_modules/.bin/tsc'), ['--strict', '--noEmit', '--skipLibCheck',
      '--target', 'ES2022', '--module', 'ES2022', '--moduleResolution', 'bundler', path.join(temp, 'types.ts')],
    { cwd: root, stdio: 'inherit' });
  });
  test('TS wrapper forwards options for Uint8Array and ArrayBuffer sources', () => {
    for (const data of [new Uint8Array(bytes), Uint8Array.from(bytes).buffer]) {
      const defaults = wrapper.Workbook.fromXlsxBytes(data);
      assert.equal(defaults.sheet('Sheet1').getValue(1, 1), 42); defaults.free();
      const wb = wrapper.Workbook.fromXlsxBytes(data, { loadLimits: { maxInputBytes: bytes.length } });
      assert.equal(wb.sheet('Sheet1').getValue(1, 1), 42); wb.free();
      assert.throws(() => wrapper.Workbook.fromXlsxBytes(data, { loadLimits: { maxInputBytes: 0 } }));
    }
  });
} finally { fs.rmSync(temp, { recursive: true, force: true }); }
console.log(`ordinary load conformance: ${passed} passed`);
