import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { loanExample } from '../examples/sheetport-loan.mjs';
import { tableExample } from '../examples/sheetport-table.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const pkg = process.env.FORMUALIZER_WASM_NODE_PACKAGE ?? path.join(root, 'pkg-node');
const wasm = await import(pathToFileURL(path.join(pkg, 'formualizer_wasm.js')));
// Compile the pure helper independently: the bundler WASM entry is not Node-compatible.
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sheetport-conformance-'));
try {
  execFileSync(path.join(root, 'node_modules/.bin/tsc'), ['--strict', '--skipLibCheck', '--target', 'ES2022', '--module', 'ES2022', '--moduleResolution', 'bundler', '--outDir', tmp, path.join(root, 'src/manifest.ts')]);
  fs.writeFileSync(path.join(tmp, 'package.json'), '{"type":"module"}');
  const { defineManifest } = await import(pathToFileURL(path.join(tmp, 'manifest.js')));
  loanExample(wasm, defineManifest);
  tableExample(wasm, defineManifest);
  // Run the README quickstart as written, resolving `formualizer` to the Node build.
  const shim = path.join(tmp, 'formualizer-shim.mjs');
  fs.writeFileSync(shim, `export default async function init() {}\nexport * from ${JSON.stringify(pathToFileURL(path.join(pkg, 'formualizer_wasm.js')).href)};\nexport { defineManifest } from ${JSON.stringify(pathToFileURL(path.join(tmp, 'manifest.js')).href)};\n`);
  const quickstart = path.join(root, 'examples/sheetport-quickstart.mjs');
  const rewritten = fs.readFileSync(quickstart, 'utf8')
    .replace("from 'formualizer';", `from ${JSON.stringify(pathToFileURL(shim).href)};`)
    .replace("new URL('../../../examples/", `new URL(${JSON.stringify(pathToFileURL(path.resolve(root, '../../examples')).href + '/')} + '`);
  assert.notEqual(rewritten, fs.readFileSync(quickstart, 'utf8'));
  const runnable = path.join(tmp, 'sheetport-quickstart.mjs');
  fs.writeFileSync(runnable, rewritten);
  await import(pathToFileURL(runnable).href);
  const fixtures = path.resolve(root, '../../crates/sheetport-spec/tests/fixtures');
  const literals = [];
  for (const file of fs.readdirSync(fixtures).filter(f => f.endsWith('.yaml'))) {
    const value = JSON.parse(fs.readFileSync(path.join(fixtures, file.replace('.yaml', '.json')), 'utf8'));
    literals.push(`const fixture${literals.length}: FioManifest = ${JSON.stringify(value)};`);
    const wb = new wasm.Workbook();
    for (const sheet of ['Inputs', 'Outputs', 'Inventory']) wb.addSheet(sheet);
    // Layout bindings discover their column headers from the workbook.
    for (const port of value.ports) {
      if (port.location.layout) {
        const layout = port.location.layout;
        for (const [index, column] of port.schema.columns.entries()) {
          const col = column.col ? column.col.charCodeAt(0) - 64 : layout.anchor_col.charCodeAt(0) - 64 + index;
          wb.setValue(layout.sheet, layout.header_row, col, column.name);
        }
        if (layout.marker_text) wb.setValue(layout.sheet, layout.header_row + 2, layout.anchor_col.charCodeAt(0) - 64, layout.marker_text);
      }
    }
    const direct = wasm.SheetPortSession.fromManifest(value, wb);
    const yaml = wasm.SheetPortSession.fromManifestYaml(fs.readFileSync(path.join(fixtures, file), 'utf8'), wb);
    assert.deepEqual(direct.manifest(), yaml.manifest());
    direct.free(); yaml.free(); wb.free();
  }
  fs.writeFileSync(path.join(tmp, 'fixtures.ts'), `import type { FioManifest } from ${JSON.stringify(path.join(root, 'src/manifest.js'))};\n${literals.join('\n')}`);
  execFileSync(path.join(root, 'node_modules/.bin/tsc'), ['--strict', '--noEmit', '--skipLibCheck', '--target', 'ES2022', '--module', 'ES2022', '--moduleResolution', 'bundler', path.join(tmp, 'fixtures.ts')], { stdio: 'inherit' });
  const invalid = defineManifest({ id: 'bad id', ports: [] });
  const wb = new wasm.Workbook();
  const error = fn => { try { fn(); } catch (e) { return e; } throw new Error('expected validation error'); };
  const direct = error(() => wasm.SheetPortSession.fromManifest(invalid, wb));
  const yaml = error(() => wasm.SheetPortSession.fromManifestYaml('spec: fio\nspec_version: 0.3.0\nmanifest: {id: "bad id", name: "bad id"}\nports: []', wb));
  assert.ok(Array.isArray(direct.issues));
  assert.ok(direct.issues.some(issue => issue.path === 'manifest.id'));
  assert.deepEqual(direct.issues, yaml.issues);
  wb.free();
  console.log(`SheetPort conformance: ${literals.length} canonical fixtures and loan/table examples passed`);
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}
