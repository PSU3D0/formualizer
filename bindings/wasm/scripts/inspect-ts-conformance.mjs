import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const wasmRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const packageDir = process.env.FORMUALIZER_WASM_NODE_PACKAGE
  ? path.resolve(process.env.FORMUALIZER_WASM_NODE_PACKAGE)
  : path.join(wasmRoot, 'pkg-node');
const packageEntry = path.join(packageDir, 'formualizer_wasm.js');

if (!process.env.FORMUALIZER_WASM_NODE_PACKAGE) {
  execFileSync(
    'wasm-pack',
    ['build', '--target', 'nodejs', '--out-dir', packageDir, '--release'],
    { cwd: wasmRoot, stdio: 'inherit' },
  );
} else if (!fs.existsSync(packageEntry)) {
  throw new Error(`reused nodejs package is missing ${packageEntry}`);
}

const wasm = await import(pathToFileURL(packageEntry).href);
const fixture = {
  version: 1,
  defined_names: [
    {
      name: 'MyInput',
      scope: 'workbook',
      definition: {
        type: 'range',
        address: { sheet: 'Model', start_row: 1, start_col: 1, end_row: 1, end_col: 1 },
      },
    },
    {
      name: 'MyRange',
      scope: 'workbook',
      definition: {
        type: 'range',
        address: { sheet: 'Model', start_row: 1, start_col: 1, end_row: 2, end_col: 1 },
      },
    },
    {
      name: 'ConstName',
      scope: 'workbook',
      definition: { type: 'literal', value: { type: 'Int', value: 7 } },
    },
  ],
  sheets: {
    Model: {
      tables: [
        {
          name: 'Table1',
          sheet: 'Model',
          range: [10, 1, 12, 2],
          headers: ['Item', 'Amount'],
          header_row: true,
          totals_row: false,
        },
      ],
      cells: [
        { row: 1, col: 1, value: { type: 'Int', value: 10 } },
        { row: 2, col: 1, value: { type: 'Int', value: 20 } },
        { row: 3, col: 1, value: { type: 'Text', value: '00123' } },
        { row: 4, col: 1, value: { type: 'Date', value: '2025-01-15' } },
        { row: 5, col: 1, value: { type: 'DateTime', value: '2025-01-15T12:34:56' } },
        { row: 6, col: 1, value: { type: 'Duration', value: 3661 } },
        { row: 7, col: 1, value: { type: 'Boolean', value: true } },
        { row: 8, col: 1, value: { type: 'Pending' } },
        { row: 1, col: 2, formula: '=A1+1' },
        { row: 2, col: 2, formula: '=SUM(A1:A2)' },
        { row: 1, col: 3, formula: '=MyInput+1' },
        { row: 2, col: 3, formula: '=SUM(MyRange)' },
        { row: 3, col: 3, formula: '=ConstName+1' },
        { row: 4, col: 3, formula: '=NoSuchName+1' },
        { row: 1, col: 4, formula: '=SEQUENCE(2,2,1,1)' },
        { row: 1, col: 6, formula: '=E2' },
        { row: 1, col: 7, formula: '=H1+1' },
        { row: 1, col: 8, formula: '=G1+1' },
        { row: 1, col: 9, formula: '=A1' },
        { row: 1, col: 10, formula: '=A1' },
        { row: 1, col: 11, formula: '=I1+J1' },
        { row: 1, col: 12, formula: '=SUM(Table1[Amount])' },
        { row: 2, col: 12, formula: '=1/0' },
        { row: 10, col: 1, value: { type: 'Text', value: 'Item' } },
        { row: 10, col: 2, value: { type: 'Text', value: 'Amount' } },
        { row: 11, col: 1, value: { type: 'Text', value: 'widget' } },
        { row: 11, col: 2, value: { type: 'Int', value: 5 } },
        { row: 12, col: 1, value: { type: 'Text', value: 'gadget' } },
        { row: 12, col: 2, value: { type: 'Int', value: 6 } },
      ],
    },
    Other: { cells: [{ row: 1, col: 1, value: { type: 'Int', value: 42 } }] },
    Ghost: { cells: [] },
  },
};

const wb = wasm.Workbook.fromJson(JSON.stringify(fixture));
wb.evaluateAll();
const address = (sheet, row, column) => ({ sheet, row, column });
const area = (sheet, startRow, startColumn, endRow, endColumn) => ({
  sheet,
  startRow,
  startColumn,
  endRow,
  endColumn,
});
const cases = [];
const add = (label, type, value) => cases.push({ label, type, value });

for (const [label, row, column] of [
  ['number', 1, 1],
  ['text', 3, 1],
  ['date', 4, 1],
  ['datetime', 5, 1],
  ['duration', 6, 1],
  ['boolean', 7, 1],
  ['pending', 8, 1],
  ['empty', 30, 30],
  ['error', 2, 12],
  ['spill-anchor', 1, 4],
  ['spill-member', 2, 5],
]) {
  add(`snapshot-${label}`, 'CellSnapshotReport', wb.inspectCell(address('Model', row, column)));
}
add(
  'snapshot-no-values',
  'CellSnapshotReport',
  wb.inspectCell(address('Model', 1, 2), { includeValues: false }),
);

for (const [label, row, column] of [
  ['cell', 1, 2],
  ['range', 2, 2],
  ['name-cell', 1, 3],
  ['name-range', 2, 3],
  ['name-literal', 3, 3],
  ['name-unresolved', 4, 3],
  ['table', 1, 12],
]) {
  add(`precedents-${label}`, 'PrecedentReport', wb.precedents(address('Model', row, column)));
}
add(
  'precedents-exact-omitted',
  'PrecedentReport',
  wb.precedents(address('Model', 2, 2), { maxLinks: 0 }),
);
add(
  'precedents-at-least-omitted',
  'PrecedentReport',
  wb.precedents(address('Model', 2, 2), { maxWork: 0 }),
);
add('dependents-normal', 'DependentsReport', wb.dependents(address('Model', 1, 1)));
add('dependents-via', 'DependentsReport', wb.dependents(address('Model', 1, 4)));
add(
  'dependents-truncated',
  'DependentsReport',
  wb.dependents(address('Model', 1, 1), { maxResults: 1 }),
);

add(
  'trace-cycle',
  'TraceGraph',
  wb.trace([address('Model', 1, 7)], { maxDepth: 4, maxNodes: 8, maxLinks: 8 }),
);
add('trace-diamond', 'TraceGraph', wb.trace([address('Model', 1, 11)]));
add(
  'trace-multi-root',
  'TraceGraph',
  wb.trace([address('Model', 1, 2), address('Model', 1, 2), address('Model', 1, 11)]),
);
add(
  'trace-dependents',
  'TraceGraph',
  wb.trace([address('Model', 1, 4)], { direction: 'dependents' }),
);
add(
  'trace-spill-anchor',
  'TraceGraph',
  wb.trace([address('Model', 2, 5)]),
);
add(
  'trace-elided',
  'TraceGraph',
  wb.trace([address('Model', 1, 2)], { maxDepth: 0 }),
);
add(
  'trace-range-omitted',
  'TraceGraph',
  wb.trace([address('Model', 2, 2)], { rangeMemberBudget: 1 }),
);
add(
  'trace-no-values',
  'TraceGraph',
  wb.trace([address('Model', 1, 2)], { includeValues: false }),
);

add(
  'range-page',
  'RangePage',
  wb.rangePage(area('Model', 1, 1, 2, 2), { limit: 3 }),
);
add(
  'range-page-offset-no-values',
  'RangePage',
  wb.rangePage(area('Model', 1, 1, 2, 2), { offset: 2, limit: 2, includeValues: false }),
);
add(
  'range-page-unresolved',
  'RangePage',
  wb.rangePage(area('Ghost', null, null, null, null), { limit: 1 }),
);

wb.setFormula('Model', 20, 1, '=A1+5');
add('snapshot-never-evaluated', 'CellSnapshotReport', wb.inspectCell(address('Model', 20, 1)));
wb.setValue('Model', 1, 1, 99);
add('snapshot-dirty', 'CellSnapshotReport', wb.inspectCell(address('Model', 1, 2)));
wb.setFormula('Model', 4, 12, "='[1]Sheet1'!A1");
wb.setFormula('Model', 5, 12, '=SUM(Model:Other!A1)');
add('precedents-external', 'PrecedentReport', wb.precedents(address('Model', 4, 12)));
add('precedents-unsupported', 'PrecedentReport', wb.precedents(address('Model', 5, 12)));

const wireText = JSON.stringify(cases.map(({ value }) => value));
for (const token of [
  '"staleness":"current"',
  '"staleness":"dirty"',
  '"staleness":"neverEvaluated"',
  '"role":"anchor"',
  '"role":"member"',
  '"kind":"table"',
  '"kind":"external"',
  '"kind":"unsupported"',
  '"kind":"unresolved"',
  '"kind":"spillAnchor"',
  '"kind":"spillReader"',
  '"disposition":"expanded"',
  '"disposition":"convergent"',
  '"disposition":"cycle"',
  '"disposition":"elided"',
  '"direction":"precedents"',
  '"direction":"dependents"',
  '"kind":"exact"',
  '"kind":"atLeast"',
]) {
  if (!wireText.includes(token)) throw new Error(`fixture failed to reach ${token}`);
}

const assertError = (code, fn) => {
  try {
    fn();
  } catch (error) {
    if (
      error.kind === 'InspectError' &&
      error.code === code &&
      error.inspect_code === code
    ) return;
    throw error;
  }
  throw new Error(`expected InspectError ${code}`);
};
assertError('SHEET_NOT_FOUND', () => wb.inspectCell(address('Missing', 1, 1)));
assertError('INVALID_ADDRESS', () => wb.inspectCell(address('Model', 0, 1)));
assertError('INVALID_OPTIONS', () => wb.rangePage(area('Model', 1, 1, 1, 1), { limit: 0 }));
assertError('REVISION_MISMATCH', () =>
  wb.rangePage(area('Model', 1, 1, 1, 1), {
    expectedStamp: { mutationRevision: '0', recalcEpoch: '0' },
  }),
);
assertError('DEPENDENCY_STATE_UNAVAILABLE', () => wb.dependents(address('Model', 1, 1)));

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'formualizer-inspect-types-'));
const generated = path.join(tempDir, 'shapes.ts');
const importPath = path.join(wasmRoot, 'src', 'index.js').replaceAll('\\', '/');
const lines = [
  `import type { CellSnapshotReport, PrecedentReport, DependentsReport, TraceGraph, RangePage } from ${JSON.stringify(importPath)};`,
  '',
  '// Self-falsification: an extra runtime key produces TS2353 under excess-property checking.',
  '// Removing a required runtime key produces TS2741; enum casing drift produces TS2820.',
  '// Emitting a u64 stamp as a number instead of a decimal string produces TS2322.',
];
for (const [index, { label, type, value }] of cases.entries()) {
  lines.push(`// ${label}`);
  lines.push(`export const shape${index}: ${type} = ${JSON.stringify(value, null, 2)};`);
}
fs.writeFileSync(generated, `${lines.join('\n')}\n`);
try {
  execFileSync(
    path.join(wasmRoot, 'node_modules', '.bin', 'tsc'),
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
      generated,
    ],
    { cwd: wasmRoot, stdio: 'inherit' },
  );
} finally {
  fs.rmSync(tempDir, { recursive: true, force: true });
}
console.log(`inspection TS/runtime conformance: ${cases.length} fresh literals passed`);
