import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'recalc-types-'));
const source = path.join(temp, 'consumer.ts');
const declaration = path.join(root, 'dist/index.js').replaceAll('\\', '/');
fs.writeFileSync(source, `
import { Workbook, RecalcJob, type RecalcOutcome, type StepReport, type RecalcJobError,
  type RecalcJobActiveError, type FiniteRangeAddress } from ${JSON.stringify(declaration)};
const wb = new Workbook();
const area: FiniteRangeAddress = { sheet: 'S', startRow: 1, startColumn: 1, endRow: 3, endColumn: 1 };
const job = wb.beginRecalc({ priority: [area], collectDeltas: true, maxDeltaRecords: 4 });
const report: StepReport = job.step(null);
if (report.phase === 'Finished') report.result.computedVertices.toFixed();
// @ts-expect-error Result is absent until Finished.
if (report.phase === 'Evaluating') report.result.computedVertices;
if (job instanceof RecalcJob) job.prioritize([area]);
function outcome(value: RecalcOutcome) {
  switch (value.status) {
    case 'Paused': value.job.step(0); break;
    case 'Finished': value.result.computedVertices.toFixed(); break;
    case 'Cancelled': break;
    default: { const exhaustive: never = value; return exhaustive; }
  }
}
outcome(wb.recalc({ onStep(report, job) { job.prioritize([area]); return false; } }));
wb.evaluateWithYield({ signal: new AbortController().signal,
  async onProgress(report, job) { job.prioritize([area]); }, yieldControl: () => Promise.resolve() }).then(outcome);
const addresses: { sheet: string; row: number; column: number }[] = wb.pendingCellsInRect(area);
function error(value: RecalcJobError) {
  if (value.code === 'ALREADY_PENDING') { const explicit: boolean = value.explicit; }
  if (value.code === 'EVALUATION') { const kind: string = value.cause.kind; const reason: string | undefined = value.resource_reason; }
}
function refusal(value: RecalcJobActiveError) { const code: 'RECALC_JOB_ACTIVE' = value.code; }
// @ts-expect-error No constructor: jobs are admitted by Workbook.
new RecalcJob();
// @ts-expect-error Internal reservation helper is not a public facade API.
wb._beginRecalcDriver();
// @ts-expect-error Internal helper is hidden on public jobs.
job._releaseDriver();
// @ts-expect-error Bounded priority rectangles cannot have open coordinates.
wb.beginRecalc({ priority: [{ ...area, startRow: null }] });
// @ts-expect-error No string budget.
job.step('5');
// @ts-expect-error No unknown options.
wb.recalc({ budget: 5 });
// @ts-expect-error Awaited callbacks belong to evaluateWithYield.
wb.recalc({ onStep: async () => false });
`);
try {
  execFileSync(path.join(root, 'node_modules/.bin/tsc'), ['--strict', '--noEmit', '--skipLibCheck',
    '--target', 'ES2022', '--module', 'ES2022', '--moduleResolution', 'bundler', source], { stdio: 'inherit' });
} finally { fs.rmSync(temp, { recursive: true, force: true }); }
console.log('recalc generated-declaration consumer/narrowing conformance passed');
