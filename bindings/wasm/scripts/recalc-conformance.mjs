import assert from 'node:assert/strict';
import { Workbook, RecalcJob } from 'formualizer';

export function chain(count = 100) {
  const wb = new Workbook();
  wb.addSheet('S');
  wb.setValue('S', 1, 1, 1);
  for (let row = 2; row <= count; row++) wb.setFormula('S', row, 1, '=A' + (row - 1) + '+1');
  return wb;
}
let passed = 0, failed = 0;
async function test(name, run) {
  try { await run(); console.log('ok ' + (++passed) + ' - ' + name); }
  catch (error) { failed++; console.error('FAIL ' + name, error); }
}
await test('sync actual package finishes and exports generated job', () => {
  const wb = chain();
  let calls = 0;
  const out = wb.recalc({ budgetMs: 0, onStep(report, job) {
    calls++; assert.ok(job instanceof RecalcJob); assert.equal(typeof report.phase, 'string');
  } });
  assert.equal(out.status, 'Finished');
  assert.ok(calls > 1);
  assert.equal(wb.getValue('S', 100, 1), 100);
});
await test('async actual package yields task turns before finishing', async () => {
  const wb = chain(500);
  let heartbeats = 0, unfinishedHeartbeat = false;
  const timer = setInterval(() => { heartbeats++; }, 0);
  try {
    const out = await wb.evaluateWithYield({ budgetMs: 0, onProgress(report) {
      if (report.phase !== 'Finished' && heartbeats > 0) unfinishedHeartbeat = true;
    } });
    assert.equal(out.status, 'Finished');
    assert.ok(unfinishedHeartbeat, 'a timer must run while recalculation is unfinished');
    assert.equal(wb.getValue('S', 500, 1), 500);
  } finally { clearInterval(timer); }
});
console.log('recalc conformance: ' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exitCode = 1;
