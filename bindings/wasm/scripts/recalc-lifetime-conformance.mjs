import assert from 'node:assert/strict';
import { Workbook } from 'formualizer';
let passed = 0;
for (const stage of ['progress', 'yield']) {
  const wb = new Workbook(), controller = new AbortController();
  wb.addSheet('S'); wb.setFormula('S', 1, 1, '=1');
  let reached, release, retainedJob;
  const ready = new Promise(resolve => { reached = resolve; });
  const pendingHook = () => { reached(); return new Promise(resolve => { release = resolve; }); };
  const promise = wb.evaluateWithYield({
    budgetMs: 0, signal: controller.signal,
    onProgress(_, job) { retainedJob = job; if (stage === 'progress') return pendingHook(); },
    ...(stage === 'yield' ? { yieldControl: pendingHook } : {}),
  });
  const settled = promise.then(() => 'resolved', error => error);
  await ready;
  wb.free();
  const uncaught = [];
  process.setUncaughtExceptionCaptureCallback(error => uncaught.push(error));
  let timeout;
  try {
    controller.abort();
    const result = await Promise.race([settled,
      new Promise(resolve => { timeout = setTimeout(() => resolve('timeout'), 100); })]);
    if (result === 'timeout') { release(); await settled; }
    assert.notEqual(result, 'timeout', 'abort must interrupt unresolved hook after wrapper.free');
    assert.equal(result.name, 'AbortError');
    assert.deepEqual(uncaught, []);
    assert.equal(retainedJob.active, false);
    // Raw helper is intentionally absent from public TS. Runtime check proves
    // finally already released the retained reservation.
    assert.equal(retainedJob._releaseDriver(), false);
    passed++;
  } finally {
    clearTimeout(timeout);
    process.setUncaughtExceptionCaptureCallback(null);
    retainedJob.free();
  }
}
{
  const wb = new Workbook(); wb.addSheet('S'); wb.setFormula('S', 1, 1, '=1');
  const old = wb.beginRecalc(); old.cancel();
  const replacement = wb.beginRecalc();
  assert.equal(old._requestCancel(), false);
  assert.equal(replacement._cancelRequested, false);
  wb.registerFunction('SIGNAL', () => { assert.equal(replacement._requestCancel(), true); return 2; });
  wb.setFormula('S', 1, 1, '=SIGNAL()');
  assert.equal(replacement.step().phase, 'Cancelled');
  old.free(); replacement.free(); wb.free();
  passed++;
}
console.log('retained driver lifetime conformance: ' + passed + ' passed');
