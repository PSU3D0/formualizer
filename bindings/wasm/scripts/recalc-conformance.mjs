import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { Workbook, RecalcJob, SheetPortSession } from 'formualizer';

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
  assert.equal(wb.sheet('S').getValue(100, 1), 100);
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
    assert.equal(wb.sheet('S').getValue(500, 1), 500);
  } finally { clearInterval(timer); }
});
await test('microtask-only custom yielding does not provide timer task turns', async () => {
  const wb = chain(500);
  let beats = 0;
  const timer = setInterval(() => { beats++; }, 0);
  try {
    assert.equal((await wb.evaluateWithYield({ budgetMs: 0, yieldControl: () => Promise.resolve() })).status, 'Finished');
    assert.equal(beats, 0, 'negative control must distinguish microtasks from default task yields');
  } finally { clearInterval(timer); }
});
const rect = (startRow = 1, endRow = 100) => ({
  sheet: 'S', startRow, startColumn: 1, endRow, endColumn: 1,
});
const code = expected => error => error.code === expected;
const finish = job => { let report; do { report = job.step(0); } while (job.active); return report; };
const aborts = promise => assert.rejects(promise, error => error.name === 'AbortError');
const tick = () => new Promise(resolve => setTimeout(resolve, 0));

await test('constructor/static loader parity and runtime export identity', () => {
  for (const wb of [new Workbook(), Workbook.fromJson(JSON.stringify({ version: 1, sheets: { S: { cells: [] } } }))]) {
    assert.equal(wb.recalc, Workbook.prototype.recalc);
    assert.equal(wb.evaluateWithYield, Workbook.prototype.evaluateWithYield);
    const job = wb.beginRecalc();
    assert.ok(job instanceof RecalcJob);
    finish(job);
  }
  const bytes = readFileSync(new URL('../../../crates/formualizer-workbook/tests/fixtures/shared_formula_above_master.xlsx', import.meta.url));
  const loaded = Workbook.fromXlsxBytes(bytes);
  assert.equal(loaded.recalc().status, 'Finished');
  assert.equal(loaded.evaluateWithYield, Workbook.prototype.evaluateWithYield);
});
await test('budgets default, zero, finite, null and undefined finish', () => {
  for (const budgetMs of [undefined, 0, 5, null]) {
    const wb = chain(25);
    assert.equal(wb.recalc({ budgetMs }).status, 'Finished');
    assert.equal(wb.sheet('S').getValue(25, 1), 25);
  }
});
await test('invalid driver and raw options rejected before admission', async () => {
  for (const options of [null, [], false, { x: 1 }, { [Symbol('x')]: true },
    ...[-1, NaN, Infinity, 2 ** 80, '5'].map(budgetMs => ({ budgetMs })),
    { onStep: true }, { priority: [rect(0)] }, { collectDeltas: 'yes' }, { maxDeltaRecords: -1 }]) {
    const wb = chain(5);
    assert.throws(() => wb.recalc(options), code(options?.priority ? 'INVALID_ADDRESS' : 'INVALID_OPTIONS'));
    assert.equal(wb.hasActiveRecalcJob, false);
    assert.equal(wb.recalc().status, 'Finished');
  }
  for (const options of [{ onProgress: 1 }, { yieldControl: false }, { signal: {} }, { budgetMs: -1 }]) {
    const wb = chain(5);
    await assert.rejects(wb.evaluateWithYield(options), code('INVALID_OPTIONS'));
    assert.equal(wb.hasActiveRecalcJob, false);
  }
});
await test('pause releases reservation; priority/input edit/inspection and manual resume', () => {
  const wb = chain();
  const out = wb.recalc({ budgetMs: 0, onStep(report, job) {
    job.prioritize([rect(70, 100)]);
    wb.setValue('S', 1, 1, 10);
    assert.ok(wb.inspectCell({ sheet: 'S', row: 1, column: 1 }));
    return false;
  } });
  assert.equal(out.status, 'Paused');
  assert.ok(out.job.active);
  assert.throws(() => wb.beginRecalc(), error => error.code === 'ALREADY_PENDING' && error.explicit === true);
  out.job.cancel();
  // Admission proves reservation was released even though the paused engine job was active.
  const replacement = wb.beginRecalc();
  finish(replacement);
  assert.equal(wb.sheet('S').getValue(100, 1), 109);
  assert.equal(out.job.active, false);
  assert.throws(() => out.job.cancel(), code('NO_ACTIVE_JOB'));
});
await test('paused handle resumes directly after repair', () => {
  const wb = chain();
  const out = wb.recalc({ budgetMs: 0, onStep: () => false });
  wb.setValue('S', 1, 1, 7);
  finish(out.job);
  assert.equal(wb.sheet('S').getValue(100, 1), 106);
});
await test('cooperative and terminal cancellation beat false; explicit reset overrides flag', () => {
  for (const mode of ['job', 'workbook', 'reset']) {
    const wb = chain();
    const out = wb.recalc({ budgetMs: 0, onStep(_, job) {
      if (mode === 'job') job.cancel();
      else { wb.cancel(); if (mode === 'reset') wb.resetCancel(); }
      return false;
    } });
    assert.equal(out.status, mode === 'reset' ? 'Paused' : 'Cancelled');
    if (out.status === 'Paused') out.job.cancel();
    assert.equal(wb.hasActiveRecalcJob, false);
    assert.equal(wb.recalc().status, 'Finished');
  }
});
await test('sync exceptions preserve exact identity including terminal callbacks', () => {
  for (const budgetMs of [0, null]) {
    const wb = chain(10), sentinel = {};
    assert.throws(() => wb.recalc({ budgetMs, onStep() { throw sentinel; } }), error => error === sentinel);
    assert.equal(wb.hasActiveRecalcJob, false);
    assert.equal(wb.recalc().status, 'Finished');
  }
});
await test('sync terminal callbacks hold shared reservation', () => {
  const wb = chain(10);
  const out = wb.recalc({ budgetMs: null, onStep(report) {
    if (report.phase !== 'Finished') return;
    assert.throws(() => wb.beginRecalc(), code('ALREADY_PENDING'));
    assert.throws(() => wb.recalc(), code('ALREADY_PENDING'));
    assert.throws(() => wb.evaluateAll(), code('RECALC_JOB_ACTIVE'));
  } });
  assert.equal(out.status, 'Finished');
});
await test('callback terminal cancellation is never mistaken for a Finished report', async () => {
  const wb = chain();
  const out = await wb.evaluateWithYield({ budgetMs: 0, onProgress(_, job) { job.cancel(); } });
  assert.deepEqual(out, { status: 'Cancelled' });
});
await test('pre-abort admits no job', async () => {
  const wb = chain(), controller = new AbortController();
  controller.abort();
  await aborts(wb.evaluateWithYield({ signal: controller.signal }));
  assert.equal(wb.hasActiveRecalcJob, false);
  assert.equal(wb.recalc().status, 'Finished');
});
await test('abort during UDF stays lock-free and monotonic after resetCancel', async () => {
  const wb = new Workbook(), controller = new AbortController();
  wb.addSheet('S');
  wb.registerFunction('ABORT_NOW', () => { controller.abort(); wb.resetCancel(); return 4; });
  wb.setFormula('S', 1, 1, '=ABORT_NOW()');
  await aborts(wb.evaluateWithYield({ signal: controller.signal, budgetMs: null }));
  assert.equal(wb.hasActiveRecalcJob, false);
  wb.unregisterFunction('ABORT_NOW');
  wb.setFormula('S', 1, 1, '=4');
  assert.equal(wb.recalc().status, 'Finished');
});
await test('abort interrupts unresolved progress (nonterminal and terminal) and yield hooks', async () => {
  for (const stage of ['progress', 'terminal', 'yield']) {
    const wb = chain(10), controller = new AbortController();
    let reached;
    const ready = new Promise(resolve => { reached = resolve; });
    const hang = () => { reached(); return new Promise(() => {}); };
    const promise = wb.evaluateWithYield({
      budgetMs: stage === 'terminal' ? null : 0,
      signal: controller.signal,
      ...(stage === 'yield' ? { yieldControl: hang } : { onProgress: hang }),
    });
    await ready;
    assert.throws(() => wb.beginRecalc(), code('ALREADY_PENDING'));
    await assert.rejects(wb.evaluateWithYield(), code('ALREADY_PENDING'));
    controller.abort();
    await aborts(promise);
    assert.equal(wb.hasActiveRecalcJob, false);
    assert.equal(wb.recalc().status, 'Finished');
  }
});
await test('awaited hook errors retain identity and cleanup, including terminal callbacks', async () => {
  for (const hook of ['progress-reject', 'progress-throw', 'terminal-reject', 'yield-reject']) {
    const wb = chain(10), sentinel = {};
    const fail = () => { if (hook.endsWith('throw')) throw sentinel; return Promise.reject(sentinel); };
    await assert.rejects(wb.evaluateWithYield({
      budgetMs: hook === 'terminal-reject' ? null : 0,
      ...(hook === 'yield-reject' ? { yieldControl: fail } : { onProgress: fail }),
    }), error => error === sentinel);
    assert.equal(wb.hasActiveRecalcJob, false);
    assert.equal(wb.recalc().status, 'Finished');
  }
});
await test('progress false does not pause and can edit/prioritize/read without locks', async () => {
  const wb = chain(20);
  let edited = false;
  const out = await wb.evaluateWithYield({ budgetMs: 0, onProgress(report, job) {
    if (!edited && report.phase !== 'Finished') {
      wb.setValue('S', 1, 1, 9);
      job.prioritize([rect(10, 20)]);
      wb.inspectCell({ sheet: 'S', row: 1, column: 1 });
      edited = true;
    }
    return false;
  } });
  assert.equal(out.status, 'Finished');
  assert.equal(wb.sheet('S').getValue(20, 1), 28);
});
await test('late losing rejection is observed; stale cleanup cannot affect replacement signal', async () => {
  const wb = chain(10), controller = new AbortController();
  let rejectHook, reached;
  const ready = new Promise(resolve => { reached = resolve; });
  const unhandled = [];
  const observe = reason => unhandled.push(reason);
  process.on('unhandledRejection', observe);
  try {
    const old = wb.evaluateWithYield({ budgetMs: 0, signal: controller.signal, onProgress() {
      reached(); return new Promise((_, reject) => { rejectHook = reject; });
    } });
    await ready;
    controller.abort();
    await aborts(old);
    const replacement = wb.beginRecalc();
    wb.cancel();
    rejectHook(new Error('late losing rejection'));
    await tick();
    assert.deepEqual(unhandled, []);
    assert.equal(replacement.step().phase, 'Cancelled');
  } finally { process.off('unhandledRejection', observe); }
});
await test('abort listeners removed on finish/pause-equivalent cancellation/error/abort', async () => {
  for (const mode of ['finish', 'cancel', 'error', 'abort']) {
    const wb = chain(5), controller = new AbortController();
    let additions = 0, removals = 0;
    const signal = controller.signal;
    const add = signal.addEventListener.bind(signal), remove = signal.removeEventListener.bind(signal);
    signal.addEventListener = (...args) => { additions++; return add(...args); };
    signal.removeEventListener = (...args) => { removals++; return remove(...args); };
    const promise = wb.evaluateWithYield({ signal, budgetMs: 0, onProgress(_, job) {
      if (mode === 'cancel') job.cancel();
      if (mode === 'error') throw new Error('hook');
      if (mode === 'abort') controller.abort();
    } });
    if (mode === 'error') await assert.rejects(promise, /hook/);
    else if (mode === 'abort') await aborts(promise);
    else await promise;
    assert.equal(additions, 1); assert.equal(removals, 1);
  }
});
await test('raw reports detached; changed/overflow/pending query shapes', () => {
  const wb = chain(100), job = wb.beginRecalc({ priority: [rect(80, 100)] });
  let sawChanges = false, sawFinalCell = false;
  let first = job.step(0), snapshot = structuredClone(first);
  assert.ok(wb.pendingCellsInRect(rect()).every(cell => cell.sheet === 'S' && cell.row >= 1 && cell.row <= 100));
  while (job.active) {
    const report = job.step(0);
    if (report.changed.length) sawChanges = true;
    if (report.changed.some(area => area.sheet === 'S' && area.startRow <= 100 &&
      area.endRow >= 100 && area.startColumn <= 1 && area.endColumn >= 1)) sawFinalCell = true;
  }
  assert.deepEqual(first, snapshot);
  assert.ok(sawChanges && sawFinalCell, 'changed reports cover the expected final cell');
  assert.equal(wb.hasPendingInRect(rect()), false);
  first.changed.push(rect());
  assert.equal(wb.sheet('S').getValue(100, 1), 100);
  wb.setValue('S', 1, 1, 2);
  const overflow = wb.beginRecalc({ maxDeltaRecords: 0 });
  let sawOverflow = false;
  while (overflow.active) if (overflow.step(0).overflowSheets.includes('S')) sawOverflow = true;
  assert.ok(sawOverflow);
});
// All aliases are made before admission, so they must share the original owner.
function terminalFixture() {
  const wb = new Workbook();
  wb.addSheet('S'); wb.setFormula('S', 1, 1, '=1+2');
  const sheet = wb.sheet('S');
  const session = SheetPortSession.fromManifest({
    spec: 'fio', spec_version: '0.3.0',
    manifest: { id: 'terminal', name: 'Terminal', workbook: { uri: 'memory://test.xlsx', locale: 'en-US', date_system: 1900 } },
    ports: [
      { id: 'out', dir: 'out', shape: 'scalar', location: { a1: 'S!A1' }, schema: { type: 'number' } },
      { id: 'input', dir: 'in', shape: 'scalar', location: { a1: 'S!B1' }, schema: { type: 'number' } },
    ],
  }, wb);
  return { wb, sheet, session };
}
function evaluationRoutes({ wb, sheet, session }, freed) {
  return [
    ...(!freed ? [
      ['Workbook.evaluateAll', () => wb.evaluateAll()],
      ['Workbook.evaluateCells', () => wb.evaluateCells([['S', 1, 1]])],
      ['Workbook.evaluateCell', () => wb.evaluateCell('S', 1, 1)],
    ] : []),
    ['Sheet.evaluateCell', () => sheet.evaluateCell(1, 1)],
    ['SheetPort.evaluateOnce', () => session.evaluateOnce()],
  ];
}
function observeRefusals(fixture, freed) {
  // Editing and inspection remain usable during the reservation.
  fixture.sheet.setFormula(1, 1, '=40+2');
  assert.equal(fixture.sheet.getFormula(1, 1).replaceAll(' ', ''), '=40+2');
  fixture.session.writeInputs({ input: 7 });
  assert.equal(fixture.session.readInputs().input, 7);
  assert.equal(fixture.session.describePorts().length, 2);
  return evaluationRoutes(fixture, freed).map(([route, run]) => {
    try { return { route, status: 'allowed', value: run() }; }
    catch (error) {
      return { route, status: 'refused', code: error.code,
        kind: error.kind, excel_kind: error.excel_kind };
    }
  });
}
function assertRefusals(observations) {
  assert.ok(observations.every(item => item.status === 'refused' &&
    item.code === 'RECALC_JOB_ACTIVE' &&
    item.kind === (item.route === 'SheetPort.evaluateOnce' ? 'Workbook' : 'Engine') && item.excel_kind === '#VALUE!'),
  JSON.stringify(observations));
}
function afterReservation(fixture, freed) {
  for (const [, run] of evaluationRoutes(fixture, freed)) run();
  assert.equal(fixture.sheet.evaluateCell(1, 1), 42);
  assert.equal(fixture.session.evaluateOnce().out, 42);
  fixture.session.free(); fixture.sheet.free();
  if (!freed) fixture.wb.free();
}
await test('all ordinary routes refuse sync and awaited Finished callbacks', async () => {
  const observations = [];
  for (const mode of ['sync', 'async']) {
    const fixture = terminalFixture();
    const callback = report => {
      if (report.phase === 'Finished') observations.push(...observeRefusals(fixture, false).map(item => ({ mode, ...item })));
    };
    const out = mode === 'sync'
      ? fixture.wb.recalc({ budgetMs: null, onStep: callback })
      : await fixture.wb.evaluateWithYield({ budgetMs: null, onProgress: async report => {
        await Promise.resolve(); callback(report);
      } });
    assert.equal(out.status, 'Finished');
    afterReservation(fixture, false);
  }
  console.log('terminal route observations: ' + JSON.stringify(observations));
  assert.equal(observations.length, 10);
  assertRefusals(observations);
});
for (const mode of ['sync', 'async']) {
  for (const interval of ['finished', 'cancelled', 'error']) {
    for (const freed of [false, true]) {
      await test(mode + ' ' + interval + ' reservation with original wrapper ' + (freed ? 'freed' : 'retained'), async () => {
        const fixture = terminalFixture(), sentinel = {};
        let observations, visited = false;
        const callback = (report, job) => {
          if (interval !== 'cancelled' && report.phase !== 'Finished') return;
          visited = true;
          if (interval === 'cancelled') {
            job.cancel(); fixture.wb.cancel();
          }
          if (freed) fixture.wb.free();
          observations = observeRefusals(fixture, freed);
          if (interval === 'cancelled') assert.equal(job._cancelRequested, true,
            'refused evaluation must preserve the retained cooperative flag');
          if (interval === 'error') throw sentinel;
          return false; // Finished/cancellation must still take precedence over pause.
        };
        const run = () => mode === 'sync'
          ? fixture.wb.recalc({ budgetMs: interval === 'cancelled' ? 0 : null, onStep: callback })
          : fixture.wb.evaluateWithYield({ budgetMs: interval === 'cancelled' ? 0 : null, onProgress: async (report, job) => {
            await Promise.resolve(); return callback(report, job);
          } });
        if (interval === 'error') {
          if (mode === 'sync') assert.throws(run, error => error === sentinel);
          else await assert.rejects(run(), error => error === sentinel);
        } else {
          const out = await run();
          assert.equal(out.status, interval === 'cancelled' ? 'Cancelled' : 'Finished',
            'refusals must not erase cooperative cancellation');
        }
        assert.ok(visited);
        // Verify ordinary evaluation after release even if an assertion below fails.
        afterReservation(fixture, freed);
        assertRefusals(observations);
      });
    }
  }
}
for (const interval of ['cancelled', 'error']) {
  await test('async ' + interval + ' cleanup retains reservation during listener removal', async () => {
    const fixture = terminalFixture(), controller = new AbortController(), sentinel = {};
    let observations;
    const remove = controller.signal.removeEventListener.bind(controller.signal);
    controller.signal.removeEventListener = (...args) => {
      assert.equal(fixture.wb.hasActiveRecalcJob, false);
      observations = observeRefusals(fixture, false);
      remove(...args);
    };
    const promise = fixture.wb.evaluateWithYield({ budgetMs: interval === 'cancelled' ? 0 : null, signal: controller.signal,
      onProgress(report, job) {
        if (interval !== 'cancelled' && report.phase !== 'Finished') return;
        if (interval === 'error') throw sentinel;
        job.cancel(); fixture.wb.cancel();
      } });
    if (interval === 'error') await assert.rejects(promise, error => error === sentinel);
    else assert.equal((await promise).status, 'Cancelled');
    afterReservation(fixture, false);
    assertRefusals(observations);
  });
}
console.log('recalc conformance: ' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exitCode = 1;
