import assert from 'node:assert/strict';
import { Workbook, SheetPortSession } from 'formualizer';

// JS error-property setters are user callbacks too. Every observation must
// happen after engine guards and SheetPort's workbook borrow have ended.
let passed = 0;
for (const mode of ['refusal', 'evaluation']) {
  const wb = new Workbook(mode === 'evaluation' ? { maxWorkUnits: 0 } : undefined);
  wb.addSheet('S'); wb.setFormula('S', 1, 1, '=1+2');
  const sheet = wb.sheet('S');
  const session = SheetPortSession.fromManifest({
    spec: 'fio', spec_version: '0.3.0',
    manifest: { id: 'conversion', name: 'Conversion', workbook: { uri: 'memory://test.xlsx', locale: 'en-US', date_system: 1900 } },
    ports: [{ id: 'out', dir: 'out', shape: 'scalar', location: { a1: 'S!A1' }, schema: { type: 'number' } }],
  }, wb);
  const job = mode === 'refusal' ? wb.beginRecalc() : undefined;
  const observations = [];
  let inside = false;
  const property = mode === 'refusal' ? 'code' : 'resource_reason';
  Object.defineProperty(Error.prototype, property, { configurable: true, set(value) {
    Object.defineProperty(this, property, { value, configurable: true, writable: true });
    if (inside) return;
    inside = true;
    try { observations.push(wb.hasActiveRecalcJob); }
    catch (error) { observations.push(error.code); }
    finally { inside = false; }
  } });
  try {
    for (const operation of [
      () => wb.evaluateAll(), () => wb.evaluateCells([['S', 1, 1]]),
      () => wb.evaluateCell('S', 1, 1), () => sheet.evaluateCell(1, 1), () => session.evaluateOnce(),
    ]) {
      const start = observations.length;
      assert.throws(operation, error => mode === 'refusal'
        ? error.code === 'RECALC_JOB_ACTIVE' : error.resource_reason === 'work_units');
      assert.ok(observations.length > start, 'must actually invoke property setter');
      assert.ok(observations.slice(start).every(value => value === (mode === 'refusal')));
      passed++;
    }
  } finally {
    delete Error.prototype[property];
    job?.cancel(); session.free(); sheet.free(); wb.free();
  }
}
console.log('error conversion conformance: ' + passed + ' passed');
