// SheetPort quickstart shown in the READMEs. The conformance test runs this file
// with the `formualizer` import pointed at the Node build.
import assert from 'node:assert/strict';
// region readme
import init, { Workbook, SheetPortSession, defineManifest } from 'formualizer';
await init();

// The model: B4 computes the monthly payment from B1:B3.
const wb = new Workbook();
wb.addSheet('Calculator');
wb.setFormula('Calculator', 4, 2, '=PMT(B2/12, B3, -B1)');

const manifest = defineManifest({
  id: 'loan-calculator',
  name: 'Loan calculator',
  ports: [
    { id: 'principal', dir: 'in', location: 'Calculator!B1', type: 'number', constraints: { min: 0 } },
    { id: 'rate', dir: 'in', location: 'Calculator!B2', type: 'number' },
    { id: 'term_months', dir: 'in', location: 'Calculator!B3', type: 'integer' },
    { id: 'monthly_payment', dir: 'out', location: 'Calculator!B4', type: 'number' },
  ],
});

const session = SheetPortSession.fromManifest(manifest, wb);
session.writeInputs({ principal: 250000, rate: 0.045, term_months: 360 });
const outputs = session.evaluateOnce({ freezeVolatile: true });
console.log(outputs.monthly_payment); // 1266.71...
// endregion

assert.ok(Math.abs(outputs.monthly_payment - 1266.7132745647) < 1e-6);
assert.throws(() => session.writeInputs({ rate: 0.05, principal: -1 }), error =>
  Array.isArray(error.violations) && error.violations.some(v => JSON.stringify(v).includes('principal')));
assert.equal(session.readInputs().rate, 0.045, 'a rejected update writes nothing');

const yaml = await import('node:fs').then(fs => fs.readFileSync(new URL('../../../examples/sheetport/loan-calculator.fio.yaml', import.meta.url), 'utf8'));
const fromYaml = SheetPortSession.fromManifestYaml(yaml, wb);
assert.deepEqual(fromYaml.manifest(), session.manifest());
fromYaml.free();
session.free();
wb.free();
