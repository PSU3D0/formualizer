import assert from 'node:assert/strict';

export function loanExample({ Workbook, SheetPortSession }, defineManifest) {
  const wb = new Workbook();
  wb.addSheet('Calculator');
  wb.setFormula('Calculator', 4, 2, '=-PMT(B2/12,360,B1)');
  const manifest = defineManifest({ id: 'loan-calculator', ports: [
    { id: 'principal', dir: 'in', location: 'Calculator!B1', type: 'number', constraints: { min: 0 } },
    { id: 'rate', dir: 'in', location: 'Calculator!B2', type: 'number' },
    { id: 'payment', dir: 'out', location: 'Calculator!B4', type: 'number' },
  ] });
  const session = SheetPortSession.fromManifest(manifest, wb);
  const yaml = SheetPortSession.fromManifestYaml(`spec: fio
spec_version: 0.3.0
manifest: {id: loan-calculator, name: loan-calculator}
ports:
- {id: principal, dir: in, shape: scalar, location: {a1: 'Calculator!B1'}, schema: {type: number}, constraints: {min: 0}}
- {id: rate, dir: in, shape: scalar, location: {a1: 'Calculator!B2'}, schema: {type: number}}
- {id: payment, dir: out, shape: scalar, location: {a1: 'Calculator!B4'}, schema: {type: number}}
`, wb);
  assert.deepEqual(session.manifest(), yaml.manifest());
  const roundtrip = SheetPortSession.fromManifest(session.manifest(), wb);
  assert.deepEqual(session.manifest(), roundtrip.manifest());
  roundtrip.free();
  for (const candidate of [session, yaml]) {
    candidate.writeInputs({ principal: 250000, rate: 0.065 });
    assert.ok(Math.abs(candidate.evaluateOnce(undefined).payment - 1580.1700587324134) < 1e-8);
  }
  yaml.free();
  session.free();
  wb.free();
}
