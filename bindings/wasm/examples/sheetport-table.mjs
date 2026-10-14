import assert from 'node:assert/strict';

export function tableExample({ Workbook, SheetPortSession }, defineManifest) {
  const wb = new Workbook();
  wb.addSheet('Orders');
  wb.setValue('Orders', 1, 1, 'sku');
  wb.setValue('Orders', 1, 2, 'quantity');
  const location = { layout: { sheet: 'Orders', header_row: 1, anchor_col: 'A', terminate: 'first_blank_row' } };
  const schema = { kind: 'table', columns: [{ name: 'sku', type: 'string' }, { name: 'quantity', type: 'integer' }], keys: ['sku'] };
  const manifest = defineManifest({ id: 'orders', ports: [
    { id: 'orders', dir: 'in', location, schema },
    { id: 'result', dir: 'out', location, schema },
  ] });
  const session = SheetPortSession.fromManifest(manifest, wb);
  const rows = [{ sku: 'ABC', quantity: 3 }, { sku: 'DEF', quantity: 7 }];
  session.writeInputs({ orders: rows });
  assert.deepEqual(session.evaluateOnce(undefined).result, rows);
  session.free(); wb.free();
}
