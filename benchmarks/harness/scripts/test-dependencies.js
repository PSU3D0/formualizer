const assert = require('node:assert/strict');
const test = require('node:test');
const ExcelJS = require('exceljs');
const yaml = require('js-yaml');
const { HyperFormula } = require('hyperformula');

// ExcelJS 4.x imports uuid.v4 through CommonJS for extended conditional formats.
// Exercise that exact path: loading ExcelJS alone does not generate an ID.
test('ExcelJS writes extended conditional formatting with the patched UUID', async () => {
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Sheet1');
  sheet.getCell('A1').value = 2;
  sheet.getCell('A2').value = { formula: 'A1*2', result: 4 };
  sheet.addConditionalFormatting({
    ref: 'A1:A2',
    rules: [{
      type: 'iconSet',
      iconSet: '3Stars',
      cfvo: [0, 33, 67].map(value => ({ type: 'percent', value })),
    }],
  });
  const buffer = await workbook.xlsx.writeBuffer();
  assert.match(sheet.conditionalFormattings[0].rules[0].x14Id,
    /^\{[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}\}$/);
  const reloaded = new ExcelJS.Workbook();
  await reloaded.xlsx.load(buffer);
  assert.equal(reloaded.getWorksheet('Sheet1').getCell('A1').value, 2);
  assert.deepEqual(reloaded.getWorksheet('Sheet1').getCell('A2').value,
    { formula: 'A1*2', result: 4 });
});

test('patched YAML loader retains the harness operation schema', () => {
  assert.deepEqual(yaml.load('case_id: smoke\noperations:\n  - op: evaluate_all\n'), {
    case_id: 'smoke', operations: [{ op: 'evaluate_all' }],
  });
});

test('HyperFormula still calculates a copied arithmetic formula', () => {
  const engine = HyperFormula.buildFromArray([[2], ['=A1*2']], { licenseKey: 'gpl-v3' });
  try {
    assert.equal(engine.getCellValue({ sheet: 0, row: 1, col: 0 }), 4);
  } finally {
    engine.destroy();
  }
});
