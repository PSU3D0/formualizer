#!/usr/bin/env node
const ExcelJS = require('exceljs');
const { HyperFormula } = require('hyperformula');

(async () => {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile('../corpus/synthetic/repro_chain3.xlsx');
  const ws = workbook.getWorksheet('Sheet1');
  const data = [];
  for (let r = 1; r <= 3; r += 1) {
    const row = [];
    const v = ws.getRow(r).getCell(1).value;
    if (v && typeof v === 'object' && typeof v.formula === 'string') {
      row.push(v.formula.startsWith('=') ? v.formula : `=${v.formula}`);
    } else {
      row.push(v);
    }
    data.push(row);
  }
  const hf = HyperFormula.buildFromSheets({ Sheet1: data }, { licenseKey: 'gpl-v3' });
  console.log('hyperformula:', 'A1=' + hf.getCellValue({sheet:0,row:0,col:0}), 'A2=' + hf.getCellValue({sheet:0,row:1,col:0}), 'A3=' + hf.getCellValue({sheet:0,row:2,col:0}));
})().catch((e) => { console.error(e); process.exit(1); });
