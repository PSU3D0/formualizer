#!/usr/bin/env node

/* eslint-disable no-console */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const ExcelJS = require('exceljs');
const { HyperFormula } = require('hyperformula');

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a.startsWith('--')) {
      args[a.slice(2)] = argv[i + 1];
      i += 1;
    }
  }
  return args;
}

function parseA1(cellRef) {
  const [sheet, a1] = cellRef.includes('!') ? cellRef.split('!') : ['Sheet1', cellRef];
  let col = 0;
  let rowDigits = '';
  for (const ch of a1) {
    if (/[A-Za-z]/.test(ch)) {
      const up = ch.toUpperCase().charCodeAt(0);
      col = col * 26 + (up - 64);
    } else if (/[0-9]/.test(ch)) {
      rowDigits += ch;
    }
  }
  const row = parseInt(rowDigits, 10);
  if (!row || !col) {
    throw new Error(`invalid A1 ref: ${cellRef}`);
  }
  return { sheet, row: row - 1, col: col - 1 };
}

function isCellError(value) {
  if (!value || typeof value !== 'object') return false;
  if (typeof value.value === 'string' && value.value.startsWith('#')) return true;
  if (typeof value.type === 'string' && typeof value.message === 'string') return true;
  return false;
}

function numericValue(value) {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const n = Number(value.replaceAll(',', ''));
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function excelCellToHyperFormula(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') return value;

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === 'object') {
    if (typeof value.formula === 'string') {
      return value.formula.startsWith('=') ? value.formula : `=${value.formula}`;
    }
    if (Array.isArray(value.richText)) {
      return value.richText.map((p) => p.text || '').join('');
    }
    if (typeof value.text === 'string') {
      return value.text;
    }
    if (typeof value.result === 'number' || typeof value.result === 'string' || typeof value.result === 'boolean') {
      return value.result;
    }
  }

  return String(value);
}

async function readWorkbookAsSheets(workbookPath) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(workbookPath);

  const sheets = {};
  for (const ws of workbook.worksheets) {
    const rowCount = ws.rowCount || 0;
    const colCount = Math.max(ws.columnCount || 1, 1);

    const data = Array.from({ length: rowCount }, () => Array(colCount).fill(null));

    for (let r = 1; r <= rowCount; r += 1) {
      const row = ws.getRow(r);
      for (let c = 1; c <= colCount; c += 1) {
        const converted = excelCellToHyperFormula(row.getCell(c).value);
        if (converted !== null && converted !== undefined) {
          data[r - 1][c - 1] = converted;
        }
      }
    }

    sheets[ws.name] = data;
  }

  return sheets;
}

function sheetIdOrThrow(hf, sheetName) {
  const id = hf.getSheetId(sheetName);
  if (id === undefined) {
    throw new Error(`sheet not found: ${sheetName}`);
  }
  return id;
}

function applyOp(hf, op) {
  switch (op.op) {
    case 'load':
      return;
    case 'evaluate_all':
      hf.rebuildAndRecalculate();
      return;
    case 'evaluate_incremental':
      hf.rebuildAndRecalculate();
      return;
    case 'edit_set_value': {
      const sid = sheetIdOrThrow(hf, op.sheet);
      hf.setCellContents({ sheet: sid, row: op.row - 1, col: op.col - 1 }, [[op.value ?? null]]);
      return;
    }
    case 'edit_set_formula': {
      const sid = sheetIdOrThrow(hf, op.sheet);
      const formula = String(op.formula || '');
      const input = formula.startsWith('=') ? formula : `=${formula}`;
      hf.setCellContents({ sheet: sid, row: op.row - 1, col: op.col - 1 }, [[input]]);
      return;
    }
    case 'add_sheet':
      hf.addSheet(op.sheet);
      return;
    case 'remove_sheet': {
      const sid = sheetIdOrThrow(hf, op.sheet);
      hf.removeSheet(sid);
      return;
    }
    case 'rename_sheet': {
      const oldName = op.old || op.sheet;
      const newName = op.new;
      if (!oldName || !newName) {
        throw new Error('rename_sheet requires old+new or sheet+new');
      }
      const sid = sheetIdOrThrow(hf, oldName);
      hf.renameSheet(sid, newName);
      return;
    }
    case 'read_cells':
      return;
    default:
      throw new Error(`unsupported op in hyperformula adapter: ${op.op}`);
  }
}

function verifyScenario(hf, scenario) {
  let mismatches = 0;
  const details = [];

  const expected = (scenario.verify && scenario.verify.expected) || {};
  for (const [cellRef, exp] of Object.entries(expected)) {
    const addr = parseA1(cellRef);
    const sid = sheetIdOrThrow(hf, addr.sheet);
    const actual = hf.getCellValue({ sheet: sid, row: addr.row, col: addr.col });

    let ok = false;
    if (typeof exp === 'number') {
      const n = numericValue(actual);
      ok = n !== null && Math.abs(n - exp) < 1e-9;
    } else if (typeof exp === 'string') {
      ok = String(actual) === exp;
    } else if (typeof exp === 'boolean') {
      ok = actual === exp;
    } else if (exp === null) {
      ok = actual === null || actual === undefined || actual === '';
    }

    if (!ok) {
      mismatches += 1;
      details.push(`expected mismatch at ${cellRef}: expected=${JSON.stringify(exp)}, actual=${JSON.stringify(actual)}`);
    }
  }

  const checks = (scenario.verify && scenario.verify.formula_checks) || [];
  for (const check of checks) {
    if (check.type !== 'non_error') continue;
    const addr = parseA1(check.cell);
    const sid = sheetIdOrThrow(hf, addr.sheet);
    const actual = hf.getCellValue({ sheet: sid, row: addr.row, col: addr.col });
    if (isCellError(actual)) {
      mismatches += 1;
      details.push(`formula check non_error failed at ${check.cell}`);
    }
  }

  return {
    passed: mismatches === 0,
    mismatches,
    details,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const scenariosPath = args.scenarios;
  const scenarioId = args.scenario;
  const repoRoot = args.root || '.';

  const doc = yaml.load(fs.readFileSync(scenariosPath, 'utf8'));
  const scenario = (doc.scenarios || []).find((s) => s.id === scenarioId);
  if (!scenario) {
    throw new Error(`unknown scenario: ${scenarioId}`);
  }

  const wbPath = path.isAbsolute(scenario.source.workbook_path)
    ? scenario.source.workbook_path
    : path.join(repoRoot, scenario.source.workbook_path);

  if (!fs.existsSync(wbPath)) {
    throw new Error(`workbook not found: ${wbPath}`);
  }

  const tLoad0 = process.hrtime.bigint();
  const sheets = await readWorkbookAsSheets(wbPath);
  const hf = HyperFormula.buildFromSheets(sheets, {
    licenseKey: 'gpl-v3',
    maxRows: 1_100_000,
    maxColumns: 20_000,
  });
  const loadMs = Number(process.hrtime.bigint() - tLoad0) / 1e6;

  let fullEvalMs = null;
  let incrementalUs = null;

  for (const op of scenario.operations || []) {
    if (op.op === 'evaluate_all') {
      const t0 = process.hrtime.bigint();
      applyOp(hf, op);
      fullEvalMs = Number(process.hrtime.bigint() - t0) / 1e6;
    } else if (op.op === 'evaluate_incremental') {
      const t0 = process.hrtime.bigint();
      applyOp(hf, op);
      incrementalUs = Number(process.hrtime.bigint() - t0) / 1e3;
    } else {
      applyOp(hf, op);
    }
  }

  const correctness = verifyScenario(hf, scenario);

  const out = {
    status: correctness.passed ? 'ok' : 'invalid',
    metrics: {
      load_ms: loadMs,
      full_eval_ms: fullEvalMs,
      incremental_us: incrementalUs,
      peak_rss_mb: process.memoryUsage().rss / (1024 * 1024),
    },
    correctness,
    notes: [],
  };

  console.log(JSON.stringify(out));
}

main().catch((err) => {
  const out = {
    status: 'failed',
    metrics: {
      load_ms: null,
      full_eval_ms: null,
      incremental_us: null,
      peak_rss_mb: null,
    },
    correctness: {
      passed: false,
      mismatches: 1,
      details: [String(err && err.message ? err.message : err)],
    },
    notes: [],
  };
  console.log(JSON.stringify(out));
  process.exit(1);
});
