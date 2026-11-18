import type { Workbook } from "formualizer";

const OUTPUT = 100;
const SHEET = "Sheet1";

export type ExampleGrid = Record<string, string | number | boolean | null>;

function inputValue(value: ExampleGrid[string]) {
  if (typeof value !== "string") return value;
  if (value === "(blank)") return null;
  if (/^"(?:[^"]|"")*"$/.test(value))
    return value.slice(1, -1).replace(/""/g, '"');
  if (value === "TRUE" || value === "FALSE") return value === "TRUE";
  return value.trim() && Number.isFinite(Number(value)) ? Number(value) : value;
}

export function evaluateExample(
  workbook: InstanceType<typeof Workbook>,
  formula: string,
  grid: ExampleGrid,
  sheets: string[] = [SHEET],
): unknown {
  workbook.setTemporalEgress("serial");

  const allSheets = new Set(sheets);
  for (const address of Object.keys(grid)) {
    if (address.includes("!")) allSheets.add(address.split("!")[0]);
  }
  const existing = new Set(workbook.sheetNames());
  for (const sheet of allSheets) {
    if (!existing.has(sheet)) workbook.addSheet(sheet);
  }

  for (const [address, value] of Object.entries(grid)) {
    const [sheet, cell] = address.includes("!")
      ? address.split("!")
      : [SHEET, address];
    const match = cell.match(/^([A-Z]+)([1-9][0-9]*)$/i);
    if (!match) continue;
    const row = Number(match[2]);
    let col = 0;
    for (const letter of match[1].toUpperCase())
      col = col * 26 + letter.charCodeAt(0) - 64;
    if (typeof value === "string" && value.startsWith("=")) {
      workbook.setFormula(sheet, row, col, value);
    } else {
      const typed = inputValue(value);
      if (typed != null) workbook.setValue(sheet, row, col, typed);
    }
  }

  workbook.setFormula(SHEET, OUTPUT, OUTPUT, formula);
  workbook.evaluateAll();
  const result: unknown = workbook.evaluateCell(SHEET, OUTPUT, OUTPUT);
  const { spill } = workbook.inspectCell({
    sheet: SHEET,
    row: OUTPUT,
    column: OUTPUT,
  }).cell;
  if (spill?.role !== "anchor") return result;

  const { startRow, startColumn, endRow, endColumn } = spill.extent;
  if (startRow === endRow && startColumn === endColumn) return result;
  const sheet = workbook.sheet(SHEET);
  try {
    return sheet.readRange(startRow, startColumn, endRow, endColumn);
  } finally {
    sheet.free();
  }
}

function displayNumber(value: number): string {
  if (value === 0) return "0";
  if (!Number.isFinite(value)) return String(value);
  const [mantissa, exponent] = value.toExponential(14).split("e");
  const power = Number(exponent);
  return power >= -6 && power <= 14
    ? Number(`${mantissa}e${exponent}`).toString()
    : `${mantissa.replace(/\.?0+$/, "")}E${exponent}`;
}

function displayComplex(value: string): string {
  const parsed = value.match(
    /^([+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?)?([+-](?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?|[+-])?([ij])$/i,
  );
  if (!parsed)
    return /^[-+]?\d/.test(value) && Number.isFinite(Number(value))
      ? displayNumber(Number(value))
      : value;
  const [, first, second, suffix] = parsed;
  if (!second) return `${first ? displayNumber(Number(first)) : ""}${suffix}`;
  const real = first ? displayNumber(Number(first)) : "";
  if (second === "+" || second === "-") return `${real}${second}${suffix}`;
  const imaginary = displayNumber(Number(second));
  return `${real}${real && !imaginary.startsWith("-") ? "+" : ""}${imaginary}${suffix}`;
}

export function displayExampleResult(
  value: unknown,
  complex = false,
  inArray = false,
): string {
  if (value == null) return "(blank)";
  if (typeof value === "number") return displayNumber(value);
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (Array.isArray(value)) {
    if (value.length === 1 && value[0].length === 1)
      return displayExampleResult(value[0][0], complex, inArray);
    const rows = value.map((row: unknown[]) =>
      row.map((cell) => displayExampleResult(cell, complex, true)).join(", "),
    );
    return `{${rows.join("; ")}}`;
  }
  if (typeof value === "string") {
    const text = complex ? displayComplex(value) : value;
    if (inArray && !/^#[A-Z0-9/?!._]+$/.test(text))
      return `"${text.replace(/"/g, '""')}"`;
    return text;
  }
  return String(value);
}

export function returnsComplexText(functionName: string): boolean {
  return functionName === "COMPLEX" || functionName.startsWith("IM");
}
