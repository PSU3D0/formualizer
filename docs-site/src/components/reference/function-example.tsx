import compatibility from "@/generated/example-editor-compatibility.json";
import { ExampleEditor } from "./example-editor";

function exampleKey(
  formula: string,
  grid: Readonly<Record<string, string | undefined>>,
) {
  return JSON.stringify([formula, Object.entries(grid).sort()]);
}

const incompatibleExamples = new Set(
  compatibility.mismatches.map(({ formula, grid }) =>
    exampleKey(formula, grid),
  ),
);

export interface FunctionExampleProps {
  title: string;
  formula: string;
  grid: Record<string, string>;
  expected: string | null;
}

function inputSheets(grid: Record<string, string>) {
  const sheets = new Map<string, Map<number, Map<string, string>>>();
  for (const [address, value] of Object.entries(grid)) {
    const match = /^(?:(.+)!)?([A-Z]+)([1-9]\d*)$/i.exec(address);
    if (!match) continue;
    const [, sheet = "Sheet1", column, rowNumber] = match;
    const rows = sheets.get(sheet) ?? new Map<number, Map<string, string>>();
    const row = Number(rowNumber);
    const cells = rows.get(row) ?? new Map<string, string>();
    cells.set(column.toUpperCase(), value);
    rows.set(row, cells);
    sheets.set(sheet, rows);
  }
  return sheets;
}

export function FunctionExample({
  title,
  formula,
  grid,
  expected,
}: FunctionExampleProps) {
  const sheets = inputSheets(grid);

  return (
    <section data-function-example>
      {title && <h3>{title}</h3>}
      <p>
        Formula <code data-example-formula>{formula}</code>
      </p>
      {Array.from(sheets, ([sheet, rows]) => {
        const columns = [
          ...new Set(
            Array.from(rows.values()).flatMap((cells) => [...cells.keys()]),
          ),
        ].sort((a, b) => a.length - b.length || a.localeCompare(b));
        const rowNumbers = [...rows.keys()].sort((a, b) => a - b);
        return (
          <table data-example-grid key={sheet}>
            <caption>
              {sheets.size > 1 || sheet !== "Sheet1"
                ? `Input cells on ${sheet}`
                : "Input cells"}
            </caption>
            <thead>
              <tr>
                <th scope="col" aria-label="Row" />
                {columns.map((column) => (
                  <th scope="col" key={column}>
                    {column}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rowNumbers.map((row) => (
                <tr key={row}>
                  <th scope="row">{row}</th>
                  {columns.map((column) => (
                    <td key={column}>{rows.get(row)?.get(column)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        );
      })}
      {expected !== null && (
        <p>
          Expected result{" "}
          <code data-example-expected>{expected === "" ? '""' : expected}</code>
        </p>
      )}
      {incompatibleExamples.has(exampleKey(formula, grid)) ? (
        <p>
          This example uses behavior that differs in the published browser
          package.
        </p>
      ) : (
        <ExampleEditor
          formula={formula}
          grid={grid}
          version={compatibility.version}
        />
      )}
    </section>
  );
}
