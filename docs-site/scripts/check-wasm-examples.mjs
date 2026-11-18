// Run with Node 22.19+ and --experimental-wasm-modules.
// The published package is the same one the browser editor imports.
import { readdir, readFile, writeFile } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import { initializeWasm, Workbook } from "formualizer";
import {
  displayExampleResult,
  evaluateExample,
  returnsComplexText,
} from "../src/lib/wasm/example-workbook.ts";

const root = new URL("../content/docs/reference/functions/", import.meta.url);
const packageJson = JSON.parse(
  await readFile(
    new URL("../node_modules/formualizer/package.json", import.meta.url),
    "utf8",
  ),
);
const version = packageJson.version;
const compatibilityFile = new URL(
  "../src/generated/example-editor-compatibility.json",
  import.meta.url,
);
// Permit small relative differences from the native math implementation.
const numericTolerance = 1e-13;
const example =
  /<FunctionExample\s+title=\{(.+)\}\s+formula=\{(.+)\}\s+grid=\{(.+)\}\s+expected=\{(.+)\}\s*\/>/g;
const number = /^[+-]?\d+(?:\.\d+)?(?:E[+-]?\d+)?$/;
const volatileShape = {
  "Date and time serial number": (result) => typeof result === "number",
  "Date serial number": Number.isInteger,
  "Decimal from 0 up to 1": (result) =>
    typeof result === "number" && result >= 0 && result < 1,
  "Integer in the requested range": Number.isInteger,
  "Random number": (result) => typeof result === "number",
  Number: (result) => typeof result === "number",
  Date: (result) => typeof result === "number",
  "Date and time": (result) => typeof result === "number",
  "Logical value": (result) => typeof result === "boolean",
};

function matchesVolatileShape(expected, result, name) {
  if (!["NOW", "TODAY", "RAND", "RANDBETWEEN", "RANDARRAY"].includes(name))
    return false;
  if (Object.hasOwn(volatileShape, expected))
    return volatileShape[expected](result);
  const shape = /^(\d+) × (\d+) array of random numbers$/.exec(expected);
  return (
    shape &&
    Array.isArray(result) &&
    result.length === Number(shape[1]) &&
    result.every(
      (row) =>
        row.length === Number(shape[2]) &&
        row.every((cell) => typeof cell === "number"),
    )
  );
}

await initializeWasm();
const mismatches = [];
const lastDigitDifferences = [];
const unavailableFunctions = [];
let volatileExamples = 0;
let pages = 0;
let examples = 0;
for (const entry of (await readdir(root, { recursive: true })).sort()) {
  if (!entry.endsWith(".mdx")) continue;
  const body = await readFile(new URL(entry, root), "utf8");
  const name = JSON.parse(body.match(/^title: (".*")$/m)?.[1] ?? "null");
  if (!/^seoTitle:/m.test(body)) continue;
  pages++;
  let missingFunctionResults = 0;
  const matches = [...body.matchAll(example)];
  if (
    matches.length !== body.split("<FunctionExample").length - 1 ||
    matches.length === 0
  ) {
    throw new Error(`Incomplete example extraction on ${entry}`);
  }
  for (const [, , formulaSource, gridSource, expectedSource] of matches) {
    const formula = JSON.parse(formulaSource);
    const grid = JSON.parse(gridSource);
    const expected = JSON.parse(expectedSource);
    const workbook = new Workbook();
    examples++;
    try {
      const result = evaluateExample(workbook, formula, grid);
      const displayed = displayExampleResult(result, returnsComplexText(name));
      if (expected === displayed) continue;
      if (matchesVolatileShape(expected, result, name)) {
        volatileExamples++;
        continue;
      }
      if (displayed === "#NAME?") missingFunctionResults++;
      const row = { name, formula, grid, native: expected, wasm: displayed };
      const native = Number(expected);
      const closeNumeric =
        number.test(expected) &&
        typeof result === "number" &&
        Math.abs(native - result) <=
          numericTolerance * Math.max(Math.abs(native), Math.abs(result));
      (closeNumeric ? lastDigitDifferences : mismatches).push(row);
    } catch (error) {
      mismatches.push({
        name,
        formula,
        grid,
        native: expected,
        wasm: String(error),
      });
    } finally {
      workbook.free();
    }
  }
  if (missingFunctionResults === matches.length)
    unavailableFunctions.push(name);
}
const compatibility = { version, unavailableFunctions, mismatches };
console.log(
  JSON.stringify(
    {
      version,
      pages,
      examples,
      volatileExamples,
      unavailableFunctions,
      mismatches,
      lastDigitDifferences,
    },
    null,
    2,
  ),
);
if (process.argv.includes("--write")) {
  await writeFile(
    compatibilityFile,
    `${JSON.stringify(compatibility, null, 2)}\n`,
  );
} else {
  const recorded = JSON.parse(await readFile(compatibilityFile, "utf8"));
  if (!isDeepStrictEqual(recorded, compatibility)) {
    throw new Error(
      "Browser package compatibility changed. Review the report, then run pnpm check:wasm-examples --write and cargo run -p xtask -- ref --apply --allow-dirty.",
    );
  }
}
