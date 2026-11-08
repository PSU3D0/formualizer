// Compare runtimes built from the SAME current canonical source contract.
// Never use a historical whole-receipt oracle that encoded false Complete.
// Object key order is not semantic.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
(async () => {
const before = require(path.resolve(process.argv[2]));
const after = process.argv[5] === "ts"
  ? await import(require("node:url").pathToFileURL(path.resolve(process.argv[3])))
  : require(path.resolve(process.argv[3]));
const root = process.argv[4];
const read = name => new Uint8Array(fs.readFileSync(path.join(root, name + ".xlsx")));
const legacy = {rngSeed: 18446744073709551615n, deterministicTimestampUtc: "2024-01-02T03:04:05Z", deterministicTimezone: "utc"};
const calculation = {workbookSeed: "18446744073709551615", fixedNowUtc: "2024-01-02T03:04:05Z", timezone: {offsetSeconds: 7200}};
let successes = 0, failures = 0;
for (const [name, options] of [
  ["errors", legacy],
  ["clock", legacy],
  ["clock", {profile: "workbook", calculation}],
  ["partial", {profile: "workbook", calculation}],
  ["iteration", {profile: "workbook", calculation: {...calculation, iterationInitialState: "cold_start"}}],
  ["precision", legacy],
]) {
  const a = await before.recalculateXlsxBytes(read(name), 1, options);
  const b = await after.recalculateXlsxBytes(read(name), 1, options);
  assert(a.bytes instanceof Uint8Array && b.bytes instanceof Uint8Array);
  assert.deepStrictEqual(b, a, name + ": every metadata field/type, exact seed/clock strings and XLSX bytes must match");
  successes++;
}
for (const [name, options] of [
  ["partial", {profile:"workbook", calculation, requireComplete:true}],
  ["iteration", {profile:"workbook", calculation}],
  ["precision", {profile:"workbook", calculation}],
  ["external-array", {profile:"workbook", externalLinks:"refuse", calculation}],
  ["external-global", {profile:"workbook", externalLinks:"refuse", calculation}],
  ["errors", {calculation:{...calculation, maxWorkUnits:0}}],
]) {
  const capture = async api => {
    let error;
    try {await api.recalculateXlsxBytes(read(name), undefined, options);} catch(e) {error=e;}
    assert(error, name + ": must reject");
    return Object.fromEntries(["name","message","kind","reason","context","resource_reason","limit","observed","request_id"].map(key=>[key,error[key]]));
  };
  assert.deepStrictEqual(await capture(after), await capture(before), name + ": error boundary and fields must match");
  failures++;
}
console.log("PASS: deep equality of all result metadata/types/XLSX bytes for " + successes + " cases; exact error-boundary equality for " + failures + " cases");
console.log(JSON.stringify({selected:successes+failures,passed:successes+failures,failed:0,skipped:0}));
})().catch(error=>{console.error(error);process.exitCode=1;});
