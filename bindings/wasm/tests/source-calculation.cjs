// Run against a freshly matched wasm-bindgen --target nodejs package.
// XLSX fixtures are supplied by the Python policy suite's fixture generator.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const fz = require(path.resolve(process.argv[2]));
const fixtures = process.argv[3];
const output = process.argv[4];
fs.mkdirSync(output, { recursive: true });
const read = name => new Uint8Array(fs.readFileSync(path.join(fixtures, name + ".xlsx")));
const save = (name, result) => {
  assert(result.bytes instanceof Uint8Array);
  fs.writeFileSync(path.join(output, name + ".xlsx"), result.bytes);
  fs.writeFileSync(path.join(output, name + ".json"), JSON.stringify(result, (key, v) => key === "bytes" ? undefined : typeof v === "bigint" ? v.toString() : v, 2));
};
assert.throws(() => fz.recalculateXlsxBytes(read("partial")), /./);
const partial = fz.recalculateXlsxBytes(read("partial"), undefined, {profile: "workbook"});
assert.equal(partial.coverage, "partial");
assert.equal(partial.coverage_scope, "conservative");
assert(partial.caches_invalidated >= 2);
assert(partial.operation_diagnostics.unavailableFunctions.NOT_IMPLEMENTED.cellReasonCount >= 1);
assert("unavailableResultCells" in partial.operation_diagnostics);
save("partial", partial);
assert.throws(() => fz.recalculateXlsxBytes(read("partial"), undefined, {profile: "workbook", requireComplete: true}), e => e.kind === "CalculationDeferred" && e.reason === "KnownCoverageIncomplete" && !!e.context);
assert.throws(() => fz.recalculateXlsxBytes(read("precision"), undefined, {profile: "workbook"}), e => e.kind === "CalculationDeferred" && e.reason === "WorkbookPrecisionUnverified" && !!e.context);
const precision = fz.recalculateXlsxBytes(read("precision"));
assert.equal(precision.coverage, "partial");
assert.deepEqual(precision.operation_diagnostics.workbookFeatures, ["precision_as_displayed"]);
for (const name of ["external-array", "external-global"]) {
  assert.throws(() => fz.recalculateXlsxBytes(read(name), undefined, {profile: "workbook", externalLinks: "cached"}), e => e.kind === "CalculationDeferred");
  assert.throws(() => fz.recalculateXlsxBytes(read(name), undefined, {profile: "workbook", externalLinks: "refuse"}), e => e.kind !== "CalculationDeferred" && /external link values/.test(e.message));
}
const errors = fz.recalculateXlsxBytes(read("errors"), undefined, {profile: "workbook", requireComplete: true});
assert.equal(errors.coverage, "complete");
save("errors", errors);
assert.throws(() => fz.recalculateXlsxBytes(read("iteration"), undefined, {profile: "workbook"}), e => e.kind === "CalculationDeferred" && e.reason === "IterativeInitialStateUnverified");
const iteration = fz.recalculateXlsxBytes(read("iteration"), undefined, {profile: "workbook", calculation: {iterationInitialState: "cold_start"}});
assert.equal(iteration.calculation.iterationOrigin, "workbook");
assert.equal(iteration.calculation.maxIterations, 100);
assert.equal(iteration.calculation.maxChange, 0.001);
assert.equal(iteration.calculation.iterationInitialState, "cold_start");
save("iteration", iteration);
const calculation = {workbookSeed: "18446744073709551615", fixedNowUtc: "2024-01-02T03:04:05Z", timezone: "utc"};
const exact = fz.recalculateXlsxBytes(read("clock"), undefined, {profile: "workbook", calculation});
assert.equal(exact.calculation.workbookSeed, calculation.workbookSeed);
assert.equal(exact.seed, 18446744073709551615n);
assert.equal(exact.calculation.dateSystem, "1904");
assert(exact.clock.fixed);
const ambient = fz.recalculateXlsxBytes(read("clock"), undefined, {calculation: {timezone: "utc"}});
assert.equal(ambient.clock.timezone, "UTC");
assert.equal(ambient.clock.fixed, false);
assert.throws(() => fz.recalculateXlsxBytes(read("clock"), undefined, {calculation, deterministicTimestampUtc: "2025-01-01T00:00:00Z"}), /conflict/);
assert.throws(() => fz.recalculateXlsxBytes(read("clock"), undefined, {calculation, deterministicTimestampUtc: calculation.fixedNowUtc, deterministicTimezone: "+02:00"}), /conflict/);
assert(exact.clock.now.startsWith("2024-01-02T03:04:05"));
save("clock", exact);
assert.deepEqual(fz.recalculateXlsxBytes(read("clock"), undefined, {profile: "workbook", calculation, rngSeed: 18446744073709551615n}).bytes, exact.bytes);
assert.throws(() => fz.recalculateXlsxBytes(read("clock"), undefined, {calculation, rngSeed: 1}), /conflict/);
for (const invalid of [{typo: 1}, {requireComplete: "yes"}, {profile: "wat"}, {calculation: {unknown: 1}}, {calculation: {workbookSeed: 9007199254740992}}, {calculation: {workbookSeed: "-1"}}, {calculation: {fixedNowUtc: "bad"}}, {calculation: {iteration: {mode: "iterate", maxIterations: 0}}}, {calculation: {iteration: {mode: "iterate", maxChange: NaN}}}]) {
  assert.throws(() => fz.recalculateXlsxBytes(new Uint8Array(), undefined, invalid), e => e.kind !== "CalculationDeferred" && !/ZIP|zip|package/i.test(e.message));
}
assert.throws(() => fz.recalculateXlsxBytes(new Uint8Array([1,2,3]), undefined, {profile: "workbook"}), e => e.kind !== "CalculationDeferred");
assert.throws(() => fz.recalculateXlsxBytes(read("errors"), undefined, {profile: "workbook", calculation: {maxWorkUnits: 0}}), e => e.kind === "Engine" && e.resource_reason === "work_units" && e.limit === "0" && BigInt(e.observed) > 0n);
console.log("PASS: native WASM shared source policy, typed deferral, errors, exact clock/seed; XLSX output saved for independent reopen");
