// Each malformed request is isolated from other cases, but recovery is tested
// on the same loaded module after every rejection.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const cases = {
  "options-map": () => new Map([["profile", "workbook"]]),
  "nested-map-nan": () => ({calculation: {iteration: new Map([["mode", "iterate"], ["maxChange", NaN]]), iterationInitialState: "cold_start"}}),
  "nested-map-infinity": () => ({calculation: {iteration: new Map([["mode", "iterate"], ["maxChange", Infinity]]), iterationInitialState: "cold_start"}}),
  "cyclic-map": () => { const x = new Map(); x.set("iteration", x); return {calculation: x}; },
  "deep-map": () => { let x = new Map(); for (let i=0;i<80;i++) x = new Map([["iteration", x]]); return {calculation: x}; },
  "plain-cycle": () => { const x = {}; x.iteration = x; return {calculation: x}; },
  "plain-depth": () => { let x = {}; for (let i=0;i<80;i++) x = {iteration: x}; return {calculation: x}; },
  "plain-nan": () => ({calculation: {iteration: {mode: "iterate", maxChange: NaN}}}),
  "plain-infinity": () => ({calculation: {iteration: {mode: "iterate", maxChange: Infinity}}}),
  "accessor-policy": () => { let count=0; return {calculation: {get iteration() { return ++count === 1 ? {} : new Map([["maxChange", NaN]]); }}}; },
  "accessor-options": () => { let count=0; return {get calculation() { return ++count === 1 ? {} : new Map(); }}; },
  "custom-prototype": () => ({calculation: Object.create({iteration: {mode: "iterate"}})}),
  "wide-object": () => ({calculation: Object.fromEntries(Array.from({length: 5000}, (_,i) => ["x"+i, i]))}),
  "top-array": () => [],
  "nested-array": () => ({calculation: {iteration: []}}),
  "nested-date": () => ({calculation: {fixedNowUtc: new Date()}}),
  "nested-symbol": () => ({calculation: {[Symbol("iteration")]: {mode: "iterate"}}}),
  "nested-bigint": () => ({calculation: {workbookSeed: 42n}}),
};
const historic = new Set(Object.keys(cases));
let getterReads = 0;
const failTrap = () => { throw new Error("reflection trap"); };
Object.assign(cases, {
  "throwing-outer-getter": () => ({ get limits() { getterReads++; throw new Error("getter"); } }),
  "throwing-limit-getter": () => ({ limits: { get maxCells() { getterReads++; throw new Error("getter"); } } }),
  "nonenumerable-outer": () => Object.defineProperty({}, "limits", {value: {}, enumerable: false}),
  "nonenumerable-limit": () => ({limits: Object.defineProperty({}, "maxCells", {value: 1})}),
  "symbol-outer": () => ({[Symbol("limits")]: {}}),
  "symbol-limit": () => ({limits: {[Symbol("maxCells")]: 1}}),
  "cyclic-limits": () => { const x = {}; x.maxCells = x; return {limits: x}; },
  "aliased-limits": () => { const x = {}; return {limits: {maxCells: x, maxEntries: x}}; },
  "limit-array": () => ({limits: []}),
  "limit-date": () => ({limits: {maxCells: new Date()}}),
  "limit-bigint": () => ({limits: {maxCells: 1n}}),
  "long-limit-string": () => ({limits: {maxCells: "x".repeat(65537)}}),
  "prototype-proxy": () => ({limits: new Proxy({}, {getPrototypeOf: failTrap})}),
  "ownkeys-proxy": () => ({limits: new Proxy({}, {ownKeys: failTrap})}),
  "descriptor-proxy": () => ({limits: new Proxy({maxCells: 1}, {getOwnPropertyDescriptor: failTrap})}),
  "revoked-proxy": () => { const x = Proxy.revocable({}, {}); x.revoke(); return {limits:x.proxy}; },
  "revoking-proxy": () => { const x = Proxy.revocable({maxCells:1}, {ownKeys(t) {x.revoke(); return ["maxCells"];}}); return {limits:x.proxy}; },
  "mutating-prototype-proxy": () => ({limits: new Proxy({maxCells:1}, {getPrototypeOf(t) {Object.setPrototypeOf(t, null); return new Date();}})}),
  "throwing-descriptor-object": () => ({limits: new Proxy({maxCells:1}, {getOwnPropertyDescriptor() {return {get value(){throw new Error("descriptor trap");}, configurable:true,enumerable:true};}})}),
});
if (require.main !== module) {
  module.exports = {cases};
} else if (process.argv[4]) {
  const fz = require(path.resolve(process.argv[2]));
  const bytes = new Uint8Array(fs.readFileSync(process.argv[3]));
  let error;
  try { fz.recalculateXlsxBytes(bytes, undefined, cases[process.argv[4]]()); }
  catch (e) { error = e; }
  // Always probe recovery, even if the bad request trapped.
  let recovery;
  try {
    const result = fz.recalculateXlsxBytes(bytes, undefined, {calculation: {workbookSeed: "42", fixedNowUtc: "2024-01-02T03:04:05Z"}});
    assert(result.bytes instanceof Uint8Array);
    assert.equal(result.calculation.workbookSeed, "42");
    recovery = "PASS";
  } catch(e) { recovery = e.name + ": " + e.message; }
  console.log(JSON.stringify({case: process.argv[4], rejection: error ? error.name + ": " + error.message : "ACCEPTED", recovery}));
  assert(error, "malformed request must reject before evaluation");
  assert(!(error instanceof WebAssembly.RuntimeError), "request must not trap WASM");
  if (historic.has(process.argv[4])) assert.match(error.message, /portable|finite|cyclic|depth|limit|accessor/, "reject at the portable request boundary");
  assert.equal(getterReads, 0, "throwing getters must never execute");
  assert.equal(recovery, "PASS", "same-module valid request must recover");
} else {
  let failures=0;
  for (const name of Object.keys(cases)) {
    const result = spawnSync(process.execPath, [__filename, process.argv[2], process.argv[3], name], {encoding:"utf8", timeout:10000});
    process.stdout.write(result.stdout || "");
    if (result.status !== 0) { failures++; process.stdout.write((result.stderr || "").slice(0, 1800)); }
  }
  assert.equal(failures, 0, failures + " request-boundary cases failed");
  // Portable null-prototype records remain supported.
  const fz = require(path.resolve(process.argv[2]));
  const bytes = new Uint8Array(fs.readFileSync(process.argv[3]));
  const calculation = Object.assign(Object.create(null), {workbookSeed: "42"});
  assert.equal(fz.recalculateXlsxBytes(bytes, undefined, {calculation}).calculation.workbookSeed, "42");
  const options = Object.assign(Object.create(null), {calculation});
  assert.equal(fz.recalculateXlsxBytes(bytes, undefined, options).calculation.workbookSeed, "42");
  let reads = 0;
  const accessor = {get calculation() { reads++; return {}; }};
  assert.throws(() => fz.recalculateXlsxBytes(bytes, undefined, accessor), /accessor/);
  assert.equal(reads, 0, "accessors must never execute");
  Object.prototype.rngSeed = 99;
  try {
    assert.equal(fz.recalculateXlsxBytes(bytes, undefined, options).seed, 42n);
  } finally { delete Object.prototype.rngSeed; }
  // Reflection traps can run. Each consumed descriptor is validated once, and
  // later mutation of the caller cannot alter the already-owned Rust request.
  let descriptors = 0;
  const target = {maxCells: 100000, maxEntries: 10000};
  const limits = new Proxy(target, {
    getOwnPropertyDescriptor(t, key) {
      descriptors++;
      const descriptor = Reflect.getOwnPropertyDescriptor(t, key);
      if (key === "maxEntries") t.maxCells = 0;
      return descriptor;
    },
  });
  assert(fz.recalculateXlsxBytes(bytes, undefined, {limits}).bytes instanceof Uint8Array);
  assert.equal(descriptors, 2);
  let ownKeys = 0;
  const mutatingKeys = new Proxy({maxCells:100000}, {
    ownKeys(t) { ownKeys++; t.maxCells = 100000; return Reflect.ownKeys(t); }
  });
  assert(fz.recalculateXlsxBytes(bytes, undefined, {limits:mutatingKeys}).bytes instanceof Uint8Array);
  assert.equal(ownKeys, 1);
  console.log("PASS: " + Object.keys(cases).length + " bounded portable request rejections and recoveries; validated proxy snapshots");
}
