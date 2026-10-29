// Run against both installed raw glue and the installed TypeScript facade.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {pathToFileURL} = require("node:url");
(async () => {
  const api = process.argv[5] === "ts"
    ? await import(pathToFileURL(path.resolve(process.argv[2])))
    : require(path.resolve(process.argv[2]));
  const root = process.argv[3];
  const read = name => new Uint8Array(fs.readFileSync(path.join(root,name+".xlsx")));
  const fixed = {rngSeed:42n,deterministicTimestampUtc:"2024-01-02T03:04:05Z",deterministicTimezone:"utc"};
  const options = {...fixed,profile:"workbook"};
  const call = (name,opts=options) => api.recalculateXlsxBytes(read(name),undefined,opts);
  let passed=0;
  const control = await call("gap");
  assert.equal(control.coverage,"partial");
  assert.equal(Object.getPrototypeOf(control),Object.prototype);
  let calls=0,result;
  const old = Object.getOwnPropertyDescriptor(Object.prototype,"coverage");
  Object.defineProperty(Object.prototype,"coverage",{
    get(){return "complete";},set(){calls++;},configurable:true
  });
  try {
    result=await call("gap");
    assert.equal(result.coverage,"partial");
    assert.equal(Object.hasOwn(result,"coverage"),true);
    assert.equal(calls,0);
    let deferred;
    try {await call("gap",{...options,requireComplete:true});} catch(e){deferred=e;}
    assert.equal(deferred.kind,"CalculationDeferred");
    assert.equal(deferred.reason,"KnownCoverageIncomplete");
  } finally {
    if(old)Object.defineProperty(Object.prototype,"coverage",old);
    else delete Object.prototype.coverage;
  }
  assert.equal((await call("gap")).coverage,"partial");
  passed++;
  console.log(JSON.stringify({runtime:process.argv[5]==="ts"?"ts":"raw",controlCoverage:control.coverage,
    poisonedCoverage:result.coverage,ownCoverage:Object.hasOwn(result,"coverage"),setCalls:calls,recovery:"partial"}));
  console.log("PASS own Partial, setter zero, requireComplete and clean recovery");

  // Check every named record field, including nested legacy summary/clock/link
  // records and shared-serde receipt dictionaries. Numeric array indices and
  // host typed-array internals are outside this record-property regression.
  const fixtures = [
    ["gap",options],
    ["ordinary-errors",options],
    ["external-global",{...fixed,externalLinks:"cached"}],
  ];
  let namedProperties=0;
  for(const [name,opts] of fixtures) {
    const baseline=await call(name,opts);
    const keys=new Set();
    function collect(value) {
      if(!value || typeof value!=="object" || value instanceof Uint8Array)return;
      if(Array.isArray(value)){for(const item of value)collect(item);return;}
      assert.equal(Object.getPrototypeOf(value),Object.prototype,"preserve ordinary result record prototypes");
      for(const key of Object.keys(value)){keys.add(key);collect(value[key]);}
    }
    collect(baseline);
    // Own property descriptors must also ignore inherited descriptor attributes.
    for(const key of ["value","get","set","writable","enumerable","configurable"])keys.add(key);
    for(const key of keys) {
      const prior=Object.getOwnPropertyDescriptor(Object.prototype,key);
      let setters=0,getters=0,actual;
      Object.defineProperty(Object.prototype,key,{
        get(){getters++;return "intercepted";},
        set(){setters++;},
        configurable:true,
      });
      try {actual=await call(name,opts);}
      finally {
        if(prior)Object.defineProperty(Object.prototype,key,prior);
        else delete Object.prototype[key];
      }
      assert.equal(setters,0,name+"."+key+": inherited setter must not run");
      assert.equal(getters,0,name+"."+key+": inherited getter must not run");
      assert.deepStrictEqual(actual,baseline,name+"."+key+": all own values must survive");
      for(const field of ["calculation","coverage","coverage_scope","caches_invalidated","calculation_flags_changed","operation_diagnostics",
        "bytes","summary","formula_cells","cache_cells_changed","worksheet_parts_changed","clock","seed"]) {
        const descriptor=Object.getOwnPropertyDescriptor(actual,field);
        assert(descriptor && Object.hasOwn(descriptor,"value"),field+" must be an own data property");
        assert.equal(descriptor.enumerable,true);
        assert.equal(descriptor.writable,true);
        assert.equal(descriptor.configurable,true);
      }
      if(name==="external-global")assert(Object.hasOwn(actual,"external_links"));
      namedProperties++;passed++;
    }
    assert.deepStrictEqual(await call(name,opts),baseline,name+": clean recovery");
  }
  console.log(JSON.stringify({selected:passed,passed,failed:0,skipped:0,namedProperties}));
})().catch(error=>{console.error(error);process.exitCode=1;});
