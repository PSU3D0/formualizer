// Same contract matrix for raw Node glue and the installed TypeScript facade.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {pathToFileURL} = require("node:url");

(async () => {
  const fz = process.argv[5] === "ts"
    ? await import(pathToFileURL(path.resolve(process.argv[2])))
    : require(path.resolve(process.argv[2]));
  const root = process.argv[3], out = process.argv[4];
  fs.mkdirSync(out, {recursive:true});
  let passed = 0;
  const run = async (name, fn) => { await fn(); passed++; console.log("PASS " + name); };
  const bytes = name => new Uint8Array(fs.readFileSync(path.join(root, name + ".xlsx")));
  const call = (name, options, sample) => fz.recalculateXlsxBytes(bytes(name), sample, options);
  const workbook = {profile:"workbook", calculation:{workbookSeed:"42", fixedNowUtc:"2024-01-02T03:04:05Z", timezone:"utc"}};
  const save = (name,r) => {
    assert(r.bytes instanceof Uint8Array);
    fs.writeFileSync(path.join(out,name+".xlsx"), r.bytes);
    fs.writeFileSync(path.join(out,name+".json"), JSON.stringify(r,(k,v)=>k==="bytes"?undefined:typeof v==="bigint"?v.toString():v,2));
  };
  const reject = async (fn, predicate) => {
    let error; try { await fn(); } catch(e) { error=e; }
    assert(error && !(error instanceof WebAssembly.RuntimeError), "ordinary rejection required");
    assert(predicate(error), error.stack || error);
    return error;
  };
  for (const [name,kind] of [
    ["cell-format","unimplemented_function_mode"],
    ["caught-mode","unimplemented_function_mode"],
    ["pivot-unsupported","unimplemented_function_mode"],
    ["callable","unverified_function_mode"],
  ]) await run(name, async () => {
    const r = await call(name,workbook);
    assert.equal(r.coverage,"partial");
    assert(r.operation_diagnostics.samples.some(s=>s.operation.kind===kind));
    assert(r.operation_diagnostics.functionModes.length);
    const sampleZero=await call(name,workbook,0);
    assert.equal(sampleZero.coverage,"partial");
    assert.deepEqual(sampleZero.operation_diagnostics.samples,[]);
    assert.deepEqual(sampleZero.operation_diagnostics.functionModes,r.operation_diagnostics.functionModes);
    assert.equal(r.operation_diagnostics.physicalFunctionDispatches,null);
    assert.equal(r.operation_diagnostics.physicalUnavailableObservations,null);
    const evidence = r.operation_diagnostics.dependencyEvidence;
    assert(evidence && evidence.semanticReadsUnattested);
    assert.equal(evidence.sourceDomain,"admitted_workbook_and_fixed_cached_sources");
    assert.equal(evidence.work,evidence.queryWork+evidence.publicationMembershipWorkBound);
    save(name,r);
    await reject(()=>call(name,{...workbook,requireComplete:true}),e=>e.kind==="CalculationDeferred" && e.reason==="KnownCoverageIncomplete");
  });
  await run("unsupported-direct-array-is-typed-deferral",async()=>{
    for(const requireComplete of [false,true])
      await reject(()=>call("pivot-array",{...workbook,requireComplete}),e=>e.kind==="CalculationDeferred"&&["AffectedArrayCoverageUnverified","KnownCoverageIncomplete"].includes(e.reason));
  });
  for(const name of ["cell-invalid","inactive","cell-supported","pivot-supported","ordinary-errors"])
    await run(name,async()=>{const r=await call(name,{...workbook,requireComplete:true}); assert.equal(r.coverage,"complete"); save(name,r);});
  for(const [field,reason] of [
    ["maxOperationRoots","operation_evidence_roots"],
    ["maxOperationTextBytes","operation_evidence_text_bytes"],
    ["maxAffectedWork","work_units"],
    ["maxAffectedRegions","dependency_closure_output_items"],
    ["maxAffectedScratchBytes","scratch_memory"],
  ]) await run("fatal-"+field,async()=>{
    const error = await reject(()=>call("gap",{...workbook,requireComplete:false,limits:{[field]:0}},0),
      e=>e.kind==="Engine"&&e.resource_reason===reason&&e.limit==="0"&&BigInt(e.observed)>0n);
    if (field === "maxOperationRoots" || field === "maxOperationTextBytes") {
      const exact = Number(error.observed);
      assert.equal((await call("gap",{...workbook,limits:{[field]:exact}},0)).coverage,"partial");
      await reject(()=>call("gap",{...workbook,limits:{[field]:exact-1}},0),e=>e.kind==="Engine"&&e.resource_reason===reason);
    }
    const recovered=await call("gap",workbook,0);
    assert.equal(recovered.coverage,"partial");
    assert.deepEqual(recovered.operation_diagnostics.samples,[]);
  });
  await run("strict-query-zero-and-independent-completeness",async()=>{
    const limits={maxAffectedWork:0,maxAffectedRegions:0,maxAffectedScratchBytes:0};
    for(const requireComplete of [false,true])
      await reject(()=>call("wide",{requireComplete}),e=>e.kind!=="CalculationDeferred");
    assert.equal((await call("gap",{limits})).coverage,"partial");
    await reject(()=>call("gap",{limits,requireComplete:true}),e=>e.kind==="CalculationDeferred");
    assert.equal((await call("ordinary-errors",{limits,requireComplete:true})).coverage,"complete");
  });
  await run("telemetry-default-null-explicit-zero-and-positive",async()=>{
    for(const options of [undefined,null,{}, {physicalTelemetry:undefined},{physicalTelemetry:false},{limits:null}]) {
      assert.equal((await call("scalar",options)).operation_diagnostics.physicalFunctionDispatches,null);
    }
    for(const profile of ["strict","workbook"]) {
      assert.equal((await call("scalar",{profile,physicalTelemetry:true})).operation_diagnostics.physicalFunctionDispatches,0);
      assert((await call("function",{profile,physicalTelemetry:true})).operation_diagnostics.physicalFunctionDispatches>0);
    }
  });
  await run("preset-legacy-overrides-and-absent-forms",async()=>{
    const r=await call("wide",{...workbook,rngSeed:42n,deterministicTimestampUtc:new Date("2024-01-02T03:04:05Z"),limits:{maxCells:100000}});
    assert.equal(r.seed,42n);
    assert.equal((await call("scalar",{profile:null,calculation:null,requireComplete:null,limits:null,rngSeed:null,deterministicTimestampUtc:null,deterministicTimezone:null,externalLinks:null})).coverage,"complete");
    await reject(()=>call("wide",{...workbook,rngSeed:42n,limits:{maxCells:0}}),e=>!/conflict/.test(e.message));
    await reject(()=>call("scalar",{limits:{maxInputBytes:0}}),e=>/input.*limit/i.test(e.message));
  });
  await run("portable-limits-all-fourteen-and-invalid-values",async()=>{
    const limits={maxInputBytes:1000000,maxEntries:1000,maxExpandedBytes:1000000,maxWorksheetBytes:1000000,maxFormulaCells:10000,maxOutputBytes:1000000,maxXmlDepth:100,maxCells:10000,maxColumns:16384,maxOperationRoots:10000,maxOperationTextBytes:1000000,maxAffectedWork:1000000,maxAffectedRegions:10000,maxAffectedScratchBytes:1000000};
    assert.equal((await call("scalar",{...workbook,limits})).coverage,"complete");
    for(const value of [null,true,0.5,-1,NaN,Infinity,9007199254740992,9007199254740993,"17",[],{}]) {
      await reject(()=>fz.recalculateXlsxBytes(new Uint8Array(),undefined,{limits:{maxAffectedWork:value}}),e=>!/ZIP|zip|package/.test(e.message));
    }
    for(const limits of [{maxCells:4294967296},{maxColumns:16385},{typo:1}])
      await reject(()=>fz.recalculateXlsxBytes(new Uint8Array(),undefined,{limits}),e=>!/ZIP|zip|package/.test(e.message));
    assert((await call("scalar",{limits:{maxAffectedWork:9007199254740991,maxCells:4294967295}})).bytes instanceof Uint8Array);
    for(const physicalTelemetry of [null,0,1,"true",{},[]])
      await reject(()=>fz.recalculateXlsxBytes(new Uint8Array(),undefined,{physicalTelemetry}),e=>/boolean|portable/.test(e.message));
    for(const key of ["maxElapsedMs","maxWorkUnits"]) {
      for(const value of [9007199254740992,9007199254740993,-1,0.5,Infinity,NaN])
        await reject(()=>fz.recalculateXlsxBytes(new Uint8Array(),undefined,{calculation:{[key]:value}}),e=>!/ZIP|zip|package/.test(e.message));
    }
  });
  await run("membership-bound-and-query-work",async()=>{
    const result=await call("dynamic-scalar",workbook);
    const evidence=result.operation_diagnostics.dependencyEvidence;
    assert(evidence.publicationMembershipWorkBound>0);
    assert.equal((await call("dynamic-scalar",{...workbook,limits:{maxAffectedWork:evidence.work}})).coverage,"partial");
    await reject(()=>call("dynamic-scalar",{...workbook,limits:{maxAffectedWork:evidence.work-1}}),e=>e.kind==="Engine"&&e.resource_reason==="work_units");
    await reject(()=>call("dynamic-scalar",{...workbook,limits:{maxAffectedWork:evidence.queryWork}}),e=>e.kind==="Engine"&&e.resource_reason==="work_units");
    save("dynamic-scalar",result);
  });
  for(const name of ["dynamic-scalar","dynamic-spill"])
    await run(name,async()=>save(name,await call(name,workbook)));
  const {cases} = require("./source-request-boundary.cjs");
  for(const [name,make] of Object.entries(cases))
    await run("boundary-"+name,async()=>{
      await reject(()=>call("scalar",make()),()=>true);
      assert.equal((await call("scalar",workbook)).coverage,"complete");
    });
  await run("snapshot-zero-getter-reads-and-aliases",async()=>{
    let reads=0;
    const bad={get limits(){reads++;throw new Error("getter");}};
    await reject(()=>call("scalar",bad),e=>/accessor/.test(e.message));
    const nested={limits:{get maxCells(){reads++;throw new Error("getter");}}};
    await reject(()=>call("scalar",nested),e=>/accessor/.test(e.message));
    assert.equal(reads,0);
    const alias=Object.create(null);
    assert.equal((await call("scalar",{calculation:alias,limits:alias})).coverage,"complete");
    let descriptors=0;
    const limits=new Proxy({maxCells:10000,maxEntries:1000},{
      getOwnPropertyDescriptor(t,k) {descriptors++;const d=Reflect.getOwnPropertyDescriptor(t,k);if(k==="maxEntries")t.maxCells=0;return d;}
    });
    assert.equal((await call("scalar",{limits})).coverage,"complete");
    assert.equal(descriptors,2);
  });
  await run("inherited-descriptor-value-cannot-disguise-accessor",async()=>{
    let reads=0, error;
    const request={get calculation(){reads++;throw new Error("caller getter");}};
    Object.defineProperty(Object.prototype,"value",{value:{},configurable:true});
    try {await call("scalar",request);} catch(e){error=e;}
    finally {delete Object.prototype.value;}
    assert(error && /accessor/.test(error.message));
    assert.equal(reads,0);
    assert.equal((await call("scalar",workbook)).coverage,"complete");
    const nested={limits:{get maxCells(){reads++;throw new Error("caller getter");}}};
    Object.defineProperty(Object.prototype,"value",{get(){reads++;throw new Error("inherited getter");},configurable:true});
    try {await reject(()=>call("scalar",nested),e=>/accessor/.test(e.message));}
    finally {delete Object.prototype.value;}
    assert.equal(reads,0);
  });
  console.log(JSON.stringify({selected:passed,passed,failed:0,skipped:0}));
})().catch(e=>{console.error(e);process.exitCode=1;});
