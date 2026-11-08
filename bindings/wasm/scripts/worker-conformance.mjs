import assert from 'node:assert/strict';
import { Worker } from 'node:worker_threads';
import { createInProcessWorkbookWorker, createWorkbookWorker, serializeWorkerError, deserializeWorkerError, prepareTransferableBytes } from '../dist/worker.js';
import * as runtime from 'formualizer';
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
const rect = { sheet:'S', startRow:1, startColumn:1, endRow:40, endColumn:2 };
const cell = {sheet:'S',row:2,column:1};
const manifest = runtime.defineManifest({id:'worker-test',ports:[
 {id:'input',dir:'in',location:'S!A1',type:'number'},
 {id:'output',dir:'out',location:'S!A2',type:'number'}]});
const fixture = readFileSync(new URL('../../../crates/formualizer-workbook/tests/fixtures/shared_formula_above_master.xlsx',import.meta.url));
for (const mode of ['channel','thread']) {
 const client = mode === 'channel' ? await createInProcessWorkbookWorker({runtime,allowFunctionFromSource:true,functions:{DOUBLE:x=>x*2}}) :
 await createWorkbookWorker(new Worker(new URL('./worker-test-host.mjs',import.meta.url)),{terminateOnDispose:true});
 try {
  await client.addSheet('S'); await client.setChangelogEnabled(true);
  await client.editBatch([{kind:'setValue',sheet:'S',row:1,column:1,value:2},{kind:'setFormula',sheet:'S',row:2,column:1,formula:'=A1*2'}],{actionDescription:'setup'});
  await client.undo(); assert.equal(await client.getFormula('S',2,1),undefined); await client.redo();
  await client.evaluateAll(); assert.equal(await client.getValue('S',2,1),4);
  await client.setValues('S',1,2,[[10],[20]]); await client.setFormulas('S',3,1,[['=A2+1']]);
  assert.deepEqual(await client.sheetNames(),['Sheet1','S']); await client.setTemporalEgress('serial');
  await client.addTable({name:'T',sheet:'S',range:[1,2,2,2],headers:['N']}); assert.equal((await client.getTables()).length,1);
  assert.ok(Array.isArray(await client.getNamedRanges())); await client.getEvalPlan([cell]); await client.lastCycleTelemetry();
  assert.ok((await client.tokenize('=1+2')).length); assert.ok(await client.parse('=1+2'));
  await assert.rejects(client.getValue('missing',1,1)); assert.deepEqual(await client.sheetNames(),['Sheet1','S']);
  await client.evaluateAll(); await client.inspectCell(cell); await client.precedents(cell); await client.dependents(cell); await client.trace([cell]); await client.rangePage(rect);
  const session=await client.createSheetPortSession(manifest);
  await session.writeInputs({input:3}); assert.equal((await session.evaluateOnce()).output,6);
  assert.equal((await client.evaluateSheetPort(manifest,{inputs:{input:4}})).output,8);
  await session.manifest(); await session.describePorts(); await session.readInputs(); await session.readOutputs(); await session.dispose();
  await client.beginRecalc(); assert.equal(client.state.recalcStatus,'paused');
  await assert.rejects(client.evaluateAll(),e=>e.code==='RECALC_JOB_ACTIVE');
  await assert.rejects(client.beginRecalc(),e=>e.code==='ALREADY_PENDING');
  await client.stepRecalc(0); await client.pauseRecalc(); await client.inspectCell(cell);
  await client.setValue('S',1,1,7); assert.equal((await client.recalc({budgetMs:0})).status,'Finished');
  const updates=[];
  const subs=[];
  for (const mode of ['values','snapshots','deltas','columnar']) subs.push(await client.subscribeViewport({rect,mode,throttleMs:0,onFrame:f=>updates.push(f)}));
  await client.setAutoRecalc(true,{budgetMs:0});
  const writes=Array.from({length:20},(_,i)=>client.setValue('S',1,1,i));
  assert.equal(client.hasInFlightMutations,true); await Promise.all(writes);
  await client.evaluateCell('S',2,1); await client.evaluateCells([cell]); await client.evaluateAll();
  assert.equal((await client.evaluateSheetPort(manifest,{inputs:{input:21}})).output,42);
  await client.recalc({budgetMs:0}); assert.equal(client.hasInFlightMutations,false);
  for(const sub of subs){await sub.setRect({...rect,endRow:3}); await sub.refresh(); await sub.unsubscribe();}
  assert.ok(updates.some(f=>f.mode==='columnar' && f.columnar.tags instanceof Uint8Array));
  assert.ok(updates.some(f=>f.mode==='snapshots' && Array.isArray(f.snapshots)));
  assert.equal(client.state.evaluatedRevision,client.state.revision);
  await client.setAutoRecalc(false);
  await client.setFormula('S',4,1,'=A3+1');
  const paused=await client.recalc({budgetMs:0,onStep:()=>false}); assert.equal(paused.status,'Paused');
  assert.equal((await client.recalc()).status,'Finished');
  await client.setFormula('S',5,1,'=A4+1');
  for await(const report of client.recalcStream({budgetMs:0})){assert.ok(report.revision>=0);}
  const controller=new AbortController();controller.abort();
  await assert.rejects(client.recalc({signal:controller.signal}),e=>e.name==='AbortError');
  await client.beginRecalc(); await client.cancelRecalc(); assert.equal(client.state.hasActiveJob,false);
  assert.ok((await client.listFunctions()).some(f=>f.name==='DOUBLE'));
  await client.registerFunctionFromSource('TRIPLE','x=>x*3'); await client.setFormula('S',6,1,'=TRIPLE(2)');assert.equal(await client.evaluateCell('S',6,1),6);
  await client.registerFunctionModule(new URL('../examples/worker-functions.mjs',import.meta.url).href);
  assert.ok((await client.listFunctions()).some(f=>f.name==='PLUSONE'));
  await client.unregisterFunction('TRIPLE');
  const fields={kind:'CalculationDeferred',code:'DEFERRED',inspect_code:'INSPECT_ERROR',explicit:true,
    workbook_kind:'Engine',excel_kind:'#VALUE!',excel_message:'detail',resource_reason:'work_units',
    limit:'18446744073709551615',observed:'18446744073709551614',request_id:'request',
    function:'UDF',panic_message:'panic',extra:{kind:'FunctionPanic',function:'UDF'},
    preparation_stale_reason:'changed',plan_stale_reason:'changed',reason:'KnownCoverageIncomplete',context:'fixture',pos:4};
  const failingModule='data:text/javascript,'+encodeURIComponent('throw Object.assign(new Error("outer",{cause:Object.assign(new Error("inner"),{kind:"Engine",limit:"18446744073709551615"})}),'+JSON.stringify(fields)+');');
  await assert.rejects(client.registerFunctionModule(failingModule),error=>{
    for(const [key,value]of Object.entries(fields))assert.deepEqual(error[key],value);
    assert.equal(error.cause.limit,fields.limit);assert.equal(error.cause.kind,'Engine');return true;
  });

  const backing=new Uint8Array(fixture.length+8);backing.set(fixture,4);
  await client.loadFromXlsxBytes(backing.subarray(4,4+fixture.length),undefined,{transfer:true});
  assert.equal(backing.byteLength,fixture.length+8);
  const output=await client.recalculateXlsxBytes(fixture.slice(),undefined,undefined,{transfer:true});
  assert.ok(output.bytes instanceof Uint8Array);assert.ok(output.bytes.length>0);
  await client.loadFromJson(JSON.stringify({version:1,sheets:{S:{cells:[]}}}),{spanEvaluation:true});
  assert.equal(client.state.revision,0);assert.ok((await client.listFunctions()).some(f=>f.name==='DOUBLE'));
  // Task turns, mid-flight cancellation, exclusivity and stream return semantics.
  await client.setFormula('S',7,1,'=A6+1');
  const stream=client.recalcStream({budgetMs:0,pauseOnReturn:true});
  await stream.next();
  await assert.rejects(client.recalc(),e=>e.code==='ALREADY_PENDING'&&e.explicit===true);
  await stream.return();assert.equal(client.state.recalcStatus,'paused');
  await client.recalc();
  await client.setFormula('S',8,1,'=A7+1');
  const stale=client.recalcStream({budgetMs:0});
  await stale.next();await client.cancelRecalc();
  await client.beginRecalc();await stale.return();
  assert.equal(client.state.hasActiveJob,true,'stale stream cleanup must preserve the replacement job');
  await client.recalc();
  await client.setFormula('S',8,1,'=A7+2');
  const mid=new AbortController();
  await assert.rejects(client.recalc({budgetMs:0,signal:mid.signal,onProgress(){mid.abort();return new Promise(()=>{});}}),e=>e.name==='AbortError');
  assert.equal(client.state.hasActiveJob,false);
  await client.recalc();
  await assert.rejects(client.inspectCell({sheet:'S',row:0,column:1}),e=>e.kind==='InspectError'&&e.code===e.inspect_code);
  // A viewport unrelated to a dirty chain must skip all intermediate steps.
  await client.addSheet('Other');
  await client.setFormulas('Other',1,1,Array.from({length:60},(_,i)=>[i?'=A'+i+'+1':'=1']));
  let unrelated=0;
  const quiet=await client.subscribeViewport({rect:{...rect,endRow:1,endColumn:1},mode:'deltas',includePendingCells:false,throttleMs:0,onFrame(){unrelated++;}});
  await client.sheetNames();const beforeQuiet=unrelated;
  await client.recalc({budgetMs:0});
  assert.equal(unrelated-beforeQuiet,1,'only terminal frame intersects an unrelated sheet');
  await quiet.unsubscribe();
  // Dirty literals in a small portion of a viewport produce patches.
  const patches=[];
  const patchSub=await client.subscribeViewport({rect,mode:'values',throttleMs:0,onFrame:f=>patches.push(f)});
  await patchSub.refresh();await client.setValue('S',20,2,123);await client.sheetNames();
  assert.ok(patches.some(f=>f.mode==='values'&&f.patches?.some(p=>p.row===20&&p.column===2&&p.value===123)));
  await patchSub.unsubscribe();
  // Hold real wire acknowledgements: rapid mutations must stay behind one credit.
  const endpoint=client.endpoint, post=endpoint.postMessage.bind(endpoint), held=[];
  endpoint.postMessage=(message,transfer)=>{if(message.type==='viewport:ack')held.push(message);else post(message,transfer);};
  let credits=0, coalesced;
  const creditSub=await client.subscribeViewport({rect,mode:'values',throttleMs:0,onFrame(frame){credits++;coalesced=frame;}});
  await Promise.all(Array.from({length:25},(_,i)=>client.setValue('S',21,2,i)));
  assert.equal(credits,1,'unacknowledged initial frame must block all mutation frames');
  endpoint.postMessage=post;for(const ack of held)post(ack);
  await client.sheetNames();
  assert.equal(credits,2,'ack flushes one coalesced frame');
  assert.ok(coalesced.patches.some(p=>p.row===21&&p.value===24));
  await creditSub.unsubscribe();
  await client.loadEmpty({maxWorkUnits:0});await client.addSheet('S');await client.setFormula('S',1,1,'=1+2');
  await assert.rejects(client.recalc(),e=>e.code==='EVALUATION'&&e.cause.resource_reason==='work_units'&&typeof e.cause.limit==='string');
  assert.equal(client.state.hasActiveJob,false);
  await client.loadEmpty();
  assert.deepEqual(await client.sheetNames(),['Sheet1']);
  console.log('worker conformance passed: '+mode);
 } finally { await client.dispose(); }
}
const source=Object.assign(new Error('outer',{cause:Object.assign(new Error('inner'),{limit:'18446744073709551615'})}),{kind:'Engine',extra:{a:1},reason:'KnownCoverageIncomplete'});
const decoded=deserializeWorkerError(serializeWorkerError(source));assert.equal(decoded.cause.limit,'18446744073709551615');assert.deepEqual(decoded.extra,{a:1});
assert.equal(deserializeWorkerError(serializeWorkerError(new DOMException('stop','AbortError'))).name,'AbortError');
assert.equal(prepareTransferableBytes(new Uint8Array(9).subarray(2,4),true).bytes.byteLength,2);
const plain=await createInProcessWorkbookWorker({runtime});
try {await assert.rejects(plain.registerFunctionFromSource('X','x=>x'),e=>e.code==='INVALID_OPTIONS');} finally{await plain.dispose();}
assert.doesNotMatch(readFileSync(new URL('../dist/worker.js',import.meta.url),'utf8'),/^import\s+(?!type).*['"](?:\.\/index\.js|\.\.\/pkg\/formualizer_wasm\.js)['"]/m);
