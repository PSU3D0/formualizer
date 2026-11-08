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
  await client.evaluateAll(); assert.equal(await client.getValue('S',2,1),4);
  await client.undo(); assert.equal(await client.getFormula('S',2,1),undefined); await client.redo();
  await client.setValues('S',1,2,[[10],[20]]); await client.setFormulas('S',3,1,[['=A2+1']]);
  assert.deepEqual(await client.sheetNames(),['S']); await client.setTemporalEgress('serial');
  await client.addTable({name:'T',sheet:'S',range:[1,2,2,2],headers:['N']}); assert.equal((await client.getTables()).length,1);
  assert.ok(Array.isArray(await client.getNamedRanges())); await client.getEvalPlan([cell]); await client.lastCycleTelemetry();
  assert.ok((await client.tokenize('=1+2')).length); assert.ok(await client.parse('=1+2'));
  await assert.rejects(client.getValue('missing',1,1)); assert.deepEqual(await client.sheetNames(),['S']);
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
  const backing=new Uint8Array(fixture.length+8);backing.set(fixture,4);
  await client.loadFromXlsxBytes(backing.subarray(4,4+fixture.length),undefined,{transfer:true});
  assert.equal(backing.byteLength,fixture.length+8);
  const output=await client.recalculateXlsxBytes(fixture.slice(),undefined,undefined,{transfer:true});
  assert.ok(output.bytes instanceof Uint8Array);assert.ok(output.bytes.length>0);
  await client.loadFromJson(JSON.stringify({version:1,sheets:{S:{cells:[]}}}),{spanEvaluation:true});
  assert.equal(client.state.revision,0);assert.ok((await client.listFunctions()).some(f=>f.name==='DOUBLE'));
  await client.loadEmpty();assert.deepEqual(await client.sheetNames(),[]);
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
