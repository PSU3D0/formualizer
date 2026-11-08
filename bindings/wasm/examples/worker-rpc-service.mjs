import {Worker} from 'node:worker_threads';
import {defineManifest} from 'formualizer';
import {createWorkbookWorker} from 'formualizer/worker';

const client=await createWorkbookWorker(new Worker(new URL('./worker-rpc-host.mjs',import.meta.url)));
try {
 await client.addSheet('Service');await client.setChangelogEnabled(true);
 await client.editBatch([
  {op:'setValue',sheet:'Service',row:1,col:1,value:3},
  {op:'setFormula',sheet:'Service',row:2,col:1,formula:'=DOUBLE(A1)'},
 ],{actionDescription:'Initialize service'});
 await client.undo();await client.redo();
 const manifest=defineManifest({id:'service',ports:[
  {id:'input',dir:'in',location:'Service!A1',type:'number'},
  {id:'output',dir:'out',location:'Service!A2',type:'number'}]});
 const session=await client.createSheetPortSession(manifest);
 console.assert((await session.evaluateOnce()).output===6);
 await session.dispose();
 await client.registerFunctionModule(new URL('./worker-functions.mjs',import.meta.url).href);
 await client.registerFunctionFromSource('TRIPLE','x => x*3',{deterministic:true});
 await client.setFormula('Service',3,1,'=TRIPLE(A2)');
 await client.beginRecalc();await client.stepRecalc(0);await client.pauseRecalc();
 await client.inspectCell({sheet:'Service',row:3,column:1});
 await client.setValue('Service',1,1,4); // Repair while the explicit job is paused.
 console.assert((await client.recalc({budgetMs:0})).status==='Finished');
 console.assert(await client.getValue('Service',3,1)===24);
 await client.setFormula('Service',4,1,'=PLUSONE(A3)');
 const controller=new AbortController();
 try {await client.recalc({budgetMs:0,signal:controller.signal,onProgress(){controller.abort();}});}
 catch(error){if(error.name!=='AbortError')throw error;}
 console.assert(!client.state.hasActiveJob);
 console.log('worker RPC service: transactions, SheetPort, UDFs, pause/repair/resume and abort passed');
} finally {await client.dispose();}
