import fs from 'node:fs';
import {execFileSync} from 'node:child_process';
const dir=new URL('../pkg-node/worker-types/',import.meta.url);fs.mkdirSync(dir,{recursive:true});
const file=new URL('consumer.ts',dir);fs.writeFileSync(file,`
import {createWorkbookWorker,type WorkerRecalcOutcome,type ViewportFrame,type WorkerStateHeader,type WorkbookWorkerClient} from '../../dist/worker.js';
import '../../dist/worker-entry.js';
declare const client:WorkbookWorkerClient;
const state:WorkerStateHeader=client.state;
const pending:boolean=client.hasInFlightMutations;
client.recalc({budgetMs:0,onStep:r=>false,signal:new AbortController().signal});
function outcome(o:WorkerRecalcOutcome){if(o.status==='Finished')o.result.computedVertices.toFixed();else if(o.status==='Paused')o.hasActiveJob;}
function frame(f:ViewportFrame){if(f.mode==='columnar')f.columnar.tags.byteLength;if(f.mode==='snapshots')f.snapshots.length;}
client.loadFromJson('{}',{spanEvaluation:true});
// @ts-expect-error No string budgets
client.stepRecalc('5');
// @ts-expect-error No unsupported sheet deletion
client.deleteSheet('S');
// @ts-expect-error No unknown modes
client.subscribeViewport({rect:{sheet:'S',startRow:1,startColumn:1,endRow:2,endColumn:2},mode:'bad',onFrame(){}});
`);execFileSync(new URL('../node_modules/.bin/tsc',import.meta.url).pathname,['--strict','--noEmit','--skipLibCheck','--target','ES2022','--module','ES2022','--moduleResolution','bundler',file.pathname],{stdio:'inherit'});console.log('worker declaration conformance passed');
