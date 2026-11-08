import {parentPort} from 'node:worker_threads';
import * as runtime from 'formualizer';
import {attachWorkbookWorker} from 'formualizer/worker';
await attachWorkbookWorker(parentPort,{runtime,allowFunctionFromSource:true,functions:{DOUBLE:x=>x*2}});
