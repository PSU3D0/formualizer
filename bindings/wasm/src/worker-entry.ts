import * as runtime from './index.js';
import { attachWorkbookWorker } from './worker.js';

if (typeof globalThis.postMessage === 'function') {
  await attachWorkbookWorker(globalThis as unknown as import('./worker.js').WorkerEndpoint, { runtime });
}
