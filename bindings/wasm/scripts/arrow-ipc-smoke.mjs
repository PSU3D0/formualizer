import assert from 'node:assert/strict';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import { Workbook } from '../pkg-node/arrow-ipc/formualizer_wasm.js';
import { createInProcessWorkbookWorker } from 'formualizer/worker';

const batch = tableFromArrays({ count: new Int32Array([12, 31]), label: ['ok', null] });
const bytes = tableToIPC(batch, 'stream');
const client = await createInProcessWorkbookWorker({ runtime: { Workbook, default: async () => {} } });
try {
  await client.addSheet('IPC');
  const report = await client.setArrowIpc('IPC', 2, 3, bytes, { headers: true });
  assert.deepEqual([report.startRow, report.startCol, report.endRow, report.endCol], [2, 3, 4, 4]);
  assert.equal(await client.getValue('IPC', 2, 3), 'count');
  assert.equal(await client.getValue('IPC', 3, 3), 12);
  assert.equal(await client.getValue('IPC', 4, 3), 31);
  assert.equal(await client.getValue('IPC', 3, 4), 'ok');
  assert.equal(await client.getValue('IPC', 4, 4), null);

  // The header fits the final row, but the record batch does not.
  const revision = client.state.revision;
  await assert.rejects(client.setArrowIpc('IPC', 1048576, 1, bytes, { headers: true }));
  assert.equal(await client.getValue('IPC', 1048576, 1), 'count');
  assert.equal(client.state.revision, revision + 1);
  console.log('ok - Arrow IPC stream, headers, nulls, readback and partial-write revision');
} finally {
  await client.dispose();
}
