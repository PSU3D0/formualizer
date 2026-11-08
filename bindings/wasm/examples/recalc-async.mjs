import { check, model } from './recalc-viewport.mjs';

/** Keep a UI/service timer running, then supersede old work with a fresh input. */
export async function asyncExample() {
  const wb = model(300), controller = new AbortController();
  let beats = 0, unfinishedBeat = false;
  const timer = setInterval(() => { beats++; }, 0);
  try {
    try {
      await wb.evaluateWithYield({
        budgetMs: 0, signal: controller.signal,
        async onProgress(report) {
          if (report.phase !== 'Finished') {
            // Simulate an awaited host/UI update; timers can run during it.
            await new Promise(resolve => setTimeout(resolve, 0));
            unfinishedBeat = beats > 0;
            controller.abort(); // Driver observes this outside engine locks.
          }
        },
      });
      throw new Error('expected abort');
    } catch (error) {
      check(error.name === 'AbortError', 'aborted generation rejects AbortError');
    }
    check(unfinishedBeat && !wb.hasActiveRecalcJob, 'heartbeat advanced before abort cleanup');
    wb.setValue('S', 1, 1, 40); // Supersede only after the old driver has settled.
    const outcome = await wb.evaluateWithYield({ budgetMs: 0 });
    check(outcome.status === 'Finished', 'replacement finished');
    check(wb.sheet('S').getValue(300, 1) === 339, 'replacement input reached final cell');
    return 'async heartbeat/abort/supersede OK';
  } finally { clearInterval(timer); wb.free(); }
}
if (import.meta.main) console.log(await asyncExample());
