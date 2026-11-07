import { check, model } from './recalc-viewport.mjs';

/** Pause for host inspection and repair, then resume the same engine-owned job. */
export function pauseExample() {
  const wb = model(30);
  const outcome = wb.recalc({ budgetMs: 0, onStep: () => false });
  check(outcome.status === 'Paused', 'driver paused');
  const snapshot = wb.inspectCell({ sheet: 'S', row: 30, column: 1 });
  check(snapshot.cell.formula !== null, 'inspect formula while paused');
  wb.setValue('S', 1, 1, 21); // Repair the source input before resuming.
  let last;
  while (outcome.job.active) last = outcome.job.step(0);
  check(last.phase === 'Finished', 'same job resumed to completion');
  check(wb.sheet('S').getValue(30, 1) === 50, 'repaired result');
  outcome.job.free(); wb.free();
  return 'pause/inspect/repair/resume OK';
}
if (import.meta.main) console.log(pauseExample());
