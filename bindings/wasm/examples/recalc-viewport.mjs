import { Workbook } from 'formualizer';

export function check(condition, message) {
  if (!condition) throw new Error(message);
}
export const viewport = (first, last) => ({
  sheet: 'S', startRow: first, startColumn: 1, endRow: last, endColumn: 1,
});
export function model(rows = 120) {
  const wb = new Workbook();
  wb.addSheet('S');
  wb.setValue('S', 1, 1, 1);
  for (let row = 2; row <= rows; row++) wb.setFormula('S', row, 1, '=A' + (row - 1) + '+1');
  return wb;
}

/** A host can paint completed viewport values and keep pending cells marked. */
export function viewportExample() {
  const wb = model();
  const job = wb.beginRecalc({ priority: [viewport(80, 100)] });
  let changed = 0, scrolled = false, edited = false;
  const pendingBefore = wb.pendingCellsInRect(viewport(80, 100));
  check(pendingBefore.length > 0, 'viewport starts pending');
  while (job.active) {
    const report = job.step(0);
    changed += report.changed.length + report.overflowSheets.length;
    if (!scrolled && report.phase !== 'Finished') {
      job.prioritize([viewport(100, 120)]); // Replace priority after a scroll.
      scrolled = true;
    }
    if (!edited && report.phase !== 'Finished') {
      wb.setValue('S', 1, 1, 10); // The same job replans after host input edits.
      edited = true;
    }
  }
  check(scrolled && edited && changed > 0, 'scroll, edit and repaint reports observed');
  check(!wb.hasPendingInRect(viewport(100, 120)), 'final viewport is current');
  check(wb.sheet('S').getValue(120, 1) === 129, 'input edit reached dependent cell');
  job.free(); wb.free();
  return 'viewport priority/scroll/edit/changed/pending OK';
}
if (import.meta.main) console.log(viewportExample());
