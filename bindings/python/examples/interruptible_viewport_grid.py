"""Viewport-first recalculation, scrolling and live edits.

Run: python bindings/python/examples/interruptible_viewport_grid.py
Requires a Formualizer build with interruptible recalculation support.
Budgets are soft; timings and rectangle grouping vary by machine.
"""

from time import perf_counter

import formualizer as fz

ROWS = 250
DASHBOARD = ("Dashboard", 1, 1, 6, 4)
SCROLLED = ("Cohorts", 80, 1, 90, 6)


def build_model(price=12):
    """1,525 formulas: cohort economics plus a small visible KPI panel."""
    wb = fz.Workbook()
    for sheet in ("Assumptions", "Cohorts", "Dashboard"):
        wb.add_sheet(sheet)
    wb.set_value("Assumptions", 1, 1, price)
    wb.set_value("Assumptions", 2, 1, 5)  # cost per customer
    wb.set_formula("Cohorts", 1, 1, f"=SEQUENCE({ROWS})")
    for row in range(1, ROWS + 1):
        wb.set_value("Cohorts", row, 2, row % 4)  # acquisition channel
        customers = "=100" if row == 1 else f"=C{row - 1}+1"
        wb.set_formula("Cohorts", row, 3, customers)
        wb.set_formula("Cohorts", row, 4, f"=C{row}*Assumptions!$A$1")
        wb.set_formula(
            "Cohorts",
            row,
            5,
            f"=SUMIFS(D${max(1, row - 7)}:D{row},B${max(1, row - 7)}:B{row},B{row})",
        )
        wb.set_formula("Cohorts", row, 6, f"=C{row}*Assumptions!$A$2")
        wb.set_formula("Cohorts", row, 7, f"=D{row}-F{row}")
        margin = f"=G{row}" if row == 1 else f"=H{row - 1}+G{row}"
        wb.set_formula("Cohorts", row, 8, margin)
    # The visible panel summarizes the first six cohorts, not the whole book.
    # Prioritizing a full-book total would also require all its dirty precedents.
    for row in range(1, 7):
        for col, source in enumerate(("C", "D", "G", "H"), 1):
            wb.set_formula("Dashboard", row, col, f"=Cohorts!{source}{row}")
    return wb


def snapshot(wb):
    return [
        wb.get_value(sheet, row, col)
        for sheet, rows, cols in (("Cohorts", ROWS, 8), ("Dashboard", 6, 4))
        for row in range(1, rows + 1)
        for col in range(1, cols + 1)
    ]


def main():
    # A grid normally starts from an already calculated, cached workbook.
    # Warm the graph, then dirty prices to simulate opening a what-if scenario.
    wb = build_model(price=11)
    wb.evaluate_all()
    wb.set_value("Assumptions", 1, 1, 12)
    assert wb.has_pending_in_rect(*DASHBOARD)
    rendered = scrolled = edited = scroll_rendered = False
    passes = set()
    started = perf_counter()
    with wb.begin_recalc(priority=[("Dashboard", 1, 1, 6, 4)]) as job:
        step = 0
        while job.active:
            report = job.step(budget_ms=0.5)
            step += 1
            passes.add(report.progress.pass_)
            # Repaint only intersections with the visible viewport. On delta
            # overflow, repaint visible cells on report.overflow_sheets too.
            # Sample repetitive background steps to keep the trace readable.
            if step <= 14 or step % 10 == 0 or report.phase != "Evaluating":
                print(
                    f"step {step:02d} {report.phase:13s} "
                    f"pass={report.progress.pass_} {report.step_elapsed_ms:.1f}ms "
                    f"repaint={report.changed[:2]} "
                    f"(rectangles={len(report.changed)})"
                )
            if not rendered and not wb.has_pending_in_rect(*DASHBOARD):
                pending = wb.pending_cells_in_rect("Cohorts", 7, 1, ROWS, 8)
                assert pending, "Dashboard should finish before background cohorts"
                rendered = True
                print(f"  render Dashboard now; background pending: {pending[:2]}")
                job.prioritize([("Cohorts", 80, 1, 90, 6)])
                scrolled = True
                print("  scroll -> prioritize Cohorts rows 80–90")
            elif scrolled and not edited:
                assert job.active
                wb.set_value("Assumptions", 1, 1, 15)
                edited = True
                print("  edit price: 12 -> 15; next reports rebuild the pass")
            if edited and not scroll_rendered and not wb.has_pending_in_rect(*SCROLLED):
                scroll_rendered = True
                print("  render scrolled Cohorts rows 80–90; remaining rows continue")
    elapsed_ms = (perf_counter() - started) * 1000
    assert rendered and scrolled and edited and scroll_rendered
    assert len(passes) >= 2, passes
    assert report.phase == "Finished"
    assert not wb.has_pending_in_rect(*SCROLLED)
    assert not wb.has_active_recalc_job
    baseline = build_model(price=15)
    baseline.evaluate_all()
    assert snapshot(wb) == snapshot(baseline)
    assert wb.get_value("Cohorts", ROWS, 1) == ROWS  # SEQUENCE spill
    assert wb.get_value("Cohorts", 90, 5) == 5610  # rolling channel revenue
    print(
        f"Finished: {step} steps, passes={sorted(passes)}, {elapsed_ms:.1f} ms; "
        f"Dashboard revenue={wb.get_value('Dashboard', 6, 2):.0f}; "
        "all cells match uninterrupted calculation"
    )


if __name__ == "__main__":
    main()
