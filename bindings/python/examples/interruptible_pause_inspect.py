"""Pause on a negative margin, inspect installed values, repair and resume.

Run: python bindings/python/examples/interruptible_pause_inspect.py
Requires a Formualizer build with interruptible recalculation support.
"""

import formualizer as fz

ROWS = 1800


def main():
    wb = fz.Workbook()
    wb.add_sheet("Assumptions")
    wb.add_sheet("Margins")
    wb.set_value("Assumptions", 1, 1, 125)  # mistakenly exceeds revenue
    wb.set_formula("Margins", 1, 2, "=100-Assumptions!A1")
    # A dependent forecast keeps work pending after the watched margin installs.
    for row in range(2, ROWS + 1):
        # Alternate formula shapes to prevent one indivisible family run.
        formula = f"=B{row - 1}+1" if row % 2 else f"=SUM(B{row - 1},1)"
        wb.set_formula("Margins", row, 2, formula)

    def callback(report):
        for sheet, r0, c0, r1, c1 in report.changed:
            if sheet == "Margins" and r0 <= 1 <= r1 and c0 <= 2 <= c1:
                margin = wb.get_value("Margins", 1, 2)
                if margin < 0:
                    print(f"Watch triggered: Margins!B1={margin}; pause for repair")
                    return False
        # With a delta overflow, inspect visible watched cells on that sheet.
        if "Margins" in report.overflow_sheets:
            margin = wb.get_value("Margins", 1, 2)
            if isinstance(margin, (int, float)) and margin < 0:
                return False
        return True

    outcome = wb.recalc(budget_ms=0.5, on_step=callback)
    assert outcome.status == "Paused"
    assert outcome.result is None
    assert outcome.job.active
    assert wb.get_value("Margins", 1, 2) == -25
    pending = wb.pending_cells_in_rect("Margins", 2, 2, ROWS, 2)
    assert pending
    print(f"Paused: job active={outcome.job.active}; pending forecast={pending[:2]}")

    wb.set_value("Assumptions", 1, 1, 80)
    print("Repair cost: 125 -> 80; resume the same job")
    passes = set()
    for report in outcome.job.iter(budget_ms=1.0):
        passes.add(report.progress.pass_)
    assert report.phase == "Finished"
    assert wb.get_value("Margins", 1, 2) == 20
    assert wb.get_value("Margins", ROWS, 2) == 20 + ROWS - 1
    assert not wb.has_active_recalc_job
    assert not wb.has_pending_in_rect("Margins", 1, 2, ROWS, 2)
    print(f"Finished: repaired margin=20; forecast={20 + ROWS - 1}; passes={sorted(passes)}")


if __name__ == "__main__":
    main()
