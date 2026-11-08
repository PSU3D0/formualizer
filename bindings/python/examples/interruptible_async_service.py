"""A responsive asyncio what-if service that supersedes an unfinished request.

Run: python bindings/python/examples/interruptible_async_service.py
Requires a native Formualizer build with interruptible recalculation support.
"""

import asyncio
import threading

import formualizer as fz

ROWS = 2400
PRIORITY = [("Dashboard", 1, 1, 1, 2)]


def build_model(price=12):
    wb = fz.Workbook()
    for sheet in ("Assumptions", "Forecast", "Dashboard"):
        wb.add_sheet(sheet)
    wb.set_value("Assumptions", 1, 1, price)
    wb.set_formula("Forecast", 1, 1, "=100*Assumptions!A1")
    for row in range(2, ROWS + 1):
        # Distinct shapes split the chain into small atomic family runs.
        formula = (
            f"=A{row - 1}+Assumptions!$A$1"
            if row % 2
            else f"=SUM(A{row - 1},Assumptions!$A$1)"
        )
        wb.set_formula("Forecast", row, 1, formula)
    wb.set_formula("Dashboard", 1, 1, "=Forecast!A1")
    wb.set_formula("Dashboard", 1, 2, "=A1*0.4")
    return wb


async def service():
    wb = build_model()
    loop_thread = threading.get_ident()
    ticks = []
    stop = asyncio.Event()
    partial_ready = asyncio.Event()
    callbacks = []

    async def heartbeat():
        while not stop.is_set():
            await asyncio.sleep(0.002)
            if not stop.is_set():
                ticks.append(asyncio.get_running_loop().time())

    def first_progress(report):
        assert threading.get_ident() == loop_thread
        callbacks.append(report.phase)
        if (
            report.phase == "Evaluating"
            and not wb.has_pending_in_rect("Dashboard", 1, 1, 1, 2)
            and wb.has_pending_in_rect("Forecast", 2, 1, ROWS, 1)
        ):
            partial_ready.set()

    ticker = asyncio.create_task(heartbeat())
    first = asyncio.create_task(
        wb.evaluate_async(budget_ms=1.0, on_progress=first_progress, priority=PRIORITY)
    )
    try:
        # Wait for installed work, rather than racing a fixed cancellation delay.
        await asyncio.wait_for(partial_ready.wait(), timeout=5)
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            print("Superseded: cancellation settled; installed results preserved")
        else:
            raise AssertionError("first request finished before superseding")
        assert not wb.has_active_recalc_job
        assert wb.get_value("Dashboard", 1, 1) == 1200
        assert wb.get_value("Dashboard", 1, 2) == 480
        assert wb.pending_cells_in_rect("Forecast", 2, 1, ROWS, 1)

        wb.set_value("Assumptions", 1, 1, 15)
        replacement_phases = []

        def replacement_progress(report):
            assert threading.get_ident() == loop_thread
            replacement_phases.append(report.phase)

        result = await wb.evaluate_async(
            budget_ms=1.0, on_progress=replacement_progress, priority=PRIORITY
        )
        assert result is not None
        assert replacement_phases[-1] == "Finished"
        assert not wb.has_active_recalc_job
        assert not wb.has_pending_in_rect("Forecast", 1, 1, ROWS, 1)
        assert wb.get_value("Dashboard", 1, 1) == 1500
        assert wb.get_value("Dashboard", 1, 2) == 600
        assert wb.get_value("Forecast", ROWS, 1) == 15 * (100 + ROWS - 1)
        assert ticks, "heartbeat must run while calculation tasks are active"
        print(
            f"Finished: callbacks={len(callbacks)} cancelled + "
            f"{len(replacement_phases)} replacement; heartbeat ticks={len(ticks)}; "
            f"revenue={wb.get_value('Dashboard', 1, 1):.0f}, "
            f"margin={wb.get_value('Dashboard', 1, 2):.0f}, "
            f"forecast={wb.get_value('Forecast', ROWS, 1):.0f}"
        )
    finally:
        if not first.done():
            first.cancel()
            try:
                await first
            except asyncio.CancelledError:
                pass
        stop.set()
        await ticker


def main():
    asyncio.run(service())


if __name__ == "__main__":
    main()
