"""A second async driver must not block the loop during job admission."""

import asyncio
import threading
import time

import pytest

import formualizer as fz


@pytest.mark.timeout(10)
@pytest.mark.parametrize("first_budget", [0, None])
def test_second_async_admission_keeps_event_loop_responsive(first_budget):
    async def scenario():
        wb = fz.Workbook()
        wb.add_sheet("S")
        entered, release = threading.Event(), threading.Event()
        release_was_delivered = []

        def blocking_udf():
            entered.set()
            # Bounded deadlock: only the event loop can release us early.
            release_was_delivered.append(release.wait(1.0))
            return 42

        wb.register_function("BLOCK", blocking_udf)
        wb.set_formula("S", 1, 1, "=BLOCK()")
        for row in range(2, 21):
            wb.set_formula("S", row, 1, f"=A{row - 1}+1")
        first = asyncio.create_task(wb.evaluate_async(first_budget))
        assert await asyncio.to_thread(entered.wait, 3)
        second = asyncio.create_task(wb.evaluate_async(None))
        # The second task is queued before this callback. A responsive async
        # admission path lets the loop run it while the UDF is still waiting.
        asyncio.get_running_loop().call_soon(release.set)
        started = time.monotonic()
        second_refused = False
        try:
            await second
        except fz.RecalcAlreadyPendingError:
            second_refused = True
        elapsed = time.monotonic() - started
        await first
        assert wb.get_value("S", 1, 1) == 42
        assert release_was_delivered == [True], (
            f"event loop could not release UDF; second admission blocked for {elapsed:.3f}s; second_refused={second_refused}"
        )
        assert second_refused, (
            "second async driver was admitted after waiting for first job to finish"
        )

    asyncio.run(scenario())


@pytest.mark.timeout(10)
def test_manual_job_refuses_async_driver_without_waiting_for_step():
    async def scenario():
        wb = fz.Workbook()
        wb.add_sheet("S")
        entered, release = threading.Event(), threading.Event()

        def blocking_udf():
            entered.set()
            assert release.wait(3)
            return 42

        wb.register_function("BLOCK", blocking_udf)
        wb.set_formula("S", 1, 1, "=BLOCK()")
        job = wb.begin_recalc()
        step = asyncio.create_task(asyncio.to_thread(job.step))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            heartbeat = asyncio.Event()
            competitor = asyncio.create_task(wb.evaluate_async())
            asyncio.get_running_loop().call_soon(heartbeat.set)
            started = time.monotonic()
            await asyncio.wait_for(heartbeat.wait(), 0.05)
            with pytest.raises(fz.RecalcAlreadyPendingError):
                await asyncio.wait_for(competitor, 0.05)
            assert time.monotonic() - started < 0.05
        finally:
            release.set()
            await step
        assert not job.active
        assert (await wb.evaluate_async()).computed_vertices == 0

    asyncio.run(scenario())


@pytest.mark.timeout(10)
def test_cancel_during_worker_admission_cleans_up_late_job():
    async def scenario():
        wb = fz.Workbook()
        wb.add_sheet("S")
        entered, release = threading.Event(), threading.Event()

        def blocking_udf():
            entered.set()
            assert release.wait(3)
            return 42

        wb.register_function("BLOCK", blocking_udf)
        wb.set_formula("S", 1, 1, "=BLOCK()")
        # An ordinary evaluator owns the lock, but has no explicit job. The
        # async driver reserves admission and its worker must wait for it.
        ordinary = asyncio.create_task(asyncio.to_thread(wb.evaluate_all))
        cancelled = None
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            cancelled = asyncio.create_task(wb.evaluate_async())
            await asyncio.sleep(0)
            heartbeat = asyncio.Event()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            started = time.monotonic()
            await asyncio.wait_for(heartbeat.wait(), 0.05)
            assert time.monotonic() - started < 0.05
            cancelled.cancel()
            await asyncio.sleep(0)
            cancelled.cancel()
            await asyncio.sleep(0)
            assert not cancelled.done(), "admission work was abandoned"
            with pytest.raises(fz.RecalcAlreadyPendingError):
                await wb.evaluate_async()
        finally:
            release.set()
            await ordinary
            if cancelled is not None:
                with pytest.raises(asyncio.CancelledError):
                    await cancelled
        assert not wb.has_active_recalc_job, "cancelled admission left an orphan job"
        assert (await wb.evaluate_async()).computed_vertices == 0
        wb.begin_recalc().cancel()

    asyncio.run(scenario())


def test_driver_reservation_released_after_finished():
    async def scenario():
        wb = fz.Workbook()
        wb.add_sheet("S")
        wb.set_formula("S", 1, 1, "=1")
        assert (await wb.evaluate_async()).computed_vertices == 1
        wb.set_formula("S", 1, 1, "=2")
        assert (await wb.evaluate_async()).computed_vertices == 1
        assert wb.get_value("S", 1, 1) == 2

    asyncio.run(scenario())


def test_failed_worker_admission_releases_driver_reservation():
    async def scenario():
        wb = fz.Workbook()
        with pytest.raises(ValueError):
            await wb.evaluate_async(priority=[("Sheet1", 0, 1, 1, 1)])
        await wb.evaluate_async()
        wb.begin_recalc().cancel()

    asyncio.run(scenario())
