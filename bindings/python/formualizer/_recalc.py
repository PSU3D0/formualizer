"""Drivers run callbacks between native steps, after releasing the engine lock."""

import asyncio
import sys
from collections.abc import Callable, Sequence

from .formualizer_py import (
    CancelToken,
    RecalcOutcome,
    RecalcResult,
    StepReport,
    Workbook,
)

_Rect = tuple[str, int, int, int, int]


def recalc(
    workbook: Workbook,
    budget_ms: float | None = 5,
    *,
    on_step: Callable[[StepReport], object] | None = None,
    priority: Sequence[_Rect] | None = None,
) -> RecalcOutcome:
    job = workbook.begin_recalc(priority=priority)
    while True:
        report = job.step(budget_ms)
        keep_going = on_step(report) if on_step is not None else None
        if report.phase == "Finished":
            return RecalcOutcome("Finished", report.result, None)
        if report.phase == "Cancelled" or not job.active:
            return RecalcOutcome("Cancelled", None, None)
        if keep_going is False:
            return RecalcOutcome("Paused", None, job)


async def evaluate_async(
    workbook: Workbook,
    budget_ms: float | None = 5,
    *,
    on_progress: Callable[[StepReport], object] | None = None,
    priority: Sequence[_Rect] | None = None,
) -> RecalcResult | None:
    job = workbook.begin_recalc(priority=priority)
    token = CancelToken()
    running = None
    try:
        while True:
            if sys.platform == "emscripten":
                report = job.step(budget_ms, cancel=token)
            else:
                running = asyncio.create_task(
                    asyncio.to_thread(job.step, budget_ms, cancel=token)
                )
                # shield keeps task cancellation from abandoning a running native step.
                report = await asyncio.shield(running)
                running = None
            if on_progress is not None:
                on_progress(report)
            if report.phase == "Finished":
                return report.result
            if report.phase == "Cancelled" or not job.active:
                return None
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        token.cancel()
        if running is not None:
            # Repeated task.cancel() must not release ownership of a running step.
            while not running.done():
                try:
                    await asyncio.shield(running)
                except asyncio.CancelledError:
                    pass
                except BaseException:
                    break
            # Retrieve any worker exception; task cancellation takes precedence.
            if running.done() and not running.cancelled():
                running.exception()
        if job.active:
            job.cancel()
        raise
    except BaseException:
        # Callback failures cannot strand an inaccessible active job.
        if job.active:
            job.cancel()
        raise
