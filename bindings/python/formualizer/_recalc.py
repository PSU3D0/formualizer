"""Drivers run callbacks between native steps, after releasing the engine lock."""

import asyncio
import sys
from collections.abc import Callable, Sequence
from typing import TypeVar

from .formualizer_py import (
    CancelToken,
    NoActiveJobError,
    RecalcJob,
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


_T = TypeVar("_T")


async def _settle_owned(task: asyncio.Task[_T]) -> _T:
    """Wait for owned native work despite repeated caller cancellation."""
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


def _cancel_if_active(job: RecalcJob) -> None:
    # This entire check/cancel runs off the native event-loop thread.
    try:
        if job.active:
            job.cancel()
    except NoActiveJobError:
        # Another owner may have ended this generation between check and cancel.
        pass


async def _cleanup_job(job: RecalcJob) -> None:
    if sys.platform == "emscripten":
        _cancel_if_active(job)
    else:
        await _settle_owned(
            asyncio.create_task(asyncio.to_thread(_cancel_if_active, job))
        )


def _is_active(job: RecalcJob) -> bool:
    return job.active


async def evaluate_async(
    workbook: Workbook,
    budget_ms: float | None = 5,
    *,
    on_progress: Callable[[StepReport], object] | None = None,
    priority: Sequence[_Rect] | None = None,
) -> RecalcResult | None:
    # This CAS never acquires the engine lock. Reserve before the first await,
    # covering both worker admission and the entire async driver lifetime.
    workbook._reserve_async_recalc()
    job = None
    admission = None
    running = None
    token = CancelToken()
    try:
        if sys.platform == "emscripten":
            job = workbook._begin_async_recalc(priority=priority)
        else:
            admission = asyncio.create_task(
                asyncio.to_thread(workbook._begin_async_recalc, priority=priority)
            )
            job = await asyncio.shield(admission)
            admission = None
        while True:
            if sys.platform == "emscripten":
                report = job.step(budget_ms, cancel=token)
            else:
                running = asyncio.create_task(
                    asyncio.to_thread(job.step, budget_ms, cancel=token)
                )
                report = await asyncio.shield(running)
                running = None
            if on_progress is not None:
                on_progress(report)
            if report.phase == "Finished":
                return report.result
            if report.phase == "Cancelled":
                return None
            active = (
                job.active
                if sys.platform == "emscripten"
                else await asyncio.to_thread(_is_active, job)
            )
            if not active:
                return None
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        token.cancel()
        if admission is not None:
            try:
                # Admission may be waiting for an ordinary evaluator's lock.
                # Its reservation remains owned until we receive/cancel its job.
                job = await _settle_owned(admission)
            except BaseException:
                pass  # no job was admitted; task cancellation takes precedence
        if running is not None:
            try:
                await _settle_owned(running)
            except BaseException:
                pass  # step failure also ends the engine job
        if job is not None:
            await _cleanup_job(job)
        raise
    except BaseException:
        if job is not None:
            await _cleanup_job(job)
        raise
    finally:
        # This operation is atomic only; it never waits for the workbook lock.
        workbook._release_async_recalc()
