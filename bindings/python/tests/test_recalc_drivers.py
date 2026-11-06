import asyncio
import concurrent.futures
import threading

import pytest
from test_recalc_job import chain

import formualizer as fz


def mixed():
    cfg = fz.EvaluationConfig()
    cfg.cycle_policy = "iterate"
    wb = fz.Workbook(config=fz.WorkbookConfig(eval_config=cfg))
    wb.add_sheet("S")
    wb.set_value("S", 1, 1, 1)
    for row in range(2, 31):
        wb.set_formula("S", row, 1, f"=A{row - 1}+1")
    for row in range(1, 31):
        wb.set_formula("S", row, 2, f"=A{row}*2")
    wb.set_formula("S", 1, 4, "=SEQUENCE(4)")
    wb.set_formula("S", 1, 6, '=INDIRECT("A30")+1')
    wb.set_formula("S", 1, 8, "=(I1+1)/2")
    wb.set_formula("S", 1, 9, "=(H1+1)/2")
    return wb


def values(wb):
    return [wb.get_value("S", r, c) for r in range(1, 31) for c in range(1, 10)]


@pytest.mark.parametrize("budget", [0, 1, 5, None])
@pytest.mark.parametrize("driver", ["step", "iter", "recalc", "async"])
def test_mixed_equivalence(budget, driver):
    baseline = mixed()
    baseline.evaluate_all()
    wb = mixed()
    if driver == "step":
        job = wb.begin_recalc()
        while job.active:
            report = job.step(budget)
        assert report.result.computed_vertices > 0
    elif driver == "iter":
        reports = list(wb.begin_recalc().iter(budget))
        assert reports[-1].phase == "Finished"
    elif driver == "recalc":
        assert wb.recalc(budget).status == "Finished"
    else:
        assert asyncio.run(wb.evaluate_async(budget)).computed_vertices > 0
    assert values(wb) == pytest.approx(values(baseline))


def test_lifecycle_generations_context_and_iterator():
    wb = chain()
    first = wb.begin_recalc()
    with pytest.raises(fz.RecalcAlreadyPendingError) as exc:
        wb.begin_recalc()
    assert exc.value.explicit is True
    first.cancel()
    second = wb.begin_recalc()
    assert first.active is False
    for action in [
        lambda: first.step(),
        first.cancel,
        lambda: first.prioritize([("S", 1, 1, 2, 1)]),
    ]:
        with pytest.raises(fz.NoActiveJobError):
            action()
        assert second.active
    second.cancel()
    with wb.begin_recalc() as third:
        third.step(0)
    assert not third.active
    it = wb.begin_recalc().iter(0)
    reports = list(it)
    assert reports[-1].phase == "Finished"
    assert list(it) == []


def test_step_resource_error_ends_job_and_preserves_typed_fields():
    cfg = fz.EvaluationConfig()
    cfg.max_work_units = 0
    wb = fz.Workbook(config=fz.WorkbookConfig(eval_config=cfg))
    wb.add_sheet("S")
    wb.set_formula("S", 1, 1, "=1+1")
    job = wb.begin_recalc()
    with pytest.raises(fz.ExcelEvaluationError) as exc:
        while job.active:
            job.step(0)
    assert exc.value.resource_reason == "work_units"
    assert not wb.has_active_recalc_job
    assert not job.active
    new = wb.begin_recalc()
    assert new.active
    new.cancel()


@pytest.mark.parametrize("method", ["evaluate_all", "evaluate_cell", "evaluate_cells"])
def test_all_existing_evaluation_routes_refuse_then_work(method):
    wb = chain()
    job = wb.begin_recalc()
    args = {
        "evaluate_all": (),
        "evaluate_cell": ("S", 30, 1),
        "evaluate_cells": ([("S", 30, 1)],),
    }[method]
    with pytest.raises(fz.RecalcJobActiveError) as exc:
        getattr(wb, method)(*args)
    assert isinstance(exc.value, fz.ExcelEvaluationError)
    assert exc.value.extra["kind"] == "RecalcJobActive"
    assert job.active
    job.cancel()
    getattr(wb, method)(*args)
    assert wb.get_value("S", 30, 1) == 30


def test_sheetport_nested_evaluation_refuses_then_works():
    from test_resource_errors import SHEETPORT_MANIFEST, resource_workbook

    wb = resource_workbook()
    session = fz.SheetPortSession.from_manifest_yaml(SHEETPORT_MANIFEST, wb)
    job = wb.begin_recalc()
    with pytest.raises(fz.RecalcJobActiveError):
        session.evaluate_once()
    assert job.active
    job.cancel()
    session.evaluate_once()
    assert wb.get_value("Outputs", 1, 1) == 2


def test_callback_edits_refusal_pause_and_priority():
    wb = chain()
    seen = []

    def on_step(report):
        seen.append(report)
        if len(seen) == 1:
            with pytest.raises(fz.RecalcJobActiveError):
                wb.evaluate_all()
            wb.set_value("S", 1, 1, 10)
            return False

    out = wb.recalc(0, on_step=on_step, priority=[("S", 30, 1, 30, 1)])
    assert out.status == "Paused"
    assert out.result is None
    out.job.prioritize([("S", 20, 1, 30, 1)])
    assert list(out.job.iter(0))[-1].phase == "Finished"
    assert wb.get_value("S", 30, 1) == 39


def test_pending_changed_and_frozen_snapshots():
    wb = chain()
    assert wb.has_pending_in_rect("S", 2, 1, 30, 1)
    pending = wb.pending_cells_in_rect("S", 2, 1, 30, 1)
    assert pending
    job = wb.begin_recalc()
    changed = set()
    reports = list(job.iter(0))
    for report in reports:
        for sheet, r0, c0, r1, c1 in report.changed:
            assert sheet == "S"
            changed.update((r, c) for r in range(r0, r1 + 1) for c in range(c0, c1 + 1))
        with pytest.raises(AttributeError):
            report.phase = "bogus"
        with pytest.raises(AttributeError):
            report.progress.cumulative = -1
        copy = report.changed
        copy.clear()
    assert {(r, 1) for r in range(2, 31)} <= changed
    assert not wb.has_pending_in_rect("S", 2, 1, 30, 1)
    assert wb.pending_cells_in_rect("S", 2, 1, 30, 1) == []
    assert reports[-1].result is not None
    assert all(r.result is None for r in reports[:-1])


def test_priority_coordinates_run_viewport_before_background_chain():
    wb = chain(100)
    wb.set_formula("S", 1, 2, "=777")
    job = wb.begin_recalc(priority=[("S", 1, 2, 1, 2)])
    for _ in range(10):
        report = job.step(0)
        if wb.get_value("S", 1, 2) == 777:
            break
    assert wb.get_value("S", 1, 2) == 777
    assert wb.has_pending_in_rect("S", 100, 1, 100, 1)
    assert report.longest_unit is not None
    assert report.progress.pass_ >= 1
    job.cancel()


def test_iterator_stops_after_external_cancel_and_stale_context_keeps_new_job():
    wb = chain()
    old = wb.begin_recalc()
    it = old.iter(0)
    next(it)
    old.cancel()
    new = wb.begin_recalc()
    assert list(it) == []
    old.__exit__(None, None, None)
    assert new.active
    new.cancel()


def test_udf_job_methods_raise_guard_and_token_cancel_remains_safe():
    wb = chain()
    seen = []
    token = fz.CancelToken()

    def callback():
        for operation in [
            job.cancel,
            lambda: job.step(0),
            lambda: job.active,
            lambda: job.prioritize([("S", 1, 1, 1, 1)]),
        ]:
            with pytest.raises(RuntimeError, match="custom function"):
                operation()
            seen.append(True)
        token.cancel()
        return 9

    wb.register_function("probe", callback)
    wb.set_formula("S", 1, 2, "=PROBE()")
    job = wb.begin_recalc()
    while job.active:
        report = job.step(0, cancel=token)
    assert seen == [True] * 4
    assert report.phase == "Cancelled"


def test_delta_overflow_and_disabled_collection():
    wb = chain()
    reports = list(wb.begin_recalc(max_delta_records=0).iter(0))
    assert any("S" in r.overflow_sheets for r in reports)
    wb.set_value("S", 1, 1, 2)
    assert all(
        not r.changed and not r.overflow_sheets
        for r in wb.begin_recalc(collect_deltas=False).iter(0)
    )


@pytest.mark.parametrize("budget", [-1, float("nan"), float("inf"), 1e300])
def test_invalid_budget_is_rejected_without_ending_job(budget):
    wb = chain()
    job = wb.begin_recalc()
    with pytest.raises(ValueError):
        job.step(budget)
    assert job.active
    job.cancel()


@pytest.mark.parametrize("rect", [(0, 1, 3, 1), (3, 1, 2, 1), (1, 0, 3, 1)])
def test_invalid_coordinates_do_not_begin_job(rect):
    wb = chain()
    with pytest.raises(ValueError):
        wb.begin_recalc(priority=[("S", *rect)])
    assert not wb.has_active_recalc_job
    with pytest.raises(ValueError):
        wb.has_pending_in_rect("S", *rect)


def test_async_progress_thread_and_callback_edits():
    wb = chain()
    loop_thread = threading.get_ident()
    seen = []

    def progress(report):
        assert threading.get_ident() == loop_thread
        seen.append(report)
        if len(seen) == 1:
            with pytest.raises(fz.RecalcJobActiveError):
                wb.evaluate_all()
            wb.set_value("S", 1, 1, 10)

    result = asyncio.run(wb.evaluate_async(0, on_progress=progress))
    assert len(seen) > 3
    assert result.computed_vertices > 0
    assert wb.get_value("S", 30, 1) == 39


@pytest.mark.timeout(20)
def test_async_cancellation_waits_for_native_step_then_rethrows():
    async def run():
        wb = chain()
        entered = threading.Event()
        release = threading.Event()

        def slow():
            entered.set()
            assert release.wait(5)
            return 3

        wb.register_function("slow", slow)
        wb.set_formula("S", 1, 1, "=SLOW()")
        task = asyncio.create_task(wb.evaluate_async(None))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()  # cleanup must also withstand repeated cancellation
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not wb.has_active_recalc_job
        wb.evaluate_all()
        assert wb.get_value("S", 30, 1) == 32

    asyncio.run(run())


def test_async_second_job_refused():
    async def run():
        wb = chain()
        seen = asyncio.Event()

        def progress(report):
            seen.set()

        first = asyncio.create_task(wb.evaluate_async(0, on_progress=progress))
        await seen.wait()
        with pytest.raises(fz.RecalcAlreadyPendingError):
            await wb.evaluate_async(0)
        await first
        assert wb.get_value("S", 30, 1) == 30

    asyncio.run(run())


@pytest.mark.timeout(20)
def test_reader_and_editor_threads_do_not_hold_gil_while_waiting_for_step():
    wb = chain()
    entered = threading.Event()
    release = threading.Event()

    def slow():
        entered.set()
        assert release.wait(5)
        return 3

    wb.register_function("slow", slow)
    wb.set_formula("S", 1, 1, "=SLOW()")
    job = wb.begin_recalc()
    with concurrent.futures.ThreadPoolExecutor(3) as pool:
        step = pool.submit(job.step)
        assert entered.wait(5)
        reader_started = threading.Event()
        editor_started = threading.Event()

        def read():
            reader_started.set()
            return wb.get_value("S", 30, 1)

        def edit():
            editor_started.set()
            wb.set_value("S", 1, 2, 99)

        reader = pool.submit(read)
        assert reader_started.wait(5)
        editor = pool.submit(edit)
        assert editor_started.wait(5)
        # Both contenders started while slow holds the engine lock. Waiting
        # for that lock must release the GIL so slow can return.
        release.set()
        assert step.result(timeout=5).phase == "Finished"
        assert reader.result(timeout=5) in (None, 32)
        editor.result(timeout=5)
    assert wb.get_value("S", 1, 2) == 99


def test_python_udf_raise_reentrancy_and_atomic_cancel():
    wb = chain()
    raised = []

    def raises():
        raised.append(True)
        raise ValueError("ordinary Python exception")

    wb.register_function("raises", raises)
    wb.set_formula("S", 1, 2, "=RAISES()")
    list(wb.begin_recalc().iter(0))
    error = wb.get_value("S", 1, 2)
    assert error["kind"] == "Value"
    assert raised
    # Existing storage may elide ordinary Python exception messages.
    if "message" in error:
        assert "ordinary Python exception" in error["message"]
    assert "extra" not in error

    reentrant = []

    def reenter():
        with pytest.raises(RuntimeError, match="custom function") as exc:
            wb.get_value("S", 30, 1)
        reentrant.append(exc.value)
        return 4

    wb.register_function("reenter", reenter)
    wb.set_formula("S", 1, 2, "=REENTER()")
    list(wb.begin_recalc().iter(0))
    assert reentrant
    assert wb.get_value("S", 1, 2) == 4

    wb.register_function("stopjob", lambda: (wb.cancel(), 9)[1])
    wb.set_formula("S", 1, 2, "=STOPJOB()")
    reports = list(wb.begin_recalc().iter(0))
    assert reports[-1].phase == "Cancelled"
    wb.unregister_function("stopjob")
    wb.set_formula("S", 1, 2, "=9")
    wb.evaluate_all()
    assert wb.get_value("S", 1, 2) == 9


def test_async_udf_can_cancel_workbook():
    wb = chain()
    wb.register_function("stopjob", lambda: (wb.cancel(), 9)[1])
    wb.set_formula("S", 1, 2, "=STOPJOB()")
    assert asyncio.run(wb.evaluate_async(0)) is None
    assert not wb.has_active_recalc_job


def test_refused_request_does_not_reset_job_cancel_signal():
    wb = chain()
    job = wb.begin_recalc()
    wb.cancel()
    with pytest.raises(fz.RecalcJobActiveError):
        wb.evaluate_all()
    assert job.step().phase == "Cancelled"


def test_async_callback_exception_cleans_up():
    wb = chain()

    def bad(report):
        raise LookupError("progress failed")

    with pytest.raises(LookupError, match="progress failed"):
        asyncio.run(wb.evaluate_async(0, on_progress=bad))
    assert not wb.has_active_recalc_job
    wb.evaluate_all()


@pytest.mark.xfail(
    strict=True,
    reason="FORM-000327: ordinary observed-read / future-spill routing on 0.14",
)
def test_ordinary_future_spill_matches_job():
    def workbook():
        wb = fz.Workbook()
        wb.add_sheet("S")
        wb.set_formula("S", 1, 4, "=SEQUENCE(4)")
        wb.set_formula("S", 1, 6, '=INDIRECT("D4")+1')
        return wb

    ordinary, job = workbook(), workbook()
    ordinary.evaluate_all()
    list(job.begin_recalc().iter(0))
    assert ordinary.get_value("S", 1, 6) == job.get_value("S", 1, 6) == 5


def test_pyodide_in_thread_loop(monkeypatch):
    import formualizer._recalc as drivers

    wb = chain()
    monkeypatch.setattr(drivers.sys, "platform", "emscripten")
    threads = []
    result = asyncio.run(
        wb.evaluate_async(
            0, on_progress=lambda r: threads.append(threading.get_ident())
        )
    )
    assert result.computed_vertices > 0
    assert len(threads) > 3
    assert set(threads) == {threading.get_ident()}
