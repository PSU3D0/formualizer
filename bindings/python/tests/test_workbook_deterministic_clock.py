"""Workbook.set_deterministic_clock pins TODAY()/NOW() on a live workbook.

An embedder holding a plain `Workbook` previously had no way to pin the
evaluation clock at all; determinism was reachable only through
`SheetPortSession.evaluate_once`. The setter must take effect on the next
recalculation of a live workbook, with no reload.
"""

import datetime

import pytest

import formualizer as fz

_PINNED = datetime.datetime(2026, 3, 14, 15, 9, 26, tzinfo=datetime.timezone.utc)


def _workbook_with(formula: str) -> fz.Workbook:
    workbook = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
    workbook.add_sheet("Sheet1")
    workbook.set_formula("Sheet1", 1, 1, formula)
    return workbook


def test_pinned_clock_fixes_today() -> None:
    workbook = _workbook_with("=TODAY()")
    workbook.set_deterministic_clock(_PINNED)
    assert workbook.evaluate_cell("Sheet1", 1, 1) == _PINNED.date()


def test_pinned_clock_fixes_now_including_the_time_fraction() -> None:
    workbook = _workbook_with("=NOW()")
    workbook.set_deterministic_clock(_PINNED)
    assert workbook.evaluate_cell("Sheet1", 1, 1) == _PINNED.replace(tzinfo=None)


def test_pin_takes_effect_on_the_next_recalc_without_reload() -> None:
    workbook = _workbook_with("=TODAY()")
    live = workbook.evaluate_cell("Sheet1", 1, 1)
    # The live wall clock cannot be reading 2026-03-14 when this suite runs;
    # the assertion below is therefore a real transition, not a coincidence.
    assert live != _PINNED.date()
    workbook.set_deterministic_clock(_PINNED)
    assert workbook.evaluate_cell("Sheet1", 1, 1) == _PINNED.date()


def test_fixed_offset_timezone_shifts_the_local_date() -> None:
    # 01:30 UTC with a -2h offset is still the previous local day.
    pinned = datetime.datetime(2026, 3, 15, 1, 30, tzinfo=datetime.timezone.utc)
    workbook = _workbook_with("=TODAY()")
    workbook.set_deterministic_clock(pinned, -7200)
    assert workbook.evaluate_cell("Sheet1", 1, 1) == datetime.date(2026, 3, 14)


def test_utc_string_timezone_spec_is_accepted() -> None:
    workbook = _workbook_with("=TODAY()")
    workbook.set_deterministic_clock(_PINNED, "utc")
    assert workbook.evaluate_cell("Sheet1", 1, 1) == _PINNED.date()


def test_malformed_timezone_spec_raises() -> None:
    workbook = _workbook_with("=TODAY()")
    with pytest.raises(TypeError):
        workbook.set_deterministic_clock(_PINNED, "mars")
