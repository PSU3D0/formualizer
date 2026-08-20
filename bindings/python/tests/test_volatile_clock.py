"""TODAY()/NOW() must read the host wall clock, not the epoch fallback.

The engine falls back to a `FixedClock` at the UTC epoch when the
`system-clock` cargo feature is off. A binding built that way answers
1970-01-01 for every volatile date/time builtin, which is silently wrong
rather than an error, so it needs a test rather than a build-time check.
"""

import datetime

import formualizer as fz

# Excel's 1900 date system counts from this day, with the 1900 leap-year
# artifact already absorbed for every date after 1900-03-01.
_EXCEL_EPOCH = datetime.date(1899, 12, 30)

# The serial the epoch FixedClock produces: 1970-01-01.
_UNIX_EPOCH_SERIAL = 25569.0


def _serial(day: datetime.date) -> float:
    return float((day - _EXCEL_EPOCH).days)


def _evaluate(formula: str, *, egress: str | None = None):
    workbook = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
    if egress is not None:
        workbook.set_temporal_egress(egress)
    workbook.add_sheet("Sheet1")
    workbook.set_formula("Sheet1", 1, 1, formula)
    return workbook.evaluate_cell("Sheet1", 1, 1)


def test_today_is_the_host_date_not_the_epoch() -> None:
    # Default egress is native: TODAY() is a datetime.date, not a raw serial.
    today = _evaluate("=TODAY()")
    assert isinstance(today, datetime.date) and not isinstance(
        today, datetime.datetime
    ), f"=TODAY() returned {today!r}"
    assert today != _EXCEL_EPOCH
    # One day of slack absorbs a midnight rollover between the two reads and
    # any offset between the engine's local zone and the interpreter's.
    assert abs((today - datetime.date.today()).days) <= 1


def test_now_is_the_host_instant_not_the_epoch() -> None:
    now = _evaluate("=NOW()")
    assert isinstance(now, datetime.datetime), f"=NOW() returned {now!r}"
    assert abs((now.date() - datetime.date.today()).days) <= 1


def test_today_serial_egress_is_the_host_serial() -> None:
    # Serial egress restores the raw-serial contract for callers that opt out.
    today = _evaluate("=TODAY()", egress="serial")
    assert isinstance(today, float), f"=TODAY() under serial egress returned {today!r}"
    assert today != _UNIX_EPOCH_SERIAL
    assert abs(today - _serial(datetime.date.today())) <= 1.0


def test_year_of_today_is_the_host_year() -> None:
    assert _evaluate("=YEAR(TODAY())") == float(datetime.date.today().year)
