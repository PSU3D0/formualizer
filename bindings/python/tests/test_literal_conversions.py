import datetime as dt

import pytest

import formualizer as fz


def make_sheet():
    wb = fz.Workbook()
    return wb.sheet("Sheet1")


def test_scalar_roundtrip():
    sheet = make_sheet()

    sheet.set_value(1, 1, 42)
    sheet.set_value(1, 2, 3.5)
    sheet.set_value(1, 3, True)
    sheet.set_value(1, 4, None)
    sheet.set_value(1, 5, "hello")

    v1 = sheet.get_cell(1, 1).value
    assert isinstance(v1, float)
    assert v1 == 42.0
    assert sheet.get_cell(1, 2).value == pytest.approx(3.5)
    assert sheet.get_cell(1, 3).value is True
    assert sheet.get_cell(1, 4).value is None
    assert sheet.get_cell(1, 5).value == "hello"


def test_temporal_roundtrip():
    sheet = make_sheet()

    date_val = dt.date(2024, 5, 17)
    time_val = dt.time(9, 45, 12, 500_000)
    dt_val = dt.datetime(2024, 5, 17, 9, 45, 12, 250_000)
    duration = dt.timedelta(days=1, seconds=90, microseconds=500_000)

    sheet.set_value(1, 1, date_val)
    sheet.set_value(1, 2, time_val)
    sheet.set_value(1, 3, dt_val)
    sheet.set_value(1, 4, duration)

    assert sheet.get_cell(1, 1).value == date_val
    assert sheet.get_cell(1, 2).value == time_val
    assert sheet.get_cell(1, 3).value == dt_val
    assert sheet.get_cell(1, 4).value == duration


def test_array_roundtrip_and_validation():
    sheet = make_sheet()

    # Scalar set_value rejects arrays cleanly (ADR Audit 2 / Contract Gap 3).
    with pytest.raises(ValueError, match="use set_values"):
        sheet.set_value(1, 1, [[1, 2], [3, 4]])
    with pytest.raises(ValueError, match="use set_values"):
        sheet.set_value(1, 2, [[1, 2], [3]])
    with pytest.raises(ValueError, match="use set_values"):
        sheet.set_value(1, 3, [1, 2, 3])

    # Rectangular-array validation lives on LiteralValue.array and custom functions.
    lit = fz.LiteralValue.array(
        [
            [fz.LiteralValue.int(1), fz.LiteralValue.int(2)],
            [fz.LiteralValue.int(3), fz.LiteralValue.int(4)],
        ]
    )
    assert lit.to_python() == [[1, 2], [3, 4]]
    with pytest.raises(ValueError, match="rectangular"):
        fz.LiteralValue.array(
            [
                [fz.LiteralValue.int(1), fz.LiteralValue.int(2)],
                [fz.LiteralValue.int(3)],
            ]
        )
    with pytest.raises(ValueError, match="use set_values"):
        sheet.set_value(1, 4, lit)


def test_error_dict_roundtrip_and_context():
    sheet = make_sheet()

    error_value = {
        "type": "Error",
        "kind": "Div",
        "message": "boom",
        "row": 5,
        "col": 7,
        "sheet": "Sheet1",
    }
    # Option A: cell storage preserves ExcelErrorKind (Excel-canonical; ErrorContext
    # is an evaluation/conversion-layer artifact, tested in Rust unit tests and via
    # LiteralValue below).
    sheet.set_value(2, 1, error_value)
    stored = sheet.get_cell(2, 1).value
    assert isinstance(stored, dict)
    assert stored["type"] == "Error"
    assert stored["kind"] == "Div"

    # Message and kind round-trip directly at the LiteralValue conversion layer.
    lit_err = fz.LiteralValue.error("Div", "boom").to_python()
    assert lit_err == {"type": "Error", "kind": "Div", "message": "boom"}


@pytest.mark.parametrize(
    "kind",
    [
        "Div",
        "Ref",
        "Name",
        "Value",
        "Num",
        "Null",
        "Na",
        "Spill",
        "Calc",
        "Circ",
        "Cancelled",
        "Error",
        "NImpl",
    ],
)
def test_literal_error_kind_roundtrip(kind):
    value = fz.LiteralValue.error(kind, "boom")

    assert value.error_kind == kind


def test_legacy_literal_dicts_still_supported():
    sheet = make_sheet()

    sheet.set_value(
        1,
        1,
        {
            "type": "Date",
            "year": 2024,
            "month": 1,
            "day": 31,
        },
    )
    sheet.set_value(
        1,
        2,
        {
            "type": "Time",
            "hour": 8,
            "minute": 30,
            "second": 15,
        },
    )
    sheet.set_value(
        1,
        3,
        {
            "type": "DateTime",
            "year": 2024,
            "month": 1,
            "day": 31,
            "hour": 8,
            "minute": 30,
            "second": 15,
        },
    )
    sheet.set_value(
        1,
        4,
        {
            "type": "Duration",
            "seconds": 65,
            "microseconds": 200,
        },
    )

    assert sheet.get_cell(1, 1).value == dt.date(2024, 1, 31)
    assert sheet.get_cell(1, 2).value == dt.time(8, 30, 15)
    assert sheet.get_cell(1, 3).value == dt.datetime(2024, 1, 31, 8, 30, 15)
    assert sheet.get_cell(1, 4).value == dt.timedelta(seconds=65, microseconds=200)
