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

    assert sheet.get_cell(1, 1).value == 42
    assert sheet.get_cell(1, 2).value == pytest.approx(3.5)
    assert sheet.get_cell(1, 3).value is True
    assert sheet.get_cell(1, 4).value is None
    assert sheet.get_cell(1, 5).value == "hello"


def test_temporal_roundtrip():
    sheet = make_sheet()

    date_val = dt.date(2024, 5, 17)
    time_val = dt.time(9, 45, 12)
    dt_val = dt.datetime(2024, 5, 17, 9, 45, 12)
    duration = dt.timedelta(days=1, seconds=90)

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

    array_value = [[1, 2], [3, 4]]
    sheet.set_value(1, 1, array_value)
    assert sheet.get_cell(1, 1).value == array_value

    with pytest.raises(ValueError):
        sheet.set_value(1, 2, [[1, 2], [3]])


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
    sheet.set_value(2, 1, error_value)
    result = sheet.get_cell(2, 1).value

    assert isinstance(result, dict)
    assert result["type"] == "Error"
    assert result["kind"] == "Div"
    assert result["message"] == "boom"
    assert result["row"] == 5
    assert result["col"] == 7
    assert result.get("sheet") == "Sheet1"


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
