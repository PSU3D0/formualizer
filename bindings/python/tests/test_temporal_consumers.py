import datetime as dt

import pytest

import formualizer as fz


def assert_consumers(workbook):
    formulas = [
        ('TEXT(DATE(2001,1,16),"mm/dd/yyyy")', "01/16/2001"),
        ("DATE(2001,1,16)+0.5", dt.datetime(2001, 1, 16, 12)),
        ("COUNTIF(A1:A2,DATE(2001,1,16))", 2),
        ('TEXT(A1,"mm/dd/yyyy")', "01/16/2001"),
        ("COUNTIF(A1:A2,A1)", 2),
        ("SUMIF(A1:A2,A1,B1:B2)", 20),
        ("MATCH(A1,A1:A2,0)", 1),
        ("VLOOKUP(A1,A1:B2,2,FALSE)", 10),
        ("MAX(A1:A2)", 36907),
        ("MIN(A1:A2)", 36907),
        ("SUM(A1:A2)", 73814),
        ("A1>DATE(2001,1,15)", True),
        ('DATEVALUE(TEXT(A1,"mm/dd/yyyy"))', dt.date(2001, 1, 16)),
        ("INT(A3)", 36907),
        ("(A4+0.25)*24", 18),
        ("(A1+0.5)-A1", 0.5),
        ("XIRR(B5:B6,A5:A6)", 0.1),
    ]
    for row, (formula, _) in enumerate(formulas, 1):
        workbook.set_formula("S", row, 4, "=" + formula)
    workbook.set_formula("S", 1, 5, "=A1+0.5")
    workbook.set_formula("S", 2, 5, "=A7+365.25*8/12")
    workbook.set_formula("S", 3, 5, "=E2*1")
    workbook.evaluate_all()
    for row, (formula, expected) in enumerate(formulas, 1):
        got = workbook.get_value("S", row, 4)
        if isinstance(expected, (int, float)) and not isinstance(expected, bool):
            assert got == pytest.approx(expected, abs=1e-8), formula
        else:
            assert got == expected, formula
    # Native API egress is retained; fractional dates must not truncate noon.
    assert workbook.get_value("S", 1, 1) == dt.date(2001, 1, 16)
    assert workbook.get_value("S", 1, 5) == dt.datetime(2001, 1, 16, 12)
    assert workbook.get_value("S", 2, 5) == dt.datetime(2001, 12, 30, 12)
    assert workbook.get_value("S", 3, 5) == 37255.5


def values():
    return [
        (1, 1, dt.date(2001, 1, 16)),
        (2, 1, dt.date(2001, 1, 16)),
        (1, 2, 10),
        (2, 2, 10),
        (3, 1, dt.datetime(2001, 1, 16, 12)),
        (4, 1, dt.time(12)),
        (5, 1, dt.date(2002, 1, 16)),
        (5, 2, 110),
        (6, 1, dt.date(2001, 1, 16)),
        (6, 2, -100),
        (7, 1, dt.date(2001, 5, 1)),
    ]


def test_native_time_egress_preserves_whole_days():
    workbook = fz.Workbook()
    workbook.add_sheet("S")
    time = dt.time(1, 30)
    workbook.set_value("S", 1, 1, time)
    workbook.set_formula("S", 1, 2, "=A1+1")
    workbook.set_formula("S", 1, 3, "=A1-1")
    workbook.set_value("S", 2, 1, dt.timedelta(hours=25, minutes=30))
    workbook.set_value("S", 3, 1, dt.timedelta(hours=-6))
    workbook.evaluate_all()
    assert workbook.get_value("S", 1, 1) == time
    fraction = dt.timedelta(hours=1, minutes=30)
    assert workbook.get_value("S", 1, 2) == dt.timedelta(days=1) + fraction
    assert workbook.get_value("S", 1, 3) == -dt.timedelta(days=1) + fraction
    assert workbook.get_value("S", 2, 1) == dt.timedelta(hours=25, minutes=30)
    assert workbook.get_value("S", 3, 1) == dt.timedelta(hours=-6)


def test_loaded_duration_egress(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    source = openpyxl.Workbook()
    source.active.title = "S"
    for row, value in [(1, 25.5 / 24), (2, -0.25), (3, 0.5), (4, 1.25 / 86400)]:
        cell = source.active.cell(row, 1, value)
        cell.number_format = "[h]:mm" if row != 3 else "hh:mm"
    path = tmp_path / "duration.xlsx"
    source.save(path)
    workbook = fz.load_workbook(str(path))
    assert workbook.get_value("S", 1, 1) == dt.timedelta(hours=25, minutes=30)
    assert workbook.get_value("S", 2, 1) == dt.timedelta(hours=-6)
    assert workbook.get_value("S", 3, 1) == dt.time(12)
    assert workbook.get_value("S", 4, 1) == dt.timedelta(seconds=1.25)
    workbook.set_formula("S", 4, 2, "=A4*86400")
    workbook.set_formula("S", 4, 3, "=SUM(A4:A4)*86400")
    workbook.set_formula("S", 4, 4, "=COUNTIF(A4:A4,A4)")
    workbook.evaluate_all()
    assert workbook.get_value("S", 4, 2) == pytest.approx(1.25)
    assert workbook.get_value("S", 4, 3) == pytest.approx(1.25)
    assert workbook.get_value("S", 4, 4) == 1


def test_python_temporal_set_value_consumers():
    workbook = fz.Workbook()
    workbook.add_sheet("S")
    for row, col, value in values():
        workbook.set_value("S", row, col, value)
    assert_consumers(workbook)


def test_loaded_temporal_consumers(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    source = openpyxl.Workbook()
    source.active.title = "S"
    for row, col, value in values():
        source.active.cell(row, col, value)
    path = tmp_path / "temporal.xlsx"
    source.save(path)
    assert_consumers(fz.load_workbook(str(path)))
