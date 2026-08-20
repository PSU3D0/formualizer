import datetime

import formualizer as fz


def test_computed_date_native_by_default_and_serial_opt_out():
    wb = fz.Workbook()
    wb.set_formula("Sheet1", 1, 1, "=DATE(2024,12,1)")
    wb.evaluate_all()

    assert wb.get_value("Sheet1", 1, 1) == datetime.date(2024, 12, 1)

    wb.set_temporal_egress("serial")
    value = wb.get_value("Sheet1", 1, 1)
    assert isinstance(value, float)
    assert value == 45627.0
