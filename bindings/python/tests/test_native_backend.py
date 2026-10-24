"""The native XLSX reader is opt-in through the existing load entrypoints."""

import pytest

import formualizer as fz


def _populate(workbook):
    sheet = workbook.active
    sheet["A1"] = 21
    sheet["B1"] = "=A1*2"
    sheet["A2"] = "retained text"


@pytest.mark.parametrize(
    "source", [fz.XlsxPathSource.SHARED_FILE, fz.XlsxPathSource.DIRECT_MMAP]
)
def test_native_path_entrypoints(xlsx_builder, source):
    path = str(xlsx_builder(_populate))
    for loader in [fz.load_workbook, fz.Workbook.load_path, fz.Workbook.from_path]:
        workbook = loader(path, backend="native", path_source=source)
        assert workbook.evaluate_cell("Sheet1", 1, 2) == 42.0
        assert workbook.evaluate_cell("Sheet1", 2, 1) == "retained text"


def test_native_bytes_entrypoints(xlsx_builder):
    payload = xlsx_builder(_populate).read_bytes()
    for loader in [fz.load_workbook_bytes, fz.Workbook.from_bytes]:
        workbook = loader(payload, backend="native", span_evaluation=False)
        assert workbook.evaluate_cell("Sheet1", 1, 2) == 42.0
        assert workbook.evaluate_cell("Sheet1", 2, 1) == "retained text"


def test_native_invalid_package_is_io_error():
    with pytest.raises(OSError, match="open failed"):
        fz.Workbook.from_bytes(b"not an XLSX package", backend="native")
