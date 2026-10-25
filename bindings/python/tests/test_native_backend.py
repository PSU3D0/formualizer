"""Native is the default reader; explicit legacy names remain compatible."""

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
@pytest.mark.parametrize("backend", [None, "native", "calamine"])
def test_native_path_entrypoints(xlsx_builder, source, backend):
    path = str(xlsx_builder(_populate))
    for loader in [fz.load_workbook, fz.Workbook.load_path, fz.Workbook.from_path]:
        kwargs = {} if backend is None else {"backend": backend}
        workbook = loader(path, path_source=source, **kwargs)
        assert workbook.evaluate_cell("Sheet1", 1, 2) == 42.0
        assert workbook.evaluate_cell("Sheet1", 2, 1) == "retained text"


@pytest.mark.parametrize("backend", [None, "native", "calamine"])
def test_native_bytes_entrypoints(xlsx_builder, backend):
    payload = xlsx_builder(_populate).read_bytes()
    for loader in [fz.load_workbook_bytes, fz.Workbook.from_bytes]:
        kwargs = {} if backend is None else {"backend": backend}
        workbook = loader(payload, span_evaluation=False, **kwargs)
        assert workbook.evaluate_cell("Sheet1", 1, 2) == 42.0
        assert workbook.evaluate_cell("Sheet1", 2, 1) == "retained text"


def test_native_invalid_package_is_io_error():
    with pytest.raises(OSError, match="open failed"):
        fz.Workbook.from_bytes(b"not an XLSX package", backend="native")


@pytest.mark.parametrize("backend", ["unknown", "Calamine", "xlsx", ""])
def test_unknown_backends_are_not_silently_fallbacks(xlsx_builder, backend):
    path = str(xlsx_builder(_populate))
    for loader in [fz.load_workbook, fz.Workbook.load_path, fz.Workbook.from_path]:
        with pytest.raises(ValueError, match="Unsupported backend"):
            loader(path, backend=backend)
    for loader in [fz.load_workbook_bytes, fz.Workbook.from_bytes]:
        with pytest.raises(ValueError, match="Unsupported backend"):
            loader(b"not an XLSX package", backend=backend)


def test_legacy_reader_name_does_not_claim_a_writer():
    with pytest.raises(
        NotImplementedError, match="does not currently support XLSX byte export"
    ):
        fz.Workbook().to_xlsx_bytes(backend="calamine")
