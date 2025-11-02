import pytest

try:
    import openpyxl  # type: ignore
except Exception:  # pragma: no cover - allow skipping if not present in dev env
    openpyxl = None


pytestmark = pytest.mark.skipif(openpyxl is None, reason="openpyxl not installed")


@pytest.fixture
def xlsx_builder(tmp_path):
    """
    Returns a helper that creates a temporary XLSX at a unique path using openpyxl.
    The helper accepts a function (wb -> None) to populate the workbook before saving.
    """

    def _build(populate_fn):
        path = tmp_path / "wb.xlsx"
        wb = openpyxl.Workbook()
        # Ensure a predictable first sheet name
        wb.active.title = "Sheet1"
        populate_fn(wb)
        wb.save(path)
        return path

    return _build
