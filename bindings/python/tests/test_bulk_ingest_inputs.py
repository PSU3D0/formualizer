import pytest

import formualizer as fz


def test_sparse_coordinates_follow_index_protocol_and_snapshot_mapping():
    wb = fz.Workbook()

    class Row:
        def __index__(self):
            cells[(5, 6)] = "late"
            return 2

    cells = {(Row(), 3): "original"}
    wb.set_cells("S", cells)
    assert wb.get_value("S", 2, 3) == "original"
    assert wb.get_value("S", 5, 6) is None

    with pytest.raises(TypeError):
        wb.set_cells("S", [(1.5, 1, "invalid")])
    with pytest.raises(ValueError, match="1-based"):
        wb.set_cells("S", [(2**80, 1, "invalid")])


def test_column_mapping_uses_original_keys_and_values():
    wb = fz.Workbook()

    def first():
        columns.clear()
        columns["replacement"] = [99]
        yield 10

    columns = {"first": first(), "second": [20]}
    wb.set_columns("S", 1, 1, columns, headers=True)
    assert [wb.get_value("S", 1, c) for c in (1, 2)] == ["first", "second"]
    assert [wb.get_value("S", 2, c) for c in (1, 2)] == [10, 20]


def test_mutating_column_can_raise_without_partially_writing():
    wb = fz.Workbook()

    def broken():
        columns["new"] = [100]
        yield 20
        raise RuntimeError("failed to read column")

    columns = {"first": [10], "second": broken()}
    with pytest.raises(RuntimeError, match="failed to read column"):
        wb.set_columns("S", 1, 1, columns, headers=True)
    assert wb.get_value("S", 1, 1) is None


def test_numpy_scalars_and_fallback_columns():
    np = pytest.importorskip("numpy")
    wb = fz.Workbook()
    wb.set_values("S", 1, 1, np.arange(6).reshape(2, 3))
    assert [wb.get_value("S", 2, c) for c in (1, 2, 3)] == [3, 4, 5]

    wb.set_cells("S", [(np.int64(3), np.uint32(2), np.float16(1.5))])
    assert wb.get_value("S", 3, 2) == 1.5

    wb.set_columns(
        "S",
        4,
        1,
        {
            "half": np.array([0.5, -1.5], dtype="f2"),
            "ids": np.array([2**53, -(2**53)], dtype=">i8"),
        },
    )
    assert [wb.get_value("S", r, 1) for r in (4, 5)] == [0.5, -1.5]
    assert [wb.get_value("S", r, 2) for r in (4, 5)] == [2**53, -(2**53)]

    for unsafe in [np.uint64(2**64 - 1), np.int64(-(2**53) - 1)]:
        with pytest.raises(ValueError, match="ids"):
            wb.set_columns("S", 6, 1, {"ok": [1], "ids": [unsafe]})
        assert wb.get_value("S", 6, 1) is None


def test_unsupported_numpy_columns_name_the_column_and_write_nothing():
    np = pytest.importorskip("numpy")
    wb = fz.Workbook()
    for column in [
        np.array(["2025-04-03"], dtype="datetime64[D]"),
        np.array([1 + 2j]),
        np.ma.array([7], mask=[True]),
    ]:
        with pytest.raises((ValueError, TypeError), match="samples"):
            wb.set_columns("S", 1, 1, {"ok": [1], "samples": column})
        assert wb.get_value("S", 1, 1) is None


def test_arrow_chunked_columns_with_nulls_and_empty_chunks():
    pa = pytest.importorskip("pyarrow")
    wb = fz.Workbook()
    chunks = pa.chunked_array([[], ["first", None], ["last"]], type=pa.string())
    wb.set_columns("S", 1, 1, {"name": chunks}, headers=True)
    wb.set_columns("S", 2, 2, [chunks])
    assert wb.get_value("S", 1, 1) == "name"
    for col in (1, 2):
        assert [wb.get_value("S", row, col) for row in (2, 3, 4)] == [
            "first",
            None,
            "last",
        ]

    nullable_rows = pa.chunked_array([pa.array([{"x": 1}, None])])
    with pytest.raises(ValueError, match="null"):
        wb.set_columns("S", 5, 1, nullable_rows)
    assert wb.get_value("S", 5, 1) is None


def test_stream_only_column_points_to_supported_input():
    class StreamColumn:
        def __arrow_c_stream__(self, requested_schema=None):
            raise AssertionError("single-column stream requires a table or array")

    wb = fz.Workbook()
    with pytest.raises(TypeError, match="measure.*Arrow array"):
        wb.set_columns("S", 1, 1, {"measure": StreamColumn()})
    assert wb.get_value("S", 1, 1) is None
