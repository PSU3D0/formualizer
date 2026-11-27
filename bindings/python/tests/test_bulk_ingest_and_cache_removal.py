"""Tests for v0.16.0 Phase 3A: Python bulk ingest (`set_values`, `set_columns` w/
Arrow PyCapsule, `set_cells`) and deletion of the legacy `self.sheets` cache."""

from __future__ import annotations

import array
import ctypes
import datetime as dt
import math
from collections.abc import Mapping
from decimal import Decimal

import pytest

import formualizer as fz

pa = pytest.importorskip("pyarrow")
pl = pytest.importorskip("polars")
np = pytest.importorskip("numpy")

MAX_SAFE_INT = 2**53


class _CustomMapping(Mapping):
    def __init__(self, inner: dict):
        self._inner = inner

    def __getitem__(self, key):
        return self._inner[key]

    def __iter__(self):
        return iter(self._inner)

    def __len__(self):
        return len(self._inner)


# ---------------------------------------------------------------------------
# Legacy `self.sheets` cache deletion & B6 behavioral tests
# ---------------------------------------------------------------------------


def test_integer_reads_back_as_float_without_cache():
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_value(1, 1, 5)
    wb.set_value("S", 1, 2, -42)

    v1 = wb.get_value("S", 1, 1)
    v2 = s.get_cell(1, 1).value
    v3 = wb.get_value("S", 1, 2)
    assert type(v1) is float
    assert type(v2) is float
    assert type(v3) is float
    assert v1 == 5.0
    assert v2 == 5.0
    assert v3 == -42.0


def test_subsecond_temporal_roundtrip_without_cache():
    wb = fz.Workbook()
    s = wb.sheet("S")
    dt_val = dt.datetime(2025, 3, 15, 12, 34, 56, 789_000)
    time_val = dt.time(8, 15, 30, 500_000)
    dur_val = dt.timedelta(days=2, seconds=45, microseconds=250_000)

    s.set_value(1, 1, dt_val)
    s.set_value(1, 2, time_val)
    s.set_value(1, 3, dur_val)

    assert wb.get_value("S", 1, 1) == dt_val
    assert s.get_cell(1, 1).value == dt_val
    assert wb.get_value("S", 1, 2) == time_val
    assert s.get_cell(1, 2).value == time_val
    assert wb.get_value("S", 1, 3) == dur_val
    assert s.get_cell(1, 3).value == dur_val


def test_sheet_get_cell_formula_canonical_after_graph_build():
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_value(1, 1, 10)
    s.set_formula(1, 2, "=sum( a1:a1 )")

    # Before graph build: staged text agrees between Sheet.get_cell and Workbook.get_formula.
    assert s.get_cell(1, 2).formula == wb.get_formula("S", 1, 2)

    wb.evaluate_all()
    assert wb.get_formula("S", 1, 2) == "=SUM(A1:A1)"
    assert s.get_cell(1, 2).formula == "=SUM(A1:A1)"
    assert s.get_cell(1, 2).value == 10.0


def test_spill_visible_after_writing_none():
    """Writing None used to stick in self.sheets and shadow a later spill."""
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_value(2, 1, None)
    s.set_value(3, 1, None)
    s.set_formula(1, 1, "=SEQUENCE(3)")
    wb.evaluate_all()

    assert wb.get_value("S", 1, 1) == 1.0
    assert wb.get_value("S", 2, 1) == 2.0
    assert wb.get_value("S", 3, 1) == 3.0
    assert s.get_cell(2, 1).value == 2.0
    assert s.get_cell(3, 1).value == 3.0


@pytest.mark.parametrize("bad_int", [MAX_SAFE_INT + 1, -(MAX_SAFE_INT + 1), 2**100])
def test_unsafe_python_integers_rejected_everywhere(bad_int):
    wb = fz.Workbook()
    s = wb.sheet("S")

    # Boundary ±2^53 succeeds.
    s.set_value(1, 1, MAX_SAFE_INT)
    s.set_value(1, 2, -MAX_SAFE_INT)
    assert wb.get_value("S", 1, 1) == float(MAX_SAFE_INT)
    assert wb.get_value("S", 1, 2) == float(-MAX_SAFE_INT)

    with pytest.raises(ValueError, match="2\\^53"):
        s.set_value(2, 1, bad_int)
    with pytest.raises(ValueError, match="2\\^53"):
        s.set_values(2, 1, [[1, bad_int]])
    with pytest.raises(ValueError, match="user_id"):
        s.set_columns(2, 1, {"user_id": [1, bad_int]})
    with pytest.raises(ValueError, match="2\\^53"):
        s.set_cells([(2, 1, bad_int)])

    # Atomic preflight: row 2 was never mutated.
    assert wb.get_value("S", 2, 1) is None


def test_negative_zero_and_non_finite_semantics():
    wb = fz.Workbook()
    s = wb.sheet("S")

    s.set_values(1, 1, [[-0.0, math.nan, math.inf, -math.inf]])
    v0 = wb.get_value("S", 1, 1)
    assert isinstance(v0, float) and v0 == 0.0 and math.copysign(1.0, v0) == -1.0
    for col in (2, 3, 4):
        err = wb.get_value("S", 1, col)
        assert isinstance(err, dict) and err["kind"] == "Num"


# ---------------------------------------------------------------------------
# set_values & deprecated set_values_batch
# ---------------------------------------------------------------------------


def test_set_values_ragged_report_and_deprecated_alias():
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_value(1, 3, "keep-me")
    s.set_value(2, 3, "overwrite-me")

    rep = s.set_values(1, 1, [[1, 2], [3, 4, 5]])
    assert isinstance(rep, fz.WriteRangeReport)
    assert "WriteRangeReport(" in repr(rep)
    assert (rep.start_row, rep.start_col, rep.end_row, rep.end_col) == (1, 1, 2, 3)
    assert rep.history_cleared is False
    assert rep == s.set_values(1, 1, [[1, 2], [3, 4, 5]])

    # Short row 1 did not clobber C1!
    assert wb.get_value("S", 1, 3) == "keep-me"
    assert wb.get_value("S", 2, 3) == 5.0
    assert isinstance(wb.get_value("S", 1, 1), float)

    empty_rep = wb.set_values("S", 1, 1, [])
    assert (
        empty_rep.start_row,
        empty_rep.start_col,
        empty_rep.end_row,
        empty_rep.end_col,
    ) == (0, 0, 0, 0)

    with pytest.warns(DeprecationWarning, match="set_values"):
        batch_rep = s.set_values_batch(3, 1, 1, 2, [[10, 20]])
    assert (batch_rep.start_row, batch_rep.end_col) == (3, 2)
    assert wb.get_value("S", 3, 2) == 20.0

    with pytest.warns(DeprecationWarning, match="set_values"):
        wb_batch_rep = wb.set_values_batch("S", 4, 1, [[30, 40]])
    assert (wb_batch_rep.start_row, wb_batch_rep.end_col) == (4, 2)
    assert wb.get_value("S", 4, 1) == 30.0


def test_history_cleared_reporting_across_bulk_methods():
    wb = fz.Workbook(enable_changelog=True)
    s = wb.sheet("S")

    # Small writes fit in default 10k history_limit -> undoable, history_cleared=False.
    r1 = s.set_values(1, 1, [[1, 2], [3, 4]])
    assert r1.history_cleared is False
    assert wb.can_undo is True
    wb.undo()
    assert wb.get_value("S", 1, 1) is None

    r2 = s.set_columns(1, 1, {"a": [10, 20], "b": [30, 40]}, headers=True)
    assert r2.history_cleared is False
    assert wb.get_value("S", 1, 1) == "a"
    assert wb.get_value("S", 2, 1) == 10.0
    wb.undo()
    assert wb.get_value("S", 1, 1) is None
    assert wb.get_value("S", 2, 1) is None

    r3 = s.set_cells([(1, 1, 99), (2, 2, "ok")])
    assert r3.history_cleared is False
    wb.undo()
    assert wb.get_value("S", 1, 1) is None

    # Oversized write (> 10,000 events) takes fast path and clears history.
    s.set_value(1, 1, "seed")
    assert wb.can_undo is True
    big = {"x": list(range(6000)), "y": list(range(6000))}
    r_big = s.set_columns(1, 1, big, headers=True)
    assert r_big.history_cleared is True
    assert wb.changelog_enabled is True
    assert wb.can_undo is False
    assert wb.get_value("S", 1, 1) == "x"
    assert wb.get_value("S", 6001, 2) == 5999.0


# ---------------------------------------------------------------------------
# set_columns: Mapping, sequences, buffer protocol (array/memoryview/numpy)
# ---------------------------------------------------------------------------


def test_set_columns_mapping_and_sequences_and_buffers():
    wb = fz.Workbook()
    s = wb.sheet("S")

    arr_f64 = array.array("d", [1.5, 2.5, 3.5])
    arr_i32 = array.array("i", [10, 20, 30])
    mv_f64 = memoryview(array.array("d", [100.0, 200.0, 300.0]))
    np_bool = np.array([True, False, True], dtype=np.bool_)
    np_strided = np.arange(0.0, 6.0, dtype=np.float64)[::2]  # [0, 2, 4]

    rep = s.set_columns(
        1,
        1,
        {
            "f64": arr_f64,
            "i32": arr_i32,
            "mv": mv_f64,
            "flag": np_bool,
            "strided": np_strided,
            "text": ["alpha", None, "gamma"],
            "nulls": [None, None, None],
            "mixed": [1, "two", dt.date(2024, 1, 2)],
        },
        headers=True,
    )
    assert (rep.start_row, rep.start_col, rep.end_row, rep.end_col) == (1, 1, 4, 8)
    assert wb.get_value("S", 1, 1) == "f64"
    assert wb.get_value("S", 1, 8) == "mixed"
    assert wb.get_value("S", 2, 1) == 1.5
    assert wb.get_value("S", 3, 2) == 20.0
    assert wb.get_value("S", 4, 3) == 300.0
    assert wb.get_value("S", 3, 4) is False
    assert wb.get_value("S", 4, 5) == 4.0
    assert wb.get_value("S", 3, 6) is None
    assert wb.get_value("S", 4, 6) == "gamma"
    assert wb.get_value("S", 2, 7) is None
    assert wb.get_value("S", 2, 8) == 1.0
    assert wb.get_value("S", 3, 8) == "two"
    assert wb.get_value("S", 4, 8) == dt.date(2024, 1, 2)

    # Sequence of columns (headers=False default).
    rep2 = wb.set_columns("S", 10, 1, [arr_f64, ["a", "b", "c"]])
    assert (rep2.start_row, rep2.start_col, rep2.end_row, rep2.end_col) == (
        10,
        1,
        12,
        2,
    )
    assert wb.get_value("S", 10, 1) == 1.5
    assert wb.get_value("S", 12, 2) == "c"

    # Custom Mapping works too.
    rep3 = s.set_columns(20, 1, _CustomMapping({"k": [7, 8]}), headers=True)
    assert (rep3.start_row, rep3.end_row) == (20, 22)
    assert wb.get_value("S", 20, 1) == "k"
    assert wb.get_value("S", 22, 1) == 8.0


def test_set_columns_validation_errors():
    wb = fz.Workbook()
    s = wb.sheet("S")

    # Mismatched column lengths.
    with pytest.raises(ValueError, match="length"):
        s.set_columns(1, 1, {"a": [1, 2], "b": [1, 2, 3]})

    # Unnamed sequence with headers=True.
    with pytest.raises(ValueError, match="headers=True"):
        s.set_columns(1, 1, [[1, 2], [3, 4]], headers=True)

    # 64-bit integer buffer exceeding ±2^53 names the column.
    bad_i64 = np.array([1, MAX_SAFE_INT + 1], dtype=np.int64)
    with pytest.raises(ValueError, match="big_ids"):
        s.set_columns(1, 1, {"big_ids": bad_i64})

    bad_u64 = np.array([1, MAX_SAFE_INT + 1], dtype=np.uint64)
    with pytest.raises(ValueError, match="big_uids"):
        s.set_columns(1, 1, {"big_uids": bad_u64})

    # 2D numpy array as single column.
    with pytest.raises(ValueError, match="1-dimensional"):
        s.set_columns(1, 1, {"mat": np.zeros((2, 2))})

    # Bare str / bytes rejected with TypeError.
    with pytest.raises(TypeError):
        s.set_columns(1, 1, "not-columns")
    with pytest.raises(TypeError):
        s.set_columns(1, 1, {"s": "not-a-column"})

    # Atomic: nothing was written.
    assert wb.get_value("S", 1, 1) is None


# ---------------------------------------------------------------------------
# set_columns: Arrow PyCapsule (__arrow_c_stream__ & __arrow_c_array__)
# ---------------------------------------------------------------------------


def test_set_columns_pyarrow_table_and_record_batch():
    wb = fz.Workbook()
    s = wb.sheet("S")

    # Multi-chunk PyArrow Table exercising all supported Arrow types.
    b1 = pa.record_batch(
        [
            pa.array([1, 2, None], type=pa.int64()),
            pa.array([1.5, float("nan"), -0.0], type=pa.float64()),
            pa.array(["foo", None, "bar"], type=pa.string()),
            pa.array([True, False, None], type=pa.bool_()),
            pa.array(
                [dt.date(2024, 1, 1), dt.date(2024, 6, 15), None], type=pa.date32()
            ),
            pa.array(
                [
                    dt.datetime(2024, 1, 1, 12, 0, 0, 500_000, tzinfo=dt.timezone.utc),
                    dt.datetime(
                        2024,
                        1,
                        1,
                        12,
                        0,
                        0,
                        tzinfo=dt.timezone(dt.timedelta(hours=9)),
                    ),
                    None,
                ],
                type=pa.timestamp("us", tz="+09:00"),
            ),
            pa.array(
                [Decimal("12.34"), Decimal("-5.50"), None], type=pa.decimal128(10, 2)
            ),
            pa.array(["cat", "dog", "cat"]).dictionary_encode(),
            pa.array([None, None, None], type=pa.null()),
        ],
        names=["i64", "f64", "str", "bool", "date", "ts_jst", "dec", "dict", "nulls"],
    )
    b2 = pa.record_batch(
        [
            pa.array([3], type=pa.int64()),
            pa.array([4.25], type=pa.float64()),
            pa.array(["baz"], type=pa.string()),
            pa.array([True], type=pa.bool_()),
            pa.array([dt.date(2025, 12, 31)], type=pa.date32()),
            pa.array(
                [dt.datetime(2025, 12, 31, 0, 0, 0, tzinfo=dt.timezone.utc)],
                type=pa.timestamp("us", tz="+09:00"),
            ),
            pa.array([Decimal("0.01")], type=pa.decimal128(10, 2)),
            pa.array(["dog"]).dictionary_encode(),
            pa.array([None], type=pa.null()),
        ],
        names=["i64", "f64", "str", "bool", "date", "ts_jst", "dec", "dict", "nulls"],
    )
    table = pa.Table.from_batches([b1, b2])

    # Pre-seed null column cell to confirm Null column clears existing values.
    s.set_value(2, 9, "stale")

    rep = s.set_columns(1, 1, table, headers=True)
    assert (rep.start_row, rep.start_col, rep.end_row, rep.end_col) == (1, 1, 5, 9)
    assert wb.get_value("S", 1, 1) == "i64"
    assert wb.get_value("S", 1, 9) == "nulls"
    assert wb.get_value("S", 2, 1) == 1.0
    assert wb.get_value("S", 4, 1) is None
    assert wb.get_value("S", 5, 1) == 3.0
    assert wb.get_value("S", 3, 2)["kind"] == "Num"
    neg0 = wb.get_value("S", 4, 2)
    assert neg0 == 0.0 and math.copysign(1.0, neg0) == -1.0
    assert wb.get_value("S", 2, 3) == "foo"
    assert wb.get_value("S", 5, 3) == "baz"
    assert wb.get_value("S", 3, 4) is False
    assert wb.get_value("S", 2, 5) == dt.date(2024, 1, 1)
    # 12:00:00.500 UTC in +09:00 wall clock -> 21:00:00.500!
    assert wb.get_value("S", 2, 6) == dt.datetime(2024, 1, 1, 21, 0, 0, 500_000)
    assert wb.get_value("S", 3, 6) == dt.datetime(2024, 1, 1, 12, 0, 0)
    assert wb.get_value("S", 2, 7) == pytest.approx(12.34)
    assert wb.get_value("S", 3, 7) == pytest.approx(-5.50)
    assert wb.get_value("S", 2, 8) == "cat"
    assert wb.get_value("S", 5, 8) == "dog"
    assert wb.get_value("S", 2, 9) is None

    # Test __arrow_c_array__ path explicitly (struct array wrapper exposing ONLY __arrow_c_array__).
    class ArrayOnlyBatch:
        def __init__(self, rb):
            self._rb = rb

        def __arrow_c_array__(self, requested_schema=None):
            return self._rb.__arrow_c_array__(requested_schema=requested_schema)

    rep_arr = s.set_columns(10, 1, ArrayOnlyBatch(b2), headers=True)
    assert (rep_arr.start_row, rep_arr.end_row, rep_arr.end_col) == (10, 11, 9)
    assert wb.get_value("S", 10, 1) == "i64"
    assert wb.get_value("S", 11, 1) == 3.0

    # Individual Arrow arrays inside a dict.
    rep_dict = s.set_columns(
        20, 1, {"x": pa.array([10, 20]), "y": pa.array(["p", "q"])}, headers=False
    )
    assert (rep_dict.start_row, rep_dict.end_row, rep_dict.end_col) == (20, 21, 2)
    assert wb.get_value("S", 20, 1) == 10.0
    assert wb.get_value("S", 21, 2) == "q"


def test_set_columns_polars_dataframe():
    wb = fz.Workbook()
    s = wb.sheet("Data")

    df = pl.DataFrame(
        {
            "price": [10.5, 20.0, 30.25],
            "qty": [2, 3, 4],
            "sku": ["A", "B", "C"],
            "active": [True, False, True],
            "cat": pl.Series(["north", "south", "north"], dtype=pl.Categorical),
        }
    )
    rep = s.set_columns(1, 1, df, headers=True)
    assert (rep.start_row, rep.start_col, rep.end_row, rep.end_col) == (1, 1, 4, 5)
    assert wb.get_value("Data", 1, 1) == "price"
    assert wb.get_value("Data", 4, 5) == "north"

    s.set_formula(5, 1, "=SUMPRODUCT(A2:A4, B2:B4)")
    assert wb.evaluate_cell("Data", 5, 1) == pytest.approx(
        10.5 * 2 + 20.0 * 3 + 30.25 * 4
    )


def test_set_columns_arrow_preflight_errors():
    wb = fz.Workbook()
    s = wb.sheet("S")

    # Int64 > 2^53 names column and leaves sheet untouched.
    t_i64 = pa.table(
        {"ok": [1, 2], "big_id": pa.array([1, MAX_SAFE_INT + 1], type=pa.int64())}
    )
    with pytest.raises(ValueError, match="big_id"):
        s.set_columns(1, 1, t_i64)

    # UInt64 > 2^53 names column.
    t_u64 = pa.table({"big_uid": pa.array([MAX_SAFE_INT + 1], type=pa.uint64())})
    with pytest.raises(ValueError, match="big_uid"):
        s.set_columns(1, 1, t_u64)

    # IANA timezone rejected with clear message naming column.
    t_iana = pa.table(
        {
            "ny_time": pa.array(
                [dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)],
                type=pa.timestamp("us", tz="America/New_York"),
            )
        }
    )
    with pytest.raises(ValueError, match="ny_time"):
        s.set_columns(1, 1, t_iana)

    # Nested / binary / map rejected naming column.
    for col_name, arr in [
        ("nested_list", pa.array([[1, 2], [3]])),
        ("raw_bin", pa.array([b"abc", b"def"], type=pa.binary())),
        ("map_col", pa.array([[("k", 1)]], type=pa.map_(pa.string(), pa.int32()))),
    ]:
        with pytest.raises(ValueError, match=col_name):
            s.set_columns(1, 1, pa.table({col_name: arr}))

    # Bad PyCapsule name rejected cleanly without UB.
    ctypes.pythonapi.PyCapsule_New.restype = ctypes.py_object
    ctypes.pythonapi.PyCapsule_New.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.c_void_p,
    ]

    class BadStream:
        def __arrow_c_stream__(self, requested_schema=None):
            return ctypes.pythonapi.PyCapsule_New(1, b"wrong_capsule_name", None)

    with pytest.raises(ValueError, match="arrow_array_stream"):
        s.set_columns(1, 1, BadStream())

    assert wb.get_value("S", 1, 1) is None


@pytest.mark.timeout(10)
def test_set_columns_drains_arrow_stream_before_workbook_lock():
    """Deadlock regression test (ADR §3.2 / REDTEAM §6):
    A RecordBatchReader backed by a Python generator that calls back into the
    SAME workbook while yielding batches MUST NOT deadlock."""
    wb = fz.Workbook()
    s = wb.sheet("S")
    s.set_value(1, 10, 42.0)

    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    observed = []

    def gen():
        # Touch the SAME workbook (both read and write!) during stream iteration.
        observed.append(wb.get_value("S", 1, 10))
        observed.append(wb.sheet_names)
        wb.set_value("S", 2, 10, 99.0)
        yield pa.record_batch([pa.array([1, 2]), pa.array(["x", "y"])], schema=schema)
        observed.append(wb.get_value("S", 2, 10))
        yield pa.record_batch([pa.array([3]), pa.array(["z"])], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, gen())
    rep = s.set_columns(1, 1, reader, headers=True)

    assert observed[0] == 42.0
    assert "S" in observed[1]
    assert observed[2] == 99.0
    assert (rep.start_row, rep.start_col, rep.end_row, rep.end_col) == (1, 1, 4, 2)
    assert wb.get_value("S", 1, 1) == "a"
    assert wb.get_value("S", 4, 2) == "z"


def test_set_columns_empty_stream_with_headers_only():
    wb = fz.Workbook()
    s = wb.sheet("S")
    schema = pa.schema([("col_a", pa.float64()), ("col_b", pa.string())])
    reader = pa.RecordBatchReader.from_batches(schema, [])

    rep = s.set_columns(1, 1, reader, headers=True)
    assert (rep.start_row, rep.start_col, rep.end_row, rep.end_col) == (1, 1, 1, 2)
    assert wb.get_value("S", 1, 1) == "col_a"
    assert wb.get_value("S", 1, 2) == "col_b"
    assert wb.get_value("S", 2, 1) is None


# ---------------------------------------------------------------------------
# set_cells (sparse)
# ---------------------------------------------------------------------------


def test_set_cells_triples_mapping_lww_and_generator():
    wb = fz.Workbook()
    s = wb.sheet("S")

    # Iterable of triples with duplicate coordinates (last-write-wins, including None).
    rep = s.set_cells(
        [
            (1, 1, 10),
            (2, 3, "hello"),
            (1, 1, 20.5),
            (3, 1, "clear-me"),
            (3, 1, None),
            (4, 1, True),
            (5, 1, dt.date(2024, 5, 17)),
            (6, 1, dt.datetime(2024, 5, 17, 9, 45, 12, 500_000)),
            (7, 1, dt.timedelta(seconds=90, microseconds=500_000)),
            (8, 1, {"type": "Error", "kind": "Na"}),
            (9, 1, -0.0),
            (10, 1, math.nan),
        ]
    )
    assert (rep.start_row, rep.start_col, rep.end_row, rep.end_col) == (1, 1, 10, 3)
    assert wb.get_value("S", 1, 1) == 20.5
    assert wb.get_value("S", 2, 3) == "hello"
    assert wb.get_value("S", 3, 1) is None
    assert wb.get_value("S", 4, 1) is True
    assert wb.get_value("S", 5, 1) == dt.date(2024, 5, 17)
    assert wb.get_value("S", 6, 1) == dt.datetime(2024, 5, 17, 9, 45, 12, 500_000)
    assert wb.get_value("S", 7, 1) == dt.timedelta(seconds=90, microseconds=500_000)
    assert wb.get_value("S", 8, 1)["kind"] == "Na"
    neg0 = wb.get_value("S", 9, 1)
    assert neg0 == 0.0 and math.copysign(1.0, neg0) == -1.0
    assert wb.get_value("S", 10, 1)["kind"] == "Num"

    # Mapping {(row, col): value} (dict and custom Mapping).
    rep_map = wb.set_cells("S", {(20, 2): 100, (25, 4): "end"})
    assert (rep_map.start_row, rep_map.start_col, rep_map.end_row, rep_map.end_col) == (
        20,
        2,
        25,
        4,
    )
    assert wb.get_value("S", 20, 2) == 100.0
    assert wb.get_value("S", 25, 4) == "end"

    rep_custom = s.set_cells(_CustomMapping({(30, 1): 7}))
    assert (rep_custom.start_row, rep_custom.end_row) == (30, 30)
    assert wb.get_value("S", 30, 1) == 7.0

    # Generator draining before lock (touches workbook mid-iteration without deadlocking).
    def cell_gen():
        assert "S" in wb.sheet_names
        yield (40, 1, 123)
        yield (40, 2, 456)

    rep_gen = s.set_cells(cell_gen())
    assert (rep_gen.start_row, rep_gen.end_col) == (40, 2)
    assert wb.get_value("S", 40, 2) == 456.0


def test_set_cells_validation():
    wb = fz.Workbook()
    s = wb.sheet("S")

    with pytest.raises(ValueError, match="1-based"):
        s.set_cells([(0, 1, 1)])
    with pytest.raises(ValueError, match="1-based"):
        s.set_cells({(1, 0): 1})
    with pytest.raises(TypeError):
        s.set_cells([(True, 1, 1)])
    with pytest.raises(TypeError):
        s.set_cells("abc")
    with pytest.raises(ValueError, match="use set_values"):
        s.set_cells([(1, 1, [1, 2])])
    assert wb.get_value("S", 1, 1) is None


# ---------------------------------------------------------------------------
# B3: Exact Python temporal type preservation across all write paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("enable_changelog", [False, True])
def test_temporal_exact_types_across_all_write_paths(enable_changelog):
    """All four Python temporal types (including midnight datetime and 00:00:00 /
    sub-second time) must round-trip with exact Python types across set_value,
    set_values, single-type set_columns, mixed-type set_columns, and set_cells."""
    samples: list[tuple[object, type]] = [
        (dt.date(2024, 5, 17), dt.date),
        (dt.time(9, 45, 12, 500_000), dt.time),
        (dt.time(0, 0, 0), dt.time),
        (dt.datetime(2024, 5, 17, 0, 0), dt.datetime),
        (dt.datetime(2024, 5, 17, 9, 45, 12, 500_000), dt.datetime),
        (dt.timedelta(days=1, seconds=90, microseconds=500_000), dt.timedelta),
    ]

    def assert_exact(got: object, expected: object, expected_type: type, label: str):
        assert isinstance(got, expected_type), (
            f"{label}: expected isinstance(..., {expected_type.__name__}), got {type(got).__name__} ({got!r})"
        )
        assert type(got) is expected_type, (
            f"{label}: expected exact type {expected_type.__name__}, got {type(got).__name__} ({got!r})"
        )
        assert got == expected, f"{label}: expected {expected!r}, got {got!r}"

    wb = fz.Workbook(enable_changelog=enable_changelog)
    s = wb.sheet("S")

    # 1. set_value
    for idx, (val, _typ) in enumerate(samples, start=1):
        s.set_value(idx, 1, val)
    for idx, (val, typ) in enumerate(samples, start=1):
        assert_exact(wb.get_value("S", idx, 1), val, typ, f"set_value[{idx}]")
        assert_exact(s.get_cell(idx, 1).value, val, typ, f"set_value.get_cell[{idx}]")

    # 2. set_values
    s.set_values(1, 2, [[val] for val, _ in samples])
    for idx, (val, typ) in enumerate(samples, start=1):
        assert_exact(wb.get_value("S", idx, 2), val, typ, f"set_values[{idx}]")

    # 3. single-type set_columns (each temporal type in its own homogeneous column)
    s.set_columns(
        1,
        3,
        {
            "d": [dt.date(2024, 5, 17)],
            "t_sub": [dt.time(9, 45, 12, 500_000)],
            "t_zero": [dt.time(0, 0, 0)],
            "dt_mid": [dt.datetime(2024, 5, 17, 0, 0)],
            "dt_sub": [dt.datetime(2024, 5, 17, 9, 45, 12, 500_000)],
            "dur": [dt.timedelta(days=1, seconds=90, microseconds=500_000)],
        },
    )
    for offset, (val, typ) in enumerate(samples):
        assert_exact(
            wb.get_value("S", 1, 3 + offset), val, typ, f"set_columns_single[{offset}]"
        )

    # 4. mixed-type set_columns (all temporal types + sentinel text in one column)
    mixed_col = [val for val, _ in samples] + ["sentinel"]
    s.set_columns(10, 1, {"mixed": mixed_col})
    for idx, (val, typ) in enumerate(samples, start=10):
        assert_exact(wb.get_value("S", idx, 1), val, typ, f"set_columns_mixed[{idx}]")
    assert wb.get_value("S", 10 + len(samples), 1) == "sentinel"

    # 5. set_cells
    s.set_cells([(20 + i, 1, val) for i, (val, _) in enumerate(samples)])
    for i, (val, typ) in enumerate(samples):
        assert_exact(wb.get_value("S", 20 + i, 1), val, typ, f"set_cells[{i}]")

    if enable_changelog:
        wb.undo()
        for i in range(len(samples)):
            assert wb.get_value("S", 20 + i, 1) is None
        wb.redo()
        for i, (val, typ) in enumerate(samples):
            assert_exact(wb.get_value("S", 20 + i, 1), val, typ, f"set_cells_redo[{i}]")


# ---------------------------------------------------------------------------
# B4: Clean ValueError (never PanicException) for nullable StructArray & reused capsule
# ---------------------------------------------------------------------------


def test_set_columns_nullable_struct_array_raises_value_error():
    wb = fz.Workbook()
    s = wb.sheet("S")

    nullable_struct = pa.array([{"a": 1}, None])
    with pytest.raises(ValueError, match="null"):
        s.set_columns(1, 1, nullable_struct)

    assert wb.get_value("S", 1, 1) is None


def test_set_columns_reused_arrow_c_array_capsule_raises_value_error():
    wb = fz.Workbook()
    s = wb.sheet("S")

    rb = pa.record_batch([pa.array([1, 2, 3])], names=["x"])
    reused_pair = rb.__arrow_c_array__()

    class ReusedBatchCapsule:
        def __arrow_c_array__(self, requested_schema=None):
            return reused_pair

    # First call consumes the capsule cleanly.
    rep = s.set_columns(1, 1, ReusedBatchCapsule())
    assert (rep.start_row, rep.end_row) == (1, 3)
    assert wb.get_value("S", 3, 1) == 3.0

    # Second call with the same already-released capsules must raise ValueError (not PanicException).
    with pytest.raises(ValueError, match="already released"):
        s.set_columns(5, 1, ReusedBatchCapsule())
    assert wb.get_value("S", 5, 1) is None

    # Same check for single-column __arrow_c_array__ inside a dict.
    col_pair = pa.array([10, 20]).__arrow_c_array__()

    class ReusedColumnCapsule:
        def __arrow_c_array__(self, requested_schema=None):
            return col_pair

    s.set_columns(10, 1, {"c": ReusedColumnCapsule()})
    assert wb.get_value("S", 11, 1) == 20.0

    with pytest.raises(ValueError, match="already released"):
        s.set_columns(15, 1, {"c": ReusedColumnCapsule()})
    assert wb.get_value("S", 15, 1) is None


# ---------------------------------------------------------------------------
# N1: Reject 2-D NumPy arrays passed directly to set_columns
# ---------------------------------------------------------------------------


def test_set_columns_rejects_2d_numpy_array_directly():
    wb = fz.Workbook()
    s = wb.sheet("S")

    mat = np.arange(6.0).reshape(2, 3)
    with pytest.raises(ValueError, match="set_values"):
        s.set_columns(1, 1, mat)

    assert wb.get_value("S", 1, 1) is None
