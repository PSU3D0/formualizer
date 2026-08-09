from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any

from . import (
    CellSnapshotReport,
    DependentsReport,
    ExcelEvaluationError,
    FormualizerHostError,
    InspectionError,
    ParserError,
    PrecedentReport,
    RangePage,
    SheetPortConstraintError,
    SheetPortError,
    SheetPortManifestError,
    SheetPortSession,
    SheetPortWorkbookError,
    TokenizerError,
    TraceGraph,
    Workbook,
)
from .formualizer_py import Workbook as NativeWorkbook

_EXCEPTION_TYPES: tuple[type[Exception], ...] = (
    TokenizerError,
    ParserError,
    FormualizerHostError,
    ExcelEvaluationError,
    InspectionError,
    SheetPortError,
    SheetPortManifestError,
    SheetPortConstraintError,
    SheetPortWorkbookError,
)


def _mypy_api_smoke(wb: Workbook, session: SheetPortSession) -> Mapping[str, Any]:
    # The public package is a typing re-export facade, not a second nominal API.
    native_wb: NativeWorkbook = wb

    # Workbook changelog metadata setters
    wb.set_actor_id(None)
    wb.set_correlation_id("corr")
    wb.set_reason("unit-test")

    # SheetPort determinism knobs
    ts = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    out1 = session.evaluate_once(
        freeze_volatile=True,
        rng_seed=123,
        deterministic_timestamp_utc=ts,
        deterministic_timezone="utc",
    )
    out2 = session.evaluate_once(
        deterministic_timestamp_utc=ts,
        deterministic_timezone=0,
    )

    # Inspection reports retain precise generated types through nested accessors.
    snapshot: CellSnapshotReport = wb.inspect_cell("Sheet1!A1", include_values=False)
    precedents: PrecedentReport = wb.precedents("Sheet1!A1", max_links=2)
    dependents: DependentsReport = wb.dependents("Sheet1!A1", max_results=2)
    trace: TraceGraph = wb.trace(["Sheet1!A1"])
    page: RangePage = wb.range_page("Sheet1!A1:B2", expected_stamp=snapshot.stamp)
    node_address: str = trace.nodes["Sheet1!A1"].address

    # Return a value so mypy checks mapping types.
    return {
        "out1": out1,
        "out2": out2,
        "native_wb": native_wb,
        "snapshot": snapshot,
        "precedents": precedents,
        "dependents": dependents,
        "page": page,
        "node_address": node_address,
    }
