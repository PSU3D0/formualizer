"""Portable source-recalculation records; validation and semantics live in Rust.

Limits are partial integer maps (u32 counts/bytes; JS-safe affected work).
PhysicalCounter None means unmeasured; decimal strings preserve large exact counts.
"""

from datetime import datetime
from typing import Literal, TypedDict

__all__ = [
    "SourceRecalculateLimits",
    "SourceCalculationPolicy",
    "SourceRecalculateResult",
    "SourceOperationDiagnostics",
    "SourceUnavailableOperation",
    "SourceDependencyEvidence",
]

PhysicalCounter = int | str | None
# Keep the runtime alias a Union too: mypy 2.x normalizes a multi-value
# Literal into this union when stubtest inspects this module.
SettingOrigin = Literal["caller"] | Literal["workbook"] | Literal["excel_default"]


class SourceRecalculateLimits(TypedDict, total=False):
    maxInputBytes: int
    maxEntries: int
    maxExpandedBytes: int
    maxWorksheetBytes: int
    maxFormulaCells: int
    maxOutputBytes: int
    maxXmlDepth: int
    maxCells: int
    maxColumns: int
    maxOperationRoots: int
    maxOperationTextBytes: int
    maxAffectedWork: int
    maxAffectedRegions: int
    maxAffectedScratchBytes: int


class SourceIterationPolicy(TypedDict, total=False):
    mode: Literal["workbook", "disabled", "iterate"]
    maxIterations: int
    maxChange: float


class SourceTimezoneOffset(TypedDict):
    offsetSeconds: int


SourceTimezone = Literal["utc", "local"] | SourceTimezoneOffset


class SourceCalculationPolicy(TypedDict, total=False):
    iteration: SourceIterationPolicy
    iterationInitialState: Literal["require_verified", "cold_start"]
    cycleDetection: Literal["runtime", "static"]
    workbookSeed: int | str
    fixedNowUtc: str
    timezone: SourceTimezone
    maxElapsedMs: int
    maxWorkUnits: int


class SourceFunctionOperation(TypedDict):
    kind: Literal[
        "unknown_function",
        "unimplemented_function",
        "unverified_provider",
        "source_feature",
    ]
    name: str


class SourceModeOperation(TypedDict):
    kind: Literal["unimplemented_function_mode", "unverified_function_mode"]
    name: str
    mode: str


class SourcePostfixOperation(TypedDict):
    kind: Literal["postfix_invocation"]


class SourceInternalOperation(TypedDict):
    kind: Literal["internal_result"]
    token: str


SourceUnavailableOperation = (
    SourceFunctionOperation
    | SourceModeOperation
    | SourcePostfixOperation
    | SourceInternalOperation
)


class SourceOperationSample(TypedDict):
    operation: SourceUnavailableOperation
    location: str | None


class SourceFunctionIncidence(TypedDict):
    cellReasonCount: int
    namedReasonCount: int
    unlocatedReasonCount: int


class SourceFunctionModeIncidence(TypedDict):
    name: str
    mode: str
    verification: Literal["unavailable", "unverified"]
    incidence: SourceFunctionIncidence


class SourceDependencyEvidence(TypedDict):
    relation: Literal["static_authority_and_committed_grid_rectangles"]
    sourceDomain: Literal["admitted_workbook_and_fixed_cached_sources"]
    semanticReadsUnattested: bool
    conservativeUnknownDynamic: bool
    unknownReasons: dict[str, int]
    unknownFacts: int
    witnessedRegions: int
    uncertainRegions: int
    work: int
    queryWork: int
    publicationMembershipWorkBound: int
    requestedCapacityPeakBytes: int


class SourceOperationDiagnostics(TypedDict):
    unavailableFunctions: dict[str, SourceFunctionIncidence]
    functionModes: list[SourceFunctionModeIncidence]
    logicalCellReasonCount: int
    logicalRootCellCount: int
    logicalNamedReasonCount: int
    sourceRegions: int
    workbookFeatures: list[str]
    unavailableResultCells: int
    physicalUnavailableObservations: PhysicalCounter
    physicalFunctionDispatches: PhysicalCounter
    dependencyEvidence: SourceDependencyEvidence | None
    samples: list[SourceOperationSample]
    samplesTruncated: int


class SourceCalculationWarning(TypedDict):
    field: str
    message: str


class ResolvedSourceCalculation(TypedDict):
    cycleDetection: Literal["runtime", "static"]
    iterationEnabled: bool
    maxIterations: int | None
    maxChange: float | None
    iterationInitialState: Literal["require_verified", "cold_start"]
    dateSystem: Literal["1900", "1904"]
    workbookSeed: str
    timezone: SourceTimezone
    fixedNowUtc: str | None
    iterationOrigin: SettingOrigin
    maxIterationsOrigin: SettingOrigin | None
    maxChangeOrigin: SettingOrigin | None
    warnings: list[SourceCalculationWarning]
    maxElapsedMs: str | None
    maxWorkUnits: str | None


class SourceClock(TypedDict):
    now: datetime | None
    timezone: str
    fixed: bool


class SourceSheetSummary(TypedDict):
    evaluated: int
    errors: int


class SourceOptionalErrorSummary(TypedDict, total=False):
    locations_truncated: int


class SourceErrorSummary(SourceOptionalErrorSummary):
    count: int
    locations: list[str]
    messages: list[str | None]


class SourceUnknownFunction(TypedDict):
    name: str
    cells: int


class SourceOptionalSummary(TypedDict, total=False):
    error_summary: dict[str, SourceErrorSummary]


class SourceSummary(SourceOptionalSummary):
    status: str
    evaluated: int
    errors: int
    total_formulas: int
    total_errors: int
    sheets: dict[str, SourceSheetSummary]
    unknown_functions: list[SourceUnknownFunction]


class SourceOptionalResult(TypedDict, total=False):
    external_links: dict[str, str | int | bool]


class SourceRecalculateResult(SourceOptionalResult):
    bytes: bytes
    summary: SourceSummary
    formula_cells: int
    cache_cells_changed: int
    worksheet_parts_changed: int
    clock: SourceClock
    seed: int
    calculation: ResolvedSourceCalculation | None
    coverage: Literal["complete", "partial"]
    coverage_scope: Literal["none", "conservative"]
    caches_invalidated: int
    calculation_flags_changed: bool
    operation_diagnostics: SourceOperationDiagnostics
