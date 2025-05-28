"""
Dependency Tracer for Formula Dependencies

This module provides a robust, resolver-agnostic system for tracing formula dependencies
using an ABC-based architecture with intelligent caching.

Key Features:
- Abstract resolver interface for flexible data source integration
- Cached formula parsing and reference resolution
- Precedent and dependent tracing with cycle detection
- Cache management with selective invalidation
- Functional programming patterns for composable operations

Example Usage:
    ```python
    from formualizer.dependency_tracer import DependencyTracer, JsonResolver

    # Create resolver and tracer
    resolver = JsonResolver(workbook_data)
    tracer = DependencyTracer(resolver)

    # Trace precedents (what this formula depends on)
    precedents = tracer.trace_precedents("Sheet1!A1")

    # Trace dependents (what depends on this cell)
    dependents = tracer.trace_dependents("Sheet1!B2")

    # Build full dependency graph
    graph = tracer.build_dependency_graph()
    ```
"""

from abc import ABC, abstractmethod
from typing import Dict, Set, List, Optional, Union, Tuple, Any, Iterator, NamedTuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
import weakref
from functools import lru_cache
import logging
from enum import Enum
import re
import random

try:
    from formualizer import parse, ASTNode, CellRef, RangeRef
    from formualizer.visitor import collect_references, walk_ast, VisitControl
except ImportError:
    # For standalone testing
    pass

logger = logging.getLogger(__name__)


class Direction(Enum):
    """Direction of dependency relationship."""

    PRECEDENT = "precedent"  # This node is a precedent (dependency) of the target
    DEPENDENT = "dependent"  # This node is a dependent (depends on) the target


class RangeType(Enum):
    """Classification of range types based on usage patterns and size."""

    DATA_RANGE = "data_range"  # Large ranges representing data sets (e.g., A1:Z10000)
    SELECTION_RANGE = (
        "selection_range"  # Small targeted ranges for calculations (e.g., K40:K42)
    )
    COLUMN_RANGE = (
        "column_range"  # Single-column ranges often used in lookups (e.g., D1:D10000)
    )
    UNKNOWN = "unknown"  # Unable to classify or invalid ranges


@dataclass
class DependencyNode:
    """Unified node representing a dependency relationship with directionality and eager resolution."""

    ref: Union[CellRef, RangeRef]
    direction: Direction
    order: Optional[int] = None  # Evaluation order (for precedents)
    formula: Optional[str] = field(default=None)  # Resolved at build time for CellRef
    value: Any = field(default=None)  # Resolved at build time for CellRef
    _resolver: Optional["FormulaResolver"] = field(default=None, repr=False)
    node_id: Optional[str] = field(
        default=None, repr=False
    )  # Unique ID for cross-referencing

    def __post_init__(self):
        """Resolve formula and value at build time for CellRef nodes."""
        if self._resolver and isinstance(self.ref, CellRef):
            if self.formula is None:
                self.formula = self._resolver.get_formula(self.ref)
            if self.value is None:
                self.value = self._resolver.get_value(self.ref)

    @property
    def is_cell(self) -> bool:
        """Check if this node represents a cell reference."""
        return isinstance(self.ref, CellRef)

    @property
    def is_range(self) -> bool:
        """Check if this node represents a range reference."""
        return isinstance(self.ref, RangeRef)

    @property
    def sheet(self) -> str:
        """Get the sheet name for this reference."""
        return self.ref.sheet

    @property
    def address(self) -> str:
        """Get the full address string for this reference."""
        return str(self.ref)

    # Range-specific properties
    @property
    def start_row(self) -> Optional[int]:
        """Get start row for range references."""
        return self.ref.start.row if self.is_range else None

    @property
    def end_row(self) -> Optional[int]:
        """Get end row for range references."""
        return self.ref.end.row if self.is_range else None

    @property
    def start_col(self) -> Optional[Union[int, str]]:
        """Get start column for range references."""
        return self.ref.start.col if self.is_range else None

    @property
    def end_col(self) -> Optional[Union[int, str]]:
        """Get end column for range references."""
        return self.ref.end.col if self.is_range else None

    @property
    def row_count(self) -> Optional[int]:
        """Get number of rows in range."""
        if self.is_range:
            return self.end_row - self.start_row + 1
        return None

    @property
    def col_count(self) -> Optional[int]:
        """Get number of columns in range (assumes numeric columns)."""
        if (
            self.is_range
            and isinstance(self.start_col, int)
            and isinstance(self.end_col, int)
        ):
            return self.end_col - self.start_col + 1
        return None

    def unpack_range(self) -> List["DependencyNode"]:
        """Unpack a range reference into individual cell dependency nodes."""
        if not self.is_range or not self._resolver:
            return [self]

        cells = []
        start_row, end_row = self.start_row, self.end_row
        start_col, end_col = self.start_col, self.end_col

        # Convert string columns to numbers for iteration
        if isinstance(start_col, str):
            start_col = self._col_letter_to_num(start_col)
        if isinstance(end_col, str):
            end_col = self._col_letter_to_num(end_col)

        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                cell_ref = CellRef(sheet=self.sheet, row=row, col=col)
                cell_node = DependencyNode(
                    ref=cell_ref, direction=self.direction, _resolver=self._resolver
                )
                cells.append(cell_node)
        return cells

    @staticmethod
    def _col_letter_to_num(col_letter: str) -> int:
        """Convert column letter to number (A=1, B=2, etc.)."""
        num = 0
        for char in col_letter.upper():
            num = num * 26 + (ord(char) - ord("A") + 1)
        return num

    def __hash__(self):
        return hash((self.ref, self.direction))

    def __eq__(self, other):
        if not isinstance(other, DependencyNode):
            return False
        return self.ref == other.ref and self.direction == other.direction


class TraceResult:
    """Container for DependencyNode results with traversal and filtering utilities."""

    def __init__(self, nodes: List[DependencyNode]):
        self.nodes = nodes

    def __iter__(self):
        return iter(self.nodes)

    def __len__(self):
        return len(self.nodes)

    def __getitem__(self, index):
        return self.nodes[index]

    def __bool__(self):
        return bool(self.nodes)

    @property
    def refs(self) -> List[Union[CellRef, RangeRef]]:
        """Get all references from the nodes."""
        return [node.ref for node in self.nodes]

    @property
    def cell_refs(self) -> List[CellRef]:
        """Get only cell references."""
        return [node.ref for node in self.nodes if node.is_cell]

    @property
    def range_refs(self) -> List[RangeRef]:
        """Get only range references."""
        return [node.ref for node in self.nodes if node.is_range]

    def filter_by_direction(self, direction: Direction) -> "TraceResult":
        """Filter nodes by direction."""
        filtered = [node for node in self.nodes if node.direction == direction]
        return TraceResult(filtered)

    def filter_by_sheet(self, sheet: str) -> "TraceResult":
        """Filter nodes by sheet name."""
        filtered = [node for node in self.nodes if node.sheet == sheet]
        return TraceResult(filtered)

    def filter_cells_only(self) -> "TraceResult":
        """Filter to only cell references."""
        filtered = [node for node in self.nodes if node.is_cell]
        return TraceResult(filtered)

    def filter_ranges_only(self) -> "TraceResult":
        """Filter to only range references."""
        filtered = [node for node in self.nodes if node.is_range]
        return TraceResult(filtered)

    def filter_with_formulas(self) -> "TraceResult":
        """Filter to only nodes that have formulas."""
        filtered = [node for node in self.nodes if node.formula]
        return TraceResult(filtered)

    def expand_ranges(self) -> "TraceResult":
        """Expand all range references into individual cell nodes."""
        expanded = []
        for node in self.nodes:
            if node.is_range:
                expanded.extend(node.unpack_range())
            else:
                expanded.append(node)
        return TraceResult(expanded)

    def sort_by_address(self) -> "TraceResult":
        """Sort nodes by address (sheet, then column, then row)."""

        def sort_key(node):
            ref = node.ref
            if node.is_cell:
                return (ref.sheet, ref.col, ref.row)
            else:  # range
                return (ref.sheet, ref.start.col, ref.start.row)

        sorted_nodes = sorted(self.nodes, key=sort_key)
        return TraceResult(sorted_nodes)

    def sort_by_order(self) -> "TraceResult":
        """Sort nodes by evaluation order (if available)."""
        sorted_nodes = sorted(
            self.nodes, key=lambda n: n.order if n.order is not None else float("inf")
        )
        return TraceResult(sorted_nodes)

    def group_by_sheet(self) -> Dict[str, "TraceResult"]:
        """Group nodes by sheet name."""
        groups = defaultdict(list)
        for node in self.nodes:
            groups[node.sheet].append(node)
        return {sheet: TraceResult(nodes) for sheet, nodes in groups.items()}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "count": len(self.nodes),
            "cells": len(self.cell_refs),
            "ranges": len(self.range_refs),
            "sheets": list(set(node.sheet for node in self.nodes)),
            "nodes": [
                {
                    "ref": str(node.ref),
                    "direction": node.direction.value,
                    "order": node.order,
                    "formula": node.formula,
                    "value": node.value,
                    "is_cell": node.is_cell,
                    "is_range": node.is_range,
                }
                for node in self.nodes
            ],
        }

    def create_range_container(self) -> "RangeContainer":
        """Create a RangeContainer from the range references in this result."""
        return RangeContainer(self.range_refs)

    def __repr__(self):
        return f"TraceResult({len(self.nodes)} nodes: {len(self.cell_refs)} cells, {len(self.range_refs)} ranges)"


@dataclass
class ConsolidatedRange:
    """A consolidated range representing multiple similar ranges with metadata."""

    sheet: str
    range_type: RangeType
    columns: Set[Union[int, str]]  # Set of columns involved
    start_row: Optional[int] = None
    end_row: Optional[int] = None
    original_ranges: List[RangeRef] = field(default_factory=list)

    def __post_init__(self):
        if not self.original_ranges:
            return

        # Calculate consolidated bounds from original ranges
        start_rows = []
        end_rows = []

        for range_ref in self.original_ranges:
            if range_ref.start and range_ref.end:
                start_rows.append(range_ref.start.row)
                end_rows.append(range_ref.end.row)

        if start_rows and end_rows:
            self.start_row = min(start_rows)
            self.end_row = max(end_rows)

    @property
    def row_count(self) -> Optional[int]:
        """Get number of rows in consolidated range."""
        if self.start_row is not None and self.end_row is not None:
            return self.end_row - self.start_row + 1
        return None

    @property
    def column_count(self) -> int:
        """Get number of columns in consolidated range."""
        return len(self.columns)

    @property
    def cell_count(self) -> Optional[int]:
        """Get total number of cells in consolidated range."""
        if self.row_count is not None:
            return self.row_count * self.column_count
        return None

    def contains_column(self, col: Union[int, str]) -> bool:
        """Check if this consolidated range contains a specific column."""
        return col in self.columns

    def get_range_for_column(self, col: Union[int, str]) -> Optional[str]:
        """Get the range string for a specific column if it exists."""
        if not self.contains_column(col):
            return None
        if self.start_row is None or self.end_row is None:
            return None

        # Convert column to string format for range
        col_str = str(col) if isinstance(col, int) else col
        return f"{self.sheet}!{col_str}{self.start_row}:{col_str}{self.end_row}"


class RangeContainer:
    """
    Container for managing and consolidating range references with classification.

    This class groups ranges by sheet and pattern, providing utilities to:
    - Consolidate similar ranges (e.g., multiple single-column ranges)
    - Classify ranges as data vs selection ranges
    - Provide ergonomic access to range information

    Heuristics for range classification:
    - DATA_RANGE: Large ranges (>1000 cells) that likely represent entire datasets
    - SELECTION_RANGE: Small ranges (<100 cells) used for targeted calculations
    - COLUMN_RANGE: Single-column ranges often used in SUMIFS/VLOOKUP operations
    - UNKNOWN: Invalid ranges or ranges that don't fit clear patterns
    """

    def __init__(self, ranges: List[RangeRef]):
        self.original_ranges = ranges
        self.consolidated_ranges: Dict[str, List[ConsolidatedRange]] = defaultdict(list)
        self.range_classifications: Dict[RangeRef, RangeType] = {}

        self._consolidate_ranges()

    def _classify_range(self, range_ref: RangeRef) -> RangeType:
        """
        Classify a range based on size, pattern, and usage heuristics.

        Enhanced classification rules for compactness specification:
        1. Invalid ranges (None start/end) -> UNKNOWN
        2. Single column with >500 rows -> COLUMN_RANGE (typical lookup column)
        3. Large ranges (>1000 cells) -> DATA_RANGE (entire datasets)
        4. Small ranges (<100 cells) -> SELECTION_RANGE (targeted calculations)
        5. Medium ranges -> DATA_RANGE if they span many columns, else SELECTION_RANGE

        Compactness rules:
        - COLUMN_RANGE and DATA_RANGE: Never expand, show as single-line metadata
        - SELECTION_RANGE: Fully expand to show all constituent cells
        """
        if not range_ref.start or not range_ref.end:
            return RangeType.UNKNOWN

        try:
            # Calculate dimensions
            start_row, end_row = range_ref.start.row, range_ref.end.row
            start_col, end_col = range_ref.start.col, range_ref.end.col

            # Handle both string and numeric columns
            if isinstance(start_col, str):
                start_col_num = self._col_letter_to_num(start_col)
            else:
                start_col_num = start_col

            if isinstance(end_col, str):
                end_col_num = self._col_letter_to_num(end_col)
            else:
                end_col_num = end_col

            row_count = end_row - start_row + 1
            col_count = end_col_num - start_col_num + 1
            total_cells = row_count * col_count

            # Single column with many rows (typical lookup pattern) - NEVER expand
            if col_count == 1 and row_count > 500:
                return RangeType.COLUMN_RANGE

            # Large ranges are data ranges - NEVER expand
            if total_cells > 1000:
                return RangeType.DATA_RANGE

            # Small ranges are selection ranges - ALWAYS expand fully
            if total_cells < 100:
                return RangeType.SELECTION_RANGE

            # Medium ranges: classify based on aspect ratio
            # Wide ranges (many columns) are more likely data ranges - NEVER expand
            if col_count > 5:
                return RangeType.DATA_RANGE
            else:
                return RangeType.SELECTION_RANGE

        except (AttributeError, TypeError, ValueError):
            return RangeType.UNKNOWN

    @staticmethod
    def _col_letter_to_num(col_letter: str) -> int:
        """Convert column letter to number (A=1, B=2, etc.)."""
        num = 0
        for char in col_letter.upper():
            num = num * 26 + (ord(char) - ord("A") + 1)
        return num

    def _consolidate_ranges(self):
        """Group and consolidate ranges by sheet and pattern."""
        # First, classify all ranges
        for range_ref in self.original_ranges:
            self.range_classifications[range_ref] = self._classify_range(range_ref)

        # Group by sheet
        ranges_by_sheet = defaultdict(list)
        for range_ref in self.original_ranges:
            if range_ref.sheet:
                ranges_by_sheet[range_ref.sheet].append(range_ref)

        # Consolidate within each sheet
        for sheet, sheet_ranges in ranges_by_sheet.items():
            self._consolidate_sheet_ranges(sheet, sheet_ranges)

    def _consolidate_sheet_ranges(self, sheet: str, ranges: List[RangeRef]):
        """Consolidate ranges within a single sheet."""
        # Group by range type first
        ranges_by_type = defaultdict(list)
        for range_ref in ranges:
            range_type = self.range_classifications[range_ref]
            ranges_by_type[range_type].append(range_ref)

        for range_type, type_ranges in ranges_by_type.items():
            if range_type == RangeType.COLUMN_RANGE:
                # Special consolidation for column ranges
                self._consolidate_column_ranges(sheet, type_ranges)
            elif range_type == RangeType.DATA_RANGE:
                # Group data ranges by similar patterns
                self._consolidate_data_ranges(sheet, type_ranges)
            else:
                # Create individual consolidated ranges for selection and unknown ranges
                for range_ref in type_ranges:
                    if range_ref.start and range_ref.end:
                        columns = self._extract_columns_from_range(range_ref)
                        consolidated = ConsolidatedRange(
                            sheet=sheet,
                            range_type=range_type,
                            columns=columns,
                            original_ranges=[range_ref],
                        )
                        self.consolidated_ranges[sheet].append(consolidated)

    def _consolidate_column_ranges(self, sheet: str, ranges: List[RangeRef]):
        """Consolidate single-column ranges that share similar row patterns."""
        # Group by start/end row pattern
        row_pattern_groups = defaultdict(list)

        for range_ref in ranges:
            if range_ref.start and range_ref.end:
                # Create a pattern key based on start and end rows
                start_row = range_ref.start.row
                end_row = range_ref.end.row

                # Round to nearest 1000 to group similar large ranges
                if end_row > 1000:
                    pattern_key = (start_row, (end_row // 1000) * 1000)
                else:
                    pattern_key = (start_row, end_row)

                row_pattern_groups[pattern_key].append(range_ref)

        # Create consolidated ranges for each pattern group
        for pattern_key, pattern_ranges in row_pattern_groups.items():
            columns = set()
            for range_ref in pattern_ranges:
                columns.update(self._extract_columns_from_range(range_ref))

            consolidated = ConsolidatedRange(
                sheet=sheet,
                range_type=RangeType.COLUMN_RANGE,
                columns=columns,
                original_ranges=pattern_ranges,
            )
            self.consolidated_ranges[sheet].append(consolidated)

    def _consolidate_data_ranges(self, sheet: str, ranges: List[RangeRef]):
        """Consolidate data ranges by overlapping or adjacent patterns."""
        # For now, create individual consolidated ranges
        # Future enhancement: detect overlapping ranges and merge them
        for range_ref in ranges:
            if range_ref.start and range_ref.end:
                columns = self._extract_columns_from_range(range_ref)
                consolidated = ConsolidatedRange(
                    sheet=sheet,
                    range_type=RangeType.DATA_RANGE,
                    columns=columns,
                    original_ranges=[range_ref],
                )
                self.consolidated_ranges[sheet].append(consolidated)

    def _extract_columns_from_range(self, range_ref: RangeRef) -> Set[Union[int, str]]:
        """Extract all columns from a range reference."""
        if not range_ref.start or not range_ref.end:
            return set()

        start_col = range_ref.start.col
        end_col = range_ref.end.col

        # Handle both string and numeric columns
        if isinstance(start_col, str) and isinstance(end_col, str):
            # String columns - convert to numbers, generate range, convert back
            start_num = self._col_letter_to_num(start_col)
            end_num = self._col_letter_to_num(end_col)
            return {self._num_to_col_letter(i) for i in range(start_num, end_num + 1)}
        elif isinstance(start_col, int) and isinstance(end_col, int):
            # Numeric columns
            return set(range(start_col, end_col + 1))
        else:
            # Mixed types - return both as-is
            return {start_col, end_col}

    @staticmethod
    def _num_to_col_letter(num: int) -> str:
        """Convert column number to letter (1=A, 2=B, etc.)."""
        result = ""
        while num > 0:
            num -= 1
            result = chr(ord("A") + num % 26) + result
            num //= 26
        return result

    # Public interface methods

    def get_sheets(self) -> List[str]:
        """Get all sheets that have ranges."""
        return list(self.consolidated_ranges.keys())

    def get_consolidated_ranges(
        self, sheet: Optional[str] = None
    ) -> List[ConsolidatedRange]:
        """Get consolidated ranges, optionally filtered by sheet."""
        if sheet:
            return self.consolidated_ranges.get(sheet, [])
        else:
            all_ranges = []
            for sheet_ranges in self.consolidated_ranges.values():
                all_ranges.extend(sheet_ranges)
            return all_ranges

    def get_ranges_by_type(
        self, range_type: RangeType, sheet: Optional[str] = None
    ) -> List[ConsolidatedRange]:
        """Get consolidated ranges filtered by type and optionally by sheet."""
        ranges = self.get_consolidated_ranges(sheet)
        return [r for r in ranges if r.range_type == range_type]

    def get_column_ranges(self, sheet: Optional[str] = None) -> List[ConsolidatedRange]:
        """Get all column ranges (commonly used for lookups)."""
        return self.get_ranges_by_type(RangeType.COLUMN_RANGE, sheet)

    def get_data_ranges(self, sheet: Optional[str] = None) -> List[ConsolidatedRange]:
        """Get all data ranges (large datasets)."""
        return self.get_ranges_by_type(RangeType.DATA_RANGE, sheet)

    def get_selection_ranges(
        self, sheet: Optional[str] = None
    ) -> List[ConsolidatedRange]:
        """Get all selection ranges (targeted calculations)."""
        return self.get_ranges_by_type(RangeType.SELECTION_RANGE, sheet)

    def find_ranges_with_column(
        self, column: Union[int, str], sheet: Optional[str] = None
    ) -> List[ConsolidatedRange]:
        """Find all consolidated ranges that include a specific column."""
        ranges = self.get_consolidated_ranges(sheet)
        return [r for r in ranges if r.contains_column(column)]

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the range container."""
        total_ranges = len(self.original_ranges)
        total_consolidated = sum(
            len(ranges) for ranges in self.consolidated_ranges.values()
        )

        type_counts = defaultdict(int)
        for range_type in self.range_classifications.values():
            type_counts[range_type.value] += 1

        return {
            "original_ranges": total_ranges,
            "consolidated_ranges": total_consolidated,
            "consolidation_ratio": total_consolidated / total_ranges
            if total_ranges > 0
            else 0,
            "sheets": len(self.consolidated_ranges),
            "range_type_distribution": dict(type_counts),
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation for debugging/serialization."""
        return {
            "stats": self.get_stats(),
            "sheets": {
                sheet: [
                    {
                        "sheet": cr.sheet,
                        "type": cr.range_type.value,
                        "columns": list(cr.columns),
                        "start_row": cr.start_row,
                        "end_row": cr.end_row,
                        "row_count": cr.row_count,
                        "column_count": cr.column_count,
                        "cell_count": cr.cell_count,
                        "original_count": len(cr.original_ranges),
                    }
                    for cr in consolidated_ranges
                ]
                for sheet, consolidated_ranges in self.consolidated_ranges.items()
            },
        }

    def __repr__(self):
        stats = self.get_stats()
        return (
            f"RangeContainer({stats['original_ranges']} ranges → "
            f"{stats['consolidated_ranges']} consolidated, "
            f"{stats['sheets']} sheets)"
        )


@dataclass
class CellLabel:
    """Context label for a cell or range, providing semantic meaning."""

    text: str
    source_ref: CellRef  # Where the label was found
    label_type: str  # "row_header", "column_header", "corner_label"
    confidence: float = 1.0  # Confidence in this being a meaningful label

    def __str__(self) -> str:
        return self.text

    def __repr__(self) -> str:
        return f"CellLabel('{self.text}', {self.label_type}, {self.confidence:.2f})"


class LabelProjector:
    """
    Utility for finding contextual labels for cells and ranges.

    This class implements heuristics to find meaningful text labels that provide
    context for numeric cells and formulas, making them more interpretable.

    Label discovery rules:
    1. For cells: Look up to N rows above and left to N columns for text labels
    2. For ranges: Look for column headers above the range start
    3. Prioritize closer labels and filter out common non-semantic text
    4. Assign confidence scores based on position and content patterns
    """

    def __init__(self, resolver: "FormulaResolver", max_search_distance: int = 10):
        self.resolver = resolver
        self.max_search_distance = max_search_distance

        # Common non-semantic patterns to filter out
        self.noise_patterns = [
            r"^\s*$",  # Empty or whitespace
            r"^[=+\-*/()0-9.,\s]+$",  # Formulas or numbers only
            r"^(sum|total|subtotal|grand total)$",  # Common generic labels
            r"^(row|column|cell|range)\s*\d*$",  # Generic structural labels
            r"^[a-z]$",  # Single letters (often column letters)
        ]

        self._label_cache: Dict[CellRef, List[CellLabel]] = {}

    def _is_meaningful_text(self, text: str) -> float:
        """
        Determine if text is meaningful and return confidence score.

        Returns confidence from 0.0 (not meaningful) to 1.0 (highly meaningful).
        """
        if not text or not isinstance(text, str):
            return 0.0

        text_clean = text.strip().lower()

        # Filter out noise patterns
        for pattern in self.noise_patterns:
            if re.match(pattern, text_clean, re.IGNORECASE):
                return 0.0

        # Score based on content characteristics
        confidence = 0.3  # Base score for non-empty text

        # Bonus for containing letters
        if re.search(r"[a-zA-Z]", text):
            confidence += 0.3

        # Bonus for multiple words
        if len(text_clean.split()) > 1:
            confidence += 0.2

        # Bonus for reasonable length
        if 3 <= len(text_clean) <= 50:
            confidence += 0.2

        # Penalty for very long text (might be data, not labels)
        if len(text_clean) > 100:
            confidence -= 0.3

        return min(1.0, confidence)

    def find_labels_for_cell(
        self, cell_ref: CellRef, search_directions: List[str] = None
    ) -> List[CellLabel]:
        """
        Find contextual labels for a single cell.

        Args:
            cell_ref: The cell to find labels for
            search_directions: List of directions to search ["up", "left", "up_left"]

        Returns:
            List of CellLabel objects ordered by relevance
        """
        if cell_ref in self._label_cache:
            return self._label_cache[cell_ref]

        if search_directions is None:
            search_directions = ["up", "left", "up_left"]

        labels = []

        # Search upward (row headers)
        if "up" in search_directions:
            labels.extend(self._search_direction(cell_ref, "up", "row_header"))

        # Search leftward (column headers)
        if "left" in search_directions:
            labels.extend(self._search_direction(cell_ref, "left", "column_header"))

        # Search diagonally up-left (corner labels)
        # if "up_left" in search_directions:
        #     labels.extend(self._search_direction(cell_ref, "up_left", "corner_label"))

        # Sort by confidence and distance
        labels.sort(
            key=lambda x: (
                -x.confidence,
                self._distance_to_cell(cell_ref, x.source_ref),
            )
        )

        self._label_cache[cell_ref] = labels
        return labels

    def find_labels_for_range(self, range_ref: RangeRef) -> Dict[str, List[CellLabel]]:
        """
        Find contextual labels for a range, focusing on column headers.

        Returns:
            Dictionary with keys "columns", "context" containing relevant labels
        """
        if not range_ref.start or not range_ref.end:
            return {"columns": [], "context": []}

        labels = {"columns": [], "context": []}

        # Find column headers for each column in the range
        start_col = range_ref.start.col
        end_col = range_ref.end.col

        # Handle both string and numeric columns
        if isinstance(start_col, str):
            start_col_num = RangeContainer._col_letter_to_num(start_col)
            end_col_num = RangeContainer._col_letter_to_num(end_col)

            for col_num in range(start_col_num, end_col_num + 1):
                col_letter = RangeContainer._num_to_col_letter(col_num)
                header_cell = CellRef(
                    range_ref.sheet, range_ref.start.row - 1, col_letter
                )
                col_labels = self.find_labels_for_cell(header_cell, ["up"])

                if col_labels:
                    labels["columns"].extend(col_labels)

        # Find general context from the top-left area
        context_cell = CellRef(
            range_ref.sheet, range_ref.start.row, range_ref.start.col
        )
        context_labels = self.find_labels_for_cell(context_cell, ["up_left"])
        labels["context"] = context_labels

        return labels

    def _search_direction(
        self, cell_ref: CellRef, direction: str, label_type: str
    ) -> List[CellLabel]:
        """Search in a specific direction for labels."""
        labels = []

        for distance in range(1, self.max_search_distance + 1):
            search_cell = self._get_cell_at_distance(cell_ref, direction, distance)
            if not search_cell:
                break

            try:
                value = self.resolver.get_value(search_cell)
                if value is not None:
                    text = str(value)
                    confidence = self._is_meaningful_text(text)

                    if confidence > 0.0:
                        # Adjust confidence based on distance
                        distance_penalty = 0.1 * (distance - 1)
                        adjusted_confidence = max(0.1, confidence - distance_penalty)

                        label = CellLabel(
                            text=text.strip(),
                            source_ref=search_cell,
                            label_type=label_type,
                            confidence=adjusted_confidence,
                        )
                        labels.append(label)

                        # Stop searching in this direction if we found a good label
                        if confidence > 0.7:
                            break

            except (KeyError, ValueError, AttributeError):
                continue

        return labels

    def _get_cell_at_distance(
        self, cell_ref: CellRef, direction: str, distance: int
    ) -> Optional[CellRef]:
        """Get cell reference at specified distance in given direction."""
        try:
            if direction == "up":
                new_row = cell_ref.row - distance
                if new_row < 1:
                    return None
                return CellRef(cell_ref.sheet, new_row, cell_ref.col)

            elif direction == "left":
                if isinstance(cell_ref.col, str):
                    col_num = RangeContainer._col_letter_to_num(cell_ref.col)
                    new_col_num = col_num - distance
                    if new_col_num < 1:
                        return None
                    new_col = RangeContainer._num_to_col_letter(new_col_num)
                else:
                    new_col = cell_ref.col - distance
                    if new_col < 1:
                        return None

                return CellRef(cell_ref.sheet, cell_ref.row, new_col)

            elif direction == "up_left":
                new_row = cell_ref.row - distance
                if new_row < 1:
                    return None

                if isinstance(cell_ref.col, str):
                    col_num = RangeContainer._col_letter_to_num(cell_ref.col)
                    new_col_num = col_num - distance
                    if new_col_num < 1:
                        return None
                    new_col = RangeContainer._num_to_col_letter(new_col_num)
                else:
                    new_col = cell_ref.col - distance
                    if new_col < 1:
                        return None

                return CellRef(cell_ref.sheet, new_row, new_col)

        except (ValueError, AttributeError):
            return None

        return None

    def _distance_to_cell(self, from_cell: CellRef, to_cell: CellRef) -> int:
        """Calculate Manhattan distance between two cells."""
        try:
            row_diff = abs(from_cell.row - to_cell.row)

            if isinstance(from_cell.col, str) and isinstance(to_cell.col, str):
                from_col_num = RangeContainer._col_letter_to_num(from_cell.col)
                to_col_num = RangeContainer._col_letter_to_num(to_cell.col)
                col_diff = abs(from_col_num - to_col_num)
            else:
                col_diff = abs(from_cell.col - to_cell.col)

            return row_diff + col_diff

        except (ValueError, AttributeError):
            return 999  # Large distance for invalid comparisons


@dataclass
class DependencyInfo:
    """Information about a cell's dependencies."""

    cell: CellRef
    formula: Optional[str] = None
    precedents: Set[Union[CellRef, RangeRef]] = field(default_factory=set)
    dependents: Set[CellRef] = field(default_factory=set)
    ast: Optional[ASTNode] = None
    last_updated: Optional[float] = None


class FormulaResolver(ABC):
    """Abstract base class for resolving formulas from various data sources."""

    @abstractmethod
    def get_formula(self, address: CellRef) -> Optional[str]:
        """Get the formula for a given cell address."""
        pass

    @abstractmethod
    def get_value(self, address: CellRef) -> Any:
        """Get the computed value for a given cell address."""
        pass

    @abstractmethod
    def get_all_formula_cells(self) -> Iterator[CellRef]:
        """Iterate over all cells that contain formulas."""
        pass

    def get_sheet_names(self) -> List[str]:
        """Get list of available sheet names. Default implementation."""
        return ["Sheet1"]

    def cell_exists(self, address: CellRef) -> bool:
        """Check if a cell exists. Default implementation."""
        try:
            self.get_value(address)
            return True
        except (KeyError, ValueError, AttributeError):
            return False


class CachingResolver:
    """Wrapper that adds caching to any FormulaResolver."""

    def __init__(self, resolver: FormulaResolver, cache_size: int = 1000):
        self._resolver = resolver
        self._formula_cache: Dict[CellRef, Optional[str]] = {}
        self._value_cache: Dict[CellRef, Any] = {}
        self._existence_cache: Dict[CellRef, bool] = {}
        self._cache_size = cache_size

    def get_formula(self, address: CellRef) -> Optional[str]:
        """Cached formula lookup."""
        if address not in self._formula_cache:
            if len(self._formula_cache) >= self._cache_size:
                # Simple LRU eviction - remove oldest 10%
                to_remove = list(self._formula_cache.keys())[: self._cache_size // 10]
                for addr in to_remove:
                    del self._formula_cache[addr]

            self._formula_cache[address] = self._resolver.get_formula(address)

        return self._formula_cache[address]

    def get_value(self, address: CellRef) -> Any:
        """Cached value lookup."""
        if address not in self._value_cache:
            if len(self._value_cache) >= self._cache_size:
                to_remove = list(self._value_cache.keys())[: self._cache_size // 10]
                for addr in to_remove:
                    del self._value_cache[addr]

            self._value_cache[address] = self._resolver.get_value(address)

        return self._value_cache[address]

    def cell_exists(self, address: CellRef) -> bool:
        """Cached existence check."""
        if address not in self._existence_cache:
            self._existence_cache[address] = self._resolver.cell_exists(address)
        return self._existence_cache[address]

    def get_all_formula_cells(self) -> Iterator[CellRef]:
        """Delegate to underlying resolver."""
        return self._resolver.get_all_formula_cells()

    def get_sheet_names(self) -> List[str]:
        """Delegate to underlying resolver."""
        return self._resolver.get_sheet_names()

    def invalidate_cell(self, address: CellRef) -> None:
        """Invalidate cache entries for a specific cell."""
        self._formula_cache.pop(address, None)
        self._value_cache.pop(address, None)
        self._existence_cache.pop(address, None)

    def invalidate_sheet(self, sheet_name: str) -> None:
        """Invalidate all cache entries for a sheet."""
        to_remove = [
            addr for addr in self._formula_cache.keys() if addr.sheet == sheet_name
        ]
        for addr in to_remove:
            self.invalidate_cell(addr)

    def clear_cache(self) -> None:
        """Clear all caches."""
        self._formula_cache.clear()
        self._value_cache.clear()
        self._existence_cache.clear()

    def cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            "formula_cache_size": len(self._formula_cache),
            "value_cache_size": len(self._value_cache),
            "existence_cache_size": len(self._existence_cache),
        }


class DependencyTracer:
    """
    Main dependency tracing engine with caching and cycle detection.

    This class provides high-level operations for tracing formula dependencies
    while maintaining internal caches for performance and consistency.
    """

    def __init__(
        self,
        resolver: FormulaResolver,
        enable_caching: bool = True,
        default_sheet: str = "Sheet1",
    ):
        """
        Initialize the dependency tracer.

        Args:
            resolver: The formula resolver to use for data access
            enable_caching: Whether to enable caching wrapper
        """
        if enable_caching:
            self.resolver = CachingResolver(resolver)
        else:
            self.resolver = resolver

        self._dependency_cache: Dict[CellRef, DependencyInfo] = {}
        self._ast_cache: Dict[str, ASTNode] = {}  # formula -> AST
        self._reverse_deps: Dict[CellRef, Set[CellRef]] = defaultdict(set)
        self._cache_dirty = True
        self._default_sheet = default_sheet

    def _parse_formula(self, formula: str) -> ASTNode:
        """Parse formula with caching."""
        if not isinstance(formula, str):
            raise ValueError("Formula must be a string, got {}".format(type(formula)))

        if formula not in self._ast_cache:
            try:
                self._ast_cache[formula] = parse(formula)
            except Exception as e:
                logger.warning(f"Failed to parse formula '{formula}': {e}")
                raise
        return self._ast_cache[formula]

    def _extract_references_from_formula(
        self, formula: str, sheet_name: Optional[str] = None
    ) -> Set[Union[CellRef, RangeRef]]:
        """Extract all references from a formula without expanding ranges."""
        try:
            ast = self._parse_formula(formula)
            refs = collect_references(ast)

            addresses = set()
            for ref in refs:
                if hasattr(ref, "row"):  # CellRef
                    sheet = (
                        ref.sheet
                        if ref.sheet is not None
                        else sheet_name
                        if sheet_name
                        else self._default_sheet
                    )
                    cell_ref = CellRef(sheet=sheet, row=ref.row, col=ref.col)
                    addresses.add(cell_ref)
                elif hasattr(ref, "start") and hasattr(ref, "end"):  # RangeRef
                    # Keep as RangeRef instead of expanding
                    sheet = (
                        ref.sheet
                        if ref.sheet is not None
                        else sheet_name
                        if sheet_name
                        else self._default_sheet
                    )
                    range_ref = RangeRef(sheet=sheet, start=ref.start, end=ref.end)
                    addresses.add(range_ref)

            return addresses

        except Exception as e:
            logger.error(f"Error extracting references from '{formula}': {e}")
            return set()

    def _extract_references_from_formula_with_order(
        self, formula: str, sheet_name: Optional[str] = None
    ) -> List[DependencyNode]:
        """Extract references with AST evaluation order as DependencyNodes."""
        try:
            ast = self._parse_formula(formula)
            precedents = []
            order = 0

            def visit_node(node: ASTNode) -> None:
                nonlocal order
                # Visit children first (post-order traversal for evaluation order)
                for child in node.children():
                    visit_node(child)

                # Check if this node contains a reference
                ref = node.get_reference()
                if ref is not None:
                    if hasattr(ref, "row"):  # CellRef
                        sheet = (
                            ref.sheet
                            if ref.sheet is not None
                            else sheet_name
                            if sheet_name
                            else self._default_sheet
                        )
                        cell_ref = CellRef(sheet=sheet, row=ref.row, col=ref.col)
                        node = DependencyNode(
                            ref=cell_ref,
                            direction=Direction.PRECEDENT,
                            order=order,
                            _resolver=self.resolver,
                        )
                        precedents.append(node)
                        order += 1
                    elif hasattr(ref, "start") and hasattr(ref, "end"):  # RangeRef
                        sheet = (
                            ref.sheet
                            if ref.sheet is not None
                            else sheet_name
                            if sheet_name
                            else self._default_sheet
                        )
                        range_ref = RangeRef(sheet=sheet, start=ref.start, end=ref.end)
                        node = DependencyNode(
                            ref=range_ref,
                            direction=Direction.PRECEDENT,
                            order=order,
                            _resolver=self.resolver,
                        )
                        precedents.append(node)
                        order += 1

            visit_node(ast)
            return precedents

        except Exception as e:
            logger.error(f"Error extracting ordered references from '{formula}': {e}")
            return []

    def _get_or_create_dependency_info(self, address: CellRef) -> DependencyInfo:
        """Get or create dependency info for a cell."""
        if address not in self._dependency_cache:
            formula = self.resolver.get_formula(address)
            info = DependencyInfo(cell=address, formula=formula)

            if formula:
                info.precedents = self._extract_references_from_formula(
                    formula, sheet_name=address.sheet
                )
                info.ast = self._parse_formula(formula)

            self._dependency_cache[address] = info

            # Update reverse dependencies (only for CellRef precedents)
            for precedent in info.precedents:
                if isinstance(precedent, CellRef):
                    self._reverse_deps[precedent].add(address)

        return self._dependency_cache[address]

    def trace_precedents(
        self,
        target: Union[str, CellRef],
        recursive: bool = True,
        max_depth: int = 100,
        sheet_name: Optional[str] = None,
    ) -> TraceResult:
        """
        Trace all precedents (dependencies) of a given cell.

        Args:
            target: Cell to trace precedents for
            recursive: Whether to recursively trace precedents of precedents
            max_depth: Maximum recursion depth to prevent infinite loops

        Returns:
            TraceResult containing ordered DependencyNode objects representing precedents
        """
        if isinstance(target, str):
            # Handle sheet references manually if needed
            if "!" in target:
                sheet_name, cell_ref = target.split("!", 1)
                target = CellRef.from_string(cell_ref, default_sheet=sheet_name)
            else:
                target = CellRef.from_string(
                    target,
                    default_sheet=sheet_name if sheet_name else self._default_sheet,
                )

        # Normalize to absolute reference for consistent lookup
        target = CellRef(
            sheet_name if sheet_name else target.sheet,
            target.row,
            target.col,
            abs_row=True,
            abs_col=True,
        )

        if max_depth <= 0:
            logger.warning(f"Max depth reached tracing precedents for {target}")
            return []

        info = self._get_or_create_dependency_info(target)

        # Always return ordered precedents with evaluation order
        if info.formula:
            ordered_precedents = self._extract_references_from_formula_with_order(
                info.formula, sheet_name=sheet_name
            )
            if recursive:
                # For recursive ordered tracing, we need to maintain the evaluation order
                # but also include recursive dependencies
                all_precedents = []
                seen_refs = set()

                for prec_node in ordered_precedents:
                    if prec_node.ref not in seen_refs:
                        all_precedents.append(prec_node)
                        seen_refs.add(prec_node.ref)

                        # Add recursive precedents
                        if isinstance(prec_node.ref, CellRef):
                            recursive_precs = self.trace_precedents(
                                prec_node.ref,
                                recursive=True,
                                max_depth=max_depth - 1,
                                sheet_name=prec_node.ref.sheet,
                            )
                            for rec_prec in recursive_precs:
                                if rec_prec.ref not in seen_refs:
                                    all_precedents.append(rec_prec)
                                    seen_refs.add(rec_prec.ref)
                        elif isinstance(prec_node.ref, RangeRef):
                            # For ranges, trace precedents of individual cells in the range
                            start_row, start_col = (
                                (
                                    prec_node.ref.start.row,
                                    prec_node.ref.start.col,
                                )
                                if prec_node.ref.start
                                else (1, 1)
                            )
                            end_row, end_col = (
                                (
                                    prec_node.ref.end.row,
                                    prec_node.ref.end.col,
                                )
                                if prec_node.ref.end
                                else (1, 1)
                            )

                            for row in range(start_row, end_row + 1):
                                for col in range(start_col, end_col + 1):
                                    cell_ref = CellRef(
                                        sheet=prec_node.ref.sheet, row=row, col=col
                                    )
                                    recursive_precs = self.trace_precedents(
                                        cell_ref,
                                        recursive=True,
                                        max_depth=max_depth - 1,
                                        sheet_name=prec_node.ref.sheet,
                                    )
                                    for rec_prec in recursive_precs:
                                        if rec_prec.ref not in seen_refs:
                                            all_precedents.append(rec_prec)
                                            seen_refs.add(rec_prec.ref)

                return TraceResult(all_precedents)
            else:
                return TraceResult(ordered_precedents)
        else:
            return TraceResult([])

    def trace_dependents(
        self,
        target: Union[str, CellRef],
        recursive: bool = True,
        max_depth: int = 100,
        sheet_name: Optional[str] = None,
    ) -> TraceResult:
        """
        Trace all dependents (cells that depend on this one).

        Args:
            target: Cell to trace dependents for
            recursive: Whether to recursively trace dependents of dependents
            max_depth: Maximum recursion depth to prevent infinite loops

        Returns:
            TraceResult containing ordered DependencyNode objects representing dependents
        """
        if isinstance(target, str):
            # Handle sheet references manually if needed
            if "!" in target:
                sheet_name, cell_ref = target.split("!", 1)
                target = CellRef.from_string(cell_ref, default_sheet=sheet_name)
            else:
                target = CellRef.from_string(
                    target,
                    default_sheet=sheet_name if sheet_name else self._default_sheet,
                )

        # Normalize to absolute reference for consistent lookup
        target = CellRef(
            target.sheet, target.row, target.col, abs_row=True, abs_col=True
        )

        if max_depth <= 0:
            logger.warning(f"Max depth reached tracing dependents for {target}")
            return TraceResult([])

        # Ensure we have analyzed all formula cells
        self._ensure_dependency_cache_complete()

        # Convert dependents to DependencyNode objects and maintain order
        dependent_nodes = []
        seen_refs = set()

        # Sort dependents for consistent ordering (by sheet, then column, then row)
        sorted_dependents = sorted(
            self._reverse_deps[target], key=lambda x: (x.sheet, x.col, x.row)
        )

        for dependent_ref in sorted_dependents:
            if dependent_ref not in seen_refs:
                node = DependencyNode(
                    ref=dependent_ref,
                    direction=Direction.DEPENDENT,
                    _resolver=self.resolver,
                )
                dependent_nodes.append(node)
                seen_refs.add(dependent_ref)

                if recursive:
                    recursive_deps = self.trace_dependents(
                        dependent_ref,
                        recursive=True,
                        max_depth=max_depth - 1,
                        sheet_name=sheet_name,
                    )
                    for rec_dep in recursive_deps:
                        if rec_dep.ref not in seen_refs:
                            dependent_nodes.append(rec_dep)
                            seen_refs.add(rec_dep.ref)

        return TraceResult(dependent_nodes)

    def _ensure_dependency_cache_complete(self) -> None:
        """Ensure all formula cells have been analyzed."""
        if self._cache_dirty:
            logger.debug("Rebuilding dependency cache...")
            for address in self.resolver.get_all_formula_cells():
                self._get_or_create_dependency_info(address)
            self._cache_dirty = False

    def find_circular_dependencies(self) -> List[List[CellRef]]:
        """
        Find circular dependency chains in the workbook.

        Returns:
            List of circular dependency chains (each chain is a list of cells)
        """
        self._ensure_dependency_cache_complete()

        visited = set()
        rec_stack = set()
        cycles = []

        def dfs_find_cycle(cell: CellRef, path: List[CellRef]) -> None:
            if cell in rec_stack:
                # Found a cycle
                cycle_start = path.index(cell)
                cycles.append(path[cycle_start:] + [cell])
                return

            if cell in visited:
                return

            visited.add(cell)
            rec_stack.add(cell)

            info = self._dependency_cache.get(cell)
            if info:
                for precedent in info.precedents:
                    dfs_find_cycle(precedent, path + [cell])

            rec_stack.remove(cell)

        for address in self._dependency_cache:
            if address not in visited:
                dfs_find_cycle(address, [])

        return cycles

    def build_dependency_graph(self) -> Dict[CellRef, TraceResult]:
        """
        Build complete dependency graph for the workbook.

        Returns:
            Dictionary mapping each cell to its direct precedents as TraceResult
        """
        self._ensure_dependency_cache_complete()

        graph = {}
        for address in self._dependency_cache.keys():
            graph[address] = self.trace_precedents(address, recursive=False)

        return graph

    def topological_sort(self) -> List[CellRef]:
        """
        Return cells in topological order (dependencies before dependents).

        Returns:
            List of cells in evaluation order

        Raises:
            ValueError: If circular dependencies exist
        """
        cycles = self.find_circular_dependencies()
        if cycles:
            raise ValueError(f"Circular dependencies detected: {cycles}")

        graph = self.build_dependency_graph()
        in_degree = defaultdict(int)

        # Calculate in-degrees
        for cell in graph:
            in_degree[cell] = 0

        for cell, precedent_result in graph.items():
            for precedent_node in precedent_result:
                if isinstance(precedent_node.ref, CellRef):
                    in_degree[cell] += 1

        # Kahn's algorithm
        queue = deque([cell for cell, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            cell = queue.popleft()
            result.append(cell)

            # Update in-degrees for dependents
            for dependent in self._reverse_deps[cell]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result

    def invalidate_cell(self, address: Union[str, CellRef]) -> None:
        """Invalidate cache for a specific cell and mark cache as dirty."""
        if isinstance(address, str):
            # Handle sheet references manually if needed
            if "!" in address:
                sheet_name, cell_ref = address.split("!", 1)
                address = CellRef.from_string(cell_ref, default_sheet=sheet_name)
            else:
                address = CellRef.from_string(
                    address, default_sheet=self._default_sheet
                )

        # Normalize to absolute reference for consistent lookup
        address = CellRef(
            address.sheet, address.row, address.col, abs_row=True, abs_col=True
        )

        # Remove from dependency cache
        if address in self._dependency_cache:
            old_info = self._dependency_cache[address]
            # Clean up reverse dependencies (only for CellRef precedents)
            for precedent in old_info.precedents:
                if isinstance(precedent, CellRef):
                    self._reverse_deps[precedent].discard(address)
            del self._dependency_cache[address]

        # Invalidate resolver cache if applicable
        if hasattr(self.resolver, "invalidate_cell"):
            self.resolver.invalidate_cell(address)

        self._cache_dirty = True

    def invalidate_sheet(self, sheet_name: str) -> None:
        """Invalidate cache for all cells in a sheet."""
        addresses_to_remove = [
            addr for addr in self._dependency_cache.keys() if addr.sheet == sheet_name
        ]

        for address in addresses_to_remove:
            self.invalidate_cell(address)

    def clear_cache(self) -> None:
        """Clear all internal caches."""
        self._dependency_cache.clear()
        self._ast_cache.clear()
        self._reverse_deps.clear()

        if hasattr(self.resolver, "clear_cache"):
            self.resolver.clear_cache()

        self._cache_dirty = True

    def get_stats(self) -> Dict[str, Any]:
        """Get tracer statistics."""
        stats = {
            "dependency_cache_size": len(self._dependency_cache),
            "ast_cache_size": len(self._ast_cache),
            "reverse_deps_size": len(self._reverse_deps),
            "cache_dirty": self._cache_dirty,
        }

        if hasattr(self.resolver, "cache_stats"):
            stats["resolver_cache"] = self.resolver.cache_stats()

        return stats
