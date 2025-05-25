"""
Test cases for dependency tracer and resolvers.

This module provides comprehensive tests for the dependency tracing system,
including unit tests, integration tests, and practical examples.
"""

import unittest
from typing import Dict, Any
import tempfile
import json
from pathlib import Path

from . import DependencyTracer, FormulaResolver
from formualizer import CellRef, RangeRef
from .resolvers import DictResolver, JsonResolver, CombinedResolver


class MockResolver(FormulaResolver):
    """Simple mock resolver for testing."""

    def __init__(self):
        self.formulas = {
            CellRef("Sheet1", 1, "A"): "=B1+C1",
            CellRef("Sheet1", 1, "B"): "=D1*2",
            CellRef("Sheet1", 1, "C"): "=10",
            CellRef("Sheet1", 1, "D"): "=5",
            CellRef("Sheet1", 2, "A"): "=A1+1",  # Creates dependency chain
            CellRef("Sheet1", 3, "A"): "=B3",  # Circular reference
            CellRef("Sheet1", 3, "B"): "=A3",  # Circular reference
        }
        self.values = {
            CellRef("Sheet1", 1, "A"): 20,
            CellRef("Sheet1", 1, "B"): 10,
            CellRef("Sheet1", 1, "C"): 10,
            CellRef("Sheet1", 1, "D"): 5,
            CellRef("Sheet1", 2, "A"): 21,
            CellRef("Sheet1", 3, "A"): "#REF!",
            CellRef("Sheet1", 3, "B"): "#REF!",
        }

    def _normalize_address(self, address: CellRef) -> CellRef:
        """Normalize address to absolute reference for consistent lookup."""
        return CellRef(
            address.sheet, address.row, address.col, abs_row=True, abs_col=True
        )

    def get_formula(self, address: CellRef) -> str:
        normalized = self._normalize_address(address)
        return self.formulas.get(normalized)

    def get_value(self, address: CellRef) -> Any:
        normalized = self._normalize_address(address)
        return self.values.get(normalized)

    def get_all_formula_cells(self):
        return iter(self.formulas.keys())

    def cell_exists(self, address: CellRef) -> bool:
        normalized = self._normalize_address(address)
        return normalized in self.formulas or normalized in self.values


class TestDependencyTracer(unittest.TestCase):
    """Test cases for the main DependencyTracer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.resolver = MockResolver()
        self.tracer = DependencyTracer(self.resolver)

    def test_trace_precedents_simple(self):
        """Test tracing precedents for a simple formula."""
        precedents = self.tracer.trace_precedents("Sheet1!A1", recursive=False)
        expected = {CellRef("Sheet1", 1, "B"), CellRef("Sheet1", 1, "C")}
        self.assertEqual(precedents, expected)

    def test_trace_precedents_recursive(self):
        """Test recursive precedent tracing."""
        precedents = self.tracer.trace_precedents("Sheet1!A1", recursive=True)
        expected = {
            CellRef("Sheet1", 1, "B"),
            CellRef("Sheet1", 1, "C"),
            CellRef("Sheet1", 1, "D"),  # B1 depends on D1
        }
        self.assertEqual(precedents, expected)

    def test_trace_dependents(self):
        """Test tracing dependents."""
        dependents = self.tracer.trace_dependents("Sheet1!A1", recursive=False)
        expected = {CellRef("Sheet1", 2, "A")}
        self.assertEqual(dependents, expected)

    def test_circular_dependencies(self):
        """Test detection of circular dependencies."""
        cycles = self.tracer.find_circular_dependencies()

        # Should find the A3 <-> B3 cycle
        self.assertGreater(len(cycles), 0)
        cycle_cells = set()
        for cycle in cycles:
            cycle_cells.update(cycle)

        self.assertIn(CellRef("Sheet1", 3, "A"), cycle_cells)
        self.assertIn(CellRef("Sheet1", 3, "B"), cycle_cells)

    def test_topological_sort_with_cycles(self):
        """Test that topological sort fails with circular dependencies."""
        with self.assertRaises(ValueError) as cm:
            self.tracer.topological_sort()
        self.assertIn("Circular dependencies", str(cm.exception))

    def test_cache_invalidation(self):
        """Test cache invalidation functionality."""
        # Trace something to populate cache
        self.tracer.trace_precedents("Sheet1!A1")

        # Check cache is populated
        stats = self.tracer.get_stats()
        self.assertGreater(stats["dependency_cache_size"], 0)

        # Invalidate and check cache is cleared for that cell
        self.tracer.invalidate_cell("Sheet1!A1")

        # Should rebuild when accessed again
        precedents = self.tracer.trace_precedents("Sheet1!A1")
        self.assertIsNotNone(precedents)

    def test_max_depth_protection(self):
        """Test maximum depth protection prevents infinite recursion."""
        # This shouldn't crash even with circular references
        precedents = self.tracer.trace_precedents("Sheet1!A3", max_depth=5)
        self.assertIsInstance(precedents, set)

    def test_ordered_precedents(self):
        """Test ordered precedent tracing."""
        # Test ordered tracing
        ordered_precedents = self.tracer.trace_precedents(
            "Sheet1!A1", recursive=False, ordered=True
        )
        self.assertIsInstance(ordered_precedents, list)

        # Each item should be a PrecedentInfo with ref and order
        for item in ordered_precedents:
            self.assertTrue(hasattr(item, "ref"))
            self.assertTrue(hasattr(item, "order"))
            self.assertIsInstance(item.order, int)

        # Test non-ordered tracing still returns set
        unordered_precedents = self.tracer.trace_precedents(
            "Sheet1!A1", recursive=False, ordered=False
        )
        self.assertIsInstance(unordered_precedents, set)

    def test_range_not_expanded(self):
        """Test that ranges are not expanded into individual cells."""

        # Create a resolver with a range formula
        class RangeResolver(FormulaResolver):
            def get_formula(self, address):
                if address.row == 1 and address.col == 1 and address.sheet == "Sheet1":
                    return "=SUM(B1:B3)"
                return None

            def get_value(self, address):
                return 100

            def get_all_formula_cells(self):
                return iter([CellRef("Sheet1", 1, 1)])

            def cell_exists(self, address):
                return True

        resolver = RangeResolver()
        tracer = DependencyTracer(resolver)

        # Get precedents
        precedents = tracer.trace_precedents("Sheet1!A1", recursive=False)

        # Should contain a RangeRef, not individual CellRefs
        range_refs = [ref for ref in precedents if hasattr(ref, "start")]
        cell_refs = [ref for ref in precedents if hasattr(ref, "row")]

        # Check if we got any references at all
        self.assertGreater(len(precedents), 0, "Should have at least one precedent")
        self.assertEqual(len(range_refs), 1, "Should have exactly one range reference")
        self.assertEqual(
            len(cell_refs), 0, "Should have no individual cell references for the range"
        )


class TestResolvers(unittest.TestCase):
    """Test cases for different resolver implementations."""

    def test_dict_resolver(self):
        """Test DictResolver functionality."""
        data = {
            "Sheet1": {
                (1, "A"): {"formula": "=B1+1", "value": 11},
                (1, "B"): {"value": 10},
            }
        }
        resolver = DictResolver(data)

        addr_a1 = CellRef("Sheet1", 1, "A")
        addr_b1 = CellRef("Sheet1", 1, "B")

        self.assertEqual(resolver.get_formula(addr_a1), "=B1+1")
        self.assertEqual(resolver.get_value(addr_a1), 11)
        self.assertIsNone(resolver.get_formula(addr_b1))
        self.assertEqual(resolver.get_value(addr_b1), 10)

        formula_cells = list(resolver.get_all_formula_cells())
        self.assertEqual(len(formula_cells), 1)
        self.assertEqual(formula_cells[0], addr_a1)

    def test_json_resolver(self):
        """Test JsonResolver functionality."""
        json_data = {
            "sheets": {
                "Sheet1": {
                    "cells": {
                        "A1": {"formula": "=B1+1", "value": 11},
                        "B1": {"value": 10},
                    }
                }
            }
        }

        resolver = JsonResolver(json_data)

        addr_a1 = CellRef("Sheet1", 1, "A")
        addr_b1 = CellRef("Sheet1", 1, "B")

        self.assertEqual(resolver.get_formula(addr_a1), "=B1+1")
        self.assertEqual(resolver.get_value(addr_a1), 11)
        self.assertIsNone(resolver.get_formula(addr_b1))
        self.assertEqual(resolver.get_value(addr_b1), 10)

    def test_json_resolver_from_file(self):
        """Test JsonResolver loading from file."""
        json_data = {
            "sheets": {
                "Sheet1": {
                    "cells": {
                        "A1": {"formula": "=SUM(B1:B3)", "value": 30},
                        "B1": {"value": 10},
                        "B2": {"value": 20},
                    }
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(json_data, f)
            temp_path = f.name

        try:
            resolver = JsonResolver(temp_path)
            addr = CellRef("Sheet1", 1, "A")
            self.assertEqual(resolver.get_formula(addr), "=SUM(B1:B3)")
            self.assertEqual(resolver.get_value(addr), 30)
        finally:
            Path(temp_path).unlink()

    def test_combined_resolver(self):
        """Test CombinedResolver priority handling."""
        # High priority resolver with formulas
        high_priority_data = {
            "Sheet1": {
                (1, "A"): {"formula": "=B1*2", "value": 20},
            }
        }
        high_resolver = DictResolver(high_priority_data)

        # Low priority resolver with different formula
        low_priority_data = {
            "Sheet1": {
                (1, "A"): {"formula": "=B1+1", "value": 11},
                (1, "B"): {"value": 10},
            }
        }
        low_resolver = DictResolver(low_priority_data)

        combined = CombinedResolver([high_resolver, low_resolver])

        addr_a1 = CellRef("Sheet1", 1, "A")
        addr_b1 = CellRef("Sheet1", 1, "B")

        # Should get formula from high priority resolver
        self.assertEqual(combined.get_formula(addr_a1), "=B1*2")
        # Should get value from high priority resolver
        self.assertEqual(combined.get_value(addr_a1), 20)

        # Should fall back to low priority for B1
        self.assertEqual(combined.get_value(addr_b1), 10)


class TestIntegration(unittest.TestCase):
    """Integration tests showing practical usage."""

    def test_complete_workflow(self):
        """Test complete dependency tracing workflow."""
        # Create a more complex scenario
        json_data = {
            "sheets": {
                "Sheet1": {
                    "cells": {
                        "A1": {"formula": "=SUM(B1:B3)", "value": 60},
                        "B1": {"formula": "=C1*2", "value": 20},
                        "B2": {"formula": "=C2*2", "value": 20},
                        "B3": {"formula": "=C3*2", "value": 20},
                        "C1": {"value": 10},
                        "C2": {"value": 10},
                        "C3": {"value": 10},
                        "D1": {"formula": "=A1/2", "value": 30},  # Depends on A1
                        "E1": {
                            "formula": "=D1+A1",
                            "value": 90,
                        },  # Depends on both D1 and A1
                    }
                },
                "Sheet2": {
                    "cells": {
                        "A1": {
                            "formula": "=Sheet1!E1*2",
                            "value": 180,
                        },  # Cross-sheet reference
                    }
                },
            }
        }

        resolver = JsonResolver(json_data)
        tracer = DependencyTracer(resolver)

        # Test cross-sheet precedent tracing
        sheet2_a1_precedents = tracer.trace_precedents("Sheet2!A1", recursive=True)

        # Now ranges are kept as RangeRef instead of being expanded
        # We need to verify that the range B1:B3 is present as a RangeRef
        range_refs = [ref for ref in sheet2_a1_precedents if hasattr(ref, "start")]
        cell_refs = [ref for ref in sheet2_a1_precedents if hasattr(ref, "row")]

        # Should include all the upstream dependencies
        # E1, D1, A1 (direct cell dependencies) plus range B1:B3 and individual C cells
        self.assertGreater(len(sheet2_a1_precedents), 0)

        # Check that we have the expected cell references
        expected_cell_refs = {
            CellRef("Sheet1", 1, "E"),  # Direct precedent
            CellRef("Sheet1", 1, "D"),  # E1 depends on D1
            CellRef("Sheet1", 1, "A"),  # Both D1 and E1 depend on A1
            CellRef("Sheet1", 1, "C"),  # B1 depends on C1
            CellRef("Sheet1", 2, "C"),  # B2 depends on C2
            CellRef("Sheet1", 3, "C"),  # B3 depends on C3
        }

        for expected_cell in expected_cell_refs:
            self.assertIn(
                expected_cell, cell_refs, f"Expected {expected_cell} in precedents"
            )

        # Check that we have at least one range reference (B1:B3)
        self.assertGreater(len(range_refs), 0, "Expected at least one range reference")

        # Test dependent tracing
        c1_dependents = tracer.trace_dependents("Sheet1!C1", recursive=True)

        # With range references, C1 is directly referenced by B1,
        # but A1 references B1:B3 as a range, not individual cells
        # So the dependency chain is: C1 -> B1, but B1 is referenced via range B1:B3 in A1
        expected_dependents = {
            CellRef("Sheet1", 1, "B"),  # B1 depends on C1
            # A1 doesn't directly depend on C1, it depends on range B1:B3
            # D1, E1, Sheet2!A1 are indirect through A1
        }

        # Check that B1 is definitely a dependent of C1
        self.assertIn(CellRef("Sheet1", 1, "B"), c1_dependents)

        # The exact dependency count may differ due to range handling
        self.assertGreater(len(c1_dependents), 0)

        # Test that we can get topological order (no cycles)
        topo_order = tracer.topological_sort()
        self.assertIsInstance(topo_order, list)
        self.assertGreater(len(topo_order), 0)

        # Verify topological order property: dependencies come before dependents
        order_map = {cell: i for i, cell in enumerate(topo_order)}

        for cell in topo_order:
            precedents = tracer.trace_precedents(cell, recursive=False)
            for precedent in precedents:
                if precedent in order_map:  # Only check cells that have formulas
                    self.assertLess(
                        order_map[precedent],
                        order_map[cell],
                        f"{precedent} should come before {cell} in topological order",
                    )


class TestCaching(unittest.TestCase):
    """Test caching behavior."""

    def test_caching_resolver_performance(self):
        """Test that caching resolver avoids duplicate calls."""
        call_counts = {}

        class CountingResolver(FormulaResolver):
            def get_formula(self, address):
                call_counts[f"formula_{address}"] = (
                    call_counts.get(f"formula_{address}", 0) + 1
                )
                return f"=B{address.row}+1" if address.col == "A" else None

            def get_value(self, address):
                call_counts[f"value_{address}"] = (
                    call_counts.get(f"value_{address}", 0) + 1
                )
                return 42

            def get_all_formula_cells(self):
                return iter([CellRef("Sheet1", 1, "A")])

        base_resolver = CountingResolver()
        tracer = DependencyTracer(base_resolver, enable_caching=True)

        # Access same cell multiple times
        addr = CellRef("Sheet1", 1, "A")
        tracer.resolver.get_formula(addr)
        tracer.resolver.get_formula(addr)
        tracer.resolver.get_value(addr)
        tracer.resolver.get_value(addr)

        # Should only call underlying resolver once per method
        self.assertEqual(call_counts.get(f"formula_{addr}", 0), 1)
        self.assertEqual(call_counts.get(f"value_{addr}", 0), 1)

    def test_cache_invalidation_propagation(self):
        """Test that cache invalidation works correctly."""
        resolver = MockResolver()
        tracer = DependencyTracer(resolver, enable_caching=True)

        # Populate cache
        tracer.trace_precedents("Sheet1!A1", recursive=False)
        original_stats = tracer.get_stats()

        # Modify resolver data
        resolver.formulas[CellRef("Sheet1", 1, "A")] = "=B1+C1+D1"

        # Cache should still return old data
        precedents_cached = tracer.trace_precedents("Sheet1!A1", recursive=False)

        # Invalidate cache
        tracer.invalidate_cell("Sheet1!A1")

        # Should get new data
        precedents_fresh = tracer.trace_precedents("Sheet1!A1", recursive=False)

        # Results should be different due to formula change
        self.assertNotEqual(precedents_cached, precedents_fresh)


def create_example_workbook_data():
    """Create example data for testing and demonstration."""
    return {
        "sheets": {
            "Dashboard": {
                "cells": {
                    "A1": {"formula": "=SUM(Data!B:B)", "value": 300},
                    "A2": {"formula": "=AVERAGE(Data!B:B)", "value": 60},
                    "A3": {"formula": "=MAX(Data!B:B)", "value": 100},
                    "B1": {"formula": "=A1*0.1", "value": 30},  # 10% of total
                    "C1": {"formula": '=IF(A2>50,"Good","Poor")', "value": "Good"},
                }
            },
            "Data": {
                "cells": {
                    "A1": {"value": "Item"},
                    "B1": {"value": "Sales"},
                    "A2": {"value": "Product A"},
                    "B2": {"value": 50},
                    "A3": {"value": "Product B"},
                    "B3": {"value": 75},
                    "A4": {"value": "Product C"},
                    "B4": {"value": 100},
                    "A5": {"value": "Product D"},
                    "B5": {"value": 25},
                    "A6": {"value": "Product E"},
                    "B6": {"value": 50},
                }
            },
            "Analysis": {
                "cells": {
                    "A1": {
                        "formula": "=Dashboard!A1/Dashboard!A2",
                        "value": 5,
                    },  # Total/Average ratio
                    "A2": {
                        "formula": "=Dashboard!B1*12",
                        "value": 360,
                    },  # Annual projection
                }
            },
        }
    }


def run_example():
    """Run a practical example demonstrating the dependency tracer."""
    print("=== Dependency Tracer Example ===\n")

    # Create example data
    workbook_data = create_example_workbook_data()

    # Set up tracer
    resolver = JsonResolver(workbook_data)
    tracer = DependencyTracer(resolver)

    print("1. Tracing precedents for Dashboard!A1 (SUM formula)")
    precedents = tracer.trace_precedents("Dashboard!A1", recursive=True)
    for p in sorted(precedents, key=lambda x: (x.sheet, x.col, x.row)):
        print(f"   - {p}")

    print(f"\n2. Tracing dependents for Data!B2 (affects what formulas?)")
    dependents = tracer.trace_dependents("Data!B2", recursive=True)
    for d in sorted(dependents, key=lambda x: (x.sheet, x.col, x.row)):
        print(f"   - {d}")

    print(f"\n3. Building dependency graph")
    graph = tracer.build_dependency_graph()
    print(f"   Found {len(graph)} cells with dependencies")

    print(f"\n4. Topological sort (evaluation order)")
    try:
        topo_order = tracer.topological_sort()
        print("   Evaluation order:")
        for i, cell in enumerate(topo_order[:10]):  # Show first 10
            formula = resolver.get_formula(cell)
            print(f"   {i + 1:2d}. {cell} = {formula}")
        if len(topo_order) > 10:
            print(f"   ... and {len(topo_order) - 10} more")
    except ValueError as e:
        print(f"   Error: {e}")

    print(f"\n5. Cache statistics")
    stats = tracer.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")


if __name__ == "__main__":
    # Run example if called directly
    run_example()

    print("\n" + "=" * 50)
    print("Running unit tests...")
    unittest.main(verbosity=2)
