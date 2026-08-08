#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/formualizer-t2-mutations.XXXXXX")
target_dir="$root/target/t2-mutations"
active_worktree=""

cleanup() {
  if [[ -n "$active_worktree" ]]; then
    git -C "$root" worktree remove --force "$active_worktree" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

mutate_and_expect_failure() {
  local label=$1
  local test_name=$2
  local mutation=$3
  local worktree="$tmp/$label"

  git -C "$root" worktree add --detach "$worktree" HEAD >/dev/null
  active_worktree=$worktree
  cp "$root/crates/formualizer-eval/src/engine/tests/ast_edge_invariant.rs" \
    "$worktree/crates/formualizer-eval/src/engine/tests/ast_edge_invariant.rs"
  cp "$root/crates/formualizer-eval/src/engine/tests/mod.rs" \
    "$worktree/crates/formualizer-eval/src/engine/tests/mod.rs"

  CASE_MUTATION=$mutation python3 - "$worktree" <<'PY'
import os
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
case = os.environ["CASE_MUTATION"]

if case in {"drop-adjusted-cell-edge", "skip-shifted-range-dependency", "wrong-sheet-after-insert"}:
    path = root / "crates/formualizer-eval/src/engine/graph/mod.rs"
    text = path.read_text()
    if case == "drop-adjusted-cell-edge":
        old = "        self.add_dependent_edges(id, &new_dependencies);"
        new = "        let _ = &new_dependencies;"
    elif case == "skip-shifted-range-dependency":
        old = "        self.add_range_dependent_edges(id, &new_range_dependencies, sheet_id);"
        new = "        let _ = &new_range_dependencies;"
    else:
        old = "    pub fn update_vertex_formula(&mut self, id: VertexId, ast: ASTNode) -> Result<(), ExcelError> {\n        // Get the sheet_id for this vertex\n        let sheet_id = self.store.sheet_id(id);"
        new = "    pub fn update_vertex_formula(&mut self, id: VertexId, ast: ASTNode) -> Result<(), ExcelError> {\n        // Mutant: resolve adjusted unqualified references on the wrong sheet.\n        let sheet_id = self.default_sheet_id;"
else:
    path = root / "crates/formualizer-eval/src/engine/graph/editor/reference_adjuster.rs"
    text = path.read_text()
    old = "                    ReferenceAdjustment::Invalidated => ASTNodeType::Literal(LiteralValue::Error(\n                        ExcelError::new(ExcelErrorKind::Ref),\n                    )),"
    new = "                    ReferenceAdjustment::Invalidated => return None,"

if text.count(old) != 1:
    raise SystemExit(f"mutation anchor count for {case}: {text.count(old)}")
path.write_text(text.replace(old, new, 1))
PY

  local output="$tmp/$label.log"
  if CARGO_TARGET_DIR="$target_dir" cargo test \
      --manifest-path "$worktree/Cargo.toml" \
      -p formualizer-eval "$test_name" -- --exact >"$output" 2>&1; then
    echo "NOT CAUGHT: $label" >&2
    exit 1
  fi
  if ! grep -q "test result: FAILED" "$output"; then
    echo "mutation did not reach a failing test: $label" >&2
    cat "$output" >&2
    exit 1
  fi

  echo "caught: $label"
  git -C "$root" worktree remove --force "$worktree" >/dev/null
  active_worktree=""
}

mutate_and_expect_failure \
  drop-adjusted-cell-edge \
  engine::tests::ast_edge_invariant::sheet_addition_then_adjustment_keeps_unqualified_edges_on_the_formula_sheet \
  drop-adjusted-cell-edge
mutate_and_expect_failure \
  skip-shifted-range-dependency \
  engine::tests::ast_edge_invariant::row_and_column_adjustment_keeps_compressed_ranges_in_ast_edge_parity \
  skip-shifted-range-dependency
mutate_and_expect_failure \
  skip-ref-tombstone-on-delete \
  engine::tests::ast_edge_invariant::deleting_referenced_row_and_column_turns_ast_to_ref_without_phantom_edges \
  skip-ref-tombstone-on-delete
mutate_and_expect_failure \
  wrong-sheet-after-insert \
  engine::tests::ast_edge_invariant::sheet_addition_then_adjustment_keeps_unqualified_edges_on_the_formula_sheet \
  wrong-sheet-after-insert

echo "all 4 AST/edge maintenance mutants caught"
