# Criteria ingestion and blank-range corrections

These changes correct differences between equivalent loaded and edited workbooks. They do not establish complete Excel/LibreOffice criteria parity.

## Typed blank masks (#457)

A null in a lowered-text array does not prove that its cell is blank: numeric, boolean and error cells can have no text-lane value. Empty equality/inequality masks now use the original overlay-aware type tags. Empty cells and actual empty text satisfy the blank criterion; nonempty numeric/boolean/error cells do not.

This avoids populating a persistent numeric-string cache merely to fix blank matching. COUNTIF/COUNTIFS and SUMIF/SUMIFS import/edit regressions cover the corrected mask path.

## Logical blank counts (part of #285)

COUNTIF and COUNTBLANK account arithmetically for addressed cells outside physical storage. Count-specific finite views include available computed/spill values beyond the generic graph-placement bounds, without evaluating reference-producing arguments twice. Direct whole-row/column references retain their logical Excel extents for these counts while the physical count view stays bounded. Whole-sheet cell-count arithmetic uses u64, including on wasm32; it does not iterate or allocate a full unstored worksheet rectangle.

This is not a global range-resolver or dependency-scheduling change. Independent preexisting spill-value retirement and spill-only consumer invalidation bugs reproduced in published 0.9.2 are tracked in [#459](https://github.com/PSU3D0/formualizer/issues/459) and [#460](https://github.com/PSU3D0/formualizer/issues/460), not claimed fixed here. COUNTIFS tail alignment, unbounded named aliases, SUM(IF(...)) and the rest of #285's acceptance remain outside this fix. Wide-reference admission/resolution can still be expensive even when counting the omitted cells is constant work.

## Existing wildcard compatibility

The existing scalar matcher permits stringified numeric/boolean values to match wildcard patterns and permits Empty to match `*`. This tranche preserves that policy rather than replacing it with a new text-only rule.

Mixed-type ranges build scalar-equivalent Boolean masks; text-only ranges retain vectorized matching. Only Boolean masks enter the existing bounded per-invocation memo. There is no new persistent string lane or cross-formula cache. Scalar coercion costs more CPU than the old incorrect null-lane shortcut, and repeated formulas can still do O(formulas × range) work.

General numeric/date-text coercion and escape/metacharacter behavior remain separate issues (#291/#295). This change must not be described as a complete wildcard-parity fix.

## Evidence and oracle limits

Synthetic native, Calamine and Python tests cover equivalent typed inputs, edits, chunk boundaries, explicit blank tails and logical extents. Independent LibreOffice 24.2 checks agree on the isolated blank equality/inequality, SUMIF/SUMIFS and bounded-tail cases.

LibreOffice disagrees on some COUNTIFS and wide COUNTIF cases, and its wildcard behavior is not an Excel oracle. Wide COUNTBLANK results agree; wide COUNTIF expectations follow the requested logical rectangle and the existing Empty predicate. No Excel runtime oracle was used. Tests preserve these distinctions instead of claiming blanket cross-engine conformance.
