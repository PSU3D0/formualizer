# Imported target preparation

Target evaluation prepares the source needed for the requested cells; it does not promise to evaluate an invalid workbook successfully.

## Supported Calamine formulas

Geometry-complete imported packages support selective preparation of ordinary formulas and supported complete/fragmented shared families. A request for `A1 = 1+2` can return 3 while an unrelated `B1 = NOSHEET!A1` remains staged. Requesting B1, or a cell depending on B1, still fails. Later full evaluation also reports the unresolved reference unless it is repaired.

Shared XLSX formulas are a storage representation in which cells derive from an anchored template, not collaborative/shared-workbook sessions. Reading the template to reconstruct a selected member does not demand the anchor's own precedents or every member of its family. Original source order is preserved, including ordinary overrides and descendants recorded before their anchor.

Selection retains the original replay source for unconsumed formulas. Subsequent requests can select other cells from the same package. Edits/deletion of already selected cells are not overwritten by later source preparation. Failed discovery, cancellation, admission and stale-revision checks do not consume the pending source.

This is not exception-to-cell-error conversion. See [the preparation error policy](preparation-error-policy.md).

## Compression and publication

Partially demanded families retain validated residual fragments around selected cells. Consumed members have explicit engine-bound ownership evidence; they are not invented holes, and suppression alone is not proof. Template origin/text, source identity/order and required replay counts remain intact. Later full preparation preserves compression of eligible residual and untouched families.

Completely demanded eligible families use compressed preparation rather than one legacy AST per member. Discovery coalesces overlapping narrow/full demands before per-member expansion. An ordinary `SUM` over a complete shared family therefore retains its compressed representation while unrelated broken sources remain staged. Families that were already ineligible for compression retain validated legacy fallback; no new generic cross-fragment refusal is introduced.

These plans still use existing admission, revision and prevalidated publication boundaries. Source retention is not graph-wide rollback of previously committed prefixes. See [the detailed source ownership contract](architecture/imported-target-source-isolation.md).

## Spill occupancy

Pending formulas remain occupied cells for spill planning and final publication without preparing their expressions. Exact geometry respects holes, residual shared families and live overrides. Blocked anchors keep one retained-admitted retry region each; successful occupancy edits dirty intersecting blocked anchors. Retry scans are linear in the number of tracked blocked anchors. Failed reservations are released.

Existing spill-consumer invalidation and interactive Empty-overlay visibility are not fixed here. An edited Empty can remain publicly visible over a subsequently successful computed spill member; this also occurs in released 0.9.2.

## Loading and resource implications

Calamine builds text-free coordinate/offset and shared-anchor/offset locators lazily. The initial scan and sort are not repeated for every selected dependency. Later selections seek/decode selected records and any necessary anchor. Cold loading and ordinary full preparation do not unconditionally build those locators.

The locator is retained cache storage, not temporary parsing scratch. Its actual live capacities participate in retained admission and request observations, including after failed preparation and warm reuse with tightened budgets. Work/scratch limits still apply; cancellation is cooperative at checkpoints, not preemption of a sort or parser call.

Shared selection also retains complete source coordinates and a point index at load time. An independent 10k-member synthetic probe measured an additional 182,192 live requested heap bytes (about 18.22/member) compared with the pre-shared implementation. This is not RSS, peak memory or an allocator-usable-size measurement. The cold storage cost is linear and not claimed to be zero. The locator is additional only after selection.

A partially consumed package retains its original source, locator and residual metadata. Many individual requests can cost more than one bulk/full request; metadata work is not constant in the number of families. Repeated arbitrary formula inspection has separate lookup behavior and is not accelerated by the target-selection guarantee.

## Remaining boundaries

Incomplete geometry, reconciliation-dependent packages, unsupported transports/metadata and replay backends without selective capability remain conservative. Missing-anchor or unsupported source shapes do not acquire an unsafe best-effort reconstruction mode. Dynamic/opaque dependencies can still widen the scope under existing policy.

Residual splitting uses existing limits of 128 fragments and 4,096 explicit legacy members. A request exceeding them is refused before consuming source; full preparation remains available. Edits to unconsumed members retain the existing conservative family invalidation behavior. Generic explicit-member family transports are not newly supported for residual splitting.

No partitioned-evaluation product API, table importer or default reference-error policy is introduced. Optional hidden replay capabilities default to unsupported for existing backends; opt-in producers must honor their geometry, ownership and retained-admission contracts.
