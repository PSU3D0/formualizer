# Imported target preparation

Target evaluation prepares the source needed for the requested cells; it does not promise to evaluate an invalid workbook successfully.

## Ordinary Calamine formulas

Geometry-complete imported packages containing ordinary, individually stored formulas support selective preparation. A request for `A1 = 1+2` can return 3 while an unrelated `B1 = NOSHEET!A1` remains staged. Requesting B1, or a cell depending on B1, still fails. Later full evaluation also reports the unresolved reference unless it is repaired.

Selection keeps the original replay source until its remaining formulas are consumed. Subsequent requests can select other cells from the same package. Edits to already selected cells are not overwritten when the remaining source is prepared. Failed discovery, cancellation, admission and stale-revision checks do not consume the pending source.

This is not exception-to-cell-error conversion. See [the preparation error policy](preparation-error-policy.md).

## Loading and resource implications

Calamine builds a text-free coordinate/offset locator lazily, on the first eligible selective request. This is one source scan and a sort, not a scan of the entire source for every selected dependency. Later lookups seek/decode the selected records. Cold loading and ordinary full preparation do not unconditionally build this locator.

The locator is retained cache storage, not temporary parsing scratch. Its live capacity participates in retained admission and request observations, including when later preparation fails. Cache reuse does not remove that accounting obligation. Existing caches are considered when budgets change. Work and scratch limits still apply to building and reading the index; cancellation is cooperative at checkpoints, not preemption of a sort or parser call.

There are real tradeoffs: the first targeted request has indexing cost, and a partially consumed package retains its original source plus locator. Asking for many cells individually can cost more than one full request. Repeated arbitrary formula inspection has separate source lookup behavior and is not accelerated by this target-selection guarantee.

## Conservative cases and follow-up

At this stage, genuine shared/fragmented-family packages, incomplete source geometry, reconciliation-dependent packages and replay backends without selective capability retain conservative package preparation. Even an ordinary cell inside such a mixed package may require broader preparation. Requested dynamic/opaque dependencies can also widen the scope under the existing policy.

Shared XLSX formulas are a storage representation in which many cells derive from an anchored template. They are not collaborative/shared-workbook sessions. Separately isolating these source families requires preserving template origins, overrides, source order, residual authority and compression; it is tracked as the remaining part of #453, not claimed complete by the ordinary-source fix.

No partitioned-evaluation product API, table importer or default error policy is introduced here. Existing backend implementations need not implement the hidden optional selective-replay capability; its default remains unsupported.
