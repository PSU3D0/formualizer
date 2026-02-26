# SUBTOTAL/AGGREGATE visibility contract (v1)

Status: Accepted (Ticket 01)

This document is the normative contract for phase-1 `SUBTOTAL`/`AGGREGATE` visibility behavior.

## Phase 1 supported

### 1) SUBTOTAL function_num mapping

| SUBTOTAL code | Hidden-aware code | Aggregate op |
|---|---|---|
| 1 | 101 | AVERAGE |
| 2 | 102 | COUNT |
| 3 | 103 | COUNTA |
| 4 | 104 | MAX |
| 5 | 105 | MIN |
| 6 | 106 | PRODUCT |
| 7 | 107 | STDEV.S (STDEV) |
| 8 | 108 | STDEV.P (STDEVP) |
| 9 | 109 | SUM |
| 10 | 110 | VAR.S (VAR) |
| 11 | 111 | VAR.P (VARP) |

### 2) SUBTOTAL visibility semantics (frozen v1)

- `SUBTOTAL(1..11, ...)` includes all rows (visible, manually hidden, filter-hidden).
- `SUBTOTAL(101..111, ...)` excludes both manually hidden rows and filter-hidden rows.
- Visibility filtering is row-based and applied before aggregation.

### 3) AGGREGATE phase-1 scope

- Supported `function_num`: `1..11` only.
- Supported `options`: `{0,1,2,3}` only.
- Supported call shape: `AGGREGATE(function_num, options, ref1, [ref2], ...)`.
- Unsupported phase-1 options/functions must return `#N/IMPL!`:
  - `function_num` in `12..19`.
  - `options` in `4..7`.

### 4) AGGREGATE option behavior table (v1)

| option | Hidden rows | Errors in aggregated refs | Nested SUBTOTAL/AGGREGATE exclusion |
|---|---|---|---|
| 0 | Include hidden rows | Propagate errors | Not excluded in v1 |
| 1 | Exclude manually hidden + filter-hidden rows | Propagate errors | Not excluded in v1 |
| 2 | Include hidden rows | Ignore errors | Not excluded in v1 |
| 3 | Exclude manually hidden + filter-hidden rows | Ignore errors | Not excluded in v1 |

## Error behavior matrix

| Function | Condition | Result |
|---|---|---|
| SUBTOTAL | `function_num` is non-numeric or non-integer | `#VALUE!` |
| SUBTOTAL | `function_num` integer not in `{1..11,101..111}` | `#VALUE!` |
| SUBTOTAL | Fewer than 2 args (`function_num`, `ref1`) | `#VALUE!` |
| AGGREGATE | `function_num` is non-numeric or non-integer | `#VALUE!` |
| AGGREGATE | `function_num` in `12..19` | `#N/IMPL!` |
| AGGREGATE | `function_num` integer outside `1..19` | `#VALUE!` |
| AGGREGATE | `options` is non-numeric or non-integer | `#VALUE!` |
| AGGREGATE | `options` in `4..7` | `#N/IMPL!` |
| AGGREGATE | `options` integer outside `0..7` | `#VALUE!` |
| AGGREGATE | Fewer than 3 args (`function_num`, `options`, `ref1`) | `#VALUE!` |

## Deferred (not in v1)

- Nested `SUBTOTAL`/`AGGREGATE` exclusion logic is deferred.
- In v1, nested results are treated as ordinary scalar inputs, so parent aggregates may double-count nested aggregates.
- `AGGREGATE` support for `function_num` `12..19` is deferred.
- `AGGREGATE` support for `options` `4..7` semantics is deferred.
- `AGGREGATE` array-form and `k`-based variants are deferred.

## Test vectors (contract vectors)

Legend: `V` = visible row, `MH` = manually hidden row, `FH` = filter-hidden row.

| ID | Setup | Formula | Expected |
|---|---|---|---|
| V01 | `A2=10(V), A3=20(MH), A4=30(FH), A5=100(V)` | `SUBTOTAL(9,A2:A5)` | `160` |
| V02 | `A2=10(V), A3=20(MH), A4=30(FH), A5=100(V)` | `SUBTOTAL(109,A2:A5)` | `110` |
| V03 | `A2=10(V), A3=20(MH), A4=30(FH), A5=100(V)` | `SUBTOTAL(2,A2:A5)` | `4` |
| V04 | `A2=10(V), A3=20(MH), A4=30(FH), A5=100(V)` | `SUBTOTAL(102,A2:A5)` | `2` |
| V05 | `A2=10(V), A3=20(MH), A4=30(FH), A5=100(V)` | `AGGREGATE(9,0,A2:A5)` | `160` |
| V06 | `A2=10(V), A3=20(MH), A4=30(FH), A5=100(V)` | `AGGREGATE(9,1,A2:A5)` | `110` |
| V07 | `A2=10(V), A3=#DIV/0!(V), A4=30(V)` | `AGGREGATE(9,0,A2:A4)` | `#DIV/0!` |
| V08 | `A2=10(V), A3=#DIV/0!(V), A4=30(V)` | `AGGREGATE(9,2,A2:A4)` | `40` |
| V09 | `A2=10(V), A3=#DIV/0!(MH), A4=30(V)` | `SUBTOTAL(109,A2:A4)` | `40` |
| V10 | `A2=10(V), A3=SUBTOTAL(9,A2:A2)=10(V), A4=30(V)` | `SUBTOTAL(9,A2:A4)` | `50` (nested not excluded) |
| V11 | `A2=10(V), A3=20(V)` | `AGGREGATE(12,0,A2:A3)` | `#N/IMPL!` |
| V12 | `A2=10(V), A3=20(V)` | `AGGREGATE(9,4,A2:A3)` | `#N/IMPL!` |
| V13 | `A2=10(V), A3=20(V)` | `AGGREGATE(9,8,A2:A3)` | `#VALUE!` |
| V14 | `A2=10(V), A3=20(V)` | `SUBTOTAL(12,A2:A3)` | `#VALUE!` |
| V15 | `A2=10(V), A3=20(V)` | `AGGREGATE(9)` | `#VALUE!` |
