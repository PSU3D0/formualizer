# Bulk ingest and evaluation in 0.16

Release build, Rust `Workbook` API, changelog off, best of 3 on one 24-core Linux host under light load, 2026-11-27. Revision `7494d03f` plus #639.

### Bulk writes

| Workload | Result |
| --- | --- |
| Overwrite 10,000 formulas with values | `set_values` 7.78 ms, `set_value` loop 59.1 ms (8×) |
| Date-formatted column, 100,000 rows | First date write 0.08 ms, dates over dates 0.07 ms, numbers over dates 0.09 ms. The last write cleared the format and read back as plain numbers. |
| Fresh sheet, 1,000,000 rows × 5 columns (5M cells) | `set_columns` 28.3 ms (6 ns/cell), `set_values` 93.9 ms (19 ns/cell). Peak RSS for the whole run was 287 MB. |
| Overwrite 100,000 rows of `Data!A` with 200 cross-sheet readers of `=SUM(Data!A:A)` | Bulk write 0.22 ms, recalculation 1.92 ms. All readers returned the correct result. |
| XLSX with one 100,000-row shared formula and deferred graph building | Load 132 ms, two bulk writes totaling 150,000 cells (half over shared-formula cells) 0.56 ms, `evaluate_all` 81.7 ms. Overwritten cells held values and surviving formulas read the new inputs. |

### Row-at-a-time writes

Row-major `set_value` loop on a fresh sheet without preallocation.

| Rows × columns | Before | After |
| --- | ---: | ---: |
| 1,000 × 10 | 46.2 ms | 26.8 ms |
| 4,000 × 10 | 410 ms | 98.9 ms |
| 16,000 × 10 | 5,625 ms | 405 ms |
| 32,768 × 10 | 23,095 ms | 865 ms |
| 65,536 × 10 | 45,896 ms | 1,663 ms |
| 100,000 × 4 | 26,560 ms | 994 ms |

### Evaluation limits

One formula on an empty sheet with a 2 s deadline.

| Operation | Before | After |
| --- | ---: | ---: |
| `SUMIF` with array criteria | 12.6 s | Deadline at 2.00 s |
| `MATCH` with array lookup values | 20.2 s | Deadline at 2.00 s |
| `DDB` with a long depreciation schedule | 3.2 s | Under 1 ms |
| `FREQUENCY` with large data and bin arrays | Over 60 s | 0.39 s |
| `TRANSPOSE` producing a wide intermediate array | Aborted over 4 GB | `#NUM!` at 41 MB |
| `LET` with an unused range binding | Aborted over 8 GB | `0` at 10 MB |
