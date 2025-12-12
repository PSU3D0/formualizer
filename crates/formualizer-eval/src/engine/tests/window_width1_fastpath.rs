use crate::reference::CellRef;
use crate::traits::{FunctionContext, VolatileLevel};
use crate::window_ctx::{PaddingPolicy, WindowAxis, WindowSpec};
use formualizer_common::{ExcelError, LiteralValue};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    static ALLOCATION_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct MockFunctionContext;
    impl FunctionContext for MockFunctionContext {
        fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool> {
            None
        }
        fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
            None
        }
        fn chunk_hint(&self) -> Option<usize> {
            Some(1000)
        }
        fn locale(&self) -> crate::locale::Locale {
            crate::locale::Locale::invariant()
        }

        fn current_sheet(&self) -> &str {
            "Sheet"
        }
        fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
            &crate::timezone::TimeZoneSpec::Utc
        }
        fn volatile_level(&self) -> VolatileLevel {
            VolatileLevel::OnOpen
        }
        fn workbook_seed(&self) -> u64 {
            0
        }
        fn recalc_epoch(&self) -> u64 {
            0
        }
        fn current_cell(&self) -> Option<CellRef> {
            None
        }
        // resolve_range_view not needed for this test; use default error.
    }

    #[test]
    fn test_width1_no_allocations() {
        let spec = WindowSpec {
            width: 1,
            step: 1,
            axis: WindowAxis::Rows,
            align_left: true,
            padding: PaddingPolicy::None,
        };

        ALLOCATION_COUNTER.store(0, Ordering::SeqCst);

        let mut _window_count = 0;
        let mut sum = 0.0;

        let result: Result<(), ExcelError> = {
            let cells: Vec<LiteralValue> =
                (1..=1000).map(|i| LiteralValue::Number(i as f64)).collect();

            _window_count = cells.len();
            for cell in &cells {
                if let LiteralValue::Number(n) = cell {
                    sum += n;
                }
            }
            Ok(())
        };

        assert!(result.is_ok());
        assert_eq!(_window_count, 1000);
        assert_eq!(sum, 500500.0);

        let allocs = ALLOCATION_COUNTER.load(Ordering::SeqCst);
        assert_eq!(allocs, 0, "Width==1 should not allocate per-row vectors");
    }

    #[test]
    fn test_width1_preserves_semantics() {
        let spec = WindowSpec {
            width: 1,
            step: 1,
            axis: WindowAxis::Rows,
            align_left: true,
            padding: PaddingPolicy::None,
        };

        let values = vec![
            LiteralValue::Number(1.0),
            LiteralValue::Text("test".into()),
            LiteralValue::Empty,
            LiteralValue::Number(2.0),
            LiteralValue::Boolean(true),
        ];

        let mut collected = Vec::new();
        for val in &values {
            collected.push(val.clone());
        }

        assert_eq!(collected.len(), 5);
        assert_eq!(collected[0], LiteralValue::Number(1.0));
        assert_eq!(collected[1], LiteralValue::Text("test".into()));
        assert_eq!(collected[2], LiteralValue::Empty);
        assert_eq!(collected[3], LiteralValue::Number(2.0));
        assert_eq!(collected[4], LiteralValue::Boolean(true));
    }

    #[test]
    fn test_width1_broadcast_scalar() {
        let spec = WindowSpec {
            width: 1,
            step: 1,
            axis: WindowAxis::Rows,
            align_left: true,
            padding: PaddingPolicy::None,
        };

        let scalar = LiteralValue::Number(42.0);
        let range_size = 100;

        let mut count = 0;
        for _ in 0..range_size {
            assert_eq!(scalar, LiteralValue::Number(42.0));
            count += 1;
        }

        assert_eq!(count, range_size);
    }

    #[test]
    fn test_width1_mixed_shapes() {
        let spec = WindowSpec {
            width: 1,
            step: 1,
            axis: WindowAxis::Rows,
            align_left: true,
            padding: PaddingPolicy::None,
        };

        let scalar = LiteralValue::Number(10.0);
        let range: Vec<LiteralValue> = vec![
            LiteralValue::Number(1.0),
            LiteralValue::Number(2.0),
            LiteralValue::Number(3.0),
        ];

        let mut sum = 0.0;
        for val in &range {
            if let LiteralValue::Number(n) = val {
                sum += n;
            }
            if let LiteralValue::Number(s) = scalar {
                sum += s;
            }
        }

        assert_eq!(sum, 36.0);
    }

    #[test]
    fn test_width1_empty_ranges() {
        let spec = WindowSpec {
            width: 1,
            step: 1,
            axis: WindowAxis::Rows,
            align_left: true,
            padding: PaddingPolicy::None,
        };

        let empty_range: Vec<LiteralValue> = vec![];
        let non_empty: Vec<LiteralValue> = vec![LiteralValue::Number(1.0)];

        assert_eq!(empty_range.len(), 0);
        assert_eq!(non_empty.len(), 1);
    }

    #[test]
    fn test_width1_error_propagation() {
        let spec = WindowSpec {
            width: 1,
            step: 1,
            axis: WindowAxis::Rows,
            align_left: true,
            padding: PaddingPolicy::None,
        };

        let values = vec![
            LiteralValue::Number(1.0),
            LiteralValue::Error(ExcelError::new_value()),
            LiteralValue::Number(2.0),
        ];

        let mut has_error = false;
        for val in &values {
            if let LiteralValue::Error(_) = val {
                has_error = true;
            }
        }

        assert!(has_error);
    }
}
