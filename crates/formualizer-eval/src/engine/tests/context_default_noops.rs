use crate::reference::CellRef;
use crate::traits::FunctionContext;
use formualizer_core::parser::ReferenceType;

#[cfg(test)]
mod tests {
    use super::*;

    // Mock implementation of FunctionContext for testing
    struct DefaultContext;

    impl FunctionContext for DefaultContext {
        // Required trait methods with minimal implementations
        fn locale(&self) -> crate::locale::Locale {
            crate::locale::Locale::invariant()
        }

        fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
            &crate::timezone::TimeZoneSpec::Utc
        }

        fn thread_pool(&self) -> Option<&std::sync::Arc<rayon::ThreadPool>> {
            None
        }

        fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool> {
            None
        }

        fn chunk_hint(&self) -> Option<usize> {
            None
        }

        fn volatile_level(&self) -> crate::traits::VolatileLevel {
            crate::traits::VolatileLevel::Always
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

        // The optional methods we're testing default to None
    }

    #[test]
    fn test_get_or_flatten_returns_none_by_default() {
        let ctx = DefaultContext;

        // Test with a simple cell reference
        let cell_ref = ReferenceType::Cell {
            sheet: Some("Sheet1".to_string()),
            row: 1,
            col: 1,
        };

        // Default implementation should return None
        assert!(ctx.get_or_flatten(&cell_ref, true).is_none());
        assert!(ctx.get_or_flatten(&cell_ref, false).is_none());

        // Test with a range reference
        let range_ref = ReferenceType::Range {
            sheet: Some("Sheet1".to_string()),
            start_row: Some(1),
            start_col: Some(1),
            end_row: Some(10),
            end_col: Some(5),
        };

        assert!(ctx.get_or_flatten(&range_ref, true).is_none());
        assert!(ctx.get_or_flatten(&range_ref, false).is_none());
    }

    #[test]
    fn test_get_or_build_mask_returns_none_by_default() {
        let ctx = DefaultContext;

        // Create a dummy criteria key
        let criteria_key = "test_criteria_key";

        // Default implementation should return None
        assert!(ctx.get_or_build_mask(criteria_key).is_none());

        // Test with different keys
        assert!(ctx.get_or_build_mask("another_key").is_none());
        assert!(ctx.get_or_build_mask("").is_none());
    }

    #[test]
    fn test_hooks_do_not_affect_existing_behavior() {
        // This test ensures that adding these hooks doesn't break existing code
        // that relies on FunctionContext
        let ctx = DefaultContext;

        // The context should still be a valid FunctionContext
        fn accepts_context(_ctx: &dyn FunctionContext) {
            // Function that accepts any FunctionContext
        }

        accepts_context(&ctx);

        // Multiple calls should all return None consistently
        for _ in 0..10 {
            let cell_ref = ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            };
            assert!(ctx.get_or_flatten(&cell_ref, true).is_none());
            assert!(ctx.get_or_build_mask("key").is_none());
        }
    }
}
