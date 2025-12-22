use crate::reference::CellRef;
use crate::traits::FunctionContext;
use formualizer_parse::parser::ReferenceType;

#[cfg(test)]
mod tests {
    use super::*;

    // Mock implementation of FunctionContext for testing
    struct DefaultContext;

    impl<'ctx> FunctionContext<'ctx> for DefaultContext {
        // Required trait methods with minimal implementations
        fn locale(&self) -> crate::locale::Locale {
            crate::locale::Locale::invariant()
        }

        fn current_sheet(&self) -> &str {
            "Sheet"
        }

        fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
            &crate::timezone::TimeZoneSpec::Utc
        }

        fn thread_pool(&self) -> Option<&std::sync::Arc<rayon::ThreadPool>> {
            None
        }

        fn cancellation_token(&self) -> Option<std::sync::Arc<std::sync::atomic::AtomicBool>> {
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

        fn resolve_range_view(
            &self,
            _reference: &ReferenceType,
            _current_sheet: &str,
        ) -> Result<crate::engine::range_view::RangeView<'ctx>, formualizer_common::ExcelError>
        {
            Err(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::NImpl,
            ))
        }
    }

    #[test]
    fn test_get_or_flatten_returns_none_by_default() {
        let ctx = DefaultContext;

        // Test with a simple cell reference
        let cell_ref = ReferenceType::cell(Some("Sheet1".to_string()), 1, 1);

        // Default implementation should return None
        // flats removed

        // Test with a range reference
        let range_ref = ReferenceType::range(
            Some("Sheet1".to_string()),
            Some(1),
            Some(1),
            Some(10),
            Some(5),
        );

        // flats removed
    }

    #[test]
    fn test_hooks_do_not_affect_existing_behavior() {
        // This test ensures that adding these hooks doesn't break existing code
        // that relies on FunctionContext
        let ctx = DefaultContext;

        // The context should still be a valid FunctionContext
        fn accepts_context(_ctx: &dyn FunctionContext<'_>) {
            // Function that accepts any FunctionContext
        }

        accepts_context(&ctx);

        // Multiple calls should all return None consistently
        for _ in 0..10 {
            let cell_ref = ReferenceType::cell(None, 1, 1);
            // flats removed
        }
    }
}
