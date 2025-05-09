#[macro_export]
macro_rules! register_functions {
    ( $($fn:path),+ $(,)? ) => {{
        use std::sync::Arc;
        $(
            $crate::function_registry::register(Arc::new($fn));
        )+
    }};
}
