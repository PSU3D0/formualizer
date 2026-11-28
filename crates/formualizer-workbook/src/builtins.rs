use std::sync::Arc;
use std::sync::OnceLock;

/// Guard returned when builtins are loaded.
#[derive(Clone, Debug)]
pub struct BuiltinsGuard {
    /// Total number of functions loaded across eval and IO (best-effort count)
    pub total_registered: usize,
}

static BUILTINS_LOADED: OnceLock<BuiltinsGuard> = OnceLock::new();

/// Ensure builtins are loaded (idempotent, thread-safe).
pub fn ensure_builtins_loaded() -> &'static BuiltinsGuard {
    BUILTINS_LOADED.get_or_init(|| {
        formualizer_eval::builtins::load_builtins();
        BuiltinsGuard {
            total_registered: 0,
        }
    })
}

/// Try to load builtins, returning an error if already loaded.
pub fn try_load_builtins() -> Result<BuiltinsGuard, &'static str> {
    if BUILTINS_LOADED.get().is_some() {
        return Err("Builtins already loaded");
    }

    formualizer_eval::builtins::load_builtins();
    let guard = BuiltinsGuard {
        total_registered: 0,
    };
    // Race-safe set; if this fails, someone else loaded concurrently.
    BUILTINS_LOADED
        .set(guard.clone())
        .map_err(|_| "Race condition in builtin loading")?;
    Ok(guard)
}

/// Register a function at runtime for dynamic extension (PyO3/WASM/C-FFI adapters).
/// Safe to call after builtins are loaded.
pub fn register_function_dynamic(f: Arc<dyn formualizer_eval::function::Function>) {
    ensure_builtins_loaded();
    formualizer_eval::function_registry::register_function(f);
}
