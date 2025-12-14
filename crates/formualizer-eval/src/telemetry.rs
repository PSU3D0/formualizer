#[cfg(any(feature = "tracing", feature = "tracing_chrome"))]
use std::sync::OnceLock;

#[cfg(feature = "tracing_chrome")]
use std::sync::Mutex;

/// Initialize tracing subscriber based on env vars.
/// - FZ_TRACING_CHROME=/path/to/trace.json (requires feature `tracing_chrome`)
/// - FZ_TRACING=1 enables a fmt subscriber with optional FZ_TRACING_FILTER (e.g., "info,formualizer_eval=debug").
///
/// No-op if already initialized or when feature is disabled.
#[cfg(feature = "tracing")]
pub fn init_tracing_from_env() -> bool {
    static INIT: OnceLock<bool> = OnceLock::new();
    *INIT.get_or_init(|| {
        let chrome_path = std::env::var("FZ_TRACING_CHROME").ok();
        if let Some(path) = chrome_path {
            #[cfg(feature = "tracing_chrome")]
            {
                use tracing_chrome::ChromeLayerBuilder;
                use tracing_subscriber::{prelude::*, registry};
                let (chrome_layer, guard) = ChromeLayerBuilder::new()
                    .include_args(true)
                    .file(path)
                    .build();
                // keep guard alive for process lifetime
                if let Ok(mut slot) = CHROME_GUARD.get_or_init(|| Mutex::new(None)).lock() {
                    *slot = Some(guard);
                }
                let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
                registry().with(chrome_layer).with(fmt_layer).init();
                return true;
            }
            #[cfg(not(feature = "tracing_chrome"))]
            {
                // Fallback to fmt when chrome not available
                install_fmt();
                return true;
            }
        }

        match std::env::var("FZ_TRACING").ok().as_deref() {
            Some("1") | Some("true") | Some("TRUE") => {
                install_fmt();
                true
            }
            _ => false,
        }
    })
}

#[cfg(feature = "tracing")]
fn install_fmt() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = std::env::var("FZ_TRACING_FILTER").unwrap_or_else(|_| "info".to_string());
    let _ = fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_target(false)
        .try_init();
}

#[cfg(feature = "tracing_chrome")]
static CHROME_GUARD: OnceLock<Mutex<Option<tracing_chrome::FlushGuard>>> = OnceLock::new();

#[cfg(not(feature = "tracing"))]
pub fn init_tracing_from_env() -> bool {
    false
}
