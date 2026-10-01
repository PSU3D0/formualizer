// See Cargo.toml: musl's allocator is pathologically slow for this workload.
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    let cancel = formualizer_cli::CancelToken::new();
    let signal_token = cancel.clone();
    if let Err(error) = ctrlc::set_handler(move || signal_token.cancel()) {
        let message = format!("Unable to install SIGINT handler: {error}");
        if std::env::args_os()
            .take_while(|arg| arg != "--")
            .any(|arg| arg == "--json")
        {
            println!(
                "{}",
                serde_json::json!({
                    "schema": "formualizer.recalc/1", "status": "error", "input": null,
                    "output": null, "written": false, "formula_cells": null,
                    "cache_cells_changed": null, "worksheet_parts_changed": null,
                    "evaluated": null, "error_cells": null, "errors": null,
                    "errors_truncated": null, "refusal": null, "message": message
                })
            );
        } else {
            eprintln!("{message}");
        }
        std::process::exit(1);
    }
    let code = formualizer_cli::run(
        std::env::args_os(),
        &mut std::io::stdout().lock(),
        &mut std::io::stderr().lock(),
        Some(cancel),
    );
    std::process::exit(code);
}
