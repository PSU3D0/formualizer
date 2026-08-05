use formualizer_sheetport::SheetPortError;

fn match_future_sheetport_error(error: &SheetPortError) -> &'static str {
    match error {
        SheetPortError::InvalidManifest { .. } => "manifest",
        SheetPortError::Engine { .. } => "engine",
        _ => "other-or-future",
    }
}

#[test]
fn downstream_sheetport_errors_allow_future_variants() {
    let _ = match_future_sheetport_error as fn(&SheetPortError) -> &'static str;
}
