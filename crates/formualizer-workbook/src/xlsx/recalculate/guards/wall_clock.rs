//! Portable builds must never publish ambient date/time values from the
//! engine's epoch fallback. A caller-supplied deterministic instant is valid.
#![cfg(not(feature = "system-clock"))]
use crate::xlsx::recalculate::{IoError, XlsxRecalculateOptions, unsupported};
use formualizer_parse::parser::{ASTNode, ASTNodeType};
fn needs_clock(ast: &ASTNode) -> bool {
    match &ast.node_type {
        ASTNodeType::Function { name, args } => {
            let name = name.rsplit('.').next().unwrap_or(name);
            name.eq_ignore_ascii_case("TODAY")
                || name.eq_ignore_ascii_case("NOW")
                || args.iter().any(needs_clock)
        }
        ASTNodeType::UnaryOp { expr, .. } => needs_clock(expr),
        ASTNodeType::BinaryOp { left, right, .. } => needs_clock(left) || needs_clock(right),
        ASTNodeType::Call { callee, args } => needs_clock(callee) || args.iter().any(needs_clock),
        ASTNodeType::Array(rows) => rows.iter().flatten().any(needs_clock),
        _ => false,
    }
}
pub(in crate::xlsx) fn validate(
    formula: &str,
    context: &str,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    crate::xlsx::recalculate::checkpoint(&options.cancel)?;
    // Portable policy is validated before admission and resolved after source
    // properties are scanned. Its explicit instant is already sufficient to
    // prove the calculation does not need an ambient wall clock.
    if options.eval_config.deterministic_mode.is_enabled()
        || options
            .calculation
            .as_ref()
            .is_some_and(|policy| policy.fixed_now_utc.is_some())
    {
        return Ok(());
    }
    let upper = formula.to_ascii_uppercase();
    if !upper.contains("TODAY") && !upper.contains("NOW") {
        return Ok(());
    }
    let ast = formualizer_parse::parser::parse(format!("={}", formula.trim_start_matches('=')))
        .map_err(|e| {
            unsupported(
                "cannot prove portable formula clock requirements",
                format!("{context}: {e}"),
            )
        })?;
    if needs_clock(&ast) {
        return Err(unsupported(
            "TODAY/NOW need a wall clock; this build has none; pass a fixed timestamp",
            context,
        ));
    }
    Ok(())
}
