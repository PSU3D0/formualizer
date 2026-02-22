use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;
use syn::{Expr, ExprCall, ImplItem, Item, ItemImpl, ItemStruct, Lit, Meta, Type};

const BUILTINS_DIR: &str = "crates/formualizer-eval/src/builtins";

#[derive(Parser, Debug)]
#[command(name = "xtask", about = "Workspace developer tasks")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Audit builtin function documentation coverage and example quality.
    DocsAudit(DocsAuditArgs),
}

#[derive(Parser, Debug)]
struct DocsAuditArgs {
    /// Glob filter(s) applied to builtin source files (relative to repo root).
    #[arg(long = "paths")]
    paths: Vec<String>,

    /// Function name filter(s), case-insensitive. May be repeated or comma-separated.
    #[arg(long = "functions", value_delimiter = ',')]
    functions: Vec<String>,

    /// Write machine-readable report JSON to this path.
    #[arg(long = "json-out")]
    json_out: Option<PathBuf>,

    /// Fail with non-zero exit code when any issue is found.
    #[arg(long)]
    strict: bool,
}

#[derive(Debug, Clone, Serialize)]
struct FunctionAuditFinding {
    function_name: String,
    type_name: String,
    category: String,
    registration_file: String,
    impl_file: Option<String>,
    issues: Vec<String>,
    rust_example_blocks: usize,
    formula_example_blocks: usize,
    doc_lines: usize,
}

#[derive(Debug, Serialize)]
struct DocsAuditReport {
    total_registered_functions: usize,
    audited_functions: usize,
    passing_functions: usize,
    failing_functions: usize,
    total_issues: usize,
    findings: Vec<FunctionAuditFinding>,
}

#[derive(Debug, Clone)]
struct ImplInfo {
    file: String,
    function_name: Option<String>,
    doc_text: String,
}

#[derive(Debug, Default)]
struct FileFacts {
    struct_docs: BTreeMap<String, String>,
    impls: Vec<(String, ImplInfo)>,
    registrations: Vec<String>,
}

#[derive(Default)]
struct RegistrationVisitor {
    registered_types: Vec<String>,
}

impl<'ast> Visit<'ast> for RegistrationVisitor {
    fn visit_expr_call(&mut self, node: &'ast ExprCall) {
        if path_ends_with_ident(&node.func, "register_function")
            && let Some(first_arg) = node.args.first()
            && let Some(type_name) = extract_arc_new_type_name(first_arg)
        {
            self.registered_types.push(type_name);
        }

        syn::visit::visit_expr_call(self, node);
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::DocsAudit(args) => run_docs_audit(args),
    }
}

fn run_docs_audit(args: DocsAuditArgs) -> Result<()> {
    let builtins_files = collect_builtin_files(Path::new(BUILTINS_DIR))?;

    let file_filter = build_glob_filter(&args.paths)?;
    let function_filter: Option<BTreeSet<String>> = if args.functions.is_empty() {
        None
    } else {
        Some(
            args.functions
                .iter()
                .map(|name| name.trim().to_uppercase())
                .filter(|name| !name.is_empty())
                .collect(),
        )
    };

    let mut struct_docs_by_type: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut impls_by_type: BTreeMap<String, Vec<ImplInfo>> = BTreeMap::new();
    let mut registrations: Vec<(String, String)> = Vec::new();

    for file in &builtins_files {
        let file_rel = path_to_repo_string(file)?;
        let facts = parse_file_facts(file, &file_rel)
            .with_context(|| format!("failed to parse builtin file: {file_rel}"))?;

        for (type_name, doc) in facts.struct_docs {
            struct_docs_by_type
                .entry(type_name)
                .or_default()
                .push((file_rel.clone(), doc));
        }

        for (type_name, impl_info) in facts.impls {
            impls_by_type.entry(type_name).or_default().push(impl_info);
        }

        for type_name in facts.registrations {
            registrations.push((file_rel.clone(), type_name));
        }
    }

    let mut findings = Vec::new();

    for (reg_file, type_name) in registrations {
        if let Some(filter) = &file_filter
            && !filter.is_match(&reg_file)
        {
            continue;
        }

        let impl_info = select_impl_for_registration(&impls_by_type, &type_name, &reg_file);
        let function_name = impl_info
            .as_ref()
            .and_then(|info| info.function_name.clone())
            .unwrap_or_else(|| type_name.clone());

        if let Some(filter) = &function_filter
            && !filter.contains(&function_name.to_uppercase())
        {
            continue;
        }

        let struct_doc =
            select_struct_doc_for_registration(&struct_docs_by_type, &type_name, &reg_file)
                .unwrap_or_default();
        let impl_doc = impl_info
            .as_ref()
            .map(|info| info.doc_text.clone())
            .unwrap_or_default();
        let doc_text = [struct_doc.trim(), impl_doc.trim()]
            .iter()
            .filter(|s| !s.is_empty())
            .copied()
            .collect::<Vec<_>>()
            .join("\n\n");

        let fenced_blocks = parse_fenced_blocks(&doc_text);
        let rust_blocks = count_fenced_blocks_by_lang(&fenced_blocks, &["rust", "rs"]);
        let formula_blocks = count_formula_example_blocks(&fenced_blocks);
        let mut issues = Vec::new();

        if impl_info.is_none() {
            issues.push("missing-function-impl".to_string());
        }

        if impl_info
            .as_ref()
            .and_then(|info| info.function_name.as_ref())
            .is_none()
        {
            issues.push("missing-name-literal".to_string());
        }

        if doc_text.trim().is_empty() {
            issues.push("missing-doc-comment".to_string());
        }

        if rust_blocks == 0 {
            issues.push("missing-rust-example".to_string());
        }

        if formula_blocks == 0 {
            issues.push("missing-formula-example".to_string());
        }

        let category = derive_category(&reg_file);
        let doc_lines = doc_text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();

        findings.push(FunctionAuditFinding {
            function_name,
            type_name,
            category,
            registration_file: reg_file,
            impl_file: impl_info.as_ref().map(|info| info.file.clone()),
            issues,
            rust_example_blocks: rust_blocks,
            formula_example_blocks: formula_blocks,
            doc_lines,
        });
    }

    findings.sort_by(|a, b| a.function_name.cmp(&b.function_name));

    let failing_functions = findings
        .iter()
        .filter(|finding| !finding.issues.is_empty())
        .count();
    let passing_functions = findings.len().saturating_sub(failing_functions);
    let total_issues = findings.iter().map(|finding| finding.issues.len()).sum();

    let report = DocsAuditReport {
        total_registered_functions: findings.len(),
        audited_functions: findings.len(),
        passing_functions,
        failing_functions,
        total_issues,
        findings,
    };

    print_report_summary(&report);

    if let Some(path) = args.json_out {
        let json = serde_json::to_string_pretty(&report)?;
        fs::write(&path, json)
            .with_context(|| format!("failed to write report: {}", path.display()))?;
        println!("wrote JSON report: {}", path.display());
    }

    if args.strict && report.failing_functions > 0 {
        bail!(
            "docs-audit failed: {} function(s) with issues",
            report.failing_functions
        );
    }

    Ok(())
}

fn print_report_summary(report: &DocsAuditReport) {
    println!("docs-audit summary");
    println!("  total registered: {}", report.total_registered_functions);
    println!("  passing: {}", report.passing_functions);
    println!("  failing: {}", report.failing_functions);
    println!("  total issues: {}", report.total_issues);

    if report.failing_functions == 0 {
        return;
    }

    println!("\nTop failing functions:");
    for finding in report
        .findings
        .iter()
        .filter(|finding| !finding.issues.is_empty())
        .take(25)
    {
        println!(
            "  - {} ({}) [{}]",
            finding.function_name,
            finding.registration_file,
            finding.issues.join(", ")
        );
    }

    if report.failing_functions > 25 {
        println!("  ... and {} more", report.failing_functions - 25);
    }
}

fn collect_builtin_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        bail!("builtins dir not found: {}", dir.display());
    }

    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path.to_path_buf());
        }
    }
    files.sort();
    Ok(files)
}

fn parse_file_facts(path: &Path, file_rel: &str) -> Result<FileFacts> {
    let source = fs::read_to_string(path)
        .with_context(|| format!("failed to read source file: {}", path.display()))?;
    let syntax = syn::parse_file(&source)
        .with_context(|| format!("failed to parse source file: {}", path.display()))?;

    let mut facts = FileFacts::default();

    for item in &syntax.items {
        if let Item::Struct(item_struct) = item {
            collect_struct_doc(item_struct, &mut facts);
        }

        if let Item::Impl(item_impl) = item {
            collect_function_impl(item_impl, file_rel, &mut facts);
        }
    }

    let mut visitor = RegistrationVisitor::default();
    visitor.visit_file(&syntax);
    facts.registrations = visitor.registered_types;

    Ok(facts)
}

fn collect_struct_doc(item_struct: &ItemStruct, facts: &mut FileFacts) {
    let type_name = item_struct.ident.to_string();
    let doc = collect_doc_attrs(&item_struct.attrs);
    if !doc.trim().is_empty() {
        facts.struct_docs.insert(type_name, doc);
    }
}

fn collect_function_impl(item_impl: &ItemImpl, file_rel: &str, facts: &mut FileFacts) {
    let Some((_, trait_path, _)) = &item_impl.trait_ else {
        return;
    };

    let trait_ident = trait_path
        .segments
        .last()
        .map(|segment| segment.ident.to_string());
    if trait_ident.as_deref() != Some("Function") {
        return;
    }

    let Type::Path(type_path) = &*item_impl.self_ty else {
        return;
    };
    let Some(type_ident) = type_path
        .path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
    else {
        return;
    };

    let mut function_name = None;
    for impl_item in &item_impl.items {
        if let ImplItem::Fn(function) = impl_item
            && function.sig.ident == "name"
        {
            function_name = extract_name_literal_from_block(&function.block);
            break;
        }
    }

    facts.impls.push((
        type_ident,
        ImplInfo {
            file: file_rel.to_string(),
            function_name,
            doc_text: collect_doc_attrs(&item_impl.attrs),
        },
    ));
}

fn extract_name_literal_from_block(block: &syn::Block) -> Option<String> {
    for stmt in block.stmts.iter().rev() {
        match stmt {
            syn::Stmt::Expr(expr, _) => {
                if let Some(value) = extract_string_literal(expr) {
                    return Some(value);
                }
            }
            syn::Stmt::Local(local) => {
                if let Some(init) = &local.init
                    && let Some(value) = extract_string_literal(&init.expr)
                {
                    return Some(value);
                }
            }
            syn::Stmt::Item(_) | syn::Stmt::Macro(_) => {}
        }
    }
    None
}

fn extract_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Lit(lit_expr) => match &lit_expr.lit {
            Lit::Str(value) => Some(value.value()),
            _ => None,
        },
        Expr::Return(return_expr) => return_expr.expr.as_deref().and_then(extract_string_literal),
        Expr::Paren(paren) => extract_string_literal(&paren.expr),
        Expr::Group(group) => extract_string_literal(&group.expr),
        Expr::Block(block_expr) => extract_name_literal_from_block(&block_expr.block),
        Expr::If(if_expr) => extract_name_literal_from_block(&if_expr.then_branch).or_else(|| {
            if_expr
                .else_branch
                .as_ref()
                .and_then(|(_, else_expr)| extract_string_literal(else_expr))
        }),
        _ => None,
    }
}

fn collect_doc_attrs(attrs: &[syn::Attribute]) -> String {
    let mut lines = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }

        if let Meta::NameValue(name_value) = &attr.meta
            && let Expr::Lit(expr_lit) = &name_value.value
            && let Lit::Str(doc_line) = &expr_lit.lit
        {
            lines.push(doc_line.value());
        }
    }

    lines.join("\n")
}

fn path_ends_with_ident(expr: &Expr, ident: &str) -> bool {
    let Expr::Path(path_expr) = expr else {
        return false;
    };
    path_expr
        .path
        .segments
        .last()
        .map(|segment| segment.ident == ident)
        .unwrap_or(false)
}

fn extract_arc_new_type_name(expr: &Expr) -> Option<String> {
    let Expr::Call(call_expr) = unwrap_expr(expr) else {
        return None;
    };

    let Expr::Path(path_expr) = unwrap_expr(&call_expr.func) else {
        return None;
    };

    let has_arc_segment = path_expr
        .path
        .segments
        .iter()
        .any(|segment| segment.ident == "Arc");
    let ends_in_new = path_expr
        .path
        .segments
        .last()
        .map(|segment| segment.ident == "new")
        .unwrap_or(false);

    if !has_arc_segment || !ends_in_new {
        return None;
    }

    let arg = call_expr.args.first()?;
    let arg = unwrap_expr(arg);

    let Expr::Path(type_path) = arg else {
        return None;
    };

    type_path
        .path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn unwrap_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(paren) => unwrap_expr(&paren.expr),
        Expr::Group(group) => unwrap_expr(&group.expr),
        _ => expr,
    }
}

fn select_impl_for_registration(
    impls_by_type: &BTreeMap<String, Vec<ImplInfo>>,
    type_name: &str,
    registration_file: &str,
) -> Option<ImplInfo> {
    let impls = impls_by_type.get(type_name)?;
    impls
        .iter()
        .find(|impl_info| impl_info.file == registration_file)
        .or_else(|| impls.first())
        .cloned()
}

fn select_struct_doc_for_registration(
    struct_docs_by_type: &BTreeMap<String, Vec<(String, String)>>,
    type_name: &str,
    registration_file: &str,
) -> Option<String> {
    let docs = struct_docs_by_type.get(type_name)?;
    docs.iter()
        .find(|(file, _)| file == registration_file)
        .map(|(_, doc)| doc.clone())
        .or_else(|| docs.first().map(|(_, doc)| doc.clone()))
}

#[derive(Debug, Clone)]
struct FencedBlock {
    language: String,
    content: String,
}

fn parse_fenced_blocks(doc_text: &str) -> Vec<FencedBlock> {
    let mut blocks = Vec::new();
    let mut lines = doc_text.lines();

    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("```") {
            continue;
        }

        let language = parse_fence_language(trimmed);
        let mut content_lines = Vec::new();
        let mut block_closed = false;

        for next_line in lines.by_ref() {
            if next_line.trim_start().starts_with("```") {
                block_closed = true;
                break;
            }
            content_lines.push(next_line);
        }

        if block_closed {
            blocks.push(FencedBlock {
                language,
                content: content_lines.join("\n"),
            });
        }
    }

    blocks
}

fn parse_fence_language(fence_line: &str) -> String {
    let raw = fence_line.trim_start_matches("```").trim();
    let language = raw
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();

    if language.is_empty() {
        "text".to_string()
    } else {
        language
    }
}

fn count_fenced_blocks_by_lang(blocks: &[FencedBlock], languages: &[&str]) -> usize {
    let language_set: BTreeSet<String> = languages
        .iter()
        .map(|lang| lang.to_ascii_lowercase())
        .collect();

    blocks
        .iter()
        .filter(|block| language_set.contains(&block.language))
        .count()
}

fn count_formula_example_blocks(blocks: &[FencedBlock]) -> usize {
    let language_set: BTreeSet<String> = ["excel", "formula", "fx"]
        .into_iter()
        .map(|lang| lang.to_string())
        .collect();

    blocks
        .iter()
        .filter(|block| language_set.contains(&block.language))
        .filter(|block| {
            block.content.lines().any(|line| {
                let trimmed = line.trim();
                !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("//")
            })
        })
        .count()
}

fn derive_category(path: &str) -> String {
    let prefix = format!("{BUILTINS_DIR}/");
    let rel = path.strip_prefix(&prefix).unwrap_or(path);

    let mut parts = rel.split('/');
    let first = parts.next().unwrap_or("builtins");
    if first.ends_with(".rs") {
        first.trim_end_matches(".rs").to_string()
    } else {
        first.to_string()
    }
}

fn path_to_repo_string(path: &Path) -> Result<String> {
    let path = path
        .strip_prefix(std::env::current_dir().context("failed to read cwd")?)
        .unwrap_or(path);

    let path = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non-utf8 path: {}", path.display()))?;

    Ok(path.replace('\\', "/"))
}

fn build_glob_filter(patterns: &[String]) -> Result<Option<GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }

    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            Glob::new(pattern).with_context(|| format!("invalid path glob pattern: {pattern}"))?,
        );
    }

    Ok(Some(builder.build()?))
}

#[cfg(test)]
mod tests {
    use super::{count_fenced_blocks_by_lang, count_formula_example_blocks, parse_fenced_blocks};

    #[test]
    fn rust_fence_with_modifiers_counts_as_rust() {
        let doc = r#"
```rust,no_run
let x = 1;
```
"#;

        let blocks = parse_fenced_blocks(doc);
        assert_eq!(count_fenced_blocks_by_lang(&blocks, &["rust"]), 1);
    }

    #[test]
    fn formula_fence_allows_comment_lines_but_requires_formula_content() {
        let doc = r#"
```excel
# returns: 6
=SUM(1,2,3)
```

```excel
# comments only
# still comments
```
"#;

        let blocks = parse_fenced_blocks(doc);
        assert_eq!(count_formula_example_blocks(&blocks), 1);
    }
}
