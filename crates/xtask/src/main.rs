use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use formualizer_eval::args::{ArgSchema, ShapeKind};
use formualizer_eval::function::FnCaps;
use globset::{Glob, GlobSet, GlobSetBuilder};
use quote::ToTokens;
use regex::Regex;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use syn::visit::Visit;
use syn::{Expr, ExprCall, ImplItem, Item, ItemImpl, ItemStruct, Lit, Meta, Type};

const BUILTINS_DIR: &str = "crates/formualizer-eval/src/builtins";
const DOCGEN_SCHEMA_START: &str = "[formualizer-docgen:schema:start]";
const DOCGEN_SCHEMA_END: &str = "[formualizer-docgen:schema:end]";
const DOCGEN_FUNC_META_START: &str = "{/* [formualizer-docgen:function-meta:start] */}";
const DOCGEN_FUNC_META_END: &str = "{/* [formualizer-docgen:function-meta:end] */}";
const DOCGEN_FUNC_META_START_LEGACY: &str = "<!-- [formualizer-docgen:function-meta:start] -->";
const DOCGEN_FUNC_META_END_LEGACY: &str = "<!-- [formualizer-docgen:function-meta:end] -->";

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
    /// Generate/apply schema metadata blocks in builtin doc comments.
    DocsSchema(DocsSchemaArgs),
    /// Generate function reference MDX pages from runtime registry metadata.
    DocsRef(DocsRefArgs),
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

#[derive(Parser, Debug)]
struct DocsSchemaArgs {
    /// Glob filter(s) applied to builtin source files (relative to repo root).
    #[arg(long = "paths")]
    paths: Vec<String>,

    /// Function name filter(s), case-insensitive. May be repeated or comma-separated.
    #[arg(long = "functions", value_delimiter = ',')]
    functions: Vec<String>,

    /// Apply generated schema sections in-place.
    #[arg(long)]
    apply: bool,

    /// Allow apply while git working tree is dirty.
    #[arg(long)]
    allow_dirty: bool,
}

#[derive(Parser, Debug)]
struct DocsRefArgs {
    /// Glob filter(s) applied to builtin source files (relative to repo root).
    #[arg(long = "paths")]
    paths: Vec<String>,

    /// Function name filter(s), case-insensitive. May be repeated or comma-separated.
    #[arg(long = "functions", value_delimiter = ',')]
    functions: Vec<String>,

    /// Output directory for generated function pages.
    #[arg(
        long = "out-dir",
        default_value = "docs-site/content/docs/reference/functions"
    )]
    out_dir: PathBuf,

    /// Apply generated reference pages in-place.
    #[arg(long)]
    apply: bool,

    /// Allow apply while git working tree is dirty.
    #[arg(long)]
    allow_dirty: bool,
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
struct DocsSchemaEntry {
    type_name: String,
    function_name: String,
    min_args: Option<usize>,
    max_args: Option<usize>,
    variadic: Option<bool>,
    arg_schema: Option<String>,
    signature: Option<String>,
    caps: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeArgMeta {
    name: String,
    kinds: Vec<String>,
    required: bool,
    shape: String,
    by_ref: bool,
    coercion: String,
    max: Option<String>,
    repeating: Option<String>,
    has_default: bool,
}

#[derive(Debug, Clone)]
struct RuntimeFunctionMeta {
    min_args: usize,
    max_args: Option<usize>,
    variadic: bool,
    arg_schema: String,
    signature: String,
    args: Vec<RuntimeArgMeta>,
    caps: Vec<String>,
}

#[derive(Debug, Clone)]
struct FunctionRefEntry {
    function_name: String,
    function_slug: String,
    category: String,
    category_slug: String,
    type_name: String,
    registration_file: String,
    impl_file: String,
    min_args: Option<usize>,
    max_args: Option<usize>,
    variadic: Option<bool>,
    signature: Option<String>,
    arg_schema: Option<String>,
    args: Vec<RuntimeArgMeta>,
    caps: Vec<String>,
}

#[derive(Debug, Clone)]
struct ImplInfo {
    file: String,
    function_name: Option<String>,
    min_args: Option<usize>,
    max_args: Option<usize>,
    variadic: Option<bool>,
    arg_schema: Option<String>,
    caps: Vec<String>,
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
        Command::DocsSchema(args) => run_docs_schema(args),
        Command::DocsRef(args) => run_docs_ref(args),
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

fn run_docs_schema(args: DocsSchemaArgs) -> Result<()> {
    if args.apply && !args.allow_dirty {
        ensure_git_clean()?;
    }

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

    let mut impls_by_type: BTreeMap<String, Vec<ImplInfo>> = BTreeMap::new();
    let mut registrations: Vec<(String, String)> = Vec::new();

    for file in &builtins_files {
        let file_rel = path_to_repo_string(file)?;
        let facts = parse_file_facts(file, &file_rel)
            .with_context(|| format!("failed to parse builtin file: {file_rel}"))?;

        for (type_name, impl_info) in facts.impls {
            impls_by_type.entry(type_name).or_default().push(impl_info);
        }

        for type_name in facts.registrations {
            registrations.push((file_rel.clone(), type_name));
        }
    }

    let runtime_meta = collect_runtime_function_meta()?;

    let mut entries_by_file: BTreeMap<String, Vec<DocsSchemaEntry>> = BTreeMap::new();

    for (registration_file, type_name) in registrations {
        if let Some(filter) = &file_filter
            && !filter.is_match(&registration_file)
        {
            continue;
        }

        let Some(impl_info) =
            select_impl_for_registration(&impls_by_type, &type_name, &registration_file)
        else {
            continue;
        };

        let function_name = impl_info
            .function_name
            .clone()
            .unwrap_or_else(|| type_name.clone());

        if let Some(filter) = &function_filter
            && !filter.contains(&function_name.to_uppercase())
        {
            continue;
        }

        let runtime = runtime_meta.get(&function_name.to_uppercase());

        entries_by_file
            .entry(impl_info.file.clone())
            .or_default()
            .push(DocsSchemaEntry {
                type_name,
                function_name: function_name.clone(),
                min_args: runtime.map(|meta| meta.min_args).or(impl_info.min_args),
                max_args: runtime
                    .and_then(|meta| meta.max_args)
                    .or(impl_info.max_args),
                variadic: runtime.map(|meta| meta.variadic).or(impl_info.variadic),
                arg_schema: runtime
                    .map(|meta| meta.arg_schema.clone())
                    .or(impl_info.arg_schema.clone()),
                signature: runtime.map(|meta| meta.signature.clone()),
                caps: runtime
                    .map(|meta| meta.caps.clone())
                    .unwrap_or_else(|| impl_info.caps.clone()),
            });
    }

    let mut touched_entries = 0usize;
    let mut scanned_files = 0usize;
    let mut changed_files = 0usize;
    let mut stale_files = Vec::new();

    for (file, entries) in entries_by_file {
        scanned_files += 1;
        let source = fs::read_to_string(&file)
            .with_context(|| format!("failed to read source file: {file}"))?;
        let (updated, touched) = apply_schema_sections_to_source(&source, &entries)?;
        touched_entries += touched;

        if source != updated {
            stale_files.push(file.clone());
            if args.apply {
                fs::write(&file, updated)
                    .with_context(|| format!("failed to write source file: {file}"))?;
                changed_files += 1;
            }
        }
    }

    let stale_count = stale_files.len();

    println!("docs-schema summary");
    println!("  files scanned: {}", scanned_files);
    println!("  entries touched: {touched_entries}");
    println!("  stale files: {stale_count}");

    if args.apply {
        println!("  files updated: {changed_files}");
    } else if stale_count > 0 {
        println!("\nFiles requiring schema update:");
        for file in stale_files.iter().take(30) {
            println!("  - {file}");
        }
        if stale_files.len() > 30 {
            println!("  ... and {} more", stale_files.len() - 30);
        }
        bail!(
            "docs-schema check failed: {} file(s) have stale or missing schema blocks",
            stale_count
        );
    }

    Ok(())
}

fn run_docs_ref(args: DocsRefArgs) -> Result<()> {
    if args.apply && !args.allow_dirty {
        ensure_git_clean()?;
    }

    let entries = collect_function_ref_entries(&args.paths, &args.functions)?;
    if entries.is_empty() {
        println!("docs-ref summary");
        println!("  functions: 0");
        println!("  stale files: 0");
        return Ok(());
    }

    let mut by_category: BTreeMap<String, Vec<FunctionRefEntry>> = BTreeMap::new();
    for entry in entries {
        by_category
            .entry(entry.category_slug.clone())
            .or_default()
            .push(entry);
    }

    let all_entries = by_category
        .values()
        .flat_map(|items| items.iter().cloned())
        .collect::<Vec<_>>();

    for values in by_category.values_mut() {
        values.sort_by(|a, b| a.function_name.cmp(&b.function_name));
    }

    let mut stale_files = Vec::new();
    let mut changed_files = 0usize;
    let mut generated_pages = 0usize;

    for (category_slug, category_entries) in &by_category {
        let category_dir = args.out_dir.join(category_slug);
        let category_index_path = category_dir.join("index.mdx");
        let category_meta_path = category_dir.join("meta.json");

        let category_name = category_entries
            .first()
            .map(|entry| display_category_name(&entry.category))
            .unwrap_or_else(|| display_category_name(category_slug));

        let category_index = render_category_index_page(&category_name, category_entries);
        apply_or_check_file(
            &category_index_path,
            &category_index,
            args.apply,
            &mut stale_files,
            &mut changed_files,
        )?;

        let mut category_pages = vec!["index".to_string()];
        category_pages.extend(
            category_entries
                .iter()
                .map(|entry| entry.function_slug.clone())
                .collect::<Vec<_>>(),
        );

        let category_meta = serde_json::to_string_pretty(&serde_json::json!({
            "title": format!("{} Functions", category_name),
            "pages": category_pages,
            "defaultOpen": false,
            "collapsible": true,
        }))?;
        apply_or_check_file(
            &category_meta_path,
            &(category_meta + "\n"),
            args.apply,
            &mut stale_files,
            &mut changed_files,
        )?;

        let mut expected_mdx_files: BTreeSet<String> = BTreeSet::new();
        expected_mdx_files.insert("index.mdx".to_string());

        for entry in category_entries {
            let filename = format!("{}.mdx", entry.function_slug);
            expected_mdx_files.insert(filename.clone());

            let page_path = category_dir.join(&filename);
            let updated = if page_path.exists() {
                let existing = fs::read_to_string(&page_path)
                    .with_context(|| format!("failed to read {}", page_path.display()))?;
                upsert_function_page_generated_block(&existing, entry)
            } else {
                render_new_function_page(entry)
            };

            apply_or_check_file(
                &page_path,
                &updated,
                args.apply,
                &mut stale_files,
                &mut changed_files,
            )?;
            generated_pages += 1;
        }

        prune_generated_function_pages(
            &category_dir,
            &expected_mdx_files,
            args.apply,
            &mut stale_files,
            &mut changed_files,
        )?;
    }

    let mut top_pages = vec!["index".to_string()];
    top_pages.extend(by_category.keys().cloned());

    let top_meta = serde_json::to_string_pretty(&serde_json::json!({
        "title": "Functions",
        "pages": top_pages,
    }))?;
    apply_or_check_file(
        &args.out_dir.join("meta.json"),
        &(top_meta + "\n"),
        args.apply,
        &mut stale_files,
        &mut changed_files,
    )?;

    let functions_meta_json = render_functions_meta_json(&all_entries)?;
    apply_or_check_file(
        Path::new("docs-site/src/generated/functions-meta.json"),
        &(functions_meta_json + "\n"),
        args.apply,
        &mut stale_files,
        &mut changed_files,
    )?;

    stale_files.sort();
    stale_files.dedup();

    println!("docs-ref summary");
    println!("  functions: {generated_pages}");
    println!("  categories: {}", by_category.len());
    println!("  stale files: {}", stale_files.len());

    if args.apply {
        println!("  files updated: {changed_files}");
    } else if !stale_files.is_empty() {
        println!("\nFiles requiring docs-ref update:");
        for file in stale_files.iter().take(40) {
            println!("  - {file}");
        }
        if stale_files.len() > 40 {
            println!("  ... and {} more", stale_files.len() - 40);
        }

        bail!(
            "docs-ref check failed: {} file(s) have stale or missing generated content",
            stale_files.len()
        );
    }

    Ok(())
}

fn collect_function_ref_entries(
    paths: &[String],
    functions: &[String],
) -> Result<Vec<FunctionRefEntry>> {
    let builtins_files = collect_builtin_files(Path::new(BUILTINS_DIR))?;
    let file_filter = build_glob_filter(paths)?;
    let function_filter: Option<BTreeSet<String>> = if functions.is_empty() {
        None
    } else {
        Some(
            functions
                .iter()
                .map(|name| name.trim().to_uppercase())
                .filter(|name| !name.is_empty())
                .collect(),
        )
    };

    let mut impls_by_type: BTreeMap<String, Vec<ImplInfo>> = BTreeMap::new();
    let mut registrations: Vec<(String, String)> = Vec::new();

    for file in &builtins_files {
        let file_rel = path_to_repo_string(file)?;
        let facts = parse_file_facts(file, &file_rel)
            .with_context(|| format!("failed to parse builtin file: {file_rel}"))?;

        for (type_name, impl_info) in facts.impls {
            impls_by_type.entry(type_name).or_default().push(impl_info);
        }

        for type_name in facts.registrations {
            registrations.push((file_rel.clone(), type_name));
        }
    }

    let runtime_meta = collect_runtime_function_meta()?;
    let mut entries = Vec::new();

    for (registration_file, type_name) in registrations {
        if let Some(filter) = &file_filter
            && !filter.is_match(&registration_file)
        {
            continue;
        }

        let Some(impl_info) =
            select_impl_for_registration(&impls_by_type, &type_name, &registration_file)
        else {
            continue;
        };

        let function_name = impl_info
            .function_name
            .clone()
            .unwrap_or_else(|| type_name.clone());

        if let Some(filter) = &function_filter
            && !filter.contains(&function_name.to_uppercase())
        {
            continue;
        }

        let category = derive_category(&registration_file);
        let category_slug = slugify_for_docs(&category);
        let function_slug = sanitize_function_slug(slugify_for_docs(&function_name));
        let runtime = runtime_meta.get(&function_name.to_uppercase());

        entries.push(FunctionRefEntry {
            function_name,
            function_slug,
            category,
            category_slug,
            type_name,
            registration_file,
            impl_file: impl_info.file.clone(),
            min_args: runtime.map(|meta| meta.min_args).or(impl_info.min_args),
            max_args: runtime
                .and_then(|meta| meta.max_args)
                .or(impl_info.max_args),
            variadic: runtime.map(|meta| meta.variadic).or(impl_info.variadic),
            signature: runtime.map(|meta| meta.signature.clone()),
            arg_schema: runtime
                .map(|meta| meta.arg_schema.clone())
                .or(impl_info.arg_schema.clone()),
            args: runtime.map(|meta| meta.args.clone()).unwrap_or_default(),
            caps: runtime
                .map(|meta| meta.caps.clone())
                .unwrap_or_else(|| impl_info.caps.clone()),
        });
    }

    entries.sort_by(|a, b| {
        a.category_slug
            .cmp(&b.category_slug)
            .then_with(|| a.function_name.cmp(&b.function_name))
    });

    let mut deduped = Vec::with_capacity(entries.len());
    let mut seen_function_keys: BTreeSet<(String, String)> = BTreeSet::new();
    for entry in entries {
        let key = (
            entry.category_slug.clone(),
            entry.function_name.to_uppercase(),
        );
        if seen_function_keys.insert(key) {
            deduped.push(entry);
        }
    }

    let mut used_slugs_by_category: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in &mut deduped {
        let used = used_slugs_by_category
            .entry(entry.category_slug.clone())
            .or_default();

        let base = entry.function_slug.clone();
        let mut candidate = base.clone();
        let mut suffix = 2usize;

        while used.contains(&candidate) {
            candidate = format!("{base}-{suffix}");
            suffix += 1;
        }

        used.insert(candidate.clone());
        entry.function_slug = candidate;
    }

    Ok(deduped)
}

fn sanitize_function_slug(slug: String) -> String {
    match slug.as_str() {
        "index" => "index-fn".to_string(),
        "meta" => "meta-fn".to_string(),
        _ => slug,
    }
}

fn slugify_for_docs(input: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;

    for ch in input.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() {
            out.push(c);
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }

    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "function".to_string()
    } else {
        out
    }
}

fn display_category_name(raw: &str) -> String {
    match raw {
        "datetime" => "Date & Time".to_string(),
        "logical-ext" | "logical_ext" => "Logical (Extended)".to_string(),
        "reference-fns" | "reference_fns" => "Reference".to_string(),
        "stats" => "Statistics".to_string(),
        "info" => "Information".to_string(),
        "lambda" => "LET / LAMBDA".to_string(),
        _ => raw
            .split(['-', '_'])
            .filter(|part| !part.is_empty())
            .map(|part| {
                let mut chars = part.chars();
                match chars.next() {
                    Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
                    None => String::new(),
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
    }
}

fn render_category_index_page(category_name: &str, entries: &[FunctionRefEntry]) -> String {
    let mut lines = vec![
        "---".to_string(),
        format!("title: {} Functions", category_name),
        format!(
            "description: Generated reference pages for {} functions.",
            category_name
        ),
        "---".to_string(),
        "".to_string(),
        format!(
            "This section contains {} generated function reference pages.",
            entries.len()
        ),
        "".to_string(),
        "## Functions".to_string(),
        "".to_string(),
    ];

    for entry in entries {
        lines.push(format!(
            "- [{}](/docs/reference/functions/{}/{})",
            entry.function_name, entry.category_slug, entry.function_slug
        ));
    }

    lines.push(String::new());
    lines.join("\n")
}

fn render_new_function_page(entry: &FunctionRefEntry) -> String {
    let example_formula = match entry.min_args.unwrap_or(1) {
        0 => format!("={}()", entry.function_name),
        1 => format!("={}({})", entry.function_name, "A1"),
        _ => format!("={}({}, {})", entry.function_name, "A1", "B1"),
    };

    format!(
        "---\ntitle: \"{name}\"\ndescription: \"Reference for the {name} function.\"\n---\n\n## Summary\n\n> TODO: Add a concise human summary for `{name}`.\n\n## Formula example\n\n```text\n{example}\n```\n\n## Runtime metadata\n\n{meta}\n",
        name = entry.function_name,
        example = example_formula,
        meta = render_function_meta_block(entry),
    )
}

fn function_meta_id(entry: &FunctionRefEntry) -> String {
    format!("{}/{}", entry.category_slug, entry.function_slug)
}

fn render_function_meta_block(entry: &FunctionRefEntry) -> String {
    [
        DOCGEN_FUNC_META_START.to_string(),
        format!("<FunctionMeta id=\"{}\" />", function_meta_id(entry)),
        DOCGEN_FUNC_META_END.to_string(),
    ]
    .join("\n")
}

fn render_functions_meta_json(entries: &[FunctionRefEntry]) -> Result<String> {
    let mut map = serde_json::Map::new();

    for entry in entries {
        map.insert(
            function_meta_id(entry),
            serde_json::json!({
                "name": entry.function_name,
                "category": entry.category,
                "typeName": entry.type_name,
                "minArgs": entry.min_args,
                "maxArgs": entry.max_args,
                "variadic": entry.variadic,
                "signature": entry.signature,
                "argSchema": entry.arg_schema,
                "args": entry.args,
                "caps": entry.caps,
                "registrationSource": entry.registration_file,
                "implementationSource": entry.impl_file,
            }),
        );
    }

    Ok(serde_json::to_string_pretty(&serde_json::Value::Object(
        map,
    ))?)
}

fn upsert_function_page_generated_block(existing: &str, entry: &FunctionRefEntry) -> String {
    let generated = render_function_meta_block(entry);

    let ranges = [
        (DOCGEN_FUNC_META_START, DOCGEN_FUNC_META_END),
        (DOCGEN_FUNC_META_START_LEGACY, DOCGEN_FUNC_META_END_LEGACY),
    ];

    for (start_marker, end_marker) in ranges {
        let start = existing.find(start_marker);
        let end = existing.find(end_marker);

        if let (Some(start_idx), Some(end_idx)) = (start, end)
            && start_idx <= end_idx
        {
            let end_inclusive = end_idx + end_marker.len();
            let mut out = String::new();
            out.push_str(&existing[..start_idx]);
            out.push_str(&generated);
            out.push_str(&existing[end_inclusive..]);
            return normalize_generated_page(out);
        }
    }

    let mut out = existing.trim_end().to_string();
    out.push_str("\n\n## Runtime metadata\n\n");
    out.push_str(&generated);
    out.push('\n');
    normalize_generated_page(out)
}

fn normalize_generated_page(input: String) -> String {
    let output = input.replace(
        "## Formula example\n\n```excel",
        "## Formula example\n\n```text",
    );

    let title_re = Regex::new(r"(?m)^title:\s*(TRUE|FALSE)\s*$").expect("valid regex");
    title_re.replace_all(&output, "title: \"$1\"").to_string()
}

fn apply_or_check_file(
    path: &Path,
    content: &str,
    apply: bool,
    stale_files: &mut Vec<String>,
    changed_files: &mut usize,
) -> Result<()> {
    let current = fs::read_to_string(path).ok();
    let needs_update = current.as_deref() != Some(content);
    if !needs_update {
        return Ok(());
    }

    stale_files.push(path_to_repo_string(path)?);

    if apply {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create dir {}", parent.display()))?;
        }
        fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;
        *changed_files += 1;
    }

    Ok(())
}

fn prune_generated_function_pages(
    category_dir: &Path,
    expected_mdx_files: &BTreeSet<String>,
    apply: bool,
    stale_files: &mut Vec<String>,
    changed_files: &mut usize,
) -> Result<()> {
    if !category_dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(category_dir)
        .with_context(|| format!("failed to read dir {}", category_dir.display()))?
    {
        let path = entry
            .with_context(|| format!("failed to read entry in {}", category_dir.display()))?
            .path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("mdx") {
            continue;
        }

        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        if expected_mdx_files.contains(filename) {
            continue;
        }

        stale_files.push(path_to_repo_string(&path)?);
        if apply {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
            *changed_files += 1;
        }
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
    let mut min_args = None;
    let mut max_args = None;
    let mut variadic = None;
    let mut arg_schema = None;
    let mut caps = Vec::new();

    for impl_item in &item_impl.items {
        match impl_item {
            ImplItem::Fn(function) if function.sig.ident == "name" => {
                function_name = extract_name_literal_from_block(&function.block);
            }
            ImplItem::Fn(function) if function.sig.ident == "min_args" => {
                min_args = extract_usize_literal_from_block(&function.block);
            }
            ImplItem::Fn(function) if function.sig.ident == "max_args" => {
                max_args = extract_option_usize_from_block(&function.block);
            }
            ImplItem::Fn(function) if function.sig.ident == "variadic" => {
                variadic = extract_bool_literal_from_block(&function.block);
            }
            ImplItem::Fn(function) if function.sig.ident == "arg_schema" => {
                arg_schema = extract_expr_string_from_block(&function.block);
            }
            ImplItem::Macro(mac)
                if mac
                    .mac
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident == "func_caps")
                    .unwrap_or(false) =>
            {
                caps = parse_caps_from_macro_tokens(mac.mac.tokens.clone());
            }
            _ => {}
        }
    }

    facts.impls.push((
        type_ident,
        ImplInfo {
            file: file_rel.to_string(),
            function_name,
            min_args,
            max_args,
            variadic,
            arg_schema,
            caps,
            doc_text: collect_doc_attrs(&item_impl.attrs),
        },
    ));
}

fn extract_name_literal_from_block(block: &syn::Block) -> Option<String> {
    extract_tail_expr(block).and_then(extract_string_literal)
}

fn extract_usize_literal_from_block(block: &syn::Block) -> Option<usize> {
    let expr = extract_tail_expr(block)?;
    extract_usize_literal(expr)
}

fn extract_bool_literal_from_block(block: &syn::Block) -> Option<bool> {
    let expr = extract_tail_expr(block)?;
    extract_bool_literal(expr)
}

fn extract_option_usize_from_block(block: &syn::Block) -> Option<usize> {
    let expr = extract_tail_expr(block)?;
    extract_option_usize_literal(expr)
}

fn extract_expr_string_from_block(block: &syn::Block) -> Option<String> {
    let expr = extract_tail_expr(block)?;
    let expr = unwrap_expr(expr);
    Some(expr.to_token_stream().to_string())
}

fn extract_tail_expr(block: &syn::Block) -> Option<&Expr> {
    for stmt in block.stmts.iter().rev() {
        match stmt {
            syn::Stmt::Expr(expr, _) => return Some(expr),
            syn::Stmt::Local(local) => {
                if let Some(init) = &local.init {
                    return Some(&init.expr);
                }
            }
            syn::Stmt::Item(_) | syn::Stmt::Macro(_) => {}
        }
    }
    None
}

fn extract_usize_literal(expr: &Expr) -> Option<usize> {
    let expr = unwrap_expr(expr);
    match expr {
        Expr::Lit(lit_expr) => match &lit_expr.lit {
            Lit::Int(value) => value.base10_parse().ok(),
            _ => None,
        },
        Expr::Return(return_expr) => return_expr.expr.as_deref().and_then(extract_usize_literal),
        Expr::Block(block_expr) => extract_usize_literal_from_block(&block_expr.block),
        _ => None,
    }
}

fn extract_bool_literal(expr: &Expr) -> Option<bool> {
    let expr = unwrap_expr(expr);
    match expr {
        Expr::Lit(lit_expr) => match &lit_expr.lit {
            Lit::Bool(value) => Some(value.value),
            _ => None,
        },
        Expr::Return(return_expr) => return_expr.expr.as_deref().and_then(extract_bool_literal),
        Expr::Block(block_expr) => extract_bool_literal_from_block(&block_expr.block),
        _ => None,
    }
}

fn extract_option_usize_literal(expr: &Expr) -> Option<usize> {
    let expr = unwrap_expr(expr);
    match expr {
        Expr::Path(path_expr)
            if path_expr
                .path
                .segments
                .last()
                .map(|segment| segment.ident == "None")
                .unwrap_or(false) =>
        {
            None
        }
        Expr::Call(call_expr) => {
            let func = unwrap_expr(&call_expr.func);
            if let Expr::Path(path_expr) = func
                && path_expr
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident == "Some")
                    .unwrap_or(false)
            {
                return call_expr.args.first().and_then(extract_usize_literal);
            }
            None
        }
        Expr::Return(return_expr) => return_expr
            .expr
            .as_deref()
            .and_then(extract_option_usize_literal),
        Expr::Block(block_expr) => extract_option_usize_from_block(&block_expr.block),
        _ => None,
    }
}

fn parse_caps_from_macro_tokens<T: ToTokens>(tokens: T) -> Vec<String> {
    let raw = tokens.to_token_stream().to_string();
    let trimmed = raw
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    trimmed
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
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

fn ensure_git_clean() -> Result<()> {
    let output = ProcessCommand::new("git")
        .args(["status", "--porcelain"])
        .output()
        .context("failed to run `git status --porcelain`")?;

    if !output.status.success() {
        bail!("`git status --porcelain` returned non-zero exit status");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        bail!("working tree is dirty; re-run with --allow-dirty to apply schema updates anyway");
    }

    Ok(())
}

fn catch_unwind_silent<F, R>(f: F) -> std::thread::Result<R>
where
    F: FnOnce() -> R,
{
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    std::panic::set_hook(hook);
    result
}

fn collect_runtime_function_meta() -> Result<BTreeMap<String, RuntimeFunctionMeta>> {
    formualizer_eval::builtins::load_builtins();

    let mut map = BTreeMap::new();
    for (namespace, name, function) in formualizer_eval::function_registry::snapshot_registered() {
        if !namespace.is_empty() {
            continue;
        }

        let variadic = function.variadic();
        let min_args = function.min_args();
        let caps = fn_caps_labels(function.caps());

        let schema_eval = catch_unwind_silent(|| function.arg_schema());

        let (arg_schema, signature, max_args, args) = match schema_eval {
            Ok(schema) => {
                let max_args = if variadic { None } else { Some(schema.len()) };
                (
                    format_arg_schema(schema),
                    format_signature(function.name(), schema, variadic),
                    max_args,
                    format_runtime_args(schema),
                )
            }
            Err(_) => (
                "<unavailable: arg_schema panicked>".to_string(),
                format!("{}(<schema unavailable>)", function.name()),
                None,
                Vec::new(),
            ),
        };

        map.insert(
            name,
            RuntimeFunctionMeta {
                min_args,
                max_args,
                variadic,
                arg_schema,
                signature,
                args,
                caps,
            },
        );
    }

    Ok(map)
}

fn fn_caps_labels(caps: FnCaps) -> Vec<String> {
    const KNOWN: &[(FnCaps, &str)] = &[
        (FnCaps::PURE, "PURE"),
        (FnCaps::VOLATILE, "VOLATILE"),
        (FnCaps::REDUCTION, "REDUCTION"),
        (FnCaps::ELEMENTWISE, "ELEMENTWISE"),
        (FnCaps::WINDOWED, "WINDOWED"),
        (FnCaps::LOOKUP, "LOOKUP"),
        (FnCaps::NUMERIC_ONLY, "NUMERIC_ONLY"),
        (FnCaps::BOOL_ONLY, "BOOL_ONLY"),
        (FnCaps::SIMD_OK, "SIMD_OK"),
        (FnCaps::STREAM_OK, "STREAM_OK"),
        (FnCaps::GPU_OK, "GPU_OK"),
        (FnCaps::RETURNS_REFERENCE, "RETURNS_REFERENCE"),
        (FnCaps::SHORT_CIRCUIT, "SHORT_CIRCUIT"),
        (FnCaps::PARALLEL_ARGS, "PARALLEL_ARGS"),
        (FnCaps::PARALLEL_CHUNKS, "PARALLEL_CHUNKS"),
        (FnCaps::DYNAMIC_DEPENDENCY, "DYNAMIC_DEPENDENCY"),
    ];

    KNOWN
        .iter()
        .filter(|(flag, _)| caps.contains(*flag))
        .map(|(_, label)| (*label).to_string())
        .collect()
}

fn format_runtime_args(schema: &[ArgSchema]) -> Vec<RuntimeArgMeta> {
    schema
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let kinds = spec
                .kinds
                .iter()
                .map(|kind| format!("{kind:?}").to_lowercase())
                .collect::<Vec<_>>();

            let shape = match spec.shape {
                ShapeKind::Scalar => "scalar",
                ShapeKind::Range => "range",
                ShapeKind::Array => "array",
            }
            .to_string();

            RuntimeArgMeta {
                name: format!("arg{}", index + 1),
                kinds,
                required: spec.required,
                shape,
                by_ref: spec.by_ref,
                coercion: format!("{:?}", spec.coercion),
                max: spec.max.map(|value| format!("{:?}", value)),
                repeating: spec.repeating.map(|value| format!("{:?}", value)),
                has_default: spec.default.is_some(),
            }
        })
        .collect()
}

fn format_arg_schema(schema: &[ArgSchema]) -> String {
    if schema.is_empty() {
        return "[]".to_string();
    }

    schema
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let kinds = spec
                .kinds
                .iter()
                .map(|kind| format!("{kind:?}").to_lowercase())
                .collect::<Vec<_>>()
                .join("|");

            let shape = match spec.shape {
                ShapeKind::Scalar => "scalar",
                ShapeKind::Range => "range",
                ShapeKind::Array => "array",
            };

            format!(
                "arg{}{{kinds={kinds},required={},shape={shape},by_ref={},coercion={:?},max={:?},repeating={:?},default={}}}",
                index + 1,
                spec.required,
                spec.by_ref,
                spec.coercion,
                spec.max,
                spec.repeating,
                spec.default.is_some()
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_signature(name: &str, schema: &[ArgSchema], variadic: bool) -> String {
    if schema.is_empty() {
        return format!("{name}()");
    }

    let mut args = Vec::new();
    for (index, spec) in schema.iter().enumerate() {
        let mut arg_name = format!("arg{}", index + 1);
        if !spec.required {
            arg_name.push('?');
        }
        if variadic && index == schema.len() - 1 {
            arg_name.push_str("...");
        }

        let kinds = spec
            .kinds
            .iter()
            .map(|kind| format!("{kind:?}").to_lowercase())
            .collect::<Vec<_>>()
            .join("|");

        let shape = match spec.shape {
            ShapeKind::Scalar => "scalar",
            ShapeKind::Range => "range",
            ShapeKind::Array => "array",
        };

        args.push(format!("{arg_name}: {kinds}@{shape}"));
    }

    format!("{name}({})", args.join(", "))
}

fn apply_schema_sections_to_source(
    source: &str,
    entries: &[DocsSchemaEntry],
) -> Result<(String, usize)> {
    if entries.is_empty() {
        return Ok((source.to_string(), 0));
    }

    let impl_re = Regex::new(
        r"^(\s*)impl\s+(?:(?:[A-Za-z_][A-Za-z0-9_]*::)*)Function\s+for\s+([A-Za-z_][A-Za-z0-9_]*)\b",
    )
    .context("failed to compile impl regex")?;

    let mut entries_by_type: BTreeMap<String, DocsSchemaEntry> = BTreeMap::new();
    for entry in entries {
        entries_by_type
            .entry(entry.type_name.clone())
            .or_insert_with(|| entry.clone());
    }

    let had_trailing_newline = source.ends_with('\n');
    let mut lines: Vec<String> = source.lines().map(|line| line.to_string()).collect();

    let mut i = 0usize;
    let mut touched = 0usize;

    while i < lines.len() {
        let line = lines[i].clone();
        let Some(caps) = impl_re.captures(&line) else {
            i += 1;
            continue;
        };

        let indent = caps.get(1).map(|m| m.as_str()).unwrap_or("");
        let type_name = caps.get(2).map(|m| m.as_str()).unwrap_or("");

        let Some(entry) = entries_by_type.get(type_name) else {
            i += 1;
            continue;
        };

        touched += 1;

        let mut doc_start = i;
        while doc_start > 0 && lines[doc_start - 1].trim_start().starts_with("///") {
            doc_start -= 1;
        }

        let existing_doc_lines: Vec<String> = lines[doc_start..i].to_vec();
        let updated_doc_lines = upsert_schema_doc_lines(existing_doc_lines, entry, indent);

        lines.splice(doc_start..i, updated_doc_lines.clone());
        i = doc_start + updated_doc_lines.len() + 1;
    }

    let mut updated = lines.join("\n");
    if had_trailing_newline {
        updated.push('\n');
    }

    Ok((updated, touched))
}

fn upsert_schema_doc_lines(
    existing_doc_lines: Vec<String>,
    entry: &DocsSchemaEntry,
    indent: &str,
) -> Vec<String> {
    let section_lines = render_schema_section_lines(entry, indent);

    if existing_doc_lines.is_empty() {
        return section_lines;
    }

    let start_idx = existing_doc_lines
        .iter()
        .position(|line| line.contains(DOCGEN_SCHEMA_START));
    let end_idx = existing_doc_lines
        .iter()
        .position(|line| line.contains(DOCGEN_SCHEMA_END));

    if let (Some(start), Some(end)) = (start_idx, end_idx)
        && start <= end
    {
        let mut replaced = Vec::new();
        replaced.extend_from_slice(&existing_doc_lines[..start]);
        replaced.extend_from_slice(&section_lines);
        replaced.extend_from_slice(&existing_doc_lines[end + 1..]);
        return replaced;
    }

    let mut appended = existing_doc_lines;
    if appended
        .last()
        .map(|line| line.trim() != "///")
        .unwrap_or(true)
    {
        appended.push(format!("{indent}///"));
    }
    appended.extend(section_lines);
    appended
}

fn render_schema_section_lines(entry: &DocsSchemaEntry, indent: &str) -> Vec<String> {
    let max_args = if entry.variadic.unwrap_or(false) {
        "variadic".to_string()
    } else {
        entry
            .max_args
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unspecified".to_string())
    };

    let variadic = entry
        .variadic
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let min_args = entry
        .min_args
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let arg_schema = entry
        .arg_schema
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    let signature = entry
        .signature
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    let caps = if entry.caps.is_empty() {
        "none".to_string()
    } else {
        entry.caps.join(", ")
    };

    vec![
        format!("{indent}/// {DOCGEN_SCHEMA_START}"),
        format!("{indent}/// Name: {}", entry.function_name),
        format!("{indent}/// Type: {}", entry.type_name),
        format!("{indent}/// Min args: {min_args}"),
        format!("{indent}/// Max args: {max_args}"),
        format!("{indent}/// Variadic: {variadic}"),
        format!("{indent}/// Signature: {signature}"),
        format!("{indent}/// Arg schema: {arg_schema}"),
        format!("{indent}/// Caps: {caps}"),
        format!("{indent}/// {DOCGEN_SCHEMA_END}"),
    ]
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
    use super::{
        DOCGEN_SCHEMA_END, DOCGEN_SCHEMA_START, DocsSchemaEntry, apply_schema_sections_to_source,
        count_fenced_blocks_by_lang, count_formula_example_blocks, parse_fenced_blocks,
    };

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

    #[test]
    fn docs_schema_appends_block_when_missing() {
        let source = r#"impl Function for SumFn {
    fn name(&self) -> &'static str { "SUM" }
}
"#;

        let entry = DocsSchemaEntry {
            type_name: "SumFn".to_string(),
            function_name: "SUM".to_string(),
            min_args: Some(0),
            max_args: None,
            variadic: Some(true),
            arg_schema: Some("arg1{kinds=number,required=true,shape=range}".to_string()),
            signature: Some("SUM(arg1...: number@range)".to_string()),
            caps: vec!["PURE".to_string(), "REDUCTION".to_string()],
        };

        let (updated, touched) = apply_schema_sections_to_source(source, &[entry]).unwrap();
        assert_eq!(touched, 1);
        assert!(updated.contains(DOCGEN_SCHEMA_START));
        assert!(updated.contains(DOCGEN_SCHEMA_END));
        assert!(updated.contains("Name: SUM"));
    }

    #[test]
    fn docs_schema_updates_existing_block_in_place() {
        let source = r#"/// Summary.
/// [formualizer-docgen:schema:start]
/// Name: OLD
/// [formualizer-docgen:schema:end]
impl Function for SumFn {
    fn name(&self) -> &'static str { "SUM" }
}
"#;

        let entry = DocsSchemaEntry {
            type_name: "SumFn".to_string(),
            function_name: "SUM".to_string(),
            min_args: Some(0),
            max_args: None,
            variadic: Some(true),
            arg_schema: Some("arg1{kinds=number,required=true,shape=range}".to_string()),
            signature: Some("SUM(arg1...: number@range)".to_string()),
            caps: vec!["PURE".to_string()],
        };

        let (updated, touched) = apply_schema_sections_to_source(source, &[entry]).unwrap();
        assert_eq!(touched, 1);
        assert!(updated.contains("/// Summary."));
        assert!(updated.contains("Name: SUM"));
        assert!(!updated.contains("Name: OLD"));
    }
}
