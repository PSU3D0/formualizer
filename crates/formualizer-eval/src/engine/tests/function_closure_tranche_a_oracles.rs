use crate::function::Function;
use crate::function_registry;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

#[derive(Debug)]
struct ReplacementFn {
    name: &'static str,
    aliases: &'static [&'static str],
    value: i64,
}

impl Function for ReplacementFn {
    fn name(&self) -> &'static str {
        self.name
    }
    fn namespace(&self) -> &'static str {
        "__FUNCTION_CLOSURE_ORACLE__"
    }
    fn aliases(&self) -> &'static [&'static str] {
        self.aliases
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            self.value,
        )))
    }
}

#[test]
#[ignore = "Swatch 0 observation: stale owned alias is corrected by Swatch 1"]
fn baseline_alias_prefix_and_replacement_mismatch_is_observed() {
    function_registry::register_function(Arc::new(ReplacementFn {
        name: "TARGET",
        aliases: &["OLD_ALIAS"],
        value: 1,
    }));
    function_registry::register_function(Arc::new(ReplacementFn {
        name: "TARGET",
        aliases: &["NEW_ALIAS"],
        value: 2,
    }));
    assert!(function_registry::get("__FUNCTION_CLOSURE_ORACLE__", "OLD_ALIAS").is_some());
}

#[test]
#[ignore = "Swatch 0 observation: exceptional cap gaps are corrected by Swatch 1"]
fn baseline_registry_classification_mismatches_are_observed() {
    use crate::function::FnCaps;
    crate::builtins::load_builtins();
    let registered = function_registry::snapshot_registered();
    assert!(registered.len() > 100);
    assert!(
        !crate::builtins::lookup::RandArrayFn
            .caps()
            .contains(FnCaps::VOLATILE)
    );
    assert!(
        !crate::builtins::lambda::LetFn
            .caps()
            .contains(FnCaps::LOCAL_ENVIRONMENT)
    );
}

#[test]
fn scalar_and_placement_context_baseline_oracle() {
    use crate::engine::{Engine, EvalConfig};
    use crate::test_workbook::TestWorkbook;
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 7, 2, parse("=ROW()+ABS(-3)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 7, 2),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn recursive_function_ast_relocation_matches_copy_oracle() {
    let anchor = parse("=SUM(A1,$B1,C$1,$D$1)").unwrap();
    let expected = parse("=SUM(D3,$B3,F$1,$D$1)").unwrap();
    let relocated =
        crate::formula_plane::structural::relocate_ast_for_template_placement(&anchor, 2, 3)
            .unwrap();
    fn references(
        ast: &formualizer_parse::ASTNode,
    ) -> Vec<formualizer_parse::parser::ReferenceType> {
        match &ast.node_type {
            formualizer_parse::ASTNodeType::Reference { reference, .. } => vec![reference.clone()],
            formualizer_parse::ASTNodeType::Function { args, .. } => {
                args.iter().flat_map(references).collect()
            }
            _ => Vec::new(),
        }
    }
    assert_eq!(references(&relocated), references(&expected));
}

#[test]
fn production_semantic_name_authorities_are_frozen_to_known_files() {
    fn visit(path: &std::path::Path, hits: &mut Vec<String>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, hits);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs")
                && !path
                    .components()
                    .any(|component| component.as_os_str() == "tests")
            {
                let source = std::fs::read_to_string(&path).unwrap();
                if source.contains("fn is_known_static_function")
                    || source.contains("fn function_arg_context")
                    || source.contains("fn function_arg_slot_context")
                {
                    hits.push(
                        path.strip_prefix(env!("CARGO_MANIFEST_DIR"))
                            .unwrap()
                            .display()
                            .to_string(),
                    );
                }
                for forbidden in [
                    "SUPPORTED_FUNCTIONS",
                    "SUPPORTED_FUNCTION_NAMES",
                    "FORMULA_PLANE_FUNCTIONS",
                ] {
                    assert!(
                        !source.contains(forbidden),
                        "{} contains {forbidden}",
                        path.display()
                    );
                }
            }
        }
    }
    let mut hits = Vec::new();
    visit(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut hits,
    );
    hits.sort();
    assert!(
        hits.is_empty(),
        "obsolete semantic authorities remain: {hits:?}"
    );
    let ingest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine/ingest_pipeline.rs"),
    )
    .unwrap();
    assert!(!ingest.contains("is_known_static_function"));
    assert!(!ingest.contains("function_arg_context"));
    let graph_analysis = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/engine/graph/formula_analysis.rs"),
    )
    .unwrap();
    assert!(!graph_analysis.contains("eq_ignore_ascii_case(\"LET\""));
    assert!(!graph_analysis.contains("eq_ignore_ascii_case(\"LAMBDA\""));
}
