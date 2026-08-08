//! Crate-private semantic reference classification and AST traversal.
//!
//! The collector deliberately streams references to a consumer. It preserves
//! source order and leaves graph/planner policy (range expansion, resolution,
//! placeholder creation, and error mapping) at the call site.

use crate::engine::arena::{AstNodeData, AstNodeId, DataStore};
use crate::engine::sheet_registry::SheetRegistry;
use formualizer_common::{ExcelError, ExcelErrorKind};
use formualizer_parse::parser::{
    ASTNode, ASTNodeType, ExternalReference, ReferenceType, TableReference,
};
use rustc_hash::FxHashSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeclaredSheet<'a> {
    Current,
    Name(&'a str),
}

impl<'a> DeclaredSheet<'a> {
    fn from_option(sheet: Option<&'a str>) -> Self {
        match sheet {
            Some(name) => Self::Name(name),
            None => Self::Current,
        }
    }

    pub(crate) fn name(self) -> Option<&'a str> {
        match self {
            Self::Current => None,
            Self::Name(name) => Some(name),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CellReference<'a> {
    pub(crate) original: &'a ReferenceType,
    pub(crate) sheet: DeclaredSheet<'a>,
    pub(crate) row: u32,
    pub(crate) col: u32,
    pub(crate) row_abs: bool,
    pub(crate) col_abs: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RangeReference<'a> {
    pub(crate) original: &'a ReferenceType,
    pub(crate) sheet: DeclaredSheet<'a>,
    pub(crate) start_row: Option<u32>,
    pub(crate) start_col: Option<u32>,
    pub(crate) end_row: Option<u32>,
    pub(crate) end_col: Option<u32>,
    pub(crate) start_row_abs: bool,
    pub(crate) start_col_abs: bool,
    pub(crate) end_row_abs: bool,
    pub(crate) end_col_abs: bool,
}

impl RangeReference<'_> {
    pub(crate) fn finite_bounds(self) -> Option<(u32, u32, u32, u32)> {
        Some((
            self.start_row?,
            self.start_col?,
            self.end_row?,
            self.end_col?,
        ))
    }

    pub(crate) fn is_reversed(self) -> bool {
        self.finite_bounds()
            .is_some_and(|(sr, sc, er, ec)| sr > er || sc > ec)
    }

    pub(crate) fn saturating_area(self) -> Option<u64> {
        self.finite_bounds().map(|(sr, sc, er, ec)| {
            u64::from(er.saturating_sub(sr) + 1) * u64::from(ec.saturating_sub(sc) + 1)
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SemanticReference<'a> {
    Cell(CellReference<'a>),
    FiniteRange(RangeReference<'a>),
    OpenRange(RangeReference<'a>),
    Name(&'a str),
    Table(&'a TableReference),
    ExternalSource(&'a ExternalReference),
    ThreeDimensional(&'a ReferenceType),
    #[allow(dead_code)]
    Unsupported(&'a ReferenceType),
}

pub(crate) fn classify(reference: &ReferenceType) -> SemanticReference<'_> {
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => SemanticReference::Cell(CellReference {
            original: reference,
            sheet: DeclaredSheet::from_option(sheet.as_deref()),
            row: *row,
            col: *col,
            row_abs: *row_abs,
            col_abs: *col_abs,
        }),
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => {
            let range = RangeReference {
                original: reference,
                sheet: DeclaredSheet::from_option(sheet.as_deref()),
                start_row: *start_row,
                start_col: *start_col,
                end_row: *end_row,
                end_col: *end_col,
                start_row_abs: *start_row_abs,
                start_col_abs: *start_col_abs,
                end_row_abs: *end_row_abs,
                end_col_abs: *end_col_abs,
            };
            if range.finite_bounds().is_some() {
                SemanticReference::FiniteRange(range)
            } else {
                SemanticReference::OpenRange(range)
            }
        }
        ReferenceType::NamedRange(name) => SemanticReference::Name(name),
        ReferenceType::Table(table) => SemanticReference::Table(table),
        ReferenceType::External(external) => SemanticReference::ExternalSource(external),
        ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {
            SemanticReference::ThreeDimensional(reference)
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum LocalBindingStyle {
    #[default]
    None,
    LocalBindingPairs,
    LambdaParameters,
}

pub(crate) fn visit_tree_references<C>(
    ast: &ASTNode,
    context: &mut C,
    local_binding_style: fn(&C, &str, usize) -> LocalBindingStyle,
    visitor: fn(&mut C, SemanticReference<'_>) -> Result<(), ExcelError>,
) -> Result<(), ExcelError> {
    enum Frame<'a> {
        Node(&'a ASTNode),
        AddBinding(&'a str),
        ExitScope,
    }

    let mut local_scopes: Vec<FxHashSet<String>> = Vec::new();
    let mut stack = vec![Frame::Node(ast)];
    while let Some(frame) = stack.pop() {
        let ast = match frame {
            Frame::AddBinding(name) => {
                if let Some(scope) = local_scopes.last_mut() {
                    scope.insert(name.to_ascii_uppercase());
                }
                continue;
            }
            Frame::ExitScope => {
                local_scopes.pop();
                continue;
            }
            Frame::Node(ast) => ast,
        };

        match &ast.node_type {
            ASTNodeType::Reference { reference, .. } => {
                if let ReferenceType::NamedRange(name) = reference
                    && !local_scopes.is_empty()
                {
                    let key = name.to_ascii_uppercase();
                    if local_scopes.iter().rev().any(|scope| scope.contains(&key)) {
                        continue;
                    }
                }
                visitor(context, classify(reference))?;
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                stack.push(Frame::Node(right));
                stack.push(Frame::Node(left));
            }
            ASTNodeType::UnaryOp { expr, .. } => stack.push(Frame::Node(expr)),
            ASTNodeType::Function { name, args } => {
                match local_binding_style(context, name, args.len()) {
                    LocalBindingStyle::LocalBindingPairs
                        if args.len() >= 3 && args.len() % 2 == 1 =>
                    {
                        local_scopes.push(FxHashSet::default());
                        stack.push(Frame::ExitScope);
                        stack.push(Frame::Node(&args[args.len() - 1]));
                        for pair_idx in (0..args.len() - 1).step_by(2).rev() {
                            if let ASTNodeType::Reference {
                                reference: ReferenceType::NamedRange(local_name),
                                ..
                            } = &args[pair_idx].node_type
                            {
                                stack.push(Frame::AddBinding(local_name));
                            }
                            stack.push(Frame::Node(&args[pair_idx + 1]));
                        }
                    }
                    LocalBindingStyle::LambdaParameters => {
                        if let Some(body) = args.last() {
                            let mut scope = FxHashSet::default();
                            for parameter in &args[..args.len().saturating_sub(1)] {
                                if let ASTNodeType::Reference {
                                    reference: ReferenceType::NamedRange(name),
                                    ..
                                } = &parameter.node_type
                                {
                                    scope.insert(name.to_ascii_uppercase());
                                }
                            }
                            local_scopes.push(scope);
                            stack.push(Frame::ExitScope);
                            stack.push(Frame::Node(body));
                        }
                    }
                    _ => {
                        for arg in args.iter().rev() {
                            stack.push(Frame::Node(arg));
                        }
                    }
                }
            }
            ASTNodeType::Call { callee, args } => {
                for arg in args.iter().rev() {
                    stack.push(Frame::Node(arg));
                }
                stack.push(Frame::Node(callee));
            }
            ASTNodeType::Array(rows) => {
                for item in rows.iter().rev().flat_map(|row| row.iter().rev()) {
                    stack.push(Frame::Node(item));
                }
            }
            ASTNodeType::Literal(_) | ASTNodeType::Omitted => {}
        }
    }
    Ok(())
}

pub(crate) fn visit_arena_references<C>(
    ast_id: AstNodeId,
    context: &mut C,
    data_store: fn(&C) -> &DataStore,
    sheet_registry: fn(&C) -> &SheetRegistry,
    visitor: fn(&mut C, SemanticReference<'_>) -> Result<(), ExcelError>,
) -> Result<(), ExcelError> {
    let node = data_store(context)
        .get_node(ast_id)
        .cloned()
        .ok_or_else(missing_ast_error)?;

    match node {
        AstNodeData::Reference { ref_type, .. } => {
            let reference = data_store(context)
                .reconstruct_reference_type_for_eval(&ref_type, sheet_registry(context));
            visitor(context, classify(&reference))
        }
        AstNodeData::UnaryOp { expr_id, .. } => {
            visit_arena_references(expr_id, context, data_store, sheet_registry, visitor)
        }
        AstNodeData::BinaryOp {
            left_id, right_id, ..
        } => {
            visit_arena_references(left_id, context, data_store, sheet_registry, visitor)?;
            visit_arena_references(right_id, context, data_store, sheet_registry, visitor)
        }
        AstNodeData::Function { .. } => {
            let arg_count = data_store(context).get_args(ast_id).map_or(0, <[_]>::len);
            for index in 0..arg_count {
                let child = data_store(context)
                    .get_args(ast_id)
                    .expect("args disappeared")[index];
                visit_arena_references(child, context, data_store, sheet_registry, visitor)?;
            }
            Ok(())
        }
        AstNodeData::Array { .. } => {
            let element_count = data_store(context)
                .get_array_elems(ast_id)
                .map_or(0, |(_, _, elements)| elements.len());
            for index in 0..element_count {
                let child = data_store(context)
                    .get_array_elems(ast_id)
                    .expect("array elements disappeared")
                    .2[index];
                visit_arena_references(child, context, data_store, sheet_registry, visitor)?;
            }
            Ok(())
        }
        AstNodeData::Literal(_) | AstNodeData::Omitted => Ok(()),
    }
}

fn missing_ast_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::Value).with_message("Missing interned formula AST")
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_parse::parse;

    #[derive(Default)]
    struct Seen(Vec<String>);

    fn no_bindings(_: &Seen, _: &str, _: usize) -> LocalBindingStyle {
        LocalBindingStyle::None
    }

    fn record(seen: &mut Seen, reference: SemanticReference<'_>) -> Result<(), ExcelError> {
        let label = match reference {
            SemanticReference::Cell(cell) => format!(
                "cell:{:?}:{}:{}:{}:{}",
                cell.sheet, cell.row, cell.col, cell.row_abs, cell.col_abs
            ),
            SemanticReference::FiniteRange(range) => format!(
                "finite:{:?}:{:?}:{}:{}:{}:{}",
                range.sheet,
                range.finite_bounds(),
                range.start_row_abs,
                range.start_col_abs,
                range.end_row_abs,
                range.end_col_abs
            ),
            SemanticReference::OpenRange(range) => format!(
                "open:{:?}:{:?}:{:?}:{:?}:{:?}",
                range.sheet, range.start_row, range.start_col, range.end_row, range.end_col
            ),
            SemanticReference::Name(name) => format!("name:{name}"),
            SemanticReference::Table(table) => format!("table:{}", table.name),
            SemanticReference::ExternalSource(external) => format!("external:{}", external.raw),
            SemanticReference::ThreeDimensional(_) => "3d".to_string(),
            SemanticReference::Unsupported(_) => "unsupported".to_string(),
        };
        seen.0.push(label);
        Ok(())
    }

    #[test]
    fn classifies_in_source_order_without_expanding_ranges() {
        let ast = parse(
            "=SUM($A1,Sheet2!B$2,$C3:D$4,A1:A,A:A,1:1,NamedThing,Table1[#Data],[book]Sheet!A1,Sheet1:Sheet3!E5)",
        )
        .unwrap();
        let mut seen = Seen::default();
        visit_tree_references(&ast, &mut seen, no_bindings, record).unwrap();

        assert_eq!(seen.0.len(), 10);
        assert!(seen.0[0].starts_with("cell:Current:1:1:false:true"));
        assert!(seen.0[1].starts_with("cell:Name(\"Sheet2\"):2:2:true:false"));
        assert_eq!(
            seen.0[2],
            "finite:Current:Some((3, 3, 4, 4)):false:true:true:false"
        );
        assert!(seen.0[3].starts_with("open:Current:Some(1):Some(1):None:Some(1)"));
        assert!(seen.0[6].starts_with("name:NamedThing"));
        assert!(seen.0[7].starts_with("table:Table1"));
        assert!(seen.0[8].starts_with("external:"));
        assert_eq!(seen.0[9], "3d");
    }

    #[test]
    fn finite_range_helpers_expose_reversal_and_saturating_area() {
        let ast = parse("=D4:B2").unwrap();
        let mut observed = None;
        fn capture(
            observed: &mut Option<(bool, Option<u64>)>,
            reference: SemanticReference<'_>,
        ) -> Result<(), ExcelError> {
            let SemanticReference::FiniteRange(range) = reference else {
                panic!("expected finite range")
            };
            *observed = Some((range.is_reversed(), range.saturating_area()));
            Ok(())
        }
        fn none(_: &Option<(bool, Option<u64>)>, _: &str, _: usize) -> LocalBindingStyle {
            LocalBindingStyle::None
        }
        visit_tree_references(&ast, &mut observed, none, capture).unwrap();
        assert_eq!(observed, Some((true, Some(1))));
    }

    #[test]
    fn finite_range_area_uses_u64_at_and_above_u32_boundary() {
        fn capture(
            observed: &mut Option<u64>,
            reference: SemanticReference<'_>,
        ) -> Result<(), ExcelError> {
            let SemanticReference::FiniteRange(range) = reference else {
                panic!("expected finite range")
            };
            *observed = range.saturating_area();
            Ok(())
        }
        fn none(_: &Option<u64>, _: &str, _: usize) -> LocalBindingStyle {
            LocalBindingStyle::None
        }

        for (formula, expected) in [
            ("=A1:FLA983055", 4_294_967_295),
            ("=A1:XFD262144", 4_294_967_296),
            ("=A1:XFD1048576", 17_179_869_184),
        ] {
            let ast = parse(formula).unwrap();
            let mut observed = None;
            visit_tree_references(&ast, &mut observed, none, capture).unwrap();
            assert_eq!(observed, Some(expected), "{formula}");
        }
    }
}
