//! Static name readers, including literal INDIRECT targets.
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType as N, ReferenceType};

pub(crate) fn is_indirect(name: &str) -> bool {
    let normalized = name.to_ascii_uppercase();
    let mut bare = normalized.as_str();
    while let Some(rest) = ["_XLFN.", "_XLL.", "_XLWS."]
        .iter()
        .find_map(|prefix| bare.strip_prefix(prefix))
    {
        bare = rest;
    }
    bare == "INDIRECT"
}

/// Visit names, indicating whether each came from INDIRECT text. Returns
/// whether the formula has any INDIRECT target that is not a literal string.
pub(crate) fn visit(ast: &ASTNode, mut reader: impl FnMut(&str, bool)) -> bool {
    let mut pending = vec![ast];
    let mut nonliteral = false;
    while let Some(node) = pending.pop() {
        match &node.node_type {
            N::Reference {
                reference: ReferenceType::NamedRange(name),
                ..
            } => reader(name, false),
            N::Function { name, args } => {
                if is_indirect(name) {
                    if let Some(ASTNode {
                        node_type: N::Literal(LiteralValue::Text(text)),
                        ..
                    }) = args.first()
                    {
                        if let Ok(ReferenceType::NamedRange(name)) =
                            ReferenceType::from_string(text)
                        {
                            reader(&name, true);
                        }
                    } else {
                        nonliteral = true;
                    }
                }
                pending.extend(args);
            }
            N::UnaryOp { expr, .. } => pending.push(expr),
            N::BinaryOp { left, right, .. } => {
                pending.push(left);
                pending.push(right);
            }
            N::Call { callee, args } => {
                pending.push(callee);
                pending.extend(args);
            }
            N::Array(rows) => pending.extend(rows.iter().flatten()),
            _ => {}
        }
    }
    nonliteral
}
