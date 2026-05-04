//! Arena-native canonical metadata computation (FP8 Phase 1 substrate).
//!
//! This module is intentionally pure and currently unused by production call
//! sites. Given an [`AstNodeData`] value plus the already-computed metadata of
//! its children, it returns the node's canonical labels and a stable 64-bit
//! FNV-1a hash.
//!
//! Hash construction mirrors the legacy `formula_plane::template_canonical`
//! payload shape without allocating a payload string:
//!
//! * mix a version tag and node-kind discriminant;
//! * mix local invariants that identify the canonical expression (literal raw
//!   value refs, normalized operator/function names, sheet bindings, reference
//!   shape, array dimensions);
//! * mix child canonical hashes in the same order the arena node references
//!   them;
//! * mix final label/reject bitsets so unsupported families do not merge during
//!   parity diagnostics.
//!
//! Reference axes are normalized in the same spirit as the tree canonicalizer:
//! absolute axes contribute their literal coordinate, while relative axes
//! contribute only a relative-axis discriminant and the abs/rel mode. Phase 1
//! arena nodes still store literal coordinates and this function has no formula
//! placement parameter, so it deliberately omits relative literal coordinates;
//! Phase 3 canonical interning will replace those leaves with anchor-free arena
//! data and can mix concrete deltas at that point.

#![allow(dead_code)]

use super::ast::{AstNodeData, AstNodeMetadata, CanonicalLabels, CompactRefType, SheetKey};
use super::string_interner::StringInterner;
use crate::function::FnCaps;
use crate::traits::FunctionProvider;
use formualizer_parse::parser::ExternalRefKind;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

const KIND_LITERAL: u8 = 1;
const KIND_REFERENCE: u8 = 2;
const KIND_UNARY: u8 = 3;
const KIND_BINARY: u8 = 4;
const KIND_FUNCTION: u8 = 5;
const KIND_ARRAY: u8 = 6;

const REF_CELL: u8 = 1;
const REF_RANGE: u8 = 2;
const REF_EXTERNAL: u8 = 3;
const REF_NAMED: u8 = 4;
const REF_TABLE: u8 = 5;
const REF_CELL_3D: u8 = 6;
const REF_RANGE_3D: u8 = 7;

const AXIS_RELATIVE: u8 = 1;
const AXIS_ABSOLUTE: u8 = 2;
const AXIS_OPEN_START: u8 = 3;
const AXIS_OPEN_END: u8 = 4;
const AXIS_WHOLE: u8 = 5;

/// Compute a node's canonical metadata from structural data plus child metadata.
///
/// `children` must be ordered as the node references them: unary expression;
/// binary left then right; function arguments in call order; array elements in
/// row-major order. Extra children are mixed defensively after node-local data;
/// missing children simply result in fewer child hash contributions.
pub(crate) fn compute_node_metadata(
    data: &AstNodeData,
    children: &[&AstNodeMetadata],
    data_store_strings: &StringInterner,
    function_provider: &dyn FunctionProvider,
) -> AstNodeMetadata {
    let mut labels = CanonicalLabels::default();
    for child in children {
        labels.flags |= child.labels.flags;
        labels.rejects |= child.labels.rejects;
    }

    let mut hasher = StableHasher::new();
    hasher.mix_bytes(b"fp8-arena-canonical:v1");

    match data {
        AstNodeData::Literal(value) => {
            hasher.mix_u8(KIND_LITERAL);
            hasher.mix_u32(value.as_raw());
        }
        AstNodeData::Reference {
            original_id,
            ref_type,
        } => {
            hasher.mix_u8(KIND_REFERENCE);
            let original = data_store_strings.get(*original_id).unwrap_or("");
            if original.trim_end().ends_with('#') {
                labels.rejects |= CanonicalLabels::REJECT_SPILL_REFERENCE;
            }
            mix_reference(&mut hasher, &mut labels, *ref_type, data_store_strings);
        }
        AstNodeData::UnaryOp { op_id, .. } => {
            hasher.mix_u8(KIND_UNARY);
            let op = data_store_strings.get(*op_id).unwrap_or("");
            mix_string(&mut hasher, op);
            match op {
                "#" => labels.rejects |= CanonicalLabels::REJECT_SPILL_RESULT_REGION_OPERATOR,
                "@" => labels.rejects |= CanonicalLabels::REJECT_IMPLICIT_INTERSECTION_OPERATOR,
                _ => {}
            }
            mix_children(&mut hasher, children);
        }
        AstNodeData::BinaryOp { op_id, .. } => {
            hasher.mix_u8(KIND_BINARY);
            let op = data_store_strings.get(*op_id).unwrap_or("");
            mix_string(&mut hasher, op);
            mix_children(&mut hasher, children);
        }
        AstNodeData::Function {
            name_id,
            args_count,
            ..
        } => {
            hasher.mix_u8(KIND_FUNCTION);
            labels.flags |= CanonicalLabels::FLAG_CONTAINS_FUNCTION;
            let raw_name = data_store_strings.get(*name_id).unwrap_or("");
            let canonical_name = normalize_function_name(raw_name);
            mix_string(&mut hasher, &canonical_name);
            hasher.mix_u16(*args_count);
            classify_function(&canonical_name, &mut labels, function_provider);
            mix_children(&mut hasher, children);
        }
        AstNodeData::Array { rows, cols, .. } => {
            hasher.mix_u8(KIND_ARRAY);
            labels.flags |= CanonicalLabels::FLAG_CONTAINS_ARRAY;
            labels.rejects |= CanonicalLabels::REJECT_ARRAY_LITERAL;
            hasher.mix_u16(*rows);
            hasher.mix_u16(*cols);
            mix_children(&mut hasher, children);
        }
    }

    finalize_anchor_flags(&mut labels);
    hasher.mix_u64(labels.flags);
    hasher.mix_u64(labels.rejects);

    AstNodeMetadata {
        canonical_hash: hasher.finish(),
        labels,
    }
}

fn mix_reference(
    hasher: &mut StableHasher,
    labels: &mut CanonicalLabels,
    ref_type: CompactRefType,
    strings: &StringInterner,
) {
    match ref_type {
        CompactRefType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => {
            hasher.mix_u8(REF_CELL);
            mix_sheet(hasher, labels, sheet, strings);
            mix_axis_value(hasher, labels, row, row_abs);
            mix_axis_value(hasher, labels, col, col_abs);
        }
        CompactRefType::Range {
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
            hasher.mix_u8(REF_RANGE);
            labels.flags |= CanonicalLabels::FLAG_CONTAINS_RANGE;
            mix_sheet(hasher, labels, sheet, strings);

            classify_range_axis(labels, start_row, end_row);
            classify_range_axis(labels, start_col, end_col);

            mix_range_axis_start(hasher, labels, start_row, start_row_abs);
            mix_range_axis_start(hasher, labels, start_col, start_col_abs);
            mix_range_axis_end(hasher, labels, end_row, end_row_abs);
            mix_range_axis_end(hasher, labels, end_col, end_col_abs);
        }
        CompactRefType::External {
            raw_id,
            book_id,
            sheet_id,
            kind,
        } => {
            hasher.mix_u8(REF_EXTERNAL);
            labels.rejects |= CanonicalLabels::REJECT_EXTERNAL_REFERENCE;
            mix_string_id(hasher, strings, raw_id);
            mix_string_id(hasher, strings, book_id);
            mix_string_id(hasher, strings, sheet_id);
            mix_external_kind(hasher, kind);
        }
        CompactRefType::NamedRange(name_id) => {
            hasher.mix_u8(REF_NAMED);
            labels.flags |= CanonicalLabels::FLAG_CONTAINS_NAME;
            labels.rejects |= CanonicalLabels::REJECT_NAMED_REFERENCE;
            let normalized = strings
                .get(name_id)
                .map(|name| name.to_ascii_uppercase())
                .unwrap_or_default();
            mix_string(hasher, &normalized);
        }
        CompactRefType::Table {
            name_id,
            specifier_id,
        } => {
            hasher.mix_u8(REF_TABLE);
            labels.flags |= CanonicalLabels::FLAG_CONTAINS_TABLE
                | CanonicalLabels::FLAG_CONTAINS_STRUCTURED_REF;
            labels.rejects |= CanonicalLabels::REJECT_STRUCTURED_REFERENCE;
            if strings.get(name_id).is_some_and(str::is_empty) {
                labels.flags |= CanonicalLabels::FLAG_NEEDS_PLACEMENT_REWRITE;
            }
            mix_string_id(hasher, strings, name_id);
            match specifier_id {
                Some(id) => {
                    hasher.mix_u8(1);
                    hasher.mix_u32(id.as_u32());
                }
                None => hasher.mix_u8(0),
            }
        }
        CompactRefType::Cell3D {
            sheet_first,
            sheet_last,
            row,
            col,
            row_abs,
            col_abs,
        } => {
            hasher.mix_u8(REF_CELL_3D);
            labels.flags |= CanonicalLabels::FLAG_EXPLICIT_SHEET;
            labels.rejects |= CanonicalLabels::REJECT_THREE_D_REFERENCE;
            mix_string_id(hasher, strings, sheet_first);
            mix_string_id(hasher, strings, sheet_last);
            mix_axis_value(hasher, labels, row, row_abs);
            mix_axis_value(hasher, labels, col, col_abs);
        }
        CompactRefType::Range3D {
            sheet_first,
            sheet_last,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => {
            hasher.mix_u8(REF_RANGE_3D);
            labels.flags |=
                CanonicalLabels::FLAG_CONTAINS_RANGE | CanonicalLabels::FLAG_EXPLICIT_SHEET;
            labels.rejects |= CanonicalLabels::REJECT_THREE_D_REFERENCE;
            mix_string_id(hasher, strings, sheet_first);
            mix_string_id(hasher, strings, sheet_last);
            classify_range_axis(labels, start_row, end_row);
            classify_range_axis(labels, start_col, end_col);
            mix_range_axis_start(hasher, labels, start_row, start_row_abs);
            mix_range_axis_start(hasher, labels, start_col, start_col_abs);
            mix_range_axis_end(hasher, labels, end_row, end_row_abs);
            mix_range_axis_end(hasher, labels, end_col, end_col_abs);
        }
    }
}

fn mix_sheet(
    hasher: &mut StableHasher,
    labels: &mut CanonicalLabels,
    sheet: Option<SheetKey>,
    strings: &StringInterner,
) {
    match sheet {
        Some(SheetKey::Id(id)) => {
            labels.flags |= CanonicalLabels::FLAG_EXPLICIT_SHEET;
            hasher.mix_u8(1);
            hasher.mix_u16(id);
        }
        Some(SheetKey::Name(id)) => {
            labels.flags |= CanonicalLabels::FLAG_EXPLICIT_SHEET;
            hasher.mix_u8(2);
            mix_string_id(hasher, strings, id);
        }
        None => {
            labels.flags |= CanonicalLabels::FLAG_CURRENT_SHEET;
            hasher.mix_u8(0);
        }
    }
}

fn mix_axis_value(hasher: &mut StableHasher, labels: &mut CanonicalLabels, value: u32, abs: bool) {
    if abs {
        labels.flags |= CanonicalLabels::FLAG_ABSOLUTE_ONLY;
        hasher.mix_u8(AXIS_ABSOLUTE);
        hasher.mix_u32(value);
    } else {
        labels.flags |= CanonicalLabels::FLAG_RELATIVE_ONLY;
        hasher.mix_u8(AXIS_RELATIVE);
    }
}

fn classify_range_axis(labels: &mut CanonicalLabels, start: u32, end: u32) {
    match (start == 0, end == u32::MAX) {
        (true, true) => labels.rejects |= CanonicalLabels::REJECT_WHOLE_AXIS_REFERENCE,
        (true, false) | (false, true) => {
            labels.rejects |= CanonicalLabels::REJECT_OPEN_RANGE_REFERENCE;
        }
        (false, false) => {}
    }
}

fn mix_range_axis_start(
    hasher: &mut StableHasher,
    labels: &mut CanonicalLabels,
    value: u32,
    abs: bool,
) {
    if value == 0 {
        hasher.mix_u8(AXIS_OPEN_START);
    } else {
        mix_axis_value(hasher, labels, value, abs);
    }
}

fn mix_range_axis_end(
    hasher: &mut StableHasher,
    labels: &mut CanonicalLabels,
    value: u32,
    abs: bool,
) {
    if value == u32::MAX {
        hasher.mix_u8(AXIS_OPEN_END);
    } else {
        mix_axis_value(hasher, labels, value, abs);
    }
}

fn finalize_anchor_flags(labels: &mut CanonicalLabels) {
    if labels.has_flag(CanonicalLabels::FLAG_RELATIVE_ONLY)
        && labels.has_flag(CanonicalLabels::FLAG_ABSOLUTE_ONLY)
    {
        labels.flags |= CanonicalLabels::FLAG_MIXED_ANCHORS;
    }
}

fn classify_function(
    canonical_name: &str,
    labels: &mut CanonicalLabels,
    function_provider: &dyn FunctionProvider,
) {
    let caps = function_provider
        .get_function("", canonical_name)
        .map(|function| function.caps());

    if caps.is_some_and(|caps| caps.contains(FnCaps::VOLATILE))
        || is_volatile_function(canonical_name)
    {
        labels.flags |= CanonicalLabels::FLAG_VOLATILE;
        labels.rejects |= CanonicalLabels::REJECT_VOLATILE_FUNCTION;
    }

    if caps.is_some_and(|caps| caps.contains(FnCaps::DYNAMIC_DEPENDENCY))
        || is_dynamic_reference_function(canonical_name)
    {
        labels.flags |= CanonicalLabels::FLAG_DYNAMIC;
        labels.rejects |= CanonicalLabels::REJECT_DYNAMIC_REFERENCE;
    }

    if caps.is_some_and(|caps| caps.contains(FnCaps::RETURNS_REFERENCE))
        || is_reference_returning_function(canonical_name)
    {
        labels.rejects |= CanonicalLabels::REJECT_REFERENCE_RETURNING_FUNCTION;
    }

    if is_local_environment_function(canonical_name) {
        labels.flags |= CanonicalLabels::FLAG_CONTAINS_LET_LAMBDA;
        labels.rejects |= CanonicalLabels::REJECT_LOCAL_ENVIRONMENT;
    }

    if is_array_or_spill_function(canonical_name) {
        labels.flags |= CanonicalLabels::FLAG_CONTAINS_ARRAY;
        labels.rejects |= CanonicalLabels::REJECT_ARRAY_OR_SPILL_FUNCTION;
    }

    let known_by_provider = caps.is_some();
    let known_special = is_dynamic_reference_function(canonical_name)
        || is_local_environment_function(canonical_name)
        || is_volatile_function(canonical_name)
        || is_reference_returning_function(canonical_name)
        || is_array_or_spill_function(canonical_name);
    if !known_by_provider && !known_special && !is_known_static_function(canonical_name) {
        labels.rejects |= CanonicalLabels::REJECT_UNKNOWN_OR_CUSTOM_FUNCTION;
    }
}

fn normalize_function_name(name: &str) -> String {
    let mut normalized = name.trim().to_ascii_uppercase();
    loop {
        if let Some(stripped) = ["_XLFN.", "_XLL.", "_XLWS."]
            .iter()
            .find_map(|prefix| normalized.strip_prefix(prefix).map(str::to_string))
        {
            normalized = stripped;
        } else {
            return normalized;
        }
    }
}

fn is_dynamic_reference_function(name: &str) -> bool {
    matches!(name, "INDIRECT" | "OFFSET")
}

fn is_local_environment_function(name: &str) -> bool {
    matches!(name, "LET" | "LAMBDA")
}

fn is_volatile_function(name: &str) -> bool {
    matches!(name, "NOW" | "TODAY" | "RAND" | "RANDBETWEEN")
}

fn is_reference_returning_function(name: &str) -> bool {
    matches!(name, "CHOOSE" | "INDEX")
}

fn is_array_or_spill_function(name: &str) -> bool {
    matches!(
        name,
        "FILTER" | "RANDARRAY" | "SEQUENCE" | "SORT" | "SORTBY" | "TEXTSPLIT" | "UNIQUE"
    )
}

fn is_known_static_function(name: &str) -> bool {
    matches!(
        name,
        "ABS"
            | "ACOS"
            | "ACOSH"
            | "AND"
            | "ASIN"
            | "ASINH"
            | "ATAN"
            | "ATAN2"
            | "ATANH"
            | "AVERAGE"
            | "CEILING"
            | "CONCAT"
            | "CONCATENATE"
            | "COS"
            | "COSH"
            | "COUNT"
            | "COUNTA"
            | "COUNTBLANK"
            | "COUNTIF"
            | "COUNTIFS"
            | "DATE"
            | "DAY"
            | "ERROR.TYPE"
            | "EVEN"
            | "EXACT"
            | "EXP"
            | "FALSE"
            | "FIND"
            | "FLOOR"
            | "IF"
            | "IFERROR"
            | "IFNA"
            | "IFS"
            | "INT"
            | "ISBLANK"
            | "ISERR"
            | "ISERROR"
            | "ISEVEN"
            | "ISLOGICAL"
            | "ISNA"
            | "ISNONTEXT"
            | "ISNUMBER"
            | "ISODD"
            | "ISTEXT"
            | "LEFT"
            | "LEN"
            | "LN"
            | "LOG"
            | "LOG10"
            | "LOWER"
            | "MAX"
            | "MID"
            | "MIN"
            | "MOD"
            | "MONTH"
            | "NOT"
            | "ODD"
            | "OR"
            | "POWER"
            | "PRODUCT"
            | "PROPER"
            | "REPLACE"
            | "REPT"
            | "RIGHT"
            | "ROUND"
            | "ROUNDDOWN"
            | "ROUNDUP"
            | "SEARCH"
            | "SIN"
            | "SINH"
            | "SQRT"
            | "SUBSTITUTE"
            | "SUM"
            | "SUMIF"
            | "SUMIFS"
            | "SWITCH"
            | "TAN"
            | "TANH"
            | "TEXT"
            | "TEXTJOIN"
            | "TIME"
            | "TRIM"
            | "TRUE"
            | "TRUNC"
            | "UPPER"
            | "VALUE"
            | "YEAR"
    )
}

fn mix_children(hasher: &mut StableHasher, children: &[&AstNodeMetadata]) {
    hasher.mix_usize(children.len());
    for child in children {
        hasher.mix_u64(child.canonical_hash);
    }
}

fn mix_string_id(
    hasher: &mut StableHasher,
    strings: &StringInterner,
    id: super::string_interner::StringId,
) {
    mix_string(hasher, strings.get(id).unwrap_or(""));
}

fn mix_string(hasher: &mut StableHasher, value: &str) {
    hasher.mix_usize(value.len());
    hasher.mix_bytes(value.as_bytes());
}

fn mix_external_kind(hasher: &mut StableHasher, kind: ExternalRefKind) {
    match kind {
        ExternalRefKind::Cell {
            row,
            col,
            row_abs,
            col_abs,
        } => {
            hasher.mix_u8(1);
            hasher.mix_u32(row);
            hasher.mix_u32(col);
            hasher.mix_u8(u8::from(row_abs));
            hasher.mix_u8(u8::from(col_abs));
        }
        ExternalRefKind::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => {
            hasher.mix_u8(2);
            mix_optional_u32(hasher, start_row);
            mix_optional_u32(hasher, start_col);
            mix_optional_u32(hasher, end_row);
            mix_optional_u32(hasher, end_col);
            hasher.mix_u8(u8::from(start_row_abs));
            hasher.mix_u8(u8::from(start_col_abs));
            hasher.mix_u8(u8::from(end_row_abs));
            hasher.mix_u8(u8::from(end_col_abs));
        }
    }
}

fn mix_optional_u32(hasher: &mut StableHasher, value: Option<u32>) {
    match value {
        Some(value) => {
            hasher.mix_u8(1);
            hasher.mix_u32(value);
        }
        None => hasher.mix_u8(0),
    }
}

struct StableHasher {
    state: u64,
}

impl StableHasher {
    fn new() -> Self {
        Self { state: FNV_OFFSET }
    }

    fn mix_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }

    fn mix_u8(&mut self, value: u8) {
        self.mix_bytes(&[value]);
    }

    fn mix_u16(&mut self, value: u16) {
        self.mix_bytes(&value.to_le_bytes());
    }

    fn mix_u32(&mut self, value: u32) {
        self.mix_bytes(&value.to_le_bytes());
    }

    fn mix_u64(&mut self, value: u64) {
        self.mix_bytes(&value.to_le_bytes());
    }

    fn mix_usize(&mut self, value: usize) {
        self.mix_u64(value as u64);
    }

    fn finish(self) -> u64 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
    use formualizer_common::{ExcelError, LiteralValue};
    use std::sync::Arc;

    struct NoopProvider;

    impl FunctionProvider for NoopProvider {
        fn get_function(&self, _ns: &str, _name: &str) -> Option<Arc<dyn Function>> {
            None
        }
    }

    struct CapsProvider {
        caps: FnCaps,
    }

    impl FunctionProvider for CapsProvider {
        fn get_function(&self, _ns: &str, _name: &str) -> Option<Arc<dyn Function>> {
            Some(Arc::new(TestFunction { caps: self.caps }))
        }
    }

    struct TestFunction {
        caps: FnCaps,
    }

    impl Function for TestFunction {
        fn caps(&self) -> FnCaps {
            self.caps
        }

        fn name(&self) -> &'static str {
            "TEST"
        }

        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<CalcValue<'b>, ExcelError> {
            Ok(CalcValue::Scalar(LiteralValue::Empty))
        }
    }

    fn meta(
        data: &AstNodeData,
        children: &[&AstNodeMetadata],
        strings: &StringInterner,
    ) -> AstNodeMetadata {
        compute_node_metadata(data, children, strings, &NoopProvider)
    }

    #[test]
    fn literal_hash_is_stable_and_unlabelled() {
        let strings = StringInterner::new();
        let data = AstNodeData::Literal(super::super::value_ref::ValueRef::small_int(42).unwrap());

        let first = meta(&data, &[], &strings);
        let second = meta(&data, &[], &strings);

        assert_eq!(first.canonical_hash, second.canonical_hash);
        assert_ne!(first.canonical_hash, 0);
        assert_eq!(first.labels, CanonicalLabels::default());
    }

    #[test]
    fn reference_axes_set_relative_absolute_and_mixed_flags() {
        let mut strings = StringInterner::new();
        let original_id = strings.intern("A1");

        let relative = AstNodeData::Reference {
            original_id,
            ref_type: CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 1,
                row_abs: false,
                col_abs: false,
            },
        };
        let relative_meta = meta(&relative, &[], &strings);
        assert!(
            relative_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_RELATIVE_ONLY)
        );
        assert!(
            !relative_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_ABSOLUTE_ONLY)
        );
        assert!(
            !relative_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_MIXED_ANCHORS)
        );

        let absolute = AstNodeData::Reference {
            original_id,
            ref_type: CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 1,
                row_abs: true,
                col_abs: true,
            },
        };
        let absolute_meta = meta(&absolute, &[], &strings);
        assert!(
            absolute_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_ABSOLUTE_ONLY)
        );
        assert!(
            !absolute_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_RELATIVE_ONLY)
        );
        assert!(
            !absolute_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_MIXED_ANCHORS)
        );

        let mixed = AstNodeData::Reference {
            original_id,
            ref_type: CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 1,
                row_abs: true,
                col_abs: false,
            },
        };
        let mixed_meta = meta(&mixed, &[], &strings);
        assert!(
            mixed_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_RELATIVE_ONLY)
        );
        assert!(
            mixed_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_ABSOLUTE_ONLY)
        );
        assert!(
            mixed_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_MIXED_ANCHORS)
        );
    }

    #[test]
    fn function_provider_volatile_caps_set_flag_and_reject() {
        let mut strings = StringInterner::new();
        let name_id = strings.intern("CUSTOMRAND");
        let data = AstNodeData::Function {
            name_id,
            args_offset: 0,
            args_count: 0,
        };
        let provider = CapsProvider {
            caps: FnCaps::VOLATILE,
        };

        let metadata = compute_node_metadata(&data, &[], &strings, &provider);

        assert!(
            metadata
                .labels
                .has_flag(CanonicalLabels::FLAG_CONTAINS_FUNCTION)
        );
        assert!(metadata.labels.has_flag(CanonicalLabels::FLAG_VOLATILE));
        assert!(
            metadata
                .labels
                .has_reject(CanonicalLabels::REJECT_VOLATILE_FUNCTION)
        );
    }

    #[test]
    fn relative_references_at_different_literal_positions_hash_the_same() {
        let mut strings = StringInterner::new();
        let plus_id = strings.intern("+");
        let a1_id = strings.intern("A1");
        let b1_id = strings.intern("B1");
        let a2_id = strings.intern("A2");
        let b2_id = strings.intern("B2");

        let a1 = AstNodeData::Reference {
            original_id: a1_id,
            ref_type: CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 1,
                row_abs: false,
                col_abs: false,
            },
        };
        let b1 = AstNodeData::Reference {
            original_id: b1_id,
            ref_type: CompactRefType::Cell {
                sheet: None,
                row: 1,
                col: 2,
                row_abs: false,
                col_abs: false,
            },
        };
        let a2 = AstNodeData::Reference {
            original_id: a2_id,
            ref_type: CompactRefType::Cell {
                sheet: None,
                row: 2,
                col: 1,
                row_abs: false,
                col_abs: false,
            },
        };
        let b2 = AstNodeData::Reference {
            original_id: b2_id,
            ref_type: CompactRefType::Cell {
                sheet: None,
                row: 2,
                col: 2,
                row_abs: false,
                col_abs: false,
            },
        };

        let a1_meta = meta(&a1, &[], &strings);
        let b1_meta = meta(&b1, &[], &strings);
        let a2_meta = meta(&a2, &[], &strings);
        let b2_meta = meta(&b2, &[], &strings);
        assert_eq!(a1_meta.canonical_hash, a2_meta.canonical_hash);
        assert_eq!(b1_meta.canonical_hash, b2_meta.canonical_hash);

        let first_sum = AstNodeData::BinaryOp {
            op_id: plus_id,
            left_id: super::super::ast::AstNodeId::from_u32(0),
            right_id: super::super::ast::AstNodeId::from_u32(1),
        };
        let second_sum = AstNodeData::BinaryOp {
            op_id: plus_id,
            left_id: super::super::ast::AstNodeId::from_u32(2),
            right_id: super::super::ast::AstNodeId::from_u32(3),
        };

        let first = meta(&first_sum, &[&a1_meta, &b1_meta], &strings);
        let second = meta(&second_sum, &[&a2_meta, &b2_meta], &strings);

        assert_eq!(first.canonical_hash, second.canonical_hash);
    }

    #[test]
    fn reject_bits_cover_let_lambda_structured_refs_and_arrays() {
        let mut strings = StringInterner::new();
        let let_id = strings.intern("LET");
        let empty_table_name = strings.intern("");
        let table_original = strings.intern("[#This Row]");

        let let_fn = AstNodeData::Function {
            name_id: let_id,
            args_offset: 0,
            args_count: 0,
        };
        let let_meta = meta(&let_fn, &[], &strings);
        assert!(
            let_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_CONTAINS_LET_LAMBDA)
        );
        assert!(
            let_meta
                .labels
                .has_reject(CanonicalLabels::REJECT_LOCAL_ENVIRONMENT)
        );

        let table_ref = AstNodeData::Reference {
            original_id: table_original,
            ref_type: CompactRefType::Table {
                name_id: empty_table_name,
                specifier_id: None,
            },
        };
        let table_meta = meta(&table_ref, &[], &strings);
        assert!(
            table_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_CONTAINS_STRUCTURED_REF)
        );
        assert!(
            table_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_CONTAINS_TABLE)
        );
        assert!(
            table_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_NEEDS_PLACEMENT_REWRITE)
        );
        assert!(
            table_meta
                .labels
                .has_reject(CanonicalLabels::REJECT_STRUCTURED_REFERENCE)
        );

        let array = AstNodeData::Array {
            rows: 1,
            cols: 0,
            elements_offset: 0,
        };
        let array_meta = meta(&array, &[], &strings);
        assert!(
            array_meta
                .labels
                .has_flag(CanonicalLabels::FLAG_CONTAINS_ARRAY)
        );
        assert!(
            array_meta
                .labels
                .has_reject(CanonicalLabels::REJECT_ARRAY_LITERAL)
        );
    }
}
