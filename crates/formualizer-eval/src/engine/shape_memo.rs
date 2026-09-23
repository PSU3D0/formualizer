//! Shape memo for `IngestPipeline::ingest_formula` (FORM-000133).
//!
//! Three products, kept separate (design: `packet-a/design.md`):
//!
//! 1. **Relative shape** (immutable): a token stream from one pre-order walk of
//!    the arena tree, relative to the placement anchor. Hash to a bucket, then
//!    compare the full token slice; the hash alone never decides a hit.
//! 2. **Specialization**, keyed by `(shape, placement sheet)`: everything the
//!    unmemoized path derives from the shape plus the pipeline's frozen
//!    bindings (names, tables, sources, sheet registry, function provider,
//!    policy). Valid only while the provider's planning revision and the
//!    global function registry's semantic epoch are unchanged; the memo lives
//!    no longer than one `IngestPipeline`.
//! 3. **Per-placement work**, never memoized: dependency-plan replay through
//!    `collect_reference` on the cell's own references (sheet resolution,
//!    reversed ranges, range expansion versus subscription from the
//!    instantiated area, name/source/table classification), the read summary
//!    and the template slot map over real arena node ids.
//!
//! Anything the key walk cannot prove shape-invariant makes the formula
//! ineligible, and it takes the unchanged per-cell path.
//!
//! Cost policy: a pipeline's first formula skips the key walk, and a shape is
//! materialized (tokens stored, specialization traced) only on its second
//! sighting; the first sighting records just its hash. Streams of distinct
//! shapes and one-formula pipelines therefore pay little beyond the walk.
//!
//! Bounds (workbook content is untrusted input):
//!
//! - **Hash flooding.** Bucket hashes are keyed with a per-memo random seed,
//!   so shape tokens cannot be chosen to collide in the memo's hash tables.
//!   Token streams whose unkeyed hash fully collides still share one bucket;
//!   a bucket holds at most [`MAX_BUCKET_SHAPES`] shapes, and a new shape
//!   landing in a full bucket takes the per-cell path. A lookup therefore
//!   compares at most [`MAX_BUCKET_SHAPES`] token slices, and adversarial
//!   input degrades to the unmemoized cost rather than quadratic work.
//! - **Memory per pipeline.** At most [`MAX_SHAPES`] shapes,
//!   [`MAX_SPECIALIZATIONS`] specializations and [`MAX_SEEN`] first-sighting
//!   hashes. Stored shapes and specializations are charged their shape's
//!   token count against [`MAX_STORED_TOKENS`], and a shape longer than
//!   [`MAX_SHAPE_TOKENS`] is ineligible. A specialization's canonical
//!   expression, keys and slot descriptors are linear in its shape's token
//!   count, so the memo's footprint is linear in the charged total: the token
//!   budget (8 MiB of stored tokens) plus proportional specialization data and
//!   about 1 MiB of first-sighting hashes. Past any bound, new shapes take the
//!   per-cell path; the memo is dropped with its pipeline.

use crate::SheetId;
use crate::engine::arena::value_ref::ValueType;
use crate::engine::arena::{AstNodeData, AstNodeId, CompactRefType, DataStore, SheetKey};
use crate::formula_plane::producer::{ProjectionFallbackReason, ReadProjection};
use crate::formula_plane::runtime::ValueRefSlotDescriptor;
use crate::formula_plane::template_canonical::{CanonicalExpr, LiteralSlotDescriptor};
use crate::reference::CellRef;
use formualizer_common::LiteralValue;
use rustc_hash::FxHashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::arena::CanonicalLabels;

/// Upper bound on distinct shapes per pipeline; beyond it new shapes bypass.
pub(crate) const MAX_SHAPES: usize = 16_384;
/// Upper bound on shapes sharing one (keyed) bucket hash.
pub(crate) const MAX_BUCKET_SHAPES: usize = 4;
/// Upper bound on seen-once shape hashes per pipeline.
pub(crate) const MAX_SEEN: usize = 4 * MAX_SHAPES;
/// Upper bound on `(shape, sheet)` specializations per pipeline.
pub(crate) const MAX_SPECIALIZATIONS: usize = MAX_SHAPES;
/// Token budget charged for stored shapes and specializations per pipeline.
pub(crate) const MAX_STORED_TOKENS: usize = 1 << 20;
/// Longest eligible shape, in tokens.
pub(crate) const MAX_SHAPE_TOKENS: usize = 4_096;

const T_EMPTY: u64 = 1;
const T_INT: u64 = 2;
const T_NUMBER: u64 = 3;
const T_TEXT: u64 = 4;
const T_BOOL: u64 = 5;
const T_OMITTED: u64 = 6;
const T_CELL: u64 = 7;
const T_RANGE: u64 = 8;
const T_NAME: u64 = 9;
const T_UNARY: u64 = 10;
const T_BINARY: u64 = 11;
const T_FUNCTION: u64 = 12;
const T_ARRAY: u64 = 13;

/// Product 2: binding-sensitive specialization of one shape on one sheet.
pub(crate) struct Specialization {
    pub(crate) canonical_hash: u64,
    pub(crate) exact_canonical_hash: u64,
    pub(crate) exact_canonical_key: Arc<str>,
    pub(crate) parameterized_canonical_hash: u64,
    pub(crate) parameterized_canonical_key: Arc<str>,
    pub(crate) literal_slot_descriptors: Arc<[LiteralSlotDescriptor]>,
    pub(crate) literal_bindings: Box<[LiteralValue]>,
    pub(crate) value_ref_slot_descriptors: Arc<[ValueRefSlotDescriptor]>,
    pub(crate) expr: CanonicalExpr,
    pub(crate) labels: CanonicalLabels,
    pub(crate) read_projections: Option<Vec<ReadProjection>>,
    pub(crate) read_projection_fallback: Option<ProjectionFallbackReason>,
    pub(crate) volatile: bool,
    pub(crate) dynamic: bool,
    /// Dependency visit sequence: indices into the formula's pre-order
    /// reference nodes, in the order the unmemoized walk consumed them.
    pub(crate) visit: Box<[u32]>,
}

/// Work counts per product for one pipeline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MemoCounts {
    pub(crate) shape_hits: u64,
    pub(crate) shape_misses: u64,
    pub(crate) specialization_hits: u64,
    pub(crate) specialization_misses: u64,
    /// Formulas that took the per-cell path (ineligible, first formula of
    /// the pipeline, first sighting, unmemoizable specialization, full memo,
    /// replay error, or non-arena input).
    pub(crate) bypasses: u64,
    /// Bypasses that were a shape's first sighting (subset of `bypasses`).
    pub(crate) first_sightings: u64,
}

/// What a memo entry's semantics depend on besides the pipeline's frozen
/// bindings: the function provider's planning revision and the global
/// function registry's semantic epoch (read lock-free, since callers may hold
/// a registry epoch read guard). Any change clears the memo.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MemoValidity {
    pub(crate) provider_revision: Option<u64>,
    pub(crate) registry_epoch: u64,
}

impl MemoValidity {
    pub(crate) fn current(provider: &dyn crate::traits::FunctionProvider) -> Self {
        Self {
            provider_revision: provider.planning_semantic_revision(),
            registry_epoch: crate::function_registry::semantic_epoch_lock_free(),
        }
    }
}

/// Memo state for one pipeline. `tokens` and `refs` hold the current
/// candidate's key walk.
pub(crate) struct ShapeMemo {
    pub(crate) counts: MemoCounts,
    validity: Option<MemoValidity>,
    /// Per-memo key for bucket hashes (hash-flooding resistance).
    seed: u64,
    /// Hashes of shapes seen once and not yet materialized.
    seen: rustc_hash::FxHashSet<u64>,
    buckets: FxHashMap<u64, Vec<u32>>,
    shapes: Vec<Box<[u64]>>,
    specializations: FxHashMap<(u32, SheetId), Option<Specialization>>,
    /// Tokens charged against `MAX_STORED_TOKENS`.
    stored_tokens: usize,
    pub(crate) tokens: Vec<u64>,
    pub(crate) refs: Vec<CompactRefType>,
    /// Test hook: replace every bucket hash with this value (simulates
    /// adversarial full collisions).
    #[cfg(test)]
    pub(crate) forced_hash: Option<u64>,
    /// Test instrumentation: token-slice comparisons performed by lookups.
    #[cfg(test)]
    pub(crate) comparisons: u64,
}

#[cfg(test)]
thread_local! {
    /// Test hook: memos created on this thread force every bucket hash to
    /// this value.
    pub(crate) static FORCED_HASH: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
}

impl Default for ShapeMemo {
    fn default() -> Self {
        use std::hash::BuildHasher;
        Self {
            counts: MemoCounts::default(),
            validity: None,
            seed: std::hash::RandomState::new().hash_one(0x5a17_u64),
            seen: Default::default(),
            buckets: Default::default(),
            shapes: Vec::new(),
            specializations: Default::default(),
            stored_tokens: 0,
            tokens: Vec::new(),
            refs: Vec::new(),
            #[cfg(test)]
            forced_hash: FORCED_HASH.with(std::cell::Cell::get),
            #[cfg(test)]
            comparisons: 0,
        }
    }
}

pub(crate) enum ShapeLookup {
    /// Shape index; `true` if it was inserted by this lookup.
    Shape(u32, bool),
    /// First sighting of the shape's hash: recorded, not materialized.
    FirstSighting,
    /// A bound was reached (shape count, token budget, first-sighting set or
    /// bucket size) and the shape is new or too long.
    Full,
}

impl ShapeMemo {
    /// Clear every entry if the validity token changed since the memo was filled.
    pub(crate) fn revalidate(&mut self, validity: MemoValidity) {
        if self.validity != Some(validity) {
            self.seen.clear();
            self.buckets.clear();
            self.shapes.clear();
            self.specializations.clear();
            self.stored_tokens = 0;
            self.validity = Some(validity);
        }
    }

    fn bucket_hash(&self) -> u64 {
        #[cfg(test)]
        if let Some(hash) = self.forced_hash {
            return hash;
        }
        let mut hasher = rustc_hash::FxHasher::default();
        self.tokens.hash(&mut hasher);
        // FxHash leaves low bits unmixed; literal f64 bits of small integers
        // have all-zero low bits, which would put every such shape in one
        // hash-table probe group. Key and finalize before using the value.
        fmix64(hasher.finish() ^ self.seed)
    }

    /// Find the current `tokens` among known shapes by bucket hash and full
    /// slice equality, inserting it on its second sighting. A hash collision
    /// with a seen-once shape only materializes a shape early.
    pub(crate) fn lookup_shape(&mut self) -> ShapeLookup {
        if self.tokens.len() > MAX_SHAPE_TOKENS {
            return ShapeLookup::Full;
        }
        let hash = self.bucket_hash();
        let bucket_len = match self.buckets.get(&hash) {
            Some(bucket) => {
                for &index in bucket {
                    #[cfg(test)]
                    {
                        self.comparisons += 1;
                    }
                    if *self.shapes[index as usize] == *self.tokens {
                        return ShapeLookup::Shape(index, false);
                    }
                }
                bucket.len()
            }
            None => 0,
        };
        if self.shapes.len() >= MAX_SHAPES
            || bucket_len >= MAX_BUCKET_SHAPES
            || self.stored_tokens + self.tokens.len() > MAX_STORED_TOKENS
        {
            return ShapeLookup::Full;
        }
        if !self.seen.contains(&hash) {
            if self.seen.len() >= MAX_SEEN {
                return ShapeLookup::Full;
            }
            self.seen.insert(hash);
            return ShapeLookup::FirstSighting;
        }
        let index = self.shapes.len() as u32;
        self.shapes.push(self.tokens.clone().into_boxed_slice());
        self.stored_tokens += self.tokens.len();
        self.buckets.entry(hash).or_default().push(index);
        ShapeLookup::Shape(index, true)
    }

    pub(crate) fn specialization(
        &self,
        shape: u32,
        sheet: SheetId,
    ) -> Option<&Option<Specialization>> {
        self.specializations.get(&(shape, sheet))
    }

    /// Whether a new specialization of `shape` fits within the bounds.
    pub(crate) fn can_insert_specialization(&self, shape: u32) -> bool {
        self.specializations.len() < MAX_SPECIALIZATIONS
            && self.stored_tokens + self.shapes[shape as usize].len() <= MAX_STORED_TOKENS
    }

    pub(crate) fn insert_specialization(
        &mut self,
        shape: u32,
        sheet: SheetId,
        specialization: Option<Specialization>,
    ) {
        if !self.can_insert_specialization(shape) {
            return;
        }
        self.stored_tokens += self.shapes[shape as usize].len();
        self.specializations.insert((shape, sheet), specialization);
    }

    #[cfg(test)]
    pub(crate) fn footprint(&self) -> (usize, usize, usize, usize) {
        (
            self.shapes.len(),
            self.specializations.len(),
            self.seen.len(),
            self.stored_tokens,
        )
    }
}

/// MurmurHash3 64-bit finalizer.
fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// Product 1 key walk. Fills `tokens` and `refs` (pre-order reference nodes)
/// and returns `false` if the formula is not eligible for the memo.
pub(crate) fn shape_tokens(
    data_store: &DataStore,
    root: AstNodeId,
    placement: CellRef,
    tokens: &mut Vec<u64>,
    refs: &mut Vec<CompactRefType>,
) -> bool {
    tokens.clear();
    refs.clear();
    let anchor_row = placement.coord.row() + 1;
    let anchor_col = placement.coord.col() + 1;
    walk(data_store, root, anchor_row, anchor_col, tokens, refs)
}

fn sheet_token(sheet: Option<SheetKey>) -> u64 {
    match sheet {
        None => 0,
        Some(SheetKey::Id(id)) => (1 << 32) | u64::from(id),
        Some(SheetKey::Name(name)) => (2 << 32) | u64::from(name.as_u32()),
    }
}

/// Absolute axes keep the coordinate; relative axes keep `value - anchor`.
/// The absolute flag is tokenized separately, so the two encodings never
/// need to be distinguishable from each other.
fn axis_token(value: u32, anchor: u32, absolute: bool) -> u64 {
    if absolute {
        u64::from(value)
    } else {
        (i64::from(value) - i64::from(anchor)) as u64
    }
}

fn walk(
    data_store: &DataStore,
    id: AstNodeId,
    anchor_row: u32,
    anchor_col: u32,
    tokens: &mut Vec<u64>,
    refs: &mut Vec<CompactRefType>,
) -> bool {
    let Some(node) = data_store.get_node(id) else {
        return false;
    };
    match *node {
        AstNodeData::Literal(value_ref) => match value_ref.value_type() {
            ValueType::Empty => tokens.push(T_EMPTY),
            ValueType::SmallInt | ValueType::LargeInt => {
                let LiteralValue::Int(value) = data_store.retrieve_value(value_ref) else {
                    return false;
                };
                tokens.extend([T_INT, value as u64]);
            }
            ValueType::Number => {
                let LiteralValue::Number(value) = data_store.retrieve_value(value_ref) else {
                    return false;
                };
                tokens.extend([T_NUMBER, value.to_bits()]);
            }
            // Literal texts are interned, so equal texts have equal refs.
            ValueType::String => tokens.extend([T_TEXT, u64::from(value_ref.as_raw())]),
            ValueType::Boolean => tokens.extend([T_BOOL, u64::from(value_ref.as_raw())]),
            _ => return false,
        },
        AstNodeData::Omitted => tokens.push(T_OMITTED),
        AstNodeData::Reference {
            original_id,
            ref_type,
        } => {
            // Spill references carry the per-placement original text into
            // canonical diagnostics.
            if data_store
                .resolve_ast_string(original_id)
                .trim_end()
                .ends_with('#')
            {
                return false;
            }
            match ref_type {
                CompactRefType::Cell {
                    sheet,
                    row,
                    col,
                    row_abs,
                    col_abs,
                } => tokens.extend([
                    T_CELL,
                    sheet_token(sheet),
                    u64::from(row_abs) | (u64::from(col_abs) << 1),
                    axis_token(row, anchor_row, row_abs),
                    axis_token(col, anchor_col, col_abs),
                ]),
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
                    // Open bounds are stored as 0 (start) / u32::MAX (end).
                    // Whole-axis pairs are placement-invariant; a pair with
                    // exactly one open bound embeds the original text in
                    // canonical reject reasons, so it is ineligible.
                    let rows_open = (start_row == 0, end_row == u32::MAX);
                    let cols_open = (start_col == 0, end_col == u32::MAX);
                    if rows_open.0 != rows_open.1 || cols_open.0 != cols_open.1 {
                        return false;
                    }
                    let flags = u64::from(start_row_abs)
                        | (u64::from(start_col_abs) << 1)
                        | (u64::from(end_row_abs) << 2)
                        | (u64::from(end_col_abs) << 3)
                        | (u64::from(rows_open.0) << 4)
                        | (u64::from(cols_open.0) << 5);
                    let (sr, er) = if rows_open.0 {
                        (0, 0)
                    } else {
                        (
                            axis_token(start_row, anchor_row, start_row_abs),
                            axis_token(end_row, anchor_row, end_row_abs),
                        )
                    };
                    let (sc, ec) = if cols_open.0 {
                        (0, 0)
                    } else {
                        (
                            axis_token(start_col, anchor_col, start_col_abs),
                            axis_token(end_col, anchor_col, end_col_abs),
                        )
                    };
                    tokens.extend([T_RANGE, sheet_token(sheet), flags, sr, sc, er, ec]);
                }
                CompactRefType::NamedRange(name) => {
                    tokens.extend([T_NAME, u64::from(name.as_u32())]);
                }
                CompactRefType::External { .. }
                | CompactRefType::Table { .. }
                | CompactRefType::Cell3D { .. }
                | CompactRefType::Range3D { .. } => return false,
            }
            refs.push(ref_type);
        }
        AstNodeData::UnaryOp { op_id, expr_id } => {
            tokens.extend([T_UNARY, u64::from(op_id.as_u32())]);
            return walk(data_store, expr_id, anchor_row, anchor_col, tokens, refs);
        }
        AstNodeData::BinaryOp {
            op_id,
            left_id,
            right_id,
        } => {
            tokens.extend([T_BINARY, u64::from(op_id.as_u32())]);
            return walk(data_store, left_id, anchor_row, anchor_col, tokens, refs)
                && walk(data_store, right_id, anchor_row, anchor_col, tokens, refs);
        }
        AstNodeData::Function { name_id, .. } => {
            let Some(args) = data_store.get_args(id) else {
                return false;
            };
            tokens.extend([T_FUNCTION, u64::from(name_id.as_u32()), args.len() as u64]);
            return args
                .iter()
                .all(|&arg| walk(data_store, arg, anchor_row, anchor_col, tokens, refs));
        }
        AstNodeData::Array { rows, cols, .. } => {
            let Some((_, _, elements)) = data_store.get_array_elems(id) else {
                return false;
            };
            tokens.extend([
                T_ARRAY,
                u64::from(rows),
                u64::from(cols),
                elements.len() as u64,
            ]);
            return elements
                .iter()
                .all(|&element| walk(data_store, element, anchor_row, anchor_col, tokens, refs));
        }
    }
    true
}

/// Pre-order identity keys of the reference nodes of a reconstructed tree, in
/// the same order as [`shape_tokens`] visits arena reference nodes. Named
/// references are keyed by their name buffer, because the dependency walk
/// hands `collect_reference` the name `&str` rather than the reference.
pub(crate) fn tree_reference_keys(ast: &formualizer_parse::parser::ASTNode, out: &mut Vec<usize>) {
    use formualizer_parse::parser::{ASTNodeType, ReferenceType};
    match &ast.node_type {
        ASTNodeType::Reference { reference, .. } => out.push(match reference {
            ReferenceType::NamedRange(name) => name.as_ptr() as usize,
            other => other as *const ReferenceType as usize,
        }),
        ASTNodeType::UnaryOp { expr, .. } => tree_reference_keys(expr, out),
        ASTNodeType::BinaryOp { left, right, .. } => {
            tree_reference_keys(left, out);
            tree_reference_keys(right, out);
        }
        ASTNodeType::Function { args, .. } => {
            for arg in args {
                tree_reference_keys(arg, out);
            }
        }
        ASTNodeType::Array(rows) => {
            for item in rows.iter().flatten() {
                tree_reference_keys(item, out);
            }
        }
        ASTNodeType::Call { callee, args } => {
            tree_reference_keys(callee, out);
            for arg in args {
                tree_reference_keys(arg, out);
            }
        }
        ASTNodeType::Literal(_) | ASTNodeType::Omitted => {}
    }
}

/// Identity key of the reference a dependency-walk callback received.
pub(crate) fn semantic_reference_key(
    reference: &crate::engine::refs::SemanticReference<'_>,
) -> Option<usize> {
    use crate::engine::refs::SemanticReference;
    match reference {
        SemanticReference::Cell(cell) => Some(cell.original as *const _ as usize),
        SemanticReference::FiniteRange(range) | SemanticReference::OpenRange(range) => {
            Some(range.original as *const _ as usize)
        }
        SemanticReference::Name(name) => Some(name.as_ptr() as usize),
        SemanticReference::Table(_)
        | SemanticReference::ExternalSource(_)
        | SemanticReference::ThreeDimensional(_)
        | SemanticReference::Unsupported(_) => None,
    }
}

/// Dependency-walk trace recorded on a specialization miss.
pub(crate) struct VisitTrace {
    index_by_key: FxHashMap<usize, u32>,
    pub(crate) visit: Vec<u32>,
    pub(crate) valid: bool,
}

impl VisitTrace {
    /// `None` if two reference nodes share an identity key (for example two
    /// empty name buffers), since the trace could then be ambiguous.
    pub(crate) fn new(keys: &[usize]) -> Option<Self> {
        let mut index_by_key = FxHashMap::default();
        for (index, key) in keys.iter().enumerate() {
            if index_by_key.insert(*key, index as u32).is_some() {
                return None;
            }
        }
        Some(Self {
            index_by_key,
            visit: Vec::new(),
            valid: true,
        })
    }

    pub(crate) fn record(&mut self, key: Option<usize>) {
        match key.and_then(|key| self.index_by_key.get(&key)) {
            Some(index) => self.visit.push(*index),
            None => self.valid = false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(memo: &mut ShapeMemo, tokens: &[u64]) -> ShapeLookup {
        memo.tokens.clear();
        memo.tokens.extend_from_slice(tokens);
        memo.lookup_shape()
    }

    fn validity() -> MemoValidity {
        MemoValidity {
            provider_revision: Some(0),
            registry_epoch: 1,
        }
    }

    // Adversarial full hash collisions: every shape lands in one bucket. The
    // bucket stops growing at MAX_BUCKET_SHAPES, each lookup compares at most
    // that many token slices, and colliding shapes past the bound bypass.
    #[test]
    fn colliding_shapes_are_bounded_per_lookup() {
        let mut memo = ShapeMemo {
            forced_hash: Some(7),
            ..ShapeMemo::default()
        };
        memo.revalidate(validity());
        let shapes = 1_000u64;
        let mut materialized = 0;
        let mut bypassed = 0;
        for round in 0..3 {
            for shape in 0..shapes {
                let before = memo.comparisons;
                match lookup(&mut memo, &[T_INT, shape, T_CELL, 0, 0, 1, 1]) {
                    ShapeLookup::Shape(_, true) => materialized += 1,
                    ShapeLookup::Shape(_, false) => {
                        assert!((1..=MAX_BUCKET_SHAPES as u64).contains(&shape))
                    }
                    ShapeLookup::FirstSighting => assert_eq!((round, shape), (0, 0)),
                    ShapeLookup::Full => bypassed += 1,
                }
                assert!(memo.comparisons - before <= MAX_BUCKET_SHAPES as u64);
            }
        }
        // The shared hash is "seen" after the first sighting, so the next
        // colliding shapes materialize until the bucket is full.
        assert_eq!(materialized, MAX_BUCKET_SHAPES);
        assert_eq!(memo.footprint().0, MAX_BUCKET_SHAPES);
        // Everything else bypassed: one first sighting, MAX materializations
        // and 2 * MAX hits in the later rounds.
        assert_eq!(bypassed, 3 * shapes as usize - 1 - 3 * MAX_BUCKET_SHAPES);
        assert!(memo.comparisons <= 3 * shapes * MAX_BUCKET_SHAPES as u64);
    }

    // Distinct shapes: shape count, first-sighting set and token budget stay
    // within their bounds however many shapes a pipeline sees.
    #[test]
    fn memory_per_pipeline_is_bounded() {
        let mut memo = ShapeMemo::default();
        memo.revalidate(validity());
        // Short shapes: the shape-count and first-sighting bounds bind.
        let distinct = (MAX_SEEN + MAX_SHAPES + 1_000) as u64;
        for shape in 0..distinct {
            for _ in 0..2 {
                if let ShapeLookup::Shape(index, _) = lookup(&mut memo, &[T_INT, shape]) {
                    memo.insert_specialization(index, 0, None);
                    memo.insert_specialization(index, 1, None);
                }
            }
        }
        let (shapes, specializations, seen, stored) = memo.footprint();
        assert_eq!(shapes, MAX_SHAPES);
        assert!(specializations <= MAX_SPECIALIZATIONS, "{specializations}");
        assert!(seen <= MAX_SEEN, "{seen}");
        assert!(stored <= MAX_STORED_TOKENS, "{stored}");
        assert!(matches!(
            lookup(&mut memo, &[T_INT, distinct + 1]),
            ShapeLookup::Full
        ));

        // Shapes seen once each: the first-sighting set is bounded.
        let mut memo = ShapeMemo::default();
        memo.revalidate(validity());
        let mut full = 0;
        for shape in 0..(MAX_SEEN + 100) as u64 {
            if matches!(lookup(&mut memo, &[T_INT, shape]), ShapeLookup::Full) {
                full += 1;
            }
        }
        assert_eq!(memo.footprint().2, MAX_SEEN);
        assert_eq!(full, 100);

        // Long shapes: the token budget binds before the shape count.
        let mut memo = ShapeMemo::default();
        memo.revalidate(validity());
        let long = MAX_SHAPE_TOKENS;
        let mut tokens = vec![T_EMPTY; long];
        for shape in 0..(2 * MAX_STORED_TOKENS / long) as u64 {
            tokens[0] = shape;
            for _ in 0..2 {
                if let ShapeLookup::Shape(index, true) = lookup(&mut memo, &tokens) {
                    memo.insert_specialization(index, 0, None);
                }
            }
        }
        let (shapes, _, _, stored) = memo.footprint();
        assert!(stored <= MAX_STORED_TOKENS, "{stored}");
        assert!(shapes < MAX_STORED_TOKENS / long, "{shapes}");

        // Over-long shapes are ineligible outright.
        let over = vec![T_EMPTY; MAX_SHAPE_TOKENS + 1];
        assert!(matches!(lookup(&mut memo, &over), ShapeLookup::Full));

        // Revalidation with a new token releases everything.
        memo.revalidate(MemoValidity {
            registry_epoch: 2,
            ..validity()
        });
        assert_eq!(memo.footprint(), (0, 0, 0, 0));
    }
}
