//! Validated output projection (FORM211-C).
//!
//! Every source formula is read once at the final engine state through
//! `inspect_cell_result` and classified as an ordinary scalar or as a
//! dynamic-array anchor in one of four shapes. Member values of a multi-cell
//! spill are read later, one at a time, by the worksheet geometry pass, after
//! the generated-cell budget was checked; nothing here materializes a spill
//! rectangle.
use super::dynamic_metadata::DynamicAnchor;
use super::sheet::{Cell, SourceRect};
use super::{Cache, IoError, unsupported, validated_result};
use crate::workbook::WBResolver;
use formualizer_common::{CellAddress, DateSystem, LiteralValue};
use formualizer_eval::engine::Engine;
use formualizer_eval::engine::inspect::SpillRole;

/// The current result shape of a dynamic-array anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    /// A successful multi-cell spill with this extent (anchor top-left).
    Spill(SourceRect),
    /// A current non-error scalar: the spill collapsed to one cell.
    Collapsed,
    /// A typed `#SPILL!` result: no generated children.
    Blocked,
    /// Any other typed error result.
    Error,
}

/// One projected anchor: an admitted (source-declared) anchor in any shape,
/// or an ordinary formula whose result is a new multi-cell spill.
#[derive(Debug)]
pub(super) struct ProjectedAnchor {
    pub row: u32,
    pub col: u32,
    /// Index into `SheetPlan::cells`.
    pub formula: usize,
    /// The admitted source declaration; `None` for a new anchor.
    pub prior: Option<DynamicAnchor>,
    pub shape: Shape,
    /// The anchor's own cache: the top-left member, the scalar or the error.
    pub cache: Cache,
}

#[derive(Debug)]
pub(super) enum Projection {
    /// An ordinary formula with a current scalar (or 1x1) result.
    Scalar(Cache),
    Anchor(ProjectedAnchor),
}

fn is_multi_cell(extent: &formualizer_common::RangeAddress) -> bool {
    extent.start_row != extent.end_row || extent.start_col != extent.end_col
}

/// Project one source formula cell. `record` sees the validated anchor or
/// scalar value (summary/error accounting) before cache encoding, exactly
/// as on the scalar path. Currentness, coerced-parse policy and finite and
/// error-token encodability are enforced as on the scalar path.
#[allow(clippy::too_many_arguments)]
pub(super) fn project_formula(
    engine: &Engine<WBResolver>,
    sheet: &str,
    cell: &Cell,
    formula: usize,
    prior: Option<&DynamicAnchor>,
    coerced: bool,
    date_system: DateSystem,
    record: &mut dyn FnMut(&LiteralValue),
) -> Result<Projection, IoError> {
    let address = CellAddress::new(sheet, cell.row, cell.col)
        .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
    let snapshot = engine
        .inspect_cell_result(&address)
        .map_err(|e| IoError::from_backend("xlsx-inspect", e))?;
    let spill = match &snapshot.spill {
        None => None,
        Some(SpillRole::Anchor { extent }) if !is_multi_cell(extent) => None,
        Some(SpillRole::Anchor { extent }) => {
            if extent.sheet != snapshot.address.sheet
                || (extent.start_row, extent.start_col) != (cell.row, cell.col)
            {
                return Err(unsupported(
                    "dynamic spill extent does not start at its anchor",
                    sheet,
                ));
            }
            if prior.is_none() && cell.formula_kind == "shared" {
                // Promoting a source shared-family member to an array anchor
                // needs de-sharing authority that this writer does not have.
                return Err(unsupported(
                    "multi-cell dynamic spill from a shared formula family member",
                    sheet,
                ));
            }
            Some(SourceRect {
                first_row: extent.start_row,
                first_col: extent.start_col,
                last_row: extent.end_row,
                last_col: extent.end_col,
            })
        }
        Some(_) => {
            return Err(unsupported("materialized multi-cell dynamic spill", sheet));
        }
    };
    let value = validated_result(
        snapshot.value,
        snapshot.has_formula,
        snapshot.staleness,
        coerced,
        sheet,
    )?;
    record(&value);
    let shape = if let Some(prior) = prior.filter(|a| a.binding.is_none()) {
        if prior.footprint.cell_count() != Some(1) && spill != Some(prior.footprint) {
            return Err(unsupported(
                "fixed array result did not retain its extent",
                sheet,
            ));
        }
        Shape::Spill(prior.footprint)
    } else {
        match (spill, &value) {
            (Some(extent), _) => Shape::Spill(extent),
            (None, LiteralValue::Error(e))
                if e.kind == formualizer_common::ExcelErrorKind::Spill =>
            {
                Shape::Blocked
            }
            (None, LiteralValue::Error(_)) => Shape::Error,
            (None, _) => Shape::Collapsed,
        }
    };
    let cache = Cache::from_value(value, date_system)?;
    if prior.is_none() && !matches!(shape, Shape::Spill(_)) {
        // Fresh unmarked 1x1/blocked/error provenance is deferred (plan
        // §1.4): an ordinary formula without a multi-cell spill is a scalar.
        return Ok(Projection::Scalar(cache));
    }
    Ok(Projection::Anchor(ProjectedAnchor {
        row: cell.row,
        col: cell.col,
        formula,
        prior: prior.copied(),
        shape,
        cache,
    }))
}

/// Read one generated member of `anchor`'s current spill at the final
/// engine state. The member must be owned by exactly that anchor and must
/// not be a formula. An empty element encodes as `Cache::Empty`.
pub(super) fn member(
    engine: &Engine<WBResolver>,
    sheet: &str,
    anchor: (u32, u32),
    row: u32,
    col: u32,
    date_system: DateSystem,
) -> Result<Cache, IoError> {
    let address = CellAddress::new(sheet, row, col)
        .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
    let snapshot = engine
        .inspect_cell_result(&address)
        .map_err(|e| IoError::from_backend("xlsx-inspect", e))?;
    let owned = matches!(
        &snapshot.spill,
        Some(SpillRole::Member { anchor: a })
            if a.sheet == snapshot.address.sheet && (a.row, a.column) == anchor
    );
    if !owned || snapshot.has_formula {
        return Err(unsupported(
            "dynamic spill member is not owned by its anchor",
            sheet,
        ));
    }
    Cache::from_value(snapshot.value.unwrap_or(LiteralValue::Empty), date_system)
}
