use std::collections::BTreeMap;
use std::sync::Arc;

use crate::formula_plane::placement::PreparedAnchorOncePlacement;

/// Zero-based source coordinate retained during workbook ingest.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceCoord {
    pub row: u32,
    pub col: u32,
}

/// Inclusive, zero-based source rectangle.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SourceRect {
    pub start: SourceCoord,
    pub end: SourceCoord,
}

/// Worksheet-local identity for a source-declared shared formula.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceFamilyId {
    pub sheet_instance: u32,
    pub source_index: usize,
}

/// Maximum amount of coordinate evidence accepted by the generic transport.
/// Larger or sparse families must stay behind backend-owned exact replay.
#[doc(hidden)]
pub const MAX_EXPLICIT_SOURCE_FAMILY_MEMBERS: usize = 4_096;

/// Backend-neutral transport for a proven complete family domain.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementDomainTransport {
    RowRun {
        row_start: u32,
        row_end: u32,
        col: u32,
    },
    ColRun {
        row: u32,
        col_start: u32,
        col_end: u32,
    },
    Rect(SourceRect),
}

impl PlacementDomainTransport {
    pub fn rect(self) -> SourceRect {
        match self {
            Self::RowRun {
                row_start,
                row_end,
                col,
            } => SourceRect {
                start: SourceCoord {
                    row: row_start,
                    col,
                },
                end: SourceCoord { row: row_end, col },
            },
            Self::ColRun {
                row,
                col_start,
                col_end,
            } => SourceRect {
                start: SourceCoord {
                    row,
                    col: col_start,
                },
                end: SourceCoord { row, col: col_end },
            },
            Self::Rect(rect) => rect,
        }
    }
}

/// A structurally bounded exact member list for less-specialized sources.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExplicitSourceFamilyMembers {
    members: Box<[SourceCoord]>,
}

impl ExplicitSourceFamilyMembers {
    pub fn try_new(members: Vec<SourceCoord>) -> Result<Self, &'static str> {
        if members.len() > MAX_EXPLICIT_SOURCE_FAMILY_MEMBERS {
            return Err("ExplicitMemberLimitExceeded");
        }
        Ok(Self {
            members: members.into_boxed_slice(),
        })
    }

    pub fn as_slice(&self) -> &[SourceCoord] {
        &self.members
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

impl TryFrom<Vec<SourceCoord>> for ExplicitSourceFamilyMembers {
    type Error = &'static str;

    fn try_from(members: Vec<SourceCoord>) -> Result<Self, Self::Error> {
        Self::try_new(members)
    }
}

/// Source evidence for either a proven complete domain or a bounded exact list.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceFamilyMembers {
    CompleteDomain(PlacementDomainTransport),
    ExplicitMembers(ExplicitSourceFamilyMembers),
}

/// Backend-neutral source-family candidate. Source identity is intentionally
/// opaque to the engine and is retained for replay skip sets and invalidation.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFormulaFamily {
    pub source_id: SourceFamilyId,
    pub anchor_coord0: SourceCoord,
    pub anchor_text: Arc<str>,
    pub members: SourceFamilyMembers,
    pub member_count: u64,
}

impl SourceFormulaFamily {
    pub(crate) fn validated_complete_domain(
        &self,
        limits: &super::WorkbookLoadLimits,
    ) -> Result<PlacementDomainTransport, &'static str> {
        if !source_coord_in_bounds(self.anchor_coord0, limits) {
            return Err("SourceAnchorOutOfBounds");
        }
        match &self.members {
            SourceFamilyMembers::CompleteDomain(domain) => {
                let rect = domain.rect();
                if !source_rect_valid(rect, limits) {
                    return Err("CompleteDomainOutOfBounds");
                }
                let rows = u64::from(rect.end.row - rect.start.row) + 1;
                let cols = u64::from(rect.end.col - rect.start.col) + 1;
                let area = rows.saturating_mul(cols);
                if area > limits.max_sheet_logical_cells {
                    return Err("CompleteDomainLogicalCellLimitExceeded");
                }
                if self.member_count != area || self.anchor_coord0 != rect.start {
                    return Err("CompleteDomainMemberMismatch");
                }
                Ok(*domain)
            }
            SourceFamilyMembers::ExplicitMembers(members) => {
                validate_explicit_members(
                    self.anchor_coord0,
                    self.member_count,
                    members.as_slice(),
                    limits,
                )?;
                Err("ExplicitMembersRequireExactRecords")
            }
        }
    }
}

fn source_coord_in_bounds(coord: SourceCoord, limits: &super::WorkbookLoadLimits) -> bool {
    coord.row < limits.max_sheet_rows && coord.col < limits.max_sheet_cols
}

fn source_rect_valid(rect: SourceRect, limits: &super::WorkbookLoadLimits) -> bool {
    source_coord_in_bounds(rect.start, limits)
        && source_coord_in_bounds(rect.end, limits)
        && rect.start.row <= rect.end.row
        && rect.start.col <= rect.end.col
}

fn validate_explicit_members(
    anchor: SourceCoord,
    member_count: u64,
    members: &[SourceCoord],
    limits: &super::WorkbookLoadLimits,
) -> Result<(), &'static str> {
    if member_count != members.len() as u64 {
        return Err("ExplicitMemberCountMismatch");
    }
    if member_count > limits.max_sheet_logical_cells {
        return Err("ExplicitMemberLogicalCellLimitExceeded");
    }
    if members
        .iter()
        .any(|coord| !source_coord_in_bounds(*coord, limits))
    {
        return Err("ExplicitMemberOutOfBounds");
    }
    let unique: std::collections::BTreeSet<_> = members.iter().copied().collect();
    if unique.len() != members.len() {
        return Err("DuplicateExplicitMember");
    }
    if !unique.contains(&anchor) {
        return Err("ExplicitMembersMissingAnchor");
    }
    Ok(())
}

/// Counters and replay-only disposition produced by a compressed source-family
/// evidence collector. Exact descendant records remain backend-owned.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FormulaCompressedSourceReport {
    pub source_formula_events: u64,
    pub source_formula_records_spooled: u64,
    pub source_spool_encoded_bytes: u64,
    pub source_spool_peak_memory_bytes: u64,
    pub source_spool_spilled_bytes: u64,
    pub source_spool_replays: u64,
    pub source_ordinary_events: u64,
    pub source_shared_anchor_events: u64,
    pub source_shared_descendant_events: u64,
    pub source_unknown_events: u64,
    pub families_seen: u64,
    pub family_cells_seen: u64,
    pub source_clean_families: u64,
    pub source_clean_cells: u64,
    pub source_fragmentable_families: u64,
    pub source_fragmentable_cells: u64,
    pub source_fragment_count: u64,
    pub source_isolated_fallback_cells: u64,
    pub source_hole_exclusions: u64,
    pub source_ordinary_exclusions: u64,
    pub source_partition_failures: u64,
    pub replay_families: u64,
    pub replay_cells: u64,
    pub forward_descendants: u64,
    pub evidence_limit_fallbacks: u64,
    pub evidence_peak_bytes: u64,
    pub fallback_reasons: BTreeMap<String, u64>,
}

/// One formula produced while consuming a deferred source package. The
/// backend-specific replay authority stays behind `DeferredFormulaReplay`.
#[doc(hidden)]
#[derive(Debug)]
pub struct DeferredReplayFormula {
    pub row: u32,
    pub col: u32,
    pub text: String,
    pub family: Option<SourceFamilyId>,
}

/// Backend-owned, single-owner replay authority used by deferred workbook
/// loading. This deliberately has no clone/snapshot operation.
#[doc(hidden)]
pub trait DeferredFormulaReplay: Send {
    fn replay(
        &mut self,
        skip_families: &std::collections::BTreeSet<SourceFamilyId>,
        suppressed: &std::collections::BTreeSet<(u32, u32)>,
    ) -> Result<Vec<DeferredReplayFormula>, String>;

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String>;
}

/// Sealed workbook-to-engine package. It owns the source spool and compressed
/// family evidence until exactly one selected deferred build consumes it.
#[doc(hidden)]
pub struct DeferredFormulaPackage {
    pub(crate) sheet_name: String,
    pub(crate) report: FormulaCompressedSourceReport,
    pub(crate) families: Vec<SourceFormulaFamily>,
    pub(crate) replay: Arc<std::sync::Mutex<Box<dyn DeferredFormulaReplay>>>,
    pub(crate) invalidated: std::collections::BTreeSet<SourceFamilyId>,
    pub(crate) suppressed: std::collections::BTreeSet<(u32, u32)>,
}

impl DeferredFormulaPackage {
    #[doc(hidden)]
    pub fn new(
        sheet_name: String,
        report: FormulaCompressedSourceReport,
        families: Vec<SourceFormulaFamily>,
        replay: Box<dyn DeferredFormulaReplay>,
    ) -> Self {
        Self {
            sheet_name,
            report,
            families,
            replay: Arc::new(std::sync::Mutex::new(replay)),
            invalidated: Default::default(),
            suppressed: Default::default(),
        }
    }
}

/// Additive replay/per-cell transport for compressed source evidence.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct FormulaCompressedSourceBatch {
    sheet_name: Arc<str>,
    report: FormulaCompressedSourceReport,
    families: Vec<SourceFormulaFamily>,
}

/// Opaque eager preparation owned by Engine between adapter classification and replay.
/// The adapter can inspect dispositions but cannot commit FormulaPlane authority.
#[doc(hidden)]
pub struct FormulaCompressedPreparation {
    pub(crate) engine_token: Arc<()>,
    pub(crate) function_semantic_epoch: u64,
    pub(crate) function_semantics_used: bool,
    pub(crate) sheet_name: Arc<str>,
    pub(crate) prepared: Vec<(SourceFamilyId, PreparedAnchorOncePlacement)>,
    pub(crate) rejected: BTreeMap<SourceFamilyId, String>,
    pub(crate) exact_replay: Option<Arc<std::sync::Mutex<Box<dyn DeferredFormulaReplay>>>>,
    pub(crate) replay_suppressed: std::collections::BTreeSet<(u32, u32)>,
}

impl FormulaCompressedPreparation {
    #[doc(hidden)]
    pub fn with_exact_replay(
        mut self,
        replay: Arc<std::sync::Mutex<Box<dyn DeferredFormulaReplay>>>,
        suppressed: std::collections::BTreeSet<(u32, u32)>,
    ) -> Self {
        self.exact_replay = Some(replay);
        self.replay_suppressed = suppressed;
        self
    }

    pub fn is_direct(&self, family: SourceFamilyId) -> bool {
        self.prepared.iter().any(|(id, _)| *id == family)
    }

    pub fn direct_family_count(&self) -> usize {
        self.prepared.len()
    }

    pub fn direct_cell_count(&self) -> u64 {
        self.prepared
            .iter()
            .map(|(_, prepared)| prepared.member_count)
            .sum()
    }
}

impl FormulaCompressedSourceBatch {
    pub fn new(sheet_name: impl Into<Arc<str>>, report: FormulaCompressedSourceReport) -> Self {
        Self {
            sheet_name: sheet_name.into(),
            report,
            families: Vec::new(),
        }
    }

    pub fn with_families(
        sheet_name: impl Into<Arc<str>>,
        report: FormulaCompressedSourceReport,
        families: Vec<SourceFormulaFamily>,
    ) -> Self {
        Self {
            sheet_name: sheet_name.into(),
            report,
            families,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        Arc<str>,
        FormulaCompressedSourceReport,
        Vec<SourceFormulaFamily>,
    ) {
        (self.sheet_name, self.report, self.families)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> super::super::WorkbookLoadLimits {
        super::super::WorkbookLoadLimits {
            max_sheet_rows: 20,
            max_sheet_cols: 20,
            max_sheet_logical_cells: 100,
            ..super::super::WorkbookLoadLimits::default()
        }
    }

    fn explicit(members: Vec<SourceCoord>) -> SourceFamilyMembers {
        SourceFamilyMembers::ExplicitMembers(
            ExplicitSourceFamilyMembers::try_new(members).expect("bounded members"),
        )
    }

    fn family(members: SourceFamilyMembers, member_count: u64) -> SourceFormulaFamily {
        SourceFormulaFamily {
            source_id: SourceFamilyId {
                sheet_instance: 7,
                source_index: 11,
            },
            anchor_coord0: SourceCoord { row: 2, col: 3 },
            anchor_text: Arc::from("A1+1"),
            members,
            member_count,
        }
    }

    #[test]
    fn complete_row_column_and_rectangle_domains_validate() {
        let row = family(
            SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 2,
                row_end: 4,
                col: 3,
            }),
            3,
        );
        let column = family(
            SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::ColRun {
                row: 2,
                col_start: 3,
                col_end: 6,
            }),
            4,
        );
        let rectangle = family(
            SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::Rect(SourceRect {
                start: SourceCoord { row: 2, col: 3 },
                end: SourceCoord { row: 4, col: 5 },
            })),
            9,
        );

        assert!(row.validated_complete_domain(&limits()).is_ok());
        assert!(column.validated_complete_domain(&limits()).is_ok());
        assert!(rectangle.validated_complete_domain(&limits()).is_ok());
    }

    #[test]
    fn explicit_members_are_bounded_validated_and_never_become_a_domain() {
        let valid = family(
            explicit(vec![
                SourceCoord { row: 2, col: 3 },
                SourceCoord { row: 3, col: 3 },
            ]),
            2,
        );
        assert_eq!(
            valid.validated_complete_domain(&limits()),
            Err("ExplicitMembersRequireExactRecords")
        );

        let duplicate = family(
            explicit(vec![
                SourceCoord { row: 2, col: 3 },
                SourceCoord { row: 2, col: 3 },
            ]),
            2,
        );
        assert_eq!(
            duplicate.validated_complete_domain(&limits()),
            Err("DuplicateExplicitMember")
        );

        let out_of_bounds = family(
            explicit(vec![
                SourceCoord { row: 2, col: 3 },
                SourceCoord { row: 20, col: 3 },
            ]),
            2,
        );
        assert_eq!(
            out_of_bounds.validated_complete_domain(&limits()),
            Err("ExplicitMemberOutOfBounds")
        );

        assert_eq!(
            ExplicitSourceFamilyMembers::try_new(vec![
                SourceCoord { row: 2, col: 3 };
                MAX_EXPLICIT_SOURCE_FAMILY_MEMBERS + 1
            ]),
            Err("ExplicitMemberLimitExceeded")
        );
    }

    #[test]
    fn complete_domains_fail_closed_on_mismatch_and_bounds() {
        let wrong_count = family(
            SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::RowRun {
                row_start: 2,
                row_end: 4,
                col: 3,
            }),
            2,
        );
        assert_eq!(
            wrong_count.validated_complete_domain(&limits()),
            Err("CompleteDomainMemberMismatch")
        );

        let out_of_bounds = family(
            SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::ColRun {
                row: 2,
                col_start: 3,
                col_end: 20,
            }),
            18,
        );
        assert_eq!(
            out_of_bounds.validated_complete_domain(&limits()),
            Err("CompleteDomainOutOfBounds")
        );

        let over_logical_limit = family(
            SourceFamilyMembers::CompleteDomain(PlacementDomainTransport::Rect(SourceRect {
                start: SourceCoord { row: 2, col: 3 },
                end: SourceCoord { row: 12, col: 12 },
            })),
            110,
        );
        assert_eq!(
            over_logical_limit.validated_complete_domain(&limits()),
            Err("CompleteDomainLogicalCellLimitExceeded")
        );
    }
}
