use std::collections::{BTreeMap, BTreeSet};
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

/// Opaque workbook-backend source ordering proof. The evaluator compares
/// tokens but does not derive them from coordinates or source-family ids.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceFormulaOrder(u64);

impl SourceFormulaOrder {
    pub fn new(sequence: u64) -> Self {
        Self(sequence)
    }
}

/// Maximum amount of coordinate evidence accepted by the generic transport.
/// Larger or sparse families must stay behind backend-owned exact replay.
#[doc(hidden)]
pub const MAX_EXPLICIT_SOURCE_FAMILY_MEMBERS: usize = 4_096;
#[doc(hidden)]
pub const MAX_PARTITIONED_SOURCE_FAMILY_FRAGMENTS: usize = 128;

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

/// Legacy graph ownership for one coordinate excluded from direct fragmented
/// authority. Shared members replay through the source-family template;
/// ordinary exceptions retain their independent source formula while joining
/// the family's atomic ingest disposition.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionLegacyMemberKind {
    SharedFamilyMember,
    OrdinaryException,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionLegacyMember {
    pub coord: SourceCoord,
    pub kind: PartitionLegacyMemberKind,
}

/// A bounded, sorted exact list of legacy-owned fragmented-family formulas.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExplicitPartitionLegacyMembers {
    members: Box<[PartitionLegacyMember]>,
}

impl ExplicitPartitionLegacyMembers {
    pub fn try_new(mut members: Vec<PartitionLegacyMember>) -> Result<Self, &'static str> {
        if members.len() > MAX_EXPLICIT_SOURCE_FAMILY_MEMBERS {
            return Err("ExplicitMemberLimitExceeded");
        }
        members.sort_unstable_by_key(|member| member.coord);
        if members
            .windows(2)
            .any(|window| window[0].coord == window[1].coord)
        {
            return Err("PartitionLegacyMembersDuplicate");
        }
        Ok(Self {
            members: members.into_boxed_slice(),
        })
    }

    pub fn as_slice(&self) -> &[PartitionLegacyMember] {
        &self.members
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn shared_member_count(&self) -> usize {
        self.members
            .iter()
            .filter(|member| member.kind == PartitionLegacyMemberKind::SharedFamilyMember)
            .count()
    }

    pub fn ordinary_exception_count(&self) -> usize {
        self.members
            .iter()
            .filter(|member| member.kind == PartitionLegacyMemberKind::OrdinaryException)
            .count()
    }
}

impl TryFrom<Vec<PartitionLegacyMember>> for ExplicitPartitionLegacyMembers {
    type Error = &'static str;

    fn try_from(members: Vec<PartitionLegacyMember>) -> Result<Self, Self::Error> {
        Self::try_new(members)
    }
}

/// Formula counts needed to prove that the compact fragmented disposition
/// covers its declared source rectangle exactly.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartitionReconciliation {
    pub shared_members: u64,
    pub ordinary_exceptions: u64,
    pub holes: u64,
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
    pub source_order: SourceFormulaOrder,
    pub anchor_coord0: SourceCoord,
    pub anchor_text: Arc<str>,
    pub members: SourceFamilyMembers,
    pub member_count: u64,
}

/// Backend-neutral capability proposal for one source template partitioned
/// across existing placement domains. Backend-specific evidence remains
/// private, while every formula excluded from direct authority has an explicit
/// legacy owner and the declared rectangle has an exact count reconciliation.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionedSourceFormulaFamily {
    pub source_id: SourceFamilyId,
    pub source_order: SourceFormulaOrder,
    pub template_origin0: SourceCoord,
    pub template_text: Arc<str>,
    pub declared: SourceRect,
    pub surviving_member_count: u64,
    pub fragments: Vec<PlacementDomainTransport>,
    pub legacy_members: ExplicitPartitionLegacyMembers,
    pub reconciliation: PartitionReconciliation,
}

impl PartitionedSourceFormulaFamily {
    pub(crate) fn reconciles_compact_geometry(&self) -> bool {
        let mut direct_cells = 0u64;
        let mut rects = Vec::with_capacity(self.fragments.len());
        for fragment in &self.fragments {
            let rect = fragment.rect();
            if rect.start.row > rect.end.row
                || rect.start.col > rect.end.col
                || !source_rect_contains(self.declared, rect.start)
                || !source_rect_contains(self.declared, rect.end)
                || rects.iter().any(|prior| source_rects_overlap(*prior, rect))
            {
                return false;
            }
            let Some(area) = source_rect_area(rect) else {
                return false;
            };
            let Some(next) = direct_cells.checked_add(area) else {
                return false;
            };
            direct_cells = next;
            rects.push(rect);
        }
        if self.legacy_members.as_slice().iter().any(|member| {
            !source_rect_contains(self.declared, member.coord)
                || rects
                    .iter()
                    .any(|rect| source_rect_contains(*rect, member.coord))
        }) {
            return false;
        }
        let shared_legacy = self.legacy_members.shared_member_count() as u64;
        let ordinary = self.legacy_members.ordinary_exception_count() as u64;
        let Some(shared) = direct_cells.checked_add(shared_legacy) else {
            return false;
        };
        let Some(accounted) = shared
            .checked_add(ordinary)
            .and_then(|count| count.checked_add(self.reconciliation.holes))
        else {
            return false;
        };
        source_rect_area(self.declared) == Some(accounted)
            && shared == self.surviving_member_count
            && self.reconciliation.shared_members == shared
            && self.reconciliation.ordinary_exceptions == ordinary
    }

    pub(crate) fn validate(&self, limits: &super::WorkbookLoadLimits) -> Result<(), &'static str> {
        if !source_coord_in_bounds(self.template_origin0, limits) {
            return Err("PartitionTemplateOriginOutOfBounds");
        }
        if !source_rect_valid(self.declared, limits) {
            return Err("PartitionDeclaredRangeOutOfBounds");
        }
        if self.fragments.is_empty() {
            return Err("PartitionHasNoFragments");
        }
        if self.fragments.len() > MAX_PARTITIONED_SOURCE_FAMILY_FRAGMENTS {
            return Err("PartitionFragmentLimitExceeded");
        }
        let mut cells = 0u64;
        let mut rects = Vec::with_capacity(self.fragments.len());
        for domain in &self.fragments {
            let rect = domain.rect();
            if !source_rect_valid(rect, limits) {
                return Err("PartitionFragmentOutOfBounds");
            }
            if !source_rect_contains(self.declared, rect.start)
                || !source_rect_contains(self.declared, rect.end)
            {
                return Err("PartitionFragmentOutsideDeclaredRange");
            }
            if rects.iter().any(|prior| source_rects_overlap(*prior, rect)) {
                return Err("PartitionFragmentsOverlap");
            }
            let area = source_rect_area(rect).ok_or("PartitionAreaOverflow")?;
            cells = cells.checked_add(area).ok_or("PartitionAreaOverflow")?;
            rects.push(rect);
        }
        validate_partition_legacy_members(self.legacy_members.as_slice(), limits)?;
        for member in self.legacy_members.as_slice() {
            if !source_rect_contains(self.declared, member.coord) {
                return Err("PartitionLegacyMemberOutsideDeclaredRange");
            }
            if rects
                .iter()
                .any(|rect| source_rect_contains(*rect, member.coord))
            {
                return Err("PartitionLegacyMemberOverlapsFragment");
            }
        }
        let shared_legacy = u64::try_from(self.legacy_members.shared_member_count())
            .map_err(|_| "PartitionAreaOverflow")?;
        let ordinary_exceptions = u64::try_from(self.legacy_members.ordinary_exception_count())
            .map_err(|_| "PartitionAreaOverflow")?;
        cells = cells
            .checked_add(shared_legacy)
            .ok_or("PartitionAreaOverflow")?;
        if cells != self.surviving_member_count
            || self.reconciliation.shared_members != self.surviving_member_count
            || self.reconciliation.ordinary_exceptions != ordinary_exceptions
        {
            return Err("PartitionMemberCountMismatch");
        }
        let declared_area = source_rect_area(self.declared).ok_or("PartitionAreaOverflow")?;
        let accounted = self
            .reconciliation
            .shared_members
            .checked_add(self.reconciliation.ordinary_exceptions)
            .and_then(|count| count.checked_add(self.reconciliation.holes))
            .ok_or("PartitionAreaOverflow")?;
        if accounted != declared_area {
            return Err("PartitionReconciliationMismatch");
        }
        if !rects
            .iter()
            .any(|rect| source_rect_contains(*rect, self.template_origin0))
            && !self.legacy_members.as_slice().iter().any(|member| {
                member.coord == self.template_origin0
                    && member.kind == PartitionLegacyMemberKind::SharedFamilyMember
            })
        {
            return Err("PartitionMissingTemplateOrigin");
        }
        if declared_area > limits.max_sheet_logical_cells {
            return Err("PartitionLogicalCellLimitExceeded");
        }
        Ok(())
    }
}

/// Replay ownership for a source coordinate. `Direct` means FormulaPlane owns
/// the shared record, while both legacy variants are emitted to the graph.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FormulaReplayCoordinateDisposition {
    Direct,
    LegacyShared,
    LegacyOrdinary,
    Suppressed,
}

/// Bounded replay routing for eager/deferred source packages. Family defaults
/// avoid expanding direct domains into per-cell state; only bounded legacy
/// points and ordinary-exception ownership are retained explicitly.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FormulaReplayDisposition {
    family_defaults: BTreeMap<SourceFamilyId, FormulaReplayCoordinateDisposition>,
    shared_overrides: BTreeMap<(SourceFamilyId, SourceCoord), FormulaReplayCoordinateDisposition>,
    ordinary_owners: BTreeMap<SourceCoord, SourceFamilyId>,
    suppressed: BTreeSet<(u32, u32)>,
}

impl FormulaReplayDisposition {
    pub fn set_family_direct(&mut self, family: SourceFamilyId) {
        self.family_defaults
            .insert(family, FormulaReplayCoordinateDisposition::Direct);
    }

    pub fn set_family_legacy(&mut self, family: SourceFamilyId) {
        self.family_defaults
            .insert(family, FormulaReplayCoordinateDisposition::LegacyShared);
    }

    pub fn register_partition(
        &mut self,
        family: &PartitionedSourceFormulaFamily,
        direct: bool,
    ) -> Result<(), &'static str> {
        if family.legacy_members.as_slice().iter().any(|member| {
            member.kind == PartitionLegacyMemberKind::OrdinaryException
                && self
                    .ordinary_owners
                    .get(&member.coord)
                    .is_some_and(|owner| *owner != family.source_id)
        }) {
            return Err("PartitionOrdinaryOwnerConflict");
        }
        if direct {
            self.set_family_direct(family.source_id);
        } else {
            self.set_family_legacy(family.source_id);
        }
        for member in family.legacy_members.as_slice() {
            match member.kind {
                PartitionLegacyMemberKind::SharedFamilyMember => {
                    self.shared_overrides.insert(
                        (family.source_id, member.coord),
                        FormulaReplayCoordinateDisposition::LegacyShared,
                    );
                }
                PartitionLegacyMemberKind::OrdinaryException => {
                    self.ordinary_owners.insert(member.coord, family.source_id);
                }
            }
        }
        Ok(())
    }

    pub fn extend_suppressed_excel_coords(
        &mut self,
        coordinates: impl IntoIterator<Item = (u32, u32)>,
    ) {
        self.suppressed.extend(coordinates);
    }

    pub fn shared_disposition(
        &self,
        family: SourceFamilyId,
        coord: SourceCoord,
    ) -> FormulaReplayCoordinateDisposition {
        if self
            .suppressed
            .contains(&(coord.row.saturating_add(1), coord.col.saturating_add(1)))
        {
            return FormulaReplayCoordinateDisposition::Suppressed;
        }
        self.shared_overrides
            .get(&(family, coord))
            .copied()
            .or_else(|| self.family_defaults.get(&family).copied())
            .unwrap_or(FormulaReplayCoordinateDisposition::LegacyShared)
    }

    pub fn ordinary_disposition(
        &self,
        coord: SourceCoord,
    ) -> (FormulaReplayCoordinateDisposition, Option<SourceFamilyId>) {
        if self
            .suppressed
            .contains(&(coord.row.saturating_add(1), coord.col.saturating_add(1)))
        {
            return (FormulaReplayCoordinateDisposition::Suppressed, None);
        }
        (
            FormulaReplayCoordinateDisposition::LegacyOrdinary,
            self.ordinary_owners.get(&coord).copied(),
        )
    }

    pub fn force_family_legacy(&mut self, family: SourceFamilyId) {
        self.set_family_legacy(family);
    }

    pub(crate) fn owns_partition_exactly(&self, family: &PartitionedSourceFormulaFamily) -> bool {
        if self.family_defaults.get(&family.source_id)
            != Some(&FormulaReplayCoordinateDisposition::Direct)
        {
            return false;
        }
        let expected_shared: BTreeSet<_> = family
            .legacy_members
            .as_slice()
            .iter()
            .filter(|member| member.kind == PartitionLegacyMemberKind::SharedFamilyMember)
            .map(|member| member.coord)
            .collect();
        let actual_shared: BTreeSet<_> = self
            .shared_overrides
            .iter()
            .filter(|((id, _), _)| *id == family.source_id)
            .map(|((_, coord), disposition)| (*coord, *disposition))
            .collect();
        let expected_shared: BTreeSet<_> = expected_shared
            .into_iter()
            .map(|coord| (coord, FormulaReplayCoordinateDisposition::LegacyShared))
            .collect();
        if actual_shared != expected_shared {
            return false;
        }
        let expected_ordinary: BTreeSet<_> = family
            .legacy_members
            .as_slice()
            .iter()
            .filter(|member| member.kind == PartitionLegacyMemberKind::OrdinaryException)
            .map(|member| member.coord)
            .collect();
        let actual_ordinary: BTreeSet<_> = self
            .ordinary_owners
            .iter()
            .filter(|(_, id)| **id == family.source_id)
            .map(|(coord, _)| *coord)
            .collect();
        if actual_ordinary != expected_ordinary {
            return false;
        }
        !self.suppressed.iter().any(|&(row, col)| {
            let Some(coord) = row
                .checked_sub(1)
                .zip(col.checked_sub(1))
                .map(|(row, col)| SourceCoord { row, col })
            else {
                return true;
            };
            family
                .fragments
                .iter()
                .any(|fragment| source_rect_contains(fragment.rect(), coord))
                || family
                    .legacy_members
                    .as_slice()
                    .iter()
                    .any(|member| member.coord == coord)
        })
    }

    #[doc(hidden)]
    pub fn partition_disposition(
        &self,
        family: &PartitionedSourceFormulaFamily,
        coord: SourceCoord,
    ) -> Option<FormulaReplayCoordinateDisposition> {
        if !source_rect_contains(family.declared, coord) {
            return None;
        }
        if let Some(member) = family
            .legacy_members
            .as_slice()
            .iter()
            .find(|member| member.coord == coord)
        {
            return match member.kind {
                PartitionLegacyMemberKind::SharedFamilyMember => {
                    Some(self.shared_disposition(family.source_id, coord))
                }
                PartitionLegacyMemberKind::OrdinaryException => {
                    let (disposition, owner) = self.ordinary_disposition(coord);
                    (owner == Some(family.source_id)).then_some(disposition)
                }
            };
        }
        family
            .fragments
            .iter()
            .any(|fragment| source_rect_contains(fragment.rect(), coord))
            .then(|| self.shared_disposition(family.source_id, coord))
    }
}

/// Transaction-local point router over partition domains. It indexes the
/// existing rectangular transports without introducing another authority or
/// persistence format.
#[doc(hidden)]
#[derive(Debug)]
pub struct FormulaReplayPartitionRouter {
    families: BTreeMap<SourceFamilyId, FormulaReplayFamilyRouter>,
}

#[derive(Debug)]
struct FormulaReplayFamilyRouter {
    declared: SourceRect,
    legacy: BTreeMap<SourceCoord, PartitionLegacyMemberKind>,
    direct: Option<Box<FormulaReplayRectNode>>,
}

#[derive(Debug)]
struct FormulaReplayRectNode {
    row_center: u32,
    by_col: Vec<SourceRect>,
    left: Option<Box<FormulaReplayRectNode>>,
    right: Option<Box<FormulaReplayRectNode>>,
}

impl FormulaReplayRectNode {
    fn build(mut rects: Vec<SourceRect>) -> Option<Box<Self>> {
        if rects.is_empty() {
            return None;
        }
        let middle = rects.len() / 2;
        rects.select_nth_unstable_by_key(middle, |rect| {
            rect.start.row + (rect.end.row - rect.start.row) / 2
        });
        let pivot = rects[middle];
        let row_center = pivot.start.row + (pivot.end.row - pivot.start.row) / 2;
        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut by_col = Vec::new();
        for rect in rects {
            if rect.end.row < row_center {
                left.push(rect);
            } else if rect.start.row > row_center {
                right.push(rect);
            } else {
                by_col.push(rect);
            }
        }
        by_col.sort_unstable_by_key(|rect| (rect.start.col, rect.end.col));
        Some(Box::new(Self {
            row_center,
            by_col,
            left: Self::build(left),
            right: Self::build(right),
        }))
    }

    fn contains(&self, coord: SourceCoord) -> bool {
        let insertion = self
            .by_col
            .partition_point(|rect| rect.start.col <= coord.col);
        if insertion > 0 && source_rect_contains(self.by_col[insertion - 1], coord) {
            return true;
        }
        if coord.row < self.row_center {
            self.left
                .as_deref()
                .is_some_and(|child| child.contains(coord))
        } else if coord.row > self.row_center {
            self.right
                .as_deref()
                .is_some_and(|child| child.contains(coord))
        } else {
            false
        }
    }
}

impl FormulaReplayPartitionRouter {
    fn validate_disjoint(rects: &[SourceRect]) -> Result<(), &'static str> {
        let mut events = Vec::with_capacity(rects.len().saturating_mul(2));
        for (index, rect) in rects.iter().copied().enumerate() {
            events.push((rect.start.row, false, index, rect));
            events.push((rect.end.row, true, index, rect));
        }
        events.sort_unstable_by_key(|(row, is_end, index, _)| (*row, *is_end, *index));
        let mut active = BTreeMap::<u32, (u32, usize)>::new();
        for (_, is_end, index, rect) in events {
            if is_end {
                active.remove(&rect.start.col);
                continue;
            }
            if active
                .range(..=rect.start.col)
                .next_back()
                .is_some_and(|(_, (end, _))| *end >= rect.start.col)
                || active
                    .range(rect.start.col..)
                    .next()
                    .is_some_and(|(start, _)| *start <= rect.end.col)
            {
                return Err("partition replay router received overlapping domains");
            }
            if active
                .insert(rect.start.col, (rect.end.col, index))
                .is_some()
            {
                return Err("partition replay router received overlapping domains");
            }
        }
        Ok(())
    }

    pub fn new(partitions: &[PartitionedSourceFormulaFamily]) -> Result<Self, &'static str> {
        let mut families = BTreeMap::new();
        for family in partitions {
            if family.declared.start.row > family.declared.end.row
                || family.declared.start.col > family.declared.end.col
            {
                return Err("partition replay router received an invalid declared domain");
            }
            let legacy = family
                .legacy_members
                .as_slice()
                .iter()
                .map(|member| (member.coord, member.kind))
                .collect();
            let rects: Vec<_> = family
                .fragments
                .iter()
                .map(|fragment| fragment.rect())
                .collect();
            if rects.iter().any(|rect| {
                rect.start.row > rect.end.row
                    || rect.start.col > rect.end.col
                    || !source_rect_contains(family.declared, rect.start)
                    || !source_rect_contains(family.declared, rect.end)
            }) {
                return Err("partition replay router received an invalid fragment domain");
            }
            Self::validate_disjoint(&rects)?;
            if families
                .insert(
                    family.source_id,
                    FormulaReplayFamilyRouter {
                        declared: family.declared,
                        legacy,
                        direct: FormulaReplayRectNode::build(rects),
                    },
                )
                .is_some()
            {
                return Err("partition replay router received duplicate family ownership");
            }
        }
        Ok(Self { families })
    }

    pub fn shared_disposition(
        &self,
        disposition: &FormulaReplayDisposition,
        family: SourceFamilyId,
        coord: SourceCoord,
    ) -> FormulaReplayCoordinateDisposition {
        let Some(router) = self.families.get(&family) else {
            return disposition.shared_disposition(family, coord);
        };
        if !source_rect_contains(router.declared, coord) {
            return FormulaReplayCoordinateDisposition::LegacyShared;
        }
        if let Some(kind) = router.legacy.get(&coord) {
            return if *kind == PartitionLegacyMemberKind::SharedFamilyMember {
                disposition.shared_disposition(family, coord)
            } else {
                FormulaReplayCoordinateDisposition::LegacyShared
            };
        }
        if router
            .direct
            .as_deref()
            .is_some_and(|node| node.contains(coord))
        {
            disposition.shared_disposition(family, coord)
        } else {
            FormulaReplayCoordinateDisposition::LegacyShared
        }
    }
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

fn source_rect_contains(rect: SourceRect, coord: SourceCoord) -> bool {
    coord.row >= rect.start.row
        && coord.row <= rect.end.row
        && coord.col >= rect.start.col
        && coord.col <= rect.end.col
}

fn source_rects_overlap(left: SourceRect, right: SourceRect) -> bool {
    left.start.row <= right.end.row
        && right.start.row <= left.end.row
        && left.start.col <= right.end.col
        && right.start.col <= left.end.col
}

fn source_rect_area(rect: SourceRect) -> Option<u64> {
    let rows = u64::from(rect.end.row.checked_sub(rect.start.row)?.checked_add(1)?);
    let cols = u64::from(rect.end.col.checked_sub(rect.start.col)?.checked_add(1)?);
    rows.checked_mul(cols)
}

fn validate_partition_legacy_members(
    members: &[PartitionLegacyMember],
    limits: &super::WorkbookLoadLimits,
) -> Result<(), &'static str> {
    if members
        .iter()
        .any(|member| !source_coord_in_bounds(member.coord, limits))
    {
        return Err("PartitionLegacyMemberOutOfBounds");
    }
    if members
        .windows(2)
        .any(|window| window[0].coord >= window[1].coord)
    {
        return Err("PartitionLegacyMembersNotStrictlySorted");
    }
    Ok(())
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
    pub source_spool_spill_files: u64,
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
#[derive(Clone, Debug)]
pub struct DeferredReplayFormula {
    pub source_order: SourceFormulaOrder,
    pub row: u32,
    pub col: u32,
    pub text: String,
    /// Shared-formula metadata identity, if this record came from a shared family.
    pub family: Option<SourceFamilyId>,
    /// Atomic fragmented-family owner. Ordinary exceptions have an owner even
    /// though they are not shared-formula records.
    pub partition_owner: Option<SourceFamilyId>,
}

/// Backend-owned, single-owner replay authority used by deferred workbook
/// loading. This deliberately has no clone/snapshot operation.
#[doc(hidden)]
pub trait DeferredFormulaReplay: Send {
    fn replay(
        &mut self,
        disposition: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String>;

    fn replay_partitioned(
        &mut self,
        disposition: &FormulaReplayDisposition,
        partitions: &[PartitionedSourceFormulaFamily],
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        if partitions.is_empty() {
            self.replay(disposition)
        } else {
            Err("partition-aware exact replay is not implemented by this backend".to_string())
        }
    }

    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String>;
}

/// Sealed workbook-to-engine package. It owns the source spool and compressed
/// family evidence until exactly one selected deferred build consumes it.
#[doc(hidden)]
pub struct DeferredFormulaPackage {
    pub(crate) sheet_name: String,
    pub(crate) report: FormulaCompressedSourceReport,
    pub(crate) families: Vec<SourceFormulaFamily>,
    pub(crate) partitioned_families: Vec<PartitionedSourceFormulaFamily>,
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
        partitioned_families: Vec<PartitionedSourceFormulaFamily>,
        replay: Box<dyn DeferredFormulaReplay>,
    ) -> Self {
        Self {
            sheet_name,
            report,
            families,
            partitioned_families,
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
    partitioned_families: Vec<PartitionedSourceFormulaFamily>,
}

pub(crate) struct PreparedFragmentedSourceProposal {
    pub(crate) source: PartitionedSourceFormulaFamily,
    pub(crate) prepared: super::fragmented_transaction::PreparedPartitionedSourceFamily,
    pub(crate) legacy: Vec<super::fragmented_transaction::PreparedFragmentedLegacyFormula>,
    pub(crate) replay: Arc<std::sync::Mutex<Box<dyn DeferredFormulaReplay>>>,
}

/// Opaque eager preparation owned by Engine between adapter classification and replay.
/// The adapter can inspect dispositions but cannot commit FormulaPlane authority.
#[doc(hidden)]
pub struct FormulaCompressedPreparation {
    pub(crate) engine_token: Arc<()>,
    pub(crate) function_semantic_epoch: u64,
    pub(crate) function_provider_revision: Option<u64>,
    pub(crate) function_semantics_used: bool,
    pub(crate) sheet_name: Arc<str>,
    pub(crate) prepared: Vec<(
        SourceFamilyId,
        SourceFormulaOrder,
        PreparedAnchorOncePlacement,
    )>,
    pub(crate) rejected: BTreeMap<SourceFamilyId, String>,
    pub(crate) fragmented: Vec<PreparedFragmentedSourceProposal>,
    pub(crate) fragmented_sources: BTreeMap<SourceFamilyId, PartitionedSourceFormulaFamily>,
    pub(crate) eager_replay: Vec<DeferredReplayFormula>,
    pub(crate) preparation_spool_replays: u64,
    pub(crate) clean_rejected_anchor_counts: [u64; 3],
    pub(crate) fragmented_rejected_anchor_counts: [u64; 3],
    pub(crate) exact_replay: Option<Arc<std::sync::Mutex<Box<dyn DeferredFormulaReplay>>>>,
    pub(crate) replay_disposition: FormulaReplayDisposition,
}

impl FormulaCompressedPreparation {
    #[doc(hidden)]
    pub fn with_exact_replay(
        mut self,
        replay: Arc<std::sync::Mutex<Box<dyn DeferredFormulaReplay>>>,
        suppressed: std::collections::BTreeSet<(u32, u32)>,
    ) -> Self {
        self.exact_replay = Some(replay);
        self.replay_disposition
            .extend_suppressed_excel_coords(suppressed);
        self
    }

    pub fn is_direct(&self, family: SourceFamilyId) -> bool {
        self.prepared.iter().any(|(id, _, _)| *id == family)
    }

    pub fn direct_family_count(&self) -> usize {
        self.prepared.len()
    }

    pub fn direct_cell_count(&self) -> u64 {
        self.prepared
            .iter()
            .map(|(_, _, prepared)| prepared.member_count)
            .sum()
    }
}

impl FormulaCompressedSourceBatch {
    pub fn new(sheet_name: impl Into<Arc<str>>, report: FormulaCompressedSourceReport) -> Self {
        Self {
            sheet_name: sheet_name.into(),
            report,
            families: Vec::new(),
            partitioned_families: Vec::new(),
        }
    }

    pub fn with_families(
        sheet_name: impl Into<Arc<str>>,
        report: FormulaCompressedSourceReport,
        families: Vec<SourceFormulaFamily>,
    ) -> Self {
        Self::with_proposals(sheet_name, report, families, Vec::new())
    }

    pub fn with_proposals(
        sheet_name: impl Into<Arc<str>>,
        report: FormulaCompressedSourceReport,
        families: Vec<SourceFormulaFamily>,
        partitioned_families: Vec<PartitionedSourceFormulaFamily>,
    ) -> Self {
        Self {
            sheet_name: sheet_name.into(),
            report,
            families,
            partitioned_families,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        Arc<str>,
        FormulaCompressedSourceReport,
        Vec<SourceFormulaFamily>,
        Vec<PartitionedSourceFormulaFamily>,
    ) {
        (
            self.sheet_name,
            self.report,
            self.families,
            self.partitioned_families,
        )
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
            source_order: SourceFormulaOrder::new(0),
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
    fn partition_transport_separates_template_origin_from_fragment_origins() {
        let partition = PartitionedSourceFormulaFamily {
            source_order: SourceFormulaOrder::new(0),
            source_id: SourceFamilyId {
                sheet_instance: 7,
                source_index: 12,
            },
            template_origin0: SourceCoord { row: 2, col: 3 },
            template_text: Arc::from("A1+1"),
            declared: SourceRect {
                start: SourceCoord { row: 2, col: 3 },
                end: SourceCoord { row: 10, col: 3 },
            },
            surviving_member_count: 7,
            fragments: vec![
                PlacementDomainTransport::RowRun {
                    row_start: 2,
                    row_end: 4,
                    col: 3,
                },
                PlacementDomainTransport::RowRun {
                    row_start: 7,
                    row_end: 9,
                    col: 3,
                },
            ],
            legacy_members: ExplicitPartitionLegacyMembers::try_new(vec![
                PartitionLegacyMember {
                    coord: SourceCoord { row: 6, col: 3 },
                    kind: PartitionLegacyMemberKind::SharedFamilyMember,
                },
                PartitionLegacyMember {
                    coord: SourceCoord { row: 5, col: 3 },
                    kind: PartitionLegacyMemberKind::OrdinaryException,
                },
            ])
            .unwrap(),
            reconciliation: PartitionReconciliation {
                shared_members: 7,
                ordinary_exceptions: 1,
                holes: 1,
            },
        };
        assert!(partition.validate(&limits()).is_ok());
        assert!(!source_rect_contains(
            partition.fragments[1].rect(),
            partition.template_origin0
        ));

        let mut replay = FormulaReplayDisposition::default();
        replay.register_partition(&partition, true).unwrap();
        assert_eq!(
            replay.shared_disposition(partition.source_id, SourceCoord { row: 2, col: 3 }),
            FormulaReplayCoordinateDisposition::Direct
        );
        assert_eq!(
            replay.shared_disposition(partition.source_id, SourceCoord { row: 6, col: 3 }),
            FormulaReplayCoordinateDisposition::LegacyShared
        );
        assert_eq!(
            replay.partition_disposition(&partition, SourceCoord { row: 10, col: 3 }),
            None,
            "holes are absent rather than inherited from the compact family default"
        );
        assert_eq!(
            replay.ordinary_disposition(SourceCoord { row: 5, col: 3 }),
            (
                FormulaReplayCoordinateDisposition::LegacyOrdinary,
                Some(partition.source_id)
            )
        );
    }

    #[test]
    fn partition_replay_router_handles_many_adjacent_fragments_and_rejects_nesting() {
        let source_id = SourceFamilyId {
            sheet_instance: 3,
            source_index: 8,
        };
        let fragments: Vec<_> = (0..2048)
            .map(|col| {
                PlacementDomainTransport::Rect(SourceRect {
                    start: SourceCoord {
                        row: 4,
                        col: col * 2,
                    },
                    end: SourceCoord {
                        row: 6,
                        col: col * 2,
                    },
                })
            })
            .collect();
        let family = PartitionedSourceFormulaFamily {
            source_order: SourceFormulaOrder::new(0),
            source_id,
            template_origin0: SourceCoord { row: 4, col: 0 },
            template_text: Arc::from("A1+1"),
            declared: SourceRect {
                start: SourceCoord { row: 4, col: 0 },
                end: SourceCoord { row: 6, col: 4095 },
            },
            surviving_member_count: 2048 * 3,
            fragments,
            legacy_members: ExplicitPartitionLegacyMembers::try_new(Vec::new()).unwrap(),
            reconciliation: PartitionReconciliation {
                shared_members: 2048 * 3,
                ordinary_exceptions: 0,
                holes: 2048 * 3,
            },
        };
        let mut disposition = FormulaReplayDisposition::default();
        disposition.register_partition(&family, true).unwrap();
        let router = FormulaReplayPartitionRouter::new(std::slice::from_ref(&family)).unwrap();
        for col in 0..2048 {
            assert_eq!(
                router.shared_disposition(
                    &disposition,
                    source_id,
                    SourceCoord {
                        row: 5,
                        col: col * 2,
                    },
                ),
                FormulaReplayCoordinateDisposition::Direct
            );
            assert_eq!(
                router.shared_disposition(
                    &disposition,
                    source_id,
                    SourceCoord {
                        row: 5,
                        col: col * 2 + 1,
                    },
                ),
                FormulaReplayCoordinateDisposition::LegacyShared
            );
        }

        let mut nested = family;
        nested.declared = SourceRect {
            start: SourceCoord { row: 0, col: 0 },
            end: SourceCoord { row: 10, col: 10 },
        };
        nested.fragments = vec![
            PlacementDomainTransport::Rect(SourceRect {
                start: SourceCoord { row: 0, col: 0 },
                end: SourceCoord { row: 10, col: 10 },
            }),
            PlacementDomainTransport::Rect(SourceRect {
                start: SourceCoord { row: 2, col: 2 },
                end: SourceCoord { row: 4, col: 4 },
            }),
        ];
        assert_eq!(
            FormulaReplayPartitionRouter::new(&[nested]).unwrap_err(),
            "partition replay router received overlapping domains"
        );
    }

    #[test]
    fn default_partition_replay_fails_closed() {
        struct Replay;
        impl DeferredFormulaReplay for Replay {
            fn replay(
                &mut self,
                _disposition: &FormulaReplayDisposition,
            ) -> Result<Vec<DeferredReplayFormula>, String> {
                Ok(Vec::new())
            }

            fn formula_at(
                &mut self,
                _row: u32,
                _col: u32,
            ) -> Result<Option<DeferredReplayFormula>, String> {
                Ok(None)
            }
        }

        let partition = PartitionedSourceFormulaFamily {
            source_order: SourceFormulaOrder::new(0),
            source_id: SourceFamilyId {
                sheet_instance: 1,
                source_index: 2,
            },
            template_origin0: SourceCoord { row: 0, col: 0 },
            template_text: Arc::from("A1"),
            declared: SourceRect {
                start: SourceCoord { row: 0, col: 0 },
                end: SourceCoord { row: 0, col: 0 },
            },
            surviving_member_count: 1,
            fragments: vec![PlacementDomainTransport::RowRun {
                row_start: 0,
                row_end: 0,
                col: 0,
            }],
            legacy_members: ExplicitPartitionLegacyMembers::try_new(Vec::new()).unwrap(),
            reconciliation: PartitionReconciliation {
                shared_members: 1,
                ordinary_exceptions: 0,
                holes: 0,
            },
        };
        let mut replay = Replay;
        assert!(
            replay
                .replay_partitioned(&FormulaReplayDisposition::default(), &[partition])
                .unwrap_err()
                .contains("not implemented")
        );
        assert!(
            replay
                .replay_partitioned(&FormulaReplayDisposition::default(), &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn partition_transport_rejects_overlap_count_and_missing_origin() {
        let base = PartitionedSourceFormulaFamily {
            source_order: SourceFormulaOrder::new(0),
            source_id: SourceFamilyId {
                sheet_instance: 7,
                source_index: 12,
            },
            template_origin0: SourceCoord { row: 2, col: 3 },
            template_text: Arc::from("A1+1"),
            declared: SourceRect {
                start: SourceCoord { row: 2, col: 3 },
                end: SourceCoord { row: 9, col: 3 },
            },
            surviving_member_count: 6,
            fragments: vec![
                PlacementDomainTransport::RowRun {
                    row_start: 2,
                    row_end: 4,
                    col: 3,
                },
                PlacementDomainTransport::RowRun {
                    row_start: 7,
                    row_end: 9,
                    col: 3,
                },
            ],
            legacy_members: ExplicitPartitionLegacyMembers::try_new(Vec::new()).unwrap(),
            reconciliation: PartitionReconciliation {
                shared_members: 6,
                ordinary_exceptions: 0,
                holes: 2,
            },
        };
        let mut overlap = base.clone();
        overlap.fragments[1] = PlacementDomainTransport::RowRun {
            row_start: 4,
            row_end: 6,
            col: 3,
        };
        assert_eq!(
            overlap.validate(&limits()),
            Err("PartitionFragmentsOverlap")
        );

        let mut wrong_count = base.clone();
        wrong_count.surviving_member_count = 5;
        assert_eq!(
            wrong_count.validate(&limits()),
            Err("PartitionMemberCountMismatch")
        );

        let mut missing_origin = base;
        missing_origin.template_origin0 = SourceCoord { row: 1, col: 1 };
        assert_eq!(
            missing_origin.validate(&limits()),
            Err("PartitionMissingTemplateOrigin")
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
