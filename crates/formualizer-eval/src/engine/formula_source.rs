use std::collections::BTreeMap;
use std::sync::Arc;

use formualizer_common::LiteralValue;

use super::FormulaIngestBatch;
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
    pub shared_index: usize,
}

/// Formula metadata that Calamine 0.36 exposes.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormulaMetadataEnvelope {
    XlsxOrdinary,
    XlsxShared {
        shared_index: usize,
        parsed_range: Option<SourceRect>,
    },
    XlsxUnknown,
}

/// Cached-value fidelity available from Calamine 0.36.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub enum SourceCachedValue {
    AbsentOrEmpty,
    Present(LiteralValue),
    Unrepresentable,
}

/// Source role and unmodified formula text, where Calamine exposes it.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub enum FormulaSourceKind {
    Ordinary {
        formula: Arc<str>,
        metadata: FormulaMetadataEnvelope,
    },
    SharedAnchor {
        family: SourceFamilyId,
        declared_range: Option<SourceRect>,
        formula: Arc<str>,
        metadata: FormulaMetadataEnvelope,
    },
    SharedDescendant {
        family: SourceFamilyId,
        metadata: FormulaMetadataEnvelope,
    },
    Unsupported {
        formula_if_available: Option<Arc<str>>,
        metadata: FormulaMetadataEnvelope,
    },
}

/// Lossless transport for one formula-bearing source cell.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct FormulaSourceEvent {
    pub sheet_name: Arc<str>,
    pub coord0: SourceCoord,
    pub source_sequence: u64,
    pub formula: FormulaSourceKind,
    pub cached: SourceCachedValue,
}

/// Additive source-aware transport. Existing `FormulaIngestBatch` callers remain unchanged.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct FormulaSourceIngestBatch {
    batch: FormulaIngestBatch,
    source_events: Vec<FormulaSourceEvent>,
}

impl FormulaSourceIngestBatch {
    pub fn new(batch: FormulaIngestBatch, source_events: Vec<FormulaSourceEvent>) -> Self {
        Self {
            batch,
            source_events,
        }
    }

    pub fn batch(&self) -> &FormulaIngestBatch {
        &self.batch
    }

    pub fn source_events(&self) -> &[FormulaSourceEvent] {
        &self.source_events
    }

    pub fn into_parts(self) -> (FormulaIngestBatch, Vec<FormulaSourceEvent>) {
        (self.batch, self.source_events)
    }

    pub(crate) fn without_source(batch: FormulaIngestBatch) -> Self {
        Self::new(batch, Vec::new())
    }
}

/// Counters and replay-only disposition produced by the compressed Calamine
/// evidence collector. Phase 3 deliberately carries no descendant events.
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
    pub replay_families: u64,
    pub replay_cells: u64,
    pub forward_descendants: u64,
    pub evidence_limit_fallbacks: u64,
    pub evidence_peak_bytes: u64,
    pub anchor_parses: u64,
    pub anchor_asts: u64,
    pub anchor_analyses: u64,
    pub descendant_strings_avoided: u64,
    pub descendant_events_avoided: u64,
    pub descendant_analyses_avoided: u64,
    pub compressed_families_prepared: u64,
    pub compressed_cells_prepared: u64,
    pub fallback_reasons: BTreeMap<String, u64>,
}

/// A complete, hole-free source domain. It is deliberately area-shaped so the
/// engine never needs descendant coordinates to prepare it.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressedPlacementDomain {
    Vertical {
        row_start: u32,
        row_end: u32,
        col: u32,
    },
    Horizontal {
        row: u32,
        col_start: u32,
        col_end: u32,
    },
    Rect(SourceRect),
}

impl CompressedPlacementDomain {
    pub fn from_rect(rect: SourceRect) -> Self {
        if rect.start.col == rect.end.col {
            Self::Vertical {
                row_start: rect.start.row,
                row_end: rect.end.row,
                col: rect.start.col,
            }
        } else if rect.start.row == rect.end.row {
            Self::Horizontal {
                row: rect.start.row,
                col_start: rect.start.col,
                col_end: rect.end.col,
            }
        } else {
            Self::Rect(rect)
        }
    }

    pub fn rect(self) -> SourceRect {
        match self {
            Self::Vertical {
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
            Self::Horizontal {
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

/// Clean-family transport used only by compressed Shadow preparation.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompressedSharedFamily {
    pub source_id: SourceFamilyId,
    pub anchor_coord0: SourceCoord,
    pub anchor_text: Arc<str>,
    pub domain: CompressedPlacementDomain,
    pub member_count: u64,
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
    pub(crate) families: Vec<CompressedSharedFamily>,
    pub(crate) replay: std::sync::Mutex<Box<dyn DeferredFormulaReplay>>,
    pub(crate) invalidated: std::collections::BTreeSet<SourceFamilyId>,
    pub(crate) suppressed: std::collections::BTreeSet<(u32, u32)>,
}

impl DeferredFormulaPackage {
    #[doc(hidden)]
    pub fn new(
        sheet_name: String,
        report: FormulaCompressedSourceReport,
        families: Vec<CompressedSharedFamily>,
        replay: Box<dyn DeferredFormulaReplay>,
    ) -> Self {
        Self {
            sheet_name,
            report,
            families,
            replay: std::sync::Mutex::new(replay),
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
    families: Vec<CompressedSharedFamily>,
}

/// Opaque eager preparation owned by Engine between adapter classification and replay.
/// The adapter can inspect dispositions but cannot commit FormulaPlane authority.
#[doc(hidden)]
pub struct FormulaCompressedPreparation {
    pub(crate) sheet_name: Arc<str>,
    pub(crate) prepared: Vec<(SourceFamilyId, PreparedAnchorOncePlacement)>,
    pub(crate) rejected: BTreeMap<SourceFamilyId, String>,
}

impl FormulaCompressedPreparation {
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
        families: Vec<CompressedSharedFamily>,
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
        Vec<CompressedSharedFamily>,
    ) {
        (self.sheet_name, self.report, self.families)
    }
}
