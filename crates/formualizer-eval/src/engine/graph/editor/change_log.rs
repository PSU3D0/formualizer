//! Standalone change logging infrastructure for tracking graph mutations
//!
//! This module provides:
//! - ChangeLog: Audit trail of all graph changes
//! - ChangeEvent: Granular representation of individual changes
//! - ChangeLogger: Trait for pluggable logging strategies

use crate::SheetId;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::row_visibility::RowVisibilitySource;
use crate::engine::vertex::VertexId;
use crate::reference::CellRef;
use formualizer_common::Coord as AbsCoord;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ASTNode;

#[derive(Debug, Clone, PartialEq)]
pub struct SpillSnapshot {
    /// Declared target cells (row-major rectangle) owned by this spill anchor.
    pub target_cells: Vec<CellRef>,
    /// Row-major rectangular values corresponding to the target rectangle.
    pub values: Vec<Vec<LiteralValue>>,
}

/// Per-event metadata attached by the caller.
///
/// This is intentionally lightweight (Strings) to avoid leaking application types
/// into the engine layer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangeEventMeta {
    pub actor_id: Option<String>,
    pub correlation_id: Option<String>,
    pub reason: Option<String>,
}

/// Represents a single change to the dependency graph
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeEvent {
    // Simple events
    SetValue {
        addr: CellRef,
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
        new: LiteralValue,
    },
    SetFormula {
        addr: CellRef,
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
        new: ASTNode,
    },
    SetRowVisibility {
        sheet_id: SheetId,
        row0: u32,
        source: RowVisibilitySource,
        old_hidden: bool,
        new_hidden: bool,
    },
    /// Vertex creation snapshot (for undo). Minimal for now.
    AddVertex {
        id: VertexId,
        coord: AbsCoord,
        sheet_id: SheetId,
        value: Option<LiteralValue>,
        formula: Option<ASTNode>,
        kind: Option<crate::engine::vertex::VertexKind>,
        flags: Option<u8>,
    },
    RemoveVertex {
        id: VertexId,
        // Need to capture more for rollback!
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
        old_dependencies: Vec<VertexId>, // outgoing
        old_dependents: Vec<VertexId>,   // incoming
        coord: Option<AbsCoord>,
        sheet_id: Option<SheetId>,
        kind: Option<crate::engine::vertex::VertexKind>,
        flags: Option<u8>,
    },

    // Compound operation markers
    CompoundStart {
        description: String, // e.g., "InsertRows(sheet=0, before=5, count=2)"
        depth: usize,
    },
    CompoundEnd {
        depth: usize,
    },

    // Granular events for compound operations
    VertexMoved {
        id: VertexId,
        sheet_id: SheetId,
        old_coord: AbsCoord,
        new_coord: AbsCoord,
    },
    FormulaAdjusted {
        id: VertexId,
        /// Cell address for replay. May be None for non-cell formula vertices.
        addr: Option<CellRef>,
        old_ast: ASTNode,
        new_ast: ASTNode,
    },
    NamedRangeAdjusted {
        name: String,
        scope: NameScope,
        old_definition: NamedDefinition,
        new_definition: NamedDefinition,
    },
    EdgeAdded {
        from: VertexId,
        to: VertexId,
    },
    EdgeRemoved {
        from: VertexId,
        to: VertexId,
    },

    // Named range operations
    DefineName {
        name: String,
        scope: NameScope,
        definition: NamedDefinition,
    },
    UpdateName {
        name: String,
        scope: NameScope,
        old_definition: NamedDefinition,
        new_definition: NamedDefinition,
    },
    DeleteName {
        name: String,
        scope: NameScope,
        old_definition: Option<NamedDefinition>,
    },

    // Spill region changes (dynamic arrays)
    SpillCommitted {
        anchor: VertexId,
        old: Option<SpillSnapshot>,
        new: SpillSnapshot,
    },
    SpillCleared {
        anchor: VertexId,
        old: SpillSnapshot,
    },
}

/// Audit trail for tracking all changes to the dependency graph
#[derive(Debug, Default)]
pub struct ChangeLog {
    events: Vec<ChangeEvent>,
    metas: Vec<ChangeEventMeta>,
    enabled: bool,
    /// Optional cap on retained events; when exceeded, oldest events are evicted (FIFO).
    max_changelog_events: Option<usize>,
    /// Track compound operations for atomic rollback
    compound_depth: usize,
    /// Monotonic sequence number per event
    seqs: Vec<u64>,
    /// Optional group id (compound) per event
    groups: Vec<Option<u64>>,
    next_seq: u64,
    /// Stack of active group ids for nested compounds
    group_stack: Vec<u64>,
    next_group_id: u64,

    current_meta: ChangeEventMeta,
}

impl ChangeLog {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            metas: Vec::new(),
            enabled: true,
            max_changelog_events: None,
            compound_depth: 0,
            seqs: Vec::new(),
            groups: Vec::new(),
            next_seq: 0,
            group_stack: Vec::new(),
            next_group_id: 1,
            current_meta: ChangeEventMeta::default(),
        }
    }

    pub fn with_max_changelog_events(max: usize) -> Self {
        let mut out = Self::new();
        out.max_changelog_events = Some(max);
        out
    }

    pub fn set_max_changelog_events(&mut self, max: Option<usize>) {
        self.max_changelog_events = max;
        self.enforce_cap();
    }

    fn enforce_cap(&mut self) {
        let Some(max) = self.max_changelog_events else {
            return;
        };
        if max == 0 {
            self.clear();
            return;
        }
        if self.events.len() <= max {
            return;
        }
        let drop_n = self.events.len() - max;
        self.events.drain(0..drop_n);
        self.metas.drain(0..drop_n);
        self.seqs.drain(0..drop_n);
        self.groups.drain(0..drop_n);
    }

    pub fn record(&mut self, event: ChangeEvent) {
        if self.enabled {
            let seq = self.next_seq;
            self.next_seq += 1;
            let current_group = self.group_stack.last().copied();
            self.events.push(event);
            self.metas.push(self.current_meta.clone());
            self.seqs.push(seq);
            self.groups.push(current_group);
            self.enforce_cap();
        }
    }

    /// Record an event with explicit metadata (used for replay/redo).
    pub fn record_with_meta(&mut self, event: ChangeEvent, meta: ChangeEventMeta) {
        if self.enabled {
            let seq = self.next_seq;
            self.next_seq += 1;
            let current_group = self.group_stack.last().copied();
            self.events.push(event);
            self.metas.push(meta);
            self.seqs.push(seq);
            self.groups.push(current_group);
            self.enforce_cap();
        }
    }

    /// Begin a compound operation (multiple changes from single action)
    pub fn begin_compound(&mut self, description: String) {
        self.compound_depth += 1;
        if self.compound_depth == 1 {
            // allocate new group id
            let gid = self.next_group_id;
            self.next_group_id += 1;
            self.group_stack.push(gid);
        } else {
            // nested: reuse top id
            if let Some(&gid) = self.group_stack.last() {
                self.group_stack.push(gid);
            }
        }
        if self.enabled {
            self.record(ChangeEvent::CompoundStart {
                description,
                depth: self.compound_depth,
            });
        }
    }

    /// End a compound operation
    pub fn end_compound(&mut self) {
        if self.compound_depth > 0 {
            if self.enabled {
                self.record(ChangeEvent::CompoundEnd {
                    depth: self.compound_depth,
                });
            }
            self.compound_depth -= 1;
            self.group_stack.pop();
        }
    }

    pub fn events(&self) -> &[ChangeEvent] {
        &self.events
    }

    pub fn patch_last_cell_event_old_state(
        &mut self,
        addr: CellRef,
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
    ) {
        // Walk backwards to find the most recent SetValue/SetFormula for this cell.
        // This is used by Arrow-canonical callers that must capture old_value/old_formula
        // from Arrow truth (graph value cache may be disabled).
        for ev in self.events.iter_mut().rev() {
            match ev {
                ChangeEvent::SetValue {
                    addr: a,
                    old_value: ov,
                    old_formula: of,
                    ..
                }
                | ChangeEvent::SetFormula {
                    addr: a,
                    old_value: ov,
                    old_formula: of,
                    ..
                } if *a == addr => {
                    if ov.is_none() {
                        *ov = old_value;
                    }
                    if of.is_none() {
                        *of = old_formula;
                    }
                    break;
                }
                _ => {}
            }
        }
    }

    pub fn event_meta(&self, index: usize) -> Option<&ChangeEventMeta> {
        self.metas.get(index)
    }

    pub fn set_actor_id(&mut self, actor_id: Option<String>) {
        self.current_meta.actor_id = actor_id;
    }

    pub fn set_correlation_id(&mut self, correlation_id: Option<String>) {
        self.current_meta.correlation_id = correlation_id;
    }

    pub fn set_reason(&mut self, reason: Option<String>) {
        self.current_meta.reason = reason;
    }

    /// Truncate log (and metadata) to len
    pub fn truncate(&mut self, len: usize) {
        self.events.truncate(len);
        self.metas.truncate(len);
        self.seqs.truncate(len);
        self.groups.truncate(len);
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.metas.clear();
        self.seqs.clear();
        self.groups.clear();
        self.compound_depth = 0;
        self.group_stack.clear();
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Extract events from index to end
    pub fn take_from(&mut self, index: usize) -> Vec<ChangeEvent> {
        let events = self.events.split_off(index);
        let _ = self.metas.split_off(index);
        let _ = self.seqs.split_off(index);
        let _ = self.groups.split_off(index);
        events
    }

    /// Temporarily disable logging (for rollback operations)
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Get current compound depth (for testing)
    pub fn compound_depth(&self) -> usize {
        self.compound_depth
    }

    /// Return (sequence_number, group_id) metadata for event index
    pub fn meta(&self, index: usize) -> Option<(u64, Option<u64>)> {
        self.seqs
            .get(index)
            .copied()
            .zip(self.groups.get(index).copied())
    }

    /// Collect indices belonging to the last (innermost) complete group. Fallback: last single event.
    pub fn last_group_indices(&self) -> Vec<usize> {
        if let Some(&last_gid) = self.groups.iter().rev().flatten().next() {
            let idxs: Vec<usize> = self
                .groups
                .iter()
                .enumerate()
                .filter_map(|(i, g)| if *g == Some(last_gid) { Some(i) } else { None })
                .collect();
            if !idxs.is_empty() {
                return idxs;
            }
        }
        self.events.len().checked_sub(1).into_iter().collect()
    }
}

/// Trait for pluggable logging strategies
pub trait ChangeLogger {
    fn record(&mut self, event: ChangeEvent);
    fn set_enabled(&mut self, enabled: bool);
    fn begin_compound(&mut self, description: String);
    fn end_compound(&mut self);
}

impl ChangeLogger for ChangeLog {
    fn record(&mut self, event: ChangeEvent) {
        ChangeLog::record(self, event);
    }

    fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    fn begin_compound(&mut self, description: String) {
        ChangeLog::begin_compound(self, description);
    }

    fn end_compound(&mut self) {
        ChangeLog::end_compound(self);
    }
}

/// Null logger for when change tracking not needed
pub struct NullChangeLogger;

impl ChangeLogger for NullChangeLogger {
    fn record(&mut self, _: ChangeEvent) {}
    fn set_enabled(&mut self, _: bool) {}
    fn begin_compound(&mut self, _: String) {}
    fn end_compound(&mut self) {}
}
