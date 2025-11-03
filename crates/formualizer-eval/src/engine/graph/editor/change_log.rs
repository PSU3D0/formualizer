//! Standalone change logging infrastructure for tracking graph mutations
//!
//! This module provides:
//! - ChangeLog: Audit trail of all graph changes
//! - ChangeEvent: Granular representation of individual changes
//! - ChangeLogger: Trait for pluggable logging strategies

use crate::SheetId;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::vertex::VertexId;
use crate::reference::CellRef;
use formualizer_common::Coord as AbsCoord;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ASTNode;

/// Represents a single change to the dependency graph
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeEvent {
    // Simple events
    SetValue {
        addr: CellRef,
        old: Option<LiteralValue>,
        new: LiteralValue,
    },
    SetFormula {
        addr: CellRef,
        old: Option<ASTNode>,
        new: ASTNode,
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
        old_coord: AbsCoord,
        new_coord: AbsCoord,
    },
    FormulaAdjusted {
        id: VertexId,
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
}

/// Audit trail for tracking all changes to the dependency graph
#[derive(Debug, Default)]
pub struct ChangeLog {
    events: Vec<ChangeEvent>,
    enabled: bool,
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
}

impl ChangeLog {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            enabled: true,
            compound_depth: 0,
            seqs: Vec::new(),
            groups: Vec::new(),
            next_seq: 0,
            group_stack: Vec::new(),
            next_group_id: 1,
        }
    }

    pub fn record(&mut self, event: ChangeEvent) {
        if self.enabled {
            let seq = self.next_seq;
            self.next_seq += 1;
            let current_group = self.group_stack.last().copied();
            self.events.push(event);
            self.seqs.push(seq);
            self.groups.push(current_group);
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

    /// Truncate log (and metadata) to len
    pub fn truncate(&mut self, len: usize) {
        self.events.truncate(len);
        self.seqs.truncate(len);
        self.groups.truncate(len);
    }

    pub fn clear(&mut self) {
        self.events.clear();
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
        self.events.split_off(index)
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
