//! Standalone change logging infrastructure for tracking graph mutations
//!
//! This module provides:
//! - ChangeLog: Audit trail of all graph changes
//! - ChangeEvent: Granular representation of individual changes
//! - ChangeLogger: Trait for pluggable logging strategies

use crate::SheetId;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::packed_coord::PackedCoord;
use crate::engine::vertex::VertexId;
use crate::reference::CellRef;
use formualizer_common::LiteralValue;
use formualizer_core::parser::ASTNode;

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
    RemoveVertex {
        id: VertexId,
        // Need to capture more for rollback!
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
        old_dependencies: Vec<VertexId>,
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
        old_coord: PackedCoord,
        new_coord: PackedCoord,
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

    // High-level operations (kept for clarity, but decomposed into granular events)
    InsertRows {
        sheet_id: SheetId,
        before: u32,
        count: u32,
    },
    DeleteRows {
        sheet_id: SheetId,
        start: u32,
        count: u32,
    },
    InsertColumns {
        sheet_id: SheetId,
        before: u32,
        count: u32,
    },
    DeleteColumns {
        sheet_id: SheetId,
        start: u32,
        count: u32,
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
    },
}

/// Audit trail for tracking all changes to the dependency graph
#[derive(Debug, Default)]
pub struct ChangeLog {
    events: Vec<ChangeEvent>,
    enabled: bool,
    /// Track compound operations for atomic rollback
    compound_depth: usize,
}

impl ChangeLog {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            enabled: true,
            compound_depth: 0,
        }
    }

    pub fn record(&mut self, event: ChangeEvent) {
        if self.enabled {
            self.events.push(event);
        }
    }

    /// Begin a compound operation (multiple changes from single action)
    pub fn begin_compound(&mut self, description: String) {
        self.compound_depth += 1;
        if self.enabled {
            self.events.push(ChangeEvent::CompoundStart {
                description,
                depth: self.compound_depth,
            });
        }
    }

    /// End a compound operation
    pub fn end_compound(&mut self) {
        if self.compound_depth > 0 {
            if self.enabled {
                self.events.push(ChangeEvent::CompoundEnd {
                    depth: self.compound_depth,
                });
            }
            self.compound_depth -= 1;
        }
    }

    pub fn events(&self) -> &[ChangeEvent] {
        &self.events
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.compound_depth = 0;
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
        self.record(event);
    }

    fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    fn begin_compound(&mut self, description: String) {
        self.begin_compound(description);
    }

    fn end_compound(&mut self) {
        self.end_compound();
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
