//! Immutable parser resource budgets. Stack-sensitive limits have fixed ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParserLimits {
    pub(crate) source_bytes: usize,
    pub(crate) tokens: usize,
    pub(crate) ast_nodes: usize,
    pub(crate) pratt_frames: usize,
    pub(crate) ast_height: usize,
}
impl Default for ParserLimits {
    fn default() -> Self {
        Self {
            source_bytes: 65536,
            tokens: 16384,
            ast_nodes: 8192,
            pratt_frames: 72,
            ast_height: 256,
        }
    }
}
impl ParserLimits {
    /// Zero budgets are permitted; stack limits cannot exceed the supported ceilings.
    pub fn new(
        source_bytes: usize,
        tokens: usize,
        ast_nodes: usize,
        pratt_frames: usize,
        ast_height: usize,
    ) -> Result<Self, crate::parser::ParserError> {
        if pratt_frames > 72 || ast_height > 256 {
            return Err(crate::parser::ParserError {
                message: "Parser stack limits exceed ceilings (Pratt 72, AST height 256)".into(),
                position: None,
            });
        }
        Ok(Self {
            source_bytes,
            tokens,
            ast_nodes,
            pratt_frames,
            ast_height,
        })
    }
    pub fn source_bytes(self) -> usize {
        self.source_bytes
    }
    pub fn tokens(self) -> usize {
        self.tokens
    }
    pub fn ast_nodes(self) -> usize {
        self.ast_nodes
    }
    /// Active Pratt frames, not an exact Excel function-nesting limit. The
    /// default leaves headroom for ordinary 64-level calls/parentheses and IF
    /// conditions; right-nested parenthesized infix uses two frames per level.
    pub fn pratt_frames(self) -> usize {
        self.pratt_frames
    }
    pub fn ast_height(self) -> usize {
        self.ast_height
    }
    pub(crate) fn check_source(self, source: &str) -> Result<(), crate::TokenizerError> {
        if source.len() > self.source_bytes {
            return Err(crate::TokenizerError {
                message: format!(
                    "Formula source byte limit exceeded (max {})",
                    self.source_bytes
                ),
                pos: 0,
            });
        }
        Ok(())
    }
}
