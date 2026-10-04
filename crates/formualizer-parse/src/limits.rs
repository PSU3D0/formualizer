//! Immutable parser resource budgets.

/// Resource budgets applied while tokenizing and parsing one formula.
///
/// Start from [`ParserLimits::default`] and adjust individual budgets with the
/// `with_*` setters. Values are stored exactly as given: nothing is clamped,
/// and a zero budget rejects every input that needs that resource.
///
/// ```
/// use formualizer_parse::{Parser, ParserLimits};
///
/// let limits = ParserLimits::default().with_ast_nodes(64).with_tokens(128);
/// assert_eq!(limits.ast_nodes(), 64);
/// let ast = Parser::builder().limits(limits).parse("=SUM(A1:B2)").unwrap();
/// # let _ = ast;
/// ```
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
    /// Maximum UTF-8 source length in bytes.
    #[must_use]
    pub const fn with_source_bytes(mut self, source_bytes: usize) -> Self {
        self.source_bytes = source_bytes;
        self
    }
    /// Maximum number of tokens, including retained whitespace tokens.
    #[must_use]
    pub const fn with_tokens(mut self, tokens: usize) -> Self {
        self.tokens = tokens;
        self
    }
    /// Maximum number of AST nodes, including omitted arguments.
    #[must_use]
    pub const fn with_ast_nodes(mut self, ast_nodes: usize) -> Self {
        self.ast_nodes = ast_nodes;
        self
    }
    /// Maximum active Pratt-parser frames (see [`Self::pratt_frames`]).
    ///
    /// Raising this above the default needs a correspondingly larger stack on
    /// the parsing thread.
    #[must_use]
    pub const fn with_pratt_frames(mut self, pratt_frames: usize) -> Self {
        self.pratt_frames = pratt_frames;
        self
    }
    /// Maximum AST height, counting the root as one.
    ///
    /// Raising this above the default needs a correspondingly larger stack on
    /// every thread that parses, clones, drops or evaluates the tree.
    #[must_use]
    pub const fn with_ast_height(mut self, ast_height: usize) -> Self {
        self.ast_height = ast_height;
        self
    }
    pub const fn source_bytes(self) -> usize {
        self.source_bytes
    }
    pub const fn tokens(self) -> usize {
        self.tokens
    }
    pub const fn ast_nodes(self) -> usize {
        self.ast_nodes
    }
    /// Active Pratt frames, not an exact Excel function-nesting limit. The
    /// default leaves headroom for ordinary 64-level calls/parentheses and IF
    /// conditions; right-nested parenthesized infix uses two frames per level.
    pub const fn pratt_frames(self) -> usize {
        self.pratt_frames
    }
    pub const fn ast_height(self) -> usize {
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
