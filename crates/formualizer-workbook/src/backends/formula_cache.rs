use formualizer_common::LiteralValue;

/// Owned cache-write update used by batch formula-cache APIs.
#[derive(Clone, Debug, PartialEq)]
pub struct FormulaCacheUpdate {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub value: LiteralValue,
}

/// Borrowed cache-write update used by zero-copy batch formula-cache APIs.
#[derive(Clone, Copy, Debug)]
pub struct FormulaCacheUpdateRef<'a> {
    pub sheet: &'a str,
    pub row: u32,
    pub col: u32,
    pub value: &'a LiteralValue,
}
