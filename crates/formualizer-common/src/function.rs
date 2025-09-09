#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ArgKind {
    Number,
    Text,
    Logical,
    Range,
    Any,
}

impl ArgKind {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "number" => Self::Number,
            "text" => Self::Text,
            "logical" => Self::Logical,
            "range" => Self::Range,
            "" | "_" | "any" => Self::Any,
            other => panic!("Unknown arg kind '{other}'"),
        }
    }
}
