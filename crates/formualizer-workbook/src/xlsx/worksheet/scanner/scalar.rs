//! The open literal has no formula payload. Formula scratch is initialized
//! only on `<f>` and becomes a retained record only at `</c>`.
use super::*;

#[test]
fn scalar_decode_and_proof_match_the_emission_adapter() {
    for kind in [
        None,
        Some("n"),
        Some("s"),
        Some("b"),
        Some("e"),
        Some("str"),
        Some("d"),
        Some("inlineStr"),
    ] {
        for text in [
            "", "1", "0", " 1 ", "-0", "NaN", "inf", "#DIV/0!", "#N/A", "A&B",
        ] {
            for empty in [false, true] {
                let scalar = ScalarCell {
                    row: 1,
                    col: 1,
                    span: 0..text.len(),
                    open_end: 0,
                    qualified: "c",
                    kind: kind.and_then(CellKind::parse),
                    kind_span: None,
                    value: Some(OpenValue {
                        span: 0..text.len(),
                        open_end: 0,
                        close_start: text.len(),
                        empty,
                        qualified: "v",
                        text: Cow::Borrowed(text),
                    }),
                    inline: None,
                    has_formula: false,
                    dynamic: None,
                };
                let oracle = OpenCell {
                    row: 1,
                    col: 1,
                    span: 0..text.len(),
                    open_end: 0,
                    qualified: "c",
                    kind: kind.and_then(CellKind::parse),
                    kind_span: None,
                    value: Some(OpenValue {
                        span: 0..text.len(),
                        open_end: 0,
                        close_start: text.len(),
                        empty,
                        qualified: "v",
                        text: Cow::Borrowed(text),
                    }),
                    inline: None,
                    has_formula: false,
                    dynamic: None,
                    formula_end: 0,
                    formula_text: Cow::Borrowed(""),
                    formula_kind: "",
                    shared_id: None,
                    shared_range: None,
                    formula_open: 0..0,
                    formula_kind_span: None,
                };
                let actual = scalar.decode(Cow::Borrowed(text), emission::DisplayString::default());
                let expected = emission::value(
                    &oracle,
                    Cow::Borrowed(text),
                    emission::DisplayString::default(),
                );
                assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
                assert_eq!(
                    scalar.refusal(text.as_bytes(), &actual),
                    emission::refusal(&oracle, text.as_bytes(), &expected)
                );
            }
        }
    }
}

pub(super) struct ScalarCell<'a> {
    pub row: u32,
    pub col: u32,
    pub span: Range<usize>,
    pub open_end: usize,
    pub qualified: &'a str,
    pub kind: Option<CellKind>,
    pub kind_span: Option<Range<usize>>,
    pub value: Option<OpenValue<'a>>,
    pub inline: Option<Range<usize>>,
    pub has_formula: bool,
    pub dynamic: Option<Box<DynamicAttributes>>,
}

#[derive(Default)]
pub(super) struct FormulaState<'a> {
    pub end: usize,
    pub text: Cow<'a, str>,
    pub kind: &'static str,
    pub shared_id: Option<u32>,
    pub shared_range: Option<(u32, u32, u32, u32)>,
    pub open: Range<usize>,
    pub kind_span: Option<Range<usize>>,
    pub raw_kind_span: Option<Range<usize>>,
    pub raw_shared_index_span: Option<Range<usize>>,
    pub raw_range_span: Option<Range<usize>>,
}

impl<'a> ScalarCell<'a> {
    pub fn cm(&self) -> Option<u32> {
        self.dynamic.as_ref().and_then(|d| d.cm)
    }
    pub fn dynamic_anchor_extent(&self, formula: &FormulaState<'_>) -> Result<(), IoError> {
        let kind = if self.has_formula { formula.kind } else { "" };
        if self.cm().is_none() && kind != "array" {
            return Ok(());
        }
        if kind == "shared" {
            return Err(unsupported(
                "shared formula family member claimed as dynamic anchor",
                "worksheet",
            ));
        }
        if kind != "array" {
            return Err(unsupported(
                "cell metadata on a non-dynamic-array cell",
                "worksheet",
            ));
        }
        let extent = self
            .dynamic
            .as_ref()
            .and_then(|d| d.array_ref)
            .ok_or_else(|| unsupported("missing dynamic array extent", "worksheet"))?;
        if (extent.first_row, extent.first_col) != (self.row, self.col) {
            return Err(unsupported(
                "dynamic array anchor is not the top-left of its extent",
                "worksheet",
            ));
        }
        if formula.text.trim().is_empty() {
            return Err(unsupported("empty dynamic array formula", "worksheet"));
        }
        Ok(())
    }
    pub fn retain(self, formula: FormulaState<'a>, bytes: &[u8]) -> FormulaMark {
        OpenCell {
            row: self.row,
            col: self.col,
            span: self.span,
            open_end: self.open_end,
            qualified: self.qualified,
            kind: self.kind,
            kind_span: self.kind_span,
            value: self.value,
            inline: self.inline,
            dynamic: self.dynamic,
            has_formula: true,
            formula_end: formula.end,
            formula_text: formula.text,
            formula_kind: formula.kind,
            shared_id: formula.shared_id,
            shared_range: formula.shared_range,
            formula_open: formula.open,
            formula_kind_span: formula.kind_span,
        }
        .into_cell(bytes)
    }
    // Keep the emission contract unchanged. These are the same scalar decode
    // and readability proof as emission::{value,refusal}, without constructing
    // an OpenCell or a retained FormulaMark for a literal.
    pub fn decode<'v>(
        &self,
        text: Cow<'v, str>,
        inline: emission::DisplayString<'v>,
    ) -> emission::Value<'v> {
        use emission::Value;
        if self.inline.is_some() {
            return Value::Text(Cow::Owned(inline.finish()));
        }
        let Some(v) = &self.value else {
            return Value::Empty;
        };
        if (v.empty || text.is_empty()) && !matches!(self.kind.as_deref(), Some("str" | "d")) {
            return Value::Empty;
        }
        match self.kind.as_deref() {
            None | Some("n") if text.is_empty() => Value::Empty,
            None | Some("n") => text.parse::<f64>().map_or(Value::Unreadable, Value::Number),
            Some("s") if text.is_empty() => Value::Empty,
            Some("s") => text.parse().map_or(Value::Unreadable, Value::Shared),
            Some("b") => Value::Boolean(text != "0"),
            Some("e") => crate::xlsx::ingest::utilities::parse_error_token(&text)
                .map_or(Value::Unreadable, Value::Error),
            Some("str" | "d") => Value::Text(text),
            Some("inlineStr") => Value::Empty,
            _ => Value::Unreadable,
        }
    }
    pub fn refusal(&self, bytes: &[u8], decoded: &emission::Value<'_>) -> Option<&'static str> {
        use emission::Value;
        if self.kind.as_deref() == Some("e") && !matches!(decoded, Value::Error(_)) {
            return Some("literal error value unsupported by Calamine");
        }
        if !matches!(self.kind.as_deref(), None | Some("n" | "s" | "b" | "e")) {
            return None;
        }
        let Some(v) = &self.value else {
            return None;
        };
        let readable = if v.empty || v.text.is_empty() {
            matches!(self.kind.as_deref(), None | Some("n"))
        } else if bytes.get(v.open_end..v.close_start) != Some(v.text.as_bytes()) {
            false
        } else {
            match decoded {
                Value::Number(n) => n.is_finite(),
                Value::Shared(_) | Value::Error(_) => true,
                Value::Boolean(_) => matches!(v.text.as_ref(), "0" | "1"),
                _ => false,
            }
        };
        (!readable).then_some("literal scalar payload is not supported by Calamine")
    }
}
