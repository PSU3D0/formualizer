//! Borrowed worksheet value and formula-source events, independent of retained
//! publication marks. Workbook-owned shared strings and style indices are
//! resolved by the sink, not copied into per-cell owned records.
use super::*;

/// Dependency-owned until the common reader's error vocabulary is extracted.
pub(in crate::xlsx) type ErrorToken = crate::xlsx::types::CellErrorType;

#[derive(Debug, Clone)]
pub(in crate::xlsx) enum Value<'a> {
    Empty,
    Number(f64),
    Boolean(bool),
    Error(ErrorToken),
    Text(Cow<'a, str>),
    Shared(usize),
    /// Bad scalar caches can occur in generated children and are masked by
    /// ownership before ingestion. Keep the refusal as an event, not a new
    /// earlier admission error.
    Unreadable,
}
#[derive(Debug, Clone)]
pub(in crate::xlsx) struct Literal<'a> {
    pub row: u32,
    pub col: u32,
    pub style: Option<Cow<'a, str>>,
    /// Advisory only: inside a validated top-left array extent already seen
    /// in source order. Final metadata ownership/refusal checks still govern.
    pub generated_child_hint: bool,
    /// Raw attribute value bytes, excluding quotes; entities remain encoded.
    pub raw_coordinate_span: Option<Range<usize>>,
    pub raw_type_span: Option<Range<usize>>,
    pub raw_style_span: Option<Range<usize>>,
    /// Inner `<v>` bytes, excluding tags. Empty for `<v/>`, absent without v.
    pub raw_value_span: Option<Range<usize>>,
    pub value: Value<'a>,
}

impl Literal<'_> {
    pub(in crate::xlsx) fn raw_type<'a>(&self, source: &'a [u8]) -> Option<&'a str> {
        self.raw_type_span
            .as_ref()
            .map(|span| std::str::from_utf8(&source[span.clone()]).expect("admitted UTF-8"))
    }
    pub(in crate::xlsx) fn raw_style<'a>(&self, source: &'a [u8]) -> Option<&'a str> {
        self.raw_style_span
            .as_ref()
            .map(|span| std::str::from_utf8(&source[span.clone()]).expect("admitted UTF-8"))
    }
    pub(in crate::xlsx) fn raw_value<'a>(&self, source: &'a [u8]) -> Option<&'a [u8]> {
        self.raw_value_span
            .as_ref()
            .map(|span| &source[span.clone()])
    }
}

/// Borrowed source identity, independent of publication marks or engine staging.
#[derive(Debug)]
pub(in crate::xlsx) enum FormulaKind<'a> {
    Ordinary {
        text: &'a str,
    },
    SharedAnchor {
        shared_index: Option<u32>,
        declared_range: SourceRect,
        text: &'a str,
    },
    SharedDescendant {
        shared_index: Option<u32>,
    },
    Unsupported {
        kind: &'a str,
        text: &'a str,
    },
}

/// Raw source evidence. Attribute spans exclude quotes and preserve entities.
#[derive(Debug)]
pub(in crate::xlsx) struct FormulaEnvelope {
    pub cell: Range<usize>,
    pub formula: Range<usize>,
    pub raw_coordinate: Option<Range<usize>>,
    pub raw_cell_type: Option<Range<usize>>,
    pub raw_formula_type: Option<Range<usize>>,
    pub raw_shared_index: Option<Range<usize>>,
    pub raw_declared_range: Option<Range<usize>>,
    pub raw_cache: Option<Range<usize>>,
}

#[derive(Debug)]
pub(in crate::xlsx) struct Formula<'a> {
    pub row: u32,
    pub col: u32,
    pub sequence: u64,
    pub offset: usize,
    pub kind: FormulaKind<'a>,
    pub envelope: FormulaEnvelope,
}

/// Ephemeral callbacks cannot retain borrowed formula scratch across cells.
pub(in crate::xlsx) trait Sink {
    /// Ordinary readers may retain formula caches independently of engine values.
    fn retain_formula_cache(&self) -> bool {
        false
    }
    fn formula_cache(&mut self, _literal: Literal<'_>) -> Result<(), IoError> {
        Ok(())
    }
    fn literal(&mut self, _literal: Literal<'_>) -> Result<(), IoError> {
        Ok(())
    }
    fn formula(&mut self, _formula: Formula<'_>) -> Result<(), IoError> {
        Ok(())
    }
}
impl<S: Sink + ?Sized> Sink for &mut S {
    fn retain_formula_cache(&self) -> bool {
        (**self).retain_formula_cache()
    }
    fn formula_cache(&mut self, literal: Literal<'_>) -> Result<(), IoError> {
        (**self).formula_cache(literal)
    }
    fn literal(&mut self, literal: Literal<'_>) -> Result<(), IoError> {
        (**self).literal(literal)
    }
    fn formula(&mut self, formula: Formula<'_>) -> Result<(), IoError> {
        (**self).formula(formula)
    }
}
pub(super) struct Literals<F>(pub F);
impl<F: FnMut(Literal<'_>) -> Result<(), IoError>> Sink for Literals<F> {
    fn literal(&mut self, literal: Literal<'_>) -> Result<(), IoError> {
        (self.0)(literal)
    }
}

/// XML 1.0 text for ingestion. Admission's XML 1.1 normalization is retained
/// separately in the marks oracle; U+0085/U+2028 must not change literal data.
pub(in crate::xlsx) fn text<'a>(
    bytes: &'a [u8],
    node: &xml::Node<'_, 'a>,
    decoded: Cow<'a, str>,
) -> Result<Cow<'a, str>, IoError> {
    let raw = bytes
        .get(node.span.clone())
        .and_then(|s| std::str::from_utf8(s).ok())
        .ok_or_else(|| unsupported("unavailable XML text span", "worksheet"))?;
    if raw.starts_with('&') {
        return Ok(decoded);
    }
    if !raw.contains('\r') {
        return Ok(Cow::Borrowed(raw));
    }
    Ok(Cow::Owned(raw.replace("\r\n", "\n").replace('\r', "\n")))
}

/// SpreadsheetML display strings decode only the byte escape family used
/// by the pinned reader (_x00HH_). Other escape-looking text stays literal.
fn display_text(text: &str) -> Cow<'_, str> {
    if !text.contains("_x00") {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while !rest.is_empty() {
        if let Some(hex) = rest.strip_prefix("_x00").and_then(|s| s.get(..2))
            && rest.as_bytes().get(6) == Some(&b'_')
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(char::from(byte));
            rest = rest.get(7..).unwrap_or_default();
        } else if let Some(ch) = rest.chars().next() {
            out.push(ch);
            rest = rest.get(ch.len_utf8()..).unwrap_or_default();
        }
    }
    Cow::Owned(out)
}

#[derive(Default)]
pub(in crate::xlsx) struct DisplayString<'a> {
    value: String,
    segment: Cow<'a, str>,
    text_closing: Option<&'a str>,
    phonetic: bool,
    preserve: bool,
}
impl<'a> DisplayString<'a> {
    pub(in crate::xlsx) fn event(
        &mut self,
        path: &[xml::Element<'a>],
        node: &xml::Node<'_, 'a>,
        bytes: &'a [u8],
    ) -> Result<(), IoError> {
        let Some(element) = path.last() else {
            return Ok(());
        };
        // The pinned reader's inner text loop owns every event until the
        // first matching qualified end name, not the opening depth. Nested
        // rPh events inside that loop never modify its outer phonetic flag.
        if let Some(closing) = self.text_closing {
            match &node.kind {
                xml::Kind::Text(decoded) => {
                    append(&mut self.segment, text(bytes, node, decoded.clone())?);
                }
                xml::Kind::Close if element.qualified == closing => self.end_text(),
                xml::Kind::Open { empty: true, .. } if element.qualified == closing => {
                    self.end_text();
                }
                _ => {}
            }
            return Ok(());
        }
        match &node.kind {
            // Calamine expands Empty into Start/End and uses a boolean:
            // even a nested <rPh/> resets an enclosing phonetic run.
            xml::Kind::Open { empty, .. } if element.local == "rPh" => {
                self.phonetic = !empty;
            }
            xml::Kind::Close if element.local == "rPh" => self.phonetic = false,
            xml::Kind::Open { empty, .. } if element.local == "t" && !self.phonetic => {
                self.segment = Cow::Borrowed("");
                self.preserve = node
                    .attribute("http://www.w3.org/XML/1998/namespace", "space")
                    .is_some_and(|a| {
                        bytes.get(a.span.clone()).is_some_and(|raw| {
                            raw.ends_with(b"\"preserve\"") || raw.ends_with(b"'preserve'")
                        })
                    });
                if !empty {
                    self.text_closing = Some(element.qualified);
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn end_text(&mut self) {
        let text = if self.preserve {
            self.segment.as_ref()
        } else {
            self.segment.trim_matches([' ', '\t', '\r', '\n'])
        };
        self.value.push_str(&display_text(text));
        self.text_closing = None;
    }
    pub(in crate::xlsx) fn consuming_text(&self) -> bool {
        self.text_closing.is_some()
    }
    pub(in crate::xlsx) fn finish(self) -> String {
        self.value
    }
}

/// Reuse the emitted scalar's decode for the original Calamine-readability
/// admission proof. Formula caches retain the separate unchanged mark policy.
#[cfg(test)]
pub(super) fn refusal(
    cell: &OpenCell<'_>,
    bytes: &[u8],
    decoded: &Value<'_>,
) -> Option<&'static str> {
    if cell.kind.as_deref() == Some("e") && !matches!(decoded, Value::Error(_)) {
        return Some("literal error value unsupported by Calamine");
    }
    if !matches!(cell.kind.as_deref(), None | Some("n" | "s" | "b" | "e")) {
        return None;
    }
    let Some(v) = &cell.value else { return None };
    let readable = if v.empty || v.text.is_empty() {
        matches!(cell.kind.as_deref(), None | Some("n"))
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

#[cfg(test)]
pub(super) fn value<'a>(
    cell: &OpenCell<'_>,
    text: Cow<'a, str>,
    inline: DisplayString<'a>,
) -> Value<'a> {
    if cell.inline.is_some() {
        return Value::Text(Cow::Owned(inline.finish()));
    }
    let Some(v) = &cell.value else {
        return Value::Empty;
    };
    if (v.empty || text.is_empty()) && !matches!(cell.kind.as_deref(), Some("str" | "d")) {
        return Value::Empty;
    }
    match cell.kind.as_deref() {
        None | Some("n") if text.is_empty() => Value::Empty,
        None | Some("n") => text.parse::<f64>().map_or(Value::Unreadable, Value::Number),
        Some("s") if text.is_empty() => Value::Empty,
        Some("s") => text.parse().map_or(Value::Unreadable, Value::Shared),
        Some("b") => Value::Boolean(text != "0"),
        Some("e") => text.parse().map_or(Value::Unreadable, Value::Error),
        Some("str" | "d") => Value::Text(text),
        Some("inlineStr") => Value::Empty,
        _ => Value::Unreadable,
    }
}
