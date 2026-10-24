//! Transient cell state. Raw source spellings stay separate from decoded text.
use super::*;

#[derive(Default)]
pub(super) struct OpenCellState<'a> {
    pub cell: Option<ScalarCell<'a>>,
    pub formula: FormulaState<'a>,
    pub style: Option<Cow<'a, str>>,
    pub raw_coordinate: Option<Range<usize>>,
    pub raw_type: Option<Range<usize>>,
    pub raw_style: Option<Range<usize>>,
    pub literal_text: Cow<'a, str>,
    pub inline_text: emission::DisplayString<'a>,
}

impl<'a> OpenCellState<'a> {
    pub fn is_inline(&self) -> bool {
        self.cell
            .as_ref()
            .is_some_and(|c| c.inline.is_some() && !c.has_formula)
    }

    pub fn text(
        &mut self,
        value: Cow<'a, str>,
        span: Range<usize>,
        text: &'a str,
        path: &[xml::Element<'a>],
        position: Position,
    ) -> Result<(), IoError> {
        if self.is_inline() {
            let node = xml::Node::new(xml::Kind::Text(value.clone()), span.clone());
            self.inline_text.event(path, &node, text.as_bytes())?;
        }
        if let Some(cell) = self.cell.as_mut() {
            match position {
                Position::CellFormula => append(&mut self.formula.text, value),
                Position::CellValue => {
                    if !cell.has_formula {
                        let raw = text
                            .get(span)
                            .ok_or_else(|| unsupported("unavailable XML text span", "worksheet"))?;
                        let literal = if raw.starts_with('&') {
                            value.clone()
                        } else if !raw.contains('\r') {
                            Cow::Borrowed(raw)
                        } else {
                            Cow::Owned(raw.replace("\r\n", "\n").replace('\r', "\n"))
                        };
                        append(&mut self.literal_text, literal);
                    }
                    append(&mut cell.value.as_mut().expect("opened v").text, value);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn close_payload(&mut self, local: Local, span: &Range<usize>) -> Result<(), IoError> {
        let cell = self
            .cell
            .as_mut()
            .ok_or_else(|| unsupported("unbalanced cell payload", "worksheet"))?;
        match local {
            Local::F => self.formula.end = span.end,
            Local::V => {
                let value = cell.value.as_mut().expect("opened v");
                value.close_start = span.start;
                value.span.end = span.end;
            }
            Local::Is => cell.inline.as_mut().expect("opened is").end = span.end,
            _ => {}
        }
        Ok(())
    }
}
