//! Positional shared strings decoded without mutating source XML.
//! Text-owned nested events and XML whitespace preserve reader compatibility.
use crate::xlsx::recalculate::sheet::events::DisplayString;
use crate::xlsx::recalculate::{IoError, XlsxRecalculateOptions, xml};

#[derive(Debug, Default)]
pub(in crate::xlsx) struct SharedStringTable(Vec<String>);
impl SharedStringTable {
    pub(in crate::xlsx) fn parse(
        bytes: &[u8],
        options: &XlsxRecalculateOptions,
    ) -> Result<Self, IoError> {
        let mut values = Vec::new();
        let mut current: Option<DisplayString<'_>> = None;
        let mut closing = None;
        let mut done = false;
        xml::walk(bytes, options, |path, node| {
            let Some(element) = path.last() else {
                return Ok(());
            };
            if done {
                return Ok(());
            }
            // Once a t segment starts, its qualified-name loop consumes even
            // nested si/sst events before the outer display reader sees them.
            if let Some(value) = current.as_mut()
                && value.consuming_text()
            {
                value.event(path, &node, bytes)?;
                return Ok(());
            }
            match &node.kind {
                xml::Kind::Close if current.is_none() && element.local == "sst" => done = true,
                xml::Kind::Open { empty, .. } if element.local == "si" && current.is_none() => {
                    if *empty {
                        // Every si has a positional entry, including <si/>.
                        values.push(String::new());
                    } else {
                        current = Some(DisplayString::default());
                        closing = Some(element.qualified);
                    }
                }
                xml::Kind::Close if closing == Some(element.qualified) => {
                    if let Some(value) = current.take() {
                        values.push(value.finish());
                    }
                    closing = None;
                }
                _ => {
                    if let Some(value) = current.as_mut() {
                        value.event(path, &node, bytes)?;
                    }
                }
            }
            Ok(())
        })?;
        Ok(Self(values))
    }
    pub(in crate::xlsx) fn len(&self) -> usize {
        self.0.len()
    }
    pub(in crate::xlsx) fn get(&self, index: usize) -> Result<&str, IoError> {
        self.0.get(index).map(String::as_str).ok_or_else(|| {
            IoError::Calamine(calamine::Error::Xlsx(calamine::XlsxError::Unexpected(
                "Cell string index not found in shared strings table",
            )))
        })
    }
}
