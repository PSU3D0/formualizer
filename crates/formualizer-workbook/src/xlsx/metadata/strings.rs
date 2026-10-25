//! Positional shared strings decoded without mutating source XML.
//! Text-owned nested events and XML whitespace preserve reader compatibility.
use crate::xlsx::worksheet::events::DisplayString;
use crate::xlsx::{options::ReadOptions, xml};
use crate::{IoError, cache_recalculate::XlsxRecalculateOptions};

#[derive(Debug, Default)]
pub(in crate::xlsx) struct SharedStringTable(Vec<String>);
impl SharedStringTable {
    pub(in crate::xlsx) fn parse(
        bytes: &[u8],
        options: &XlsxRecalculateOptions,
    ) -> Result<Self, IoError> {
        Self::parse_read(bytes, &ReadOptions::from(options))
    }

    pub(in crate::xlsx) fn parse_direct(
        bytes: &[u8],
        options: &ReadOptions,
    ) -> Result<Self, IoError> {
        crate::xlsx::loader::cells::shared_strings(bytes, options).map(Self)
    }

    /// Extraction uses shared read bounds, not evaluation/publication admission.
    pub(in crate::xlsx) fn parse_read<'a>(
        bytes: &'a [u8],
        options: &ReadOptions,
    ) -> Result<Self, IoError> {
        let mut values = Vec::new();
        let mut current: Option<DisplayString<'_>> = None;
        let mut closing = None;
        let mut done = false;
        let visit = |path: &[xml::Element<'a>], node: xml::Node<'_, 'a>| {
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
        };
        if options.policy == crate::xlsx::options::ReadPolicy::Compatible {
            xml::walk_display_cdata(bytes, options, visit)?;
        } else {
            xml::walk(bytes, options, visit)?;
        }
        Ok(Self(values))
    }
    pub(in crate::xlsx) fn len(&self) -> usize {
        self.0.len()
    }
    pub(in crate::xlsx) fn get(&self, index: usize) -> Result<&str, IoError> {
        self.0.get(index).map(String::as_str).ok_or_else(|| {
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::Unexpected(
                    "Cell string index not found in shared strings table",
                ),
            ))
        })
    }
}
