//! Recalculation's options and differential oracle over the shared XML walker.
use super::{IoError, XlsxRecalculateOptions};
pub(super) use crate::xlsx_xml::*;

#[cfg(test)]
pub(super) mod reference;

pub(super) fn walk<'a>(
    bytes: &'a [u8],
    options: &XlsxRecalculateOptions,
    visit: impl FnMut(&[Element<'a>], Node<'_, 'a>) -> Result<(), IoError>,
) -> Result<(), IoError> {
    #[cfg(test)]
    super::tests::scan_differential::shadow_walk(bytes, options);
    crate::xlsx_xml::walk(bytes, &options.into(), visit)
}
