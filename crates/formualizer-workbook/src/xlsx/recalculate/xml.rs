//! Recalculation's options and differential oracle over the shared XML walker.
use super::{IoError, XlsxRecalculateOptions};
pub(in crate::xlsx) use crate::xlsx::xml::*;

#[cfg(test)]
pub(in crate::xlsx) use super::tests::oracles::xml_legacy as reference;

pub(in crate::xlsx) fn walk<'a>(
    bytes: &'a [u8],
    options: &XlsxRecalculateOptions,
    visit: impl FnMut(&[Element<'a>], Node<'_, 'a>) -> Result<(), IoError>,
) -> Result<(), IoError> {
    #[cfg(test)]
    super::tests::oracles::shadow_walk(bytes, options);
    crate::xlsx::xml::walk(bytes, &options.into(), visit)
}
