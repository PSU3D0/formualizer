use crate::binding::{ManifestBindings, PortBinding};
use crate::error::SheetPortError;
use formualizer_workbook::traits::SpreadsheetReader;
use sheetport_spec::Manifest;

/// Runtime container that pairs a manifest with a concrete workbook reader.
pub struct SheetPort<R: SpreadsheetReader> {
    reader: R,
    bindings: ManifestBindings,
}

impl<R: SpreadsheetReader> SheetPort<R> {
    /// Validate the manifest, bind selectors, and retain the reader for future I/O.
    pub fn new(reader: R, manifest: Manifest) -> Result<Self, SheetPortError> {
        let bindings = ManifestBindings::new(manifest)?;
        Ok(Self { reader, bindings })
    }

    /// Immutable access to the underlying reader.
    pub fn reader(&self) -> &R {
        &self.reader
    }

    /// Mutable access to the underlying reader.
    pub fn reader_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Manifest metadata.
    pub fn manifest(&self) -> &Manifest {
        self.bindings.manifest()
    }

    /// Bound ports with resolved selectors.
    pub fn bindings(&self) -> &[PortBinding] {
        self.bindings.bindings()
    }

    /// Split into reader and manifest bindings.
    pub fn into_parts(self) -> (R, ManifestBindings) {
        (self.reader, self.bindings)
    }
}
