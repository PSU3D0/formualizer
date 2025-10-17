use crate::SheetPortError;
use crate::binding::ManifestBindings;
use crate::runtime::{EvalOptions, SheetPort};
use crate::value::{InputSnapshot, InputUpdate, OutputSnapshot};
use formualizer_eval::engine::RecalcPlan;
use formualizer_workbook::Workbook;
use sheetport_spec::Manifest;

/// Owned runtime session that keeps workbook state and manifest bindings together.
pub struct SheetPortSession {
    workbook: Workbook,
    bindings: ManifestBindings,
}

impl SheetPortSession {
    /// Construct a new session from an owned workbook and manifest.
    pub fn new(mut workbook: Workbook, manifest: Manifest) -> Result<Self, SheetPortError> {
        let sheetport = SheetPort::new(&mut workbook, manifest)?;
        let (_, bindings) = sheetport.into_parts();
        Ok(Self { workbook, bindings })
    }

    /// Construct a session from components that are already bound.
    pub fn from_parts(
        mut workbook: Workbook,
        bindings: ManifestBindings,
    ) -> Result<Self, SheetPortError> {
        let sheetport = SheetPort::from_bindings(&mut workbook, bindings)?;
        let (_, bindings) = sheetport.into_parts();
        Ok(Self { workbook, bindings })
    }

    /// Immutable access to the underlying workbook.
    pub fn workbook(&self) -> &Workbook {
        &self.workbook
    }

    /// Mutable access to the underlying workbook.
    pub fn workbook_mut(&mut self) -> &mut Workbook {
        &mut self.workbook
    }

    /// Access the manifest that seeded this session.
    pub fn manifest(&self) -> &Manifest {
        self.bindings.manifest()
    }

    /// Borrow the manifest bindings.
    pub fn bindings(&self) -> &[crate::binding::PortBinding] {
        self.bindings.bindings()
    }

    /// Consume the session and return the owned components.
    pub fn into_parts(self) -> (Workbook, ManifestBindings) {
        (self.workbook, self.bindings)
    }

    fn with_sheetport<T>(
        &mut self,
        f: impl FnOnce(&mut SheetPort<'_>) -> Result<T, SheetPortError>,
    ) -> Result<T, SheetPortError> {
        let bindings_clone = self.bindings.clone();
        let mut sheetport = SheetPort::from_bindings(&mut self.workbook, bindings_clone)?;
        let result = f(&mut sheetport)?;
        let (_, bindings) = sheetport.into_parts();
        self.bindings = bindings;
        Ok(result)
    }

    /// Read all input ports as an `InputSnapshot`.
    pub fn read_inputs(&mut self) -> Result<InputSnapshot, SheetPortError> {
        self.with_sheetport(|sp| sp.read_inputs())
    }

    /// Read all output ports as an `OutputSnapshot`.
    pub fn read_outputs(&mut self) -> Result<OutputSnapshot, SheetPortError> {
        self.with_sheetport(|sp| sp.read_outputs())
    }

    /// Apply an input update to the underlying workbook.
    pub fn write_inputs(&mut self, update: InputUpdate) -> Result<(), SheetPortError> {
        self.with_sheetport(|sp| sp.write_inputs(update))
    }

    /// Evaluate the manifest once using the provided options and return outputs.
    pub fn evaluate_once(
        &mut self,
        options: EvalOptions,
    ) -> Result<OutputSnapshot, SheetPortError> {
        self.with_sheetport(move |sp| sp.evaluate_once(options))
    }

    /// Evaluate using a precomputed recalculation plan.
    pub fn evaluate_with_plan(
        &mut self,
        plan: &RecalcPlan,
        options: EvalOptions,
    ) -> Result<OutputSnapshot, SheetPortError> {
        self.with_sheetport(move |sp| sp.evaluate_with_plan(plan, options))
    }
}
