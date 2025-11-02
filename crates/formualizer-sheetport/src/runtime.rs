use crate::binding::{
    BoundPort, ManifestBindings, PortBinding, RangeBinding, RecordBinding, RecordFieldBinding,
    ScalarBinding, TableBinding,
};
use crate::context::WorkbookContext;
use crate::error::SheetPortError;
use crate::layout::{resolve_range_layout, resolve_table_layout};
use crate::validation::{ValidationScope, validate_port_value};
use crate::value::{InputSnapshot, InputUpdate, OutputSnapshot, PortValue, TableRow, TableValue};
use crate::{BatchExecutor, BatchOptions};
use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_eval::engine::RecalcPlan;
use formualizer_eval::traits::VolatileLevel;
use formualizer_workbook::Workbook;
use sheetport_spec::{Direction, Manifest};
use std::collections::{BTreeMap, BTreeSet};

struct GridWrite<'a> {
    port_id: &'a str,
    sheet: &'a str,
    start_row: u32,
    start_col: u32,
    height: u32,
    width: u32,
    grid: Vec<Vec<LiteralValue>>,
}

/// Runtime container that pairs a manifest with a concrete workbook.
pub struct SheetPort<'a> {
    workbook: &'a mut Workbook,
    bindings: ManifestBindings,
}

#[derive(Debug, Clone)]
pub struct EvalOptions {
    pub freeze_volatile: bool,
    pub rng_seed: Option<u64>,
    pub mode: EvalMode,
}

impl Default for EvalOptions {
    fn default() -> Self {
        Self {
            freeze_volatile: false,
            rng_seed: None,
            mode: EvalMode::Full,
        }
    }
}

#[derive(Debug, Clone)]
pub enum EvalMode {
    Full,
}

impl<'a> SheetPort<'a> {
    /// Validate the manifest, bind selectors, and retain the reader for future I/O.
    pub fn new(workbook: &'a mut Workbook, manifest: Manifest) -> Result<Self, SheetPortError> {
        let bindings = ManifestBindings::new(manifest)?;
        let ctx = WorkbookContext::new(&*workbook);
        ctx.validate(&bindings)?;
        Ok(Self { workbook, bindings })
    }

    /// Construct a SheetPort using pre-bound manifest bindings.
    pub fn from_bindings(
        workbook: &'a mut Workbook,
        bindings: ManifestBindings,
    ) -> Result<Self, SheetPortError> {
        let ctx = WorkbookContext::new(&*workbook);
        ctx.validate(&bindings)?;
        Ok(Self { workbook, bindings })
    }

    /// Immutable access to the underlying workbook.
    pub fn workbook(&self) -> &Workbook {
        &*self.workbook
    }

    /// Mutable access to the underlying workbook.
    pub fn workbook_mut(&mut self) -> &mut Workbook {
        &mut *self.workbook
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
    pub fn into_parts(self) -> (&'a mut Workbook, ManifestBindings) {
        (self.workbook, self.bindings)
    }

    pub fn read_inputs(&mut self) -> Result<InputSnapshot, SheetPortError> {
        let bindings: Vec<PortBinding> = self
            .bindings
            .bindings()
            .iter()
            .filter(|binding| binding.direction == Direction::In)
            .cloned()
            .collect();
        let mut map = BTreeMap::new();
        for binding in bindings.iter() {
            let value = self.read_port_value(binding)?;
            map.insert(binding.id.clone(), value);
        }
        Ok(InputSnapshot::new(map))
    }

    pub fn read_outputs(&mut self) -> Result<OutputSnapshot, SheetPortError> {
        let bindings: Vec<PortBinding> = self
            .bindings
            .bindings()
            .iter()
            .filter(|binding| binding.direction == Direction::Out)
            .cloned()
            .collect();
        let mut map = BTreeMap::new();
        for binding in bindings.iter() {
            let value = self.read_port_value(binding)?;
            map.insert(binding.id.clone(), value);
        }
        Ok(OutputSnapshot::new(map))
    }

    pub fn write_inputs(&mut self, update: InputUpdate) -> Result<(), SheetPortError> {
        for (port_id, value) in update.into_inner() {
            let binding =
                self.bindings
                    .get(&port_id)
                    .ok_or_else(|| SheetPortError::InvariantViolation {
                        port: port_id.clone(),
                        message: "unknown port".to_string(),
                    })?;
            if binding.direction != Direction::In {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id,
                    message: "cannot write to output port".to_string(),
                });
            }
            let scope = match &binding.kind {
                BoundPort::Record(_) => ValidationScope::Partial,
                _ => ValidationScope::Full,
            };
            if let Err(violations) = validate_port_value(binding, &value, scope) {
                return Err(SheetPortError::ConstraintViolation { violations });
            }
            let binding_clone = binding.clone();
            self.write_port_value(&binding_clone, value)?;
        }
        Ok(())
    }

    pub fn evaluate_once(
        &mut self,
        options: EvalOptions,
    ) -> Result<OutputSnapshot, SheetPortError> {
        let restore = self.apply_eval_options(&options);
        let result = (|| -> Result<OutputSnapshot, SheetPortError> {
            if self.outputs_require_full_eval() {
                self.workbook.prepare_graph_all()?;
                self.workbook.evaluate_all()?;
                return self.read_outputs();
            }

            let target_specs = self.collect_output_targets()?;
            if target_specs.is_empty() {
                self.workbook.prepare_graph_all()?;
                self.workbook.evaluate_all()?;
            } else {
                let mut sheets: BTreeSet<&str> = BTreeSet::new();
                for (sheet, _, _) in target_specs.iter() {
                    sheets.insert(sheet.as_str());
                }
                self.workbook
                    .prepare_graph_for_sheets(sheets.iter().copied())?;
                let borrowed: Vec<(&str, u32, u32)> = target_specs
                    .iter()
                    .map(|(sheet, row, col)| (sheet.as_str(), *row, *col))
                    .collect();
                if self.workbook.evaluate_cells(&borrowed).is_err() {
                    self.workbook.prepare_graph_all()?;
                    self.workbook.evaluate_all()?;
                }
            }
            self.read_outputs()
        })();
        self.restore_eval_options(restore);
        result
    }

    fn outputs_require_full_eval(&self) -> bool {
        self.bindings
            .bindings()
            .iter()
            .filter(|binding| binding.direction == Direction::Out)
            .any(|binding| match &binding.kind {
                BoundPort::Scalar(scalar) => {
                    matches!(scalar.location, crate::location::ScalarLocation::Name(_))
                }
                BoundPort::Record(record) => record
                    .fields
                    .values()
                    .any(|field| matches!(field.location, crate::location::FieldLocation::Name(_))),
                BoundPort::Range(range) => {
                    matches!(range.location, crate::location::AreaLocation::Name(_))
                }
                BoundPort::Table(table) => {
                    matches!(table.location, crate::location::TableLocation::Table(_))
                }
            })
    }

    fn collect_output_targets(&mut self) -> Result<Vec<(String, u32, u32)>, SheetPortError> {
        let mut targets: BTreeSet<(String, u32, u32)> = BTreeSet::new();
        let output_bindings: Vec<PortBinding> = self
            .bindings
            .bindings()
            .iter()
            .filter(|binding| binding.direction == Direction::Out)
            .cloned()
            .collect();
        for binding in output_bindings.iter() {
            self.collect_binding_targets(binding, &mut targets)?;
        }
        Ok(targets.into_iter().collect())
    }

    fn collect_binding_targets(
        &mut self,
        binding: &PortBinding,
        targets: &mut BTreeSet<(String, u32, u32)>,
    ) -> Result<(), SheetPortError> {
        match &binding.kind {
            BoundPort::Scalar(scalar) => match &scalar.location {
                crate::location::ScalarLocation::Cell(addr) => {
                    Self::add_range_cells(targets, addr);
                    Ok(())
                }
                crate::location::ScalarLocation::Name(name) => {
                    let addr = self.named_range_address(&binding.id, name)?;
                    if addr.height() != 1 || addr.width() != 1 {
                        return Err(SheetPortError::InvariantViolation {
                            port: binding.id.clone(),
                            message: format!(
                                "named range `{name}` must resolve to a single cell for scalar ports"
                            ),
                        });
                    }
                    Self::add_range_cells(targets, &addr);
                    Ok(())
                }
                crate::location::ScalarLocation::StructRef(struct_ref) => {
                    Err(SheetPortError::UnsupportedSelector {
                        port: binding.id.clone(),
                        reason: format!(
                            "scalar selectors using structured reference `{struct_ref}` \
                             are not supported yet"
                        ),
                    })
                }
            },
            BoundPort::Record(record) => {
                for (field_name, field_binding) in &record.fields {
                    match &field_binding.location {
                        crate::location::FieldLocation::Cell(addr) => {
                            Self::add_range_cells(targets, addr);
                        }
                        crate::location::FieldLocation::Name(name) => {
                            let addr = self.named_range_address(&binding.id, name)?;
                            if addr.height() != 1 || addr.width() != 1 {
                                return Err(SheetPortError::InvariantViolation {
                                    port: binding.id.clone(),
                                    message: format!(
                                        "record field `{field_name}` named range `{name}` \
                                         must resolve to a single cell"
                                    ),
                                });
                            }
                            Self::add_range_cells(targets, &addr);
                        }
                        crate::location::FieldLocation::StructRef(struct_ref) => {
                            return Err(SheetPortError::UnsupportedSelector {
                                port: binding.id.clone(),
                                reason: format!(
                                    "record field `{field_name}` uses unsupported selector `{struct_ref}`"
                                ),
                            });
                        }
                    }
                }
                Ok(())
            }
            BoundPort::Range(range) => match &range.location {
                crate::location::AreaLocation::Range(addr) => {
                    Self::add_range_cells(targets, addr);
                    Ok(())
                }
                crate::location::AreaLocation::Name(name) => {
                    let addr = self.named_range_address(&binding.id, name)?;
                    Self::add_range_cells(targets, &addr);
                    Ok(())
                }
                crate::location::AreaLocation::Layout(layout) => {
                    let bounds = resolve_range_layout(&binding.id, self.workbook, layout)?;
                    let start_row = bounds.start_row;
                    let end_row = bounds.end_row.max(bounds.start_row);
                    let start_col = bounds.start_col;
                    let end_col = bounds.end_col.max(bounds.start_col);
                    let sheet = bounds.sheet.clone();
                    let addr =
                        RangeAddress::new(sheet.clone(), start_row, start_col, end_row, end_col)
                            .map_err(|msg| SheetPortError::InvariantViolation {
                                port: binding.id.clone(),
                                message: msg.to_string(),
                            })?;
                    Self::add_range_cells(targets, &addr);
                    match layout.terminate {
                        sheetport_spec::LayoutTermination::FirstBlankRow
                        | sheetport_spec::LayoutTermination::UntilMarker => {
                            let sentinel_row = if end_row >= start_row {
                                end_row.saturating_add(1)
                            } else {
                                start_row
                            };
                            for col in start_col..=end_col {
                                Self::add_cell(targets, sheet.as_str(), sentinel_row, col);
                            }
                        }
                        sheetport_spec::LayoutTermination::SheetEnd => {}
                    }
                    Ok(())
                }
                other => Err(SheetPortError::UnsupportedSelector {
                    port: binding.id.clone(),
                    reason: format!("unsupported area selector `{other:?}` for range port"),
                }),
            },
            BoundPort::Table(table) => match &table.location {
                crate::location::TableLocation::Layout(layout) => {
                    let column_hints: Vec<Option<String>> = table
                        .columns
                        .iter()
                        .map(|c| c.column_hint.clone())
                        .collect();
                    let bounds =
                        resolve_table_layout(&binding.id, self.workbook, layout, &column_hints)?;
                    let header_row = layout.header_row;
                    for &col in &bounds.column_indices {
                        Self::add_cell(targets, &bounds.sheet, header_row, col);
                    }
                    if bounds.data_end_row >= bounds.data_start_row {
                        for row in bounds.data_start_row..=bounds.data_end_row {
                            for &col in &bounds.column_indices {
                                Self::add_cell(targets, &bounds.sheet, row, col);
                            }
                        }
                    }
                    match layout.terminate {
                        sheetport_spec::LayoutTermination::FirstBlankRow
                        | sheetport_spec::LayoutTermination::UntilMarker => {
                            let sentinel_row = if bounds.data_end_row >= bounds.data_start_row {
                                bounds.data_end_row.saturating_add(1)
                            } else {
                                bounds.data_start_row
                            };
                            for &col in &bounds.column_indices {
                                Self::add_cell(targets, &bounds.sheet, sentinel_row, col);
                            }
                        }
                        sheetport_spec::LayoutTermination::SheetEnd => {}
                    }
                    Ok(())
                }
                crate::location::TableLocation::Table(selector) => {
                    Err(SheetPortError::UnsupportedSelector {
                        port: binding.id.clone(),
                        reason: format!(
                            "native table `{}` selectors are not supported yet",
                            selector.name
                        ),
                    })
                }
            },
        }
    }

    fn add_range_cells(targets: &mut BTreeSet<(String, u32, u32)>, addr: &RangeAddress) {
        for row in addr.start_row..=addr.end_row {
            for col in addr.start_col..=addr.end_col {
                Self::add_cell(targets, &addr.sheet, row, col);
            }
        }
    }

    fn add_cell(targets: &mut BTreeSet<(String, u32, u32)>, sheet: &str, row: u32, col: u32) {
        targets.insert((sheet.to_string(), row, col));
    }

    pub fn evaluate_with_plan(
        &mut self,
        plan: &RecalcPlan,
        options: EvalOptions,
    ) -> Result<OutputSnapshot, SheetPortError> {
        let restore = self.apply_eval_options(&options);
        let result = (|| -> Result<OutputSnapshot, SheetPortError> {
            self.workbook.evaluate_with_plan(plan)?;
            self.read_outputs()
        })();
        self.restore_eval_options(restore);
        result
    }

    pub fn batch(
        &'a mut self,
        options: BatchOptions<'a>,
    ) -> Result<BatchExecutor<'a>, SheetPortError> {
        let baseline = self.read_inputs()?;
        let baseline_update = baseline.to_update();
        let plan = self.workbook().engine().build_recalc_plan()?;
        Ok(BatchExecutor::new(self, baseline_update, options, plan))
    }

    fn read_port_value(&mut self, binding: &PortBinding) -> Result<PortValue, SheetPortError> {
        let mut value = match &binding.kind {
            BoundPort::Scalar(scalar) => self.read_scalar(binding, scalar),
            BoundPort::Record(record) => self.read_record(binding, record),
            BoundPort::Range(range) => self.read_range(binding, range),
            BoundPort::Table(table) => self.read_table(binding, table),
        }?;
        value = apply_defaults(binding, value);
        if let Err(violations) = validate_port_value(binding, &value, ValidationScope::Full) {
            return Err(SheetPortError::ConstraintViolation { violations });
        }
        Ok(value)
    }

    fn read_scalar(
        &self,
        binding: &PortBinding,
        scalar: &ScalarBinding,
    ) -> Result<PortValue, SheetPortError> {
        match &scalar.location {
            crate::location::ScalarLocation::Cell(addr) => {
                let value = self
                    .workbook
                    .get_value(&addr.sheet, addr.start_row, addr.start_col)
                    .unwrap_or(LiteralValue::Empty);
                Ok(PortValue::Scalar(value))
            }
            crate::location::ScalarLocation::Name(name) => {
                let addr = self.named_range_address(&binding.id, name)?;
                if addr.height() != 1 || addr.width() != 1 {
                    return Err(SheetPortError::InvariantViolation {
                        port: binding.id.clone(),
                        message: format!(
                            "named range `{name}` must resolve to a single cell for scalar ports"
                        ),
                    });
                }
                let value = self
                    .workbook
                    .get_value(&addr.sheet, addr.start_row, addr.start_col)
                    .unwrap_or(LiteralValue::Empty);
                Ok(PortValue::Scalar(value))
            }
            _ => Err(SheetPortError::UnsupportedSelector {
                port: binding.id.clone(),
                reason: "scalar selectors beyond cells or named ranges are not supported yet"
                    .to_string(),
            }),
        }
    }

    fn read_record(
        &self,
        binding: &PortBinding,
        record: &RecordBinding,
    ) -> Result<PortValue, SheetPortError> {
        let mut map = BTreeMap::new();
        for (field_name, field_binding) in &record.fields {
            let value = self.read_field_value(binding.id.as_str(), field_binding)?;
            map.insert(field_name.clone(), value);
        }
        Ok(PortValue::Record(map))
    }

    fn read_field_value(
        &self,
        port_id: &str,
        field: &RecordFieldBinding,
    ) -> Result<LiteralValue, SheetPortError> {
        match &field.location {
            crate::location::FieldLocation::Cell(addr) => Ok(self
                .workbook
                .get_value(&addr.sheet, addr.start_row, addr.start_col)
                .unwrap_or(LiteralValue::Empty)),
            crate::location::FieldLocation::Name(name) => {
                let addr = self.named_range_address(port_id, name)?;
                if addr.height() != 1 || addr.width() != 1 {
                    return Err(SheetPortError::InvariantViolation {
                        port: port_id.to_string(),
                        message: format!(
                            "named range `{name}` must resolve to a single cell for record fields"
                        ),
                    });
                }
                Ok(self
                    .workbook
                    .get_value(&addr.sheet, addr.start_row, addr.start_col)
                    .unwrap_or(LiteralValue::Empty))
            }
            crate::location::FieldLocation::StructRef(struct_ref) => {
                Err(SheetPortError::UnsupportedSelector {
                    port: port_id.to_string(),
                    reason: format!("structured reference `{struct_ref}` is not yet supported"),
                })
            }
        }
    }

    fn read_range(
        &mut self,
        binding: &PortBinding,
        range: &RangeBinding,
    ) -> Result<PortValue, SheetPortError> {
        let grid = match &range.location {
            crate::location::AreaLocation::Range(addr) => self.workbook.read_range(addr),
            crate::location::AreaLocation::Name(name) => {
                let addr = self.named_range_address(&binding.id, name)?;
                self.workbook.read_range(&addr)
            }
            crate::location::AreaLocation::Layout(layout) => {
                let bounds = resolve_range_layout(&binding.id, self.workbook, layout)?;
                let start_row = bounds.start_row;
                let end_row = bounds.end_row.max(bounds.start_row);
                let start_col = bounds.start_col;
                let end_col = bounds.end_col.max(bounds.start_col);
                let addr = RangeAddress::new(bounds.sheet, start_row, start_col, end_row, end_col)
                    .map_err(|msg| SheetPortError::InvariantViolation {
                        port: binding.id.clone(),
                        message: msg.to_string(),
                    })?;
                self.workbook.read_range(&addr)
            }
            other => {
                return Err(SheetPortError::UnsupportedSelector {
                    port: binding.id.clone(),
                    reason: format!("unsupported area selector `{other:?}` for range port"),
                });
            }
        };
        Ok(PortValue::Range(grid))
    }

    fn read_table(
        &mut self,
        binding: &PortBinding,
        table: &TableBinding,
    ) -> Result<PortValue, SheetPortError> {
        match &table.location {
            crate::location::TableLocation::Layout(layout) => {
                let column_hints: Vec<Option<String>> = table
                    .columns
                    .iter()
                    .map(|c| c.column_hint.clone())
                    .collect();
                let bounds =
                    resolve_table_layout(&binding.id, self.workbook, layout, &column_hints)?;
                let mut rows = Vec::new();
                if bounds.data_end_row >= bounds.data_start_row {
                    for row_idx in bounds.data_start_row..=bounds.data_end_row {
                        let mut values = BTreeMap::new();
                        for (col_binding, &col_index) in
                            table.columns.iter().zip(bounds.column_indices.iter())
                        {
                            let value = self
                                .workbook
                                .get_value(&bounds.sheet, row_idx, col_index)
                                .unwrap_or(LiteralValue::Empty);
                            values.insert(col_binding.name.clone(), value);
                        }
                        rows.push(TableRow::new(values));
                    }
                }
                Ok(PortValue::Table(TableValue::new(rows)))
            }
            crate::location::TableLocation::Table(table_selector) => {
                Err(SheetPortError::UnsupportedSelector {
                    port: binding.id.clone(),
                    reason: format!(
                        "native table `{}` selectors are not supported yet",
                        table_selector.name
                    ),
                })
            }
        }
    }

    fn write_port_value(
        &mut self,
        binding: &PortBinding,
        value: PortValue,
    ) -> Result<(), SheetPortError> {
        match (binding.kind.clone(), value) {
            (BoundPort::Scalar(scalar), PortValue::Scalar(val)) => {
                self.write_scalar(binding, &scalar, val)
            }
            (BoundPort::Record(record), PortValue::Record(map)) => {
                self.write_record(binding, &record, map)
            }
            (BoundPort::Range(range), PortValue::Range(grid)) => {
                self.write_range(binding, &range, grid)
            }
            (BoundPort::Table(table), PortValue::Table(rows)) => {
                self.write_table(binding, &table, rows)
            }
            (_, unexpected) => Err(SheetPortError::InvariantViolation {
                port: binding.id.clone(),
                message: format!(
                    "port value did not match expected shape: got {:?}",
                    unexpected
                ),
            }),
        }
    }

    fn write_scalar(
        &mut self,
        binding: &PortBinding,
        scalar: &ScalarBinding,
        value: LiteralValue,
    ) -> Result<(), SheetPortError> {
        match &scalar.location {
            crate::location::ScalarLocation::Cell(addr) => self
                .workbook
                .set_value(&addr.sheet, addr.start_row, addr.start_col, value)
                .map_err(SheetPortError::from),
            crate::location::ScalarLocation::Name(name) => {
                let addr = self.named_range_address(&binding.id, name)?;
                if addr.height() != 1 || addr.width() != 1 {
                    return Err(SheetPortError::InvariantViolation {
                        port: binding.id.clone(),
                        message: format!(
                            "named range `{name}` must resolve to a single cell for scalar ports"
                        ),
                    });
                }
                self.workbook
                    .set_value(&addr.sheet, addr.start_row, addr.start_col, value)
                    .map_err(SheetPortError::from)
            }
            _ => Err(SheetPortError::UnsupportedSelector {
                port: binding.id.clone(),
                reason: "scalar selectors beyond cells are not supported yet".to_string(),
            }),
        }
    }

    fn write_record(
        &mut self,
        binding: &PortBinding,
        record: &RecordBinding,
        update: BTreeMap<String, LiteralValue>,
    ) -> Result<(), SheetPortError> {
        for (field_name, value) in update {
            let field_binding = record.fields.get(&field_name).ok_or_else(|| {
                SheetPortError::InvariantViolation {
                    port: binding.id.clone(),
                    message: format!("unknown record field `{field_name}`"),
                }
            })?;
            match &field_binding.location {
                crate::location::FieldLocation::Cell(addr) => {
                    self.workbook
                        .set_value(&addr.sheet, addr.start_row, addr.start_col, value)
                        .map_err(SheetPortError::from)?;
                }
                crate::location::FieldLocation::Name(name) => {
                    let addr = self.named_range_address(&binding.id, name)?;
                    if addr.height() != 1 || addr.width() != 1 {
                        return Err(SheetPortError::InvariantViolation {
                            port: binding.id.clone(),
                            message: format!(
                                "record field `{field_name}` named range `{name}` must resolve to a single cell"
                            ),
                        });
                    }
                    self.workbook
                        .set_value(&addr.sheet, addr.start_row, addr.start_col, value)
                        .map_err(SheetPortError::from)?;
                }
                _ => {
                    return Err(SheetPortError::UnsupportedSelector {
                        port: binding.id.clone(),
                        reason: format!("record field `{field_name}` uses unsupported selector"),
                    });
                }
            }
        }
        Ok(())
    }

    fn write_range(
        &mut self,
        binding: &PortBinding,
        range: &RangeBinding,
        grid: Vec<Vec<LiteralValue>>,
    ) -> Result<(), SheetPortError> {
        match &range.location {
            crate::location::AreaLocation::Range(addr) => self.write_grid(GridWrite {
                port_id: binding.id.as_str(),
                sheet: &addr.sheet,
                start_row: addr.start_row,
                start_col: addr.start_col,
                height: addr.height(),
                width: addr.width(),
                grid,
            }),
            crate::location::AreaLocation::Layout(layout) => {
                let bounds = resolve_range_layout(&binding.id, self.workbook, layout)?;
                let expected_width = bounds.columns.len() as u32;
                if grid.first().map(|row| row.len() as u32).unwrap_or(0) != expected_width {
                    return Err(SheetPortError::InvariantViolation {
                        port: binding.id.clone(),
                        message: "range update width does not match layout".to_string(),
                    });
                }
                let height = grid.len() as u32;
                self.write_grid(GridWrite {
                    port_id: binding.id.as_str(),
                    sheet: &bounds.sheet,
                    start_row: bounds.start_row,
                    start_col: bounds.start_col,
                    height,
                    width: expected_width,
                    grid,
                })
            }
            crate::location::AreaLocation::Name(name) => {
                let addr = self.named_range_address(&binding.id, name)?;
                let expected_width = addr.width();
                if grid.first().map(|row| row.len() as u32).unwrap_or(0) != expected_width {
                    return Err(SheetPortError::InvariantViolation {
                        port: binding.id.clone(),
                        message: format!(
                            "range update width does not match named range `{name}` width"
                        ),
                    });
                }
                let height = grid.len() as u32;
                if height != addr.height() {
                    return Err(SheetPortError::InvariantViolation {
                        port: binding.id.clone(),
                        message: format!(
                            "range update height does not match named range `{name}` height"
                        ),
                    });
                }
                self.write_grid(GridWrite {
                    port_id: binding.id.as_str(),
                    sheet: &addr.sheet,
                    start_row: addr.start_row,
                    start_col: addr.start_col,
                    height,
                    width: expected_width,
                    grid,
                })
            }
            other => Err(SheetPortError::UnsupportedSelector {
                port: binding.id.clone(),
                reason: format!("unsupported area selector `{other:?}` for range port"),
            }),
        }
    }

    fn write_grid(&mut self, params: GridWrite<'_>) -> Result<(), SheetPortError> {
        let GridWrite {
            port_id,
            sheet,
            start_row,
            start_col,
            height,
            width,
            grid,
        } = params;
        if grid.len() as u32 != height {
            return Err(SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: "range update height mismatch".to_string(),
            });
        }
        for (row_offset, row) in grid.into_iter().enumerate() {
            if row.len() as u32 != width {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: "range row width mismatch".to_string(),
                });
            }
            let row_idx = start_row + row_offset as u32;
            for (col_offset, value) in row.into_iter().enumerate() {
                let col_idx = start_col + col_offset as u32;
                self.workbook
                    .set_value(sheet, row_idx, col_idx, value)
                    .map_err(SheetPortError::from)?;
            }
        }
        Ok(())
    }

    fn named_range_address(
        &self,
        port_id: &str,
        name: &str,
    ) -> Result<RangeAddress, SheetPortError> {
        self.workbook
            .named_range_address(name)
            .ok_or_else(|| SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: format!("named range `{name}` was not found in the workbook"),
            })
    }

    fn write_table(
        &mut self,
        binding: &PortBinding,
        table: &TableBinding,
        value: TableValue,
    ) -> Result<(), SheetPortError> {
        match &table.location {
            crate::location::TableLocation::Layout(layout) => {
                let column_hints: Vec<Option<String>> = table
                    .columns
                    .iter()
                    .map(|c| c.column_hint.clone())
                    .collect();
                let bounds =
                    resolve_table_layout(&binding.id, self.workbook, layout, &column_hints)?;

                let existing_row_count = if bounds.data_end_row >= bounds.data_start_row {
                    bounds.data_end_row - bounds.data_start_row + 1
                } else {
                    0
                };
                let rows = value.rows;
                let new_row_count = rows.len() as u32;

                for (row_offset, row) in rows.iter().enumerate() {
                    let row_idx = bounds.data_start_row + row_offset as u32;
                    for (col_binding, &col_index) in
                        table.columns.iter().zip(bounds.column_indices.iter())
                    {
                        let cell_value = row
                            .values
                            .get(&col_binding.name)
                            .cloned()
                            .unwrap_or(LiteralValue::Empty);
                        self.workbook
                            .set_value(&bounds.sheet, row_idx, col_index, cell_value)
                            .map_err(SheetPortError::from)?;
                    }
                }

                if new_row_count < existing_row_count {
                    for row in (bounds.data_start_row + new_row_count)..=bounds.data_end_row {
                        for &col_index in &bounds.column_indices {
                            self.workbook
                                .set_value(&bounds.sheet, row, col_index, LiteralValue::Empty)
                                .map_err(SheetPortError::from)?;
                        }
                    }
                }

                match layout.terminate {
                    sheetport_spec::LayoutTermination::FirstBlankRow
                    | sheetport_spec::LayoutTermination::UntilMarker => {
                        let blank_row = bounds.data_start_row + new_row_count;
                        for &col_index in &bounds.column_indices {
                            self.workbook
                                .set_value(&bounds.sheet, blank_row, col_index, LiteralValue::Empty)
                                .map_err(SheetPortError::from)?;
                        }
                    }
                    sheetport_spec::LayoutTermination::SheetEnd => {}
                }
                Ok(())
            }
            crate::location::TableLocation::Table(selector) => {
                Err(SheetPortError::UnsupportedSelector {
                    port: binding.id.clone(),
                    reason: format!(
                        "native table `{}` selectors are not supported yet",
                        selector.name
                    ),
                })
            }
        }
    }
}

fn apply_defaults(binding: &PortBinding, value: PortValue) -> PortValue {
    if let Some(default) = &binding.resolved_default {
        merge_with_default(value, default)
    } else {
        value
    }
}

fn merge_with_default(mut current: PortValue, default: &PortValue) -> PortValue {
    match (&mut current, default) {
        (PortValue::Scalar(current_lit), PortValue::Scalar(default_lit)) => {
            if matches!(current_lit, LiteralValue::Empty) {
                *current_lit = default_lit.clone();
            }
        }
        (PortValue::Record(current_fields), PortValue::Record(default_fields)) => {
            for (field, default_value) in default_fields {
                let entry = current_fields
                    .entry(field.clone())
                    .or_insert(LiteralValue::Empty);
                if matches!(entry, LiteralValue::Empty) {
                    *entry = default_value.clone();
                }
            }
        }
        (PortValue::Range(current_rows), PortValue::Range(default_rows)) => {
            let is_empty = current_rows.is_empty()
                || current_rows
                    .iter()
                    .all(|row| row.iter().all(|cell| matches!(cell, LiteralValue::Empty)));
            if is_empty {
                *current_rows = default_rows.clone();
            }
        }
        (PortValue::Table(current_table), PortValue::Table(default_table)) => {
            if current_table.is_empty() {
                *current_table = default_table.clone();
            }
        }
        _ => {}
    }
    current
}

struct EvalConfigRestore {
    seed: u64,
    volatile_level: VolatileLevel,
    seed_overridden: bool,
    volatile_overridden: bool,
}

impl<'a> SheetPort<'a> {
    fn apply_eval_options(&mut self, options: &EvalOptions) -> EvalConfigRestore {
        let seed = self.workbook.engine().config.workbook_seed;
        let volatile_level = self.workbook.engine().config.volatile_level;

        let mut seed_overridden = false;
        let mut volatile_overridden = false;

        if let Some(desired_seed) = options.rng_seed
            && desired_seed != seed
        {
            self.workbook.engine_mut().set_workbook_seed(desired_seed);
            seed_overridden = true;
        }

        if options.freeze_volatile && volatile_level != VolatileLevel::OnOpen {
            self.workbook
                .engine_mut()
                .set_volatile_level(VolatileLevel::OnOpen);
            volatile_overridden = true;
        }

        EvalConfigRestore {
            seed,
            volatile_level,
            seed_overridden,
            volatile_overridden,
        }
    }

    fn restore_eval_options(&mut self, restore: EvalConfigRestore) {
        if restore.seed_overridden {
            self.workbook.engine_mut().set_workbook_seed(restore.seed);
        }

        if restore.volatile_overridden {
            self.workbook
                .engine_mut()
                .set_volatile_level(restore.volatile_level);
        }
    }
}
